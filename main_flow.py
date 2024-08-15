from metaflow import Parameter, step, FlowSpec, schedule
import wandb

from data_validation import retrieve_data_validation_metrics, record_validation_results
# Runs each day at 1 AM UTC
@schedule(cron='0 1 * * ? *', timezone='Etc/UTC')
class SearchTermDataValidationFlow(FlowSpec):
    data_validation_origin = Parameter('data_validation_origin',
                                       help='The table from which to draw the data for validation',
                                       required=True,
                                       default='moz-fx-data-shared-prod.search_terms.sanitization_job_data_validation_metrics',
                                       type=str)

    data_validation_reporting_destination = Parameter('data_validation_reporting_destination',
                                                      help='The table into which to put the validation results',
                                                      required=True,
                                                      default='moz-fx-data-shared-prod.search_terms_derived.search_term_data_validation_reports_v1',
                                                      type=str)

    @step
    def start(self):
        '''
        Metaflow flows must begin with a function called 'start.'
        So here's the start function.
        It prints out the input parameters to the job
        and initializes an experiment tracking run.
        '''
        print(f"Data Validation Origin: {self.data_validation_origin}")
        print(f"Data Validation Reporting Destination: {self.data_validation_reporting_destination}")

        self.next(self.retrieve_metrics)

    @step
    def retrieve_metrics(self):
        '''
        Retrieves search term sanitization aggregation data from BigQuery,
        then checks that they have not varied outside appreciable tolerances
        in the past X days ('X' is a window set for each metric)
        '''
        print("Retrieving Data Validation Metrics Now...")

        self.validation_df = retrieve_data_validation_metrics(self.data_validation_origin)
        self.next(self.record_results)

    @step
    def record_results(self):
        '''
        Shoves the validation metrics calculated in the prior step into a BigQuery table.
        That table has a dashboard in looker, complete with alerts
        to notify data scientists if there are any changes that require manual inspection.
        '''
        print(f"Input Dataframe Shape: {self.validation_df.shape}")
        print("Recording validation results...")
        record_validation_results(self.validation_df, self.data_validation_reporting_destination)
        self.next(self.end)

    @step
    def end(self):
        '''
         Metaflow flows end with a function called 'end.'
         So here's the end function. It logs artifacts to wandb
         and then prints an encouraging message.
         We could all use one every now and then.
         '''
        run = wandb.init(
            project="instep-wandb-search-term-data-validation",
            config={
                "example_metric": "Example Value!"
            }
        )

        run.log({"search-term-data-validation-df": wandb.Table(dataframe=self.validation_df)})

        print(f'That was easy!')

if __name__ == '__main__':
    SearchTermDataValidationFlow()