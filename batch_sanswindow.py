# env
import apache_beam as beam
import argparse
import logging
import json
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions



class PrintFn(beam.DoFn):
    def process(self, element):
        print('****************')
        print(element)
        print('Type of element: ', type(element))
        # | 'jsondumps' >> beam.Map(lambda x: json.dumps(x)) 
        # | 'encode' >> beam.Map(lambda x: x.encode('utf-8')) 
        return [element]


def replacing(element):
    str(element).strip("'<>() ").replace('\'', '\"')




def run(argv=None):
    # Use Python argparse module to parse custom arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://rim-bucket/market.txt',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        # CHANGE 1/5: The Google Cloud Storage path is required
                        # for outputting the results.
                        default='gs://rim-bucket/output/',
                        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    p_options = PipelineOptions(pipeline_args)
    google_cloud_options = p_options.view_as(GoogleCloudOptions)
    google_cloud_options.region = 'europe-west1'
    google_cloud_options.project = 'smartlive'
    '''google_cloud_options.job_name = 'dataflow-job-{}'.format(
        datetime.datetime.now().strftime("%Y-%m-%d%H%M%S")
    )'''
    google_cloud_options.staging_location = 'gs://rim-bucket/binaries'
    google_cloud_options.temp_location = 'gs://rim-bucket/temp'

    p_options.view_as(StandardOptions).runner = 'DirectRunner'
    p_options.view_as(SetupOptions).save_main_session = True
    p_options.view_as(StandardOptions).streaming = True
    p_options.view_as(WorkerOptions).subnetwork = (
        'regions/europe-west1/subnetworks/test'
    )
    p = beam.Pipeline(options=p_options)

    lines = p | 'receive_data' >> beam.io.ReadFromText(
        known_args.input)\
        | 'jsonload' >> beam.Map(lambda x: json.loads(x))\
        | 'count' >> beam.Map(lambda x: len(x))\
        | 'printnbrarticles' >> beam.ParDo(PrintFn()) \

# ----- window fixe + Trigger AfterWatermark + Accumulating mode  ------ #
    (lines | 'CountGlobally' >> beam.CombineGlobally(
                beam.combiners.CountCombineFn()
            ).without_defaults()
     )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
