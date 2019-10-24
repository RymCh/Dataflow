# env
import apache_beam as beam
import argparse
import json
import logging
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from datetime import datetime

# ------------------------------------Timestamp------------------------------------#


def get_timestamp(data):
    my_date = (data['timestamp']) # date : 2010-09-18......string
    times = datetime.fromisoformat(my_date) #type: datetime.datetime
    return beam.window.TimestampedValue(data, datetime.timestamp(times))


# ---------------------------------- PrinFn Class ----------------------------#


class PrintFn(beam.DoFn):
    def process(self, element):
        print('*****************')
        print(element)
        return [element]

# -----------------------------------run-------------------------------------#


def run(argv=None):
    # Use Python argparse module to parse custom arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--network')
    parser.add_argument('--input',
                        dest='input',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        help='Output file to write results to.')
    parser.add_argument('--output_topic',
                        dest='out_topic',
                        help=('Output PubSub topic of the form '
                              '"projects/<PROJECT>/topic/<TOPIC>".'))
    parser.add_argument('--input_topic',
                        dest='in_topic',
                        help=('Input PubSub topic of the form '
                              '"projects/<PROJECT>/topic/<TOPIC>".'))
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
    # Local execution we set the runner as "DirectRunner"
    # Cloud execution we set the runner as "DataflowRunner"
    p_options.view_as(StandardOptions).runner = 'DirectRunner'
    p_options.view_as(SetupOptions).save_main_session = True
    p_options.view_as(StandardOptions).streaming = True
    p_options.view_as(WorkerOptions).subnetwork = (
        'regions/europe-west1/subnetworks/test'
    )
    p = beam.Pipeline(options=p_options)

    # Read from PubSub into a PCollection.
    lines = p | 'receive_data' >> beam.io.ReadFromPubSub(
        subscription=known_args.in_topic).with_input_types(str) \
        | 'decode' >> beam.Map(lambda x: x.decode('utf-8')) \
        | 'jsonload' >> beam.Map(lambda x: json.loads(x))


# -------------------window glissant ------------------- #

    lines| 'timestamp' >> beam.Map(get_timestamp) \
        | 'window' >>  beam.WindowInto(window.SlidingWindows(20, 10)) \
        | 'Count' >> beam.CombineGlobally(beam.combiners.CountCombineFn()).without_defaults()\
        | 'printnbrarticles' >> beam.ParDo(PrintFn())

    lines | 'jsondumps' >> beam.Map(lambda x: json.dumps(x)) \
        | 'encode' >> beam.Map(lambda x: x.encode('utf-8')) \
        | 'send_to_Pub/Sub' >> beam.io.WriteToPubSub(known_args.out_topic)
    
    p.run().wait_until_finish()

# ------------------------------------run------------------------------------#

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
