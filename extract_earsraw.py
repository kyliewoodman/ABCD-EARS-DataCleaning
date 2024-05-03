import re
import zipfile
import glob
import click
import pandas


@click.command()
@click.argument('INPUT_PATH')
@click.option("-o", "--output-file", default="results.csv", help="Where to write the resulting csv file. Defaults to 'results.csv' in the current directory.")
def main(input_path, output_file):
    """
    This script aggregates ears data from a directory of participant zips. INPUT_PATH is the directory containing
    "earrawdata_NDAR_INV*.zip" files.
    """
    # If memory usage is an issue, concatenating all participant data in memory will need to be rethought.
    pandas.concat(process_participants(input_path, output_file)).to_csv(output_file)



def process_participants(input_path, output_file):
    participant_files = glob.iglob("{}/earrawdata*.zip".format(input_path))
    for participant_zip_file in participant_files:
        ndar_id = get_ndar_id_from_zip_name(participant_zip_file)
        print("Processing '{}' for participant '{}'".format(participant_zip_file, ndar_id))
        with zipfile.ZipFile(participant_zip_file, 'r') as zip_file:
            participant_data = process_single_participant(ndar_id, zip_file, make_data_transformer(ndar_id))
            print("\t->{} cols and {} rows".format(len(participant_data.columns), len(participant_data)))
            yield participant_data



def process_single_participant(ndar_id: str, zip_file: zipfile.ZipFile, transform_data):
    csv_filenames = list(filter(lambda filename: filename.endswith('.csv'), zip_file.namelist()))
    def read_csv(filename):
        with zip_file.open(filename, 'r') as csv_file:
            return pandas.read_csv(csv_file)

    return pandas.concat([transform_data(filename, read_csv(filename)) for filename in csv_filenames])


def make_data_transformer(ndar_id):
    """
    Produce a function that can manipulate individual csv file data before being merged.
    The innter function (below) only takes filename and the csv data (as a dataframe).
    Any additional context (such as the ndar_id) should be passed in through this function (above).
    """
    def transform_data(filename, csv_dataframe):
        # Add participant_id to all rows.
        if 'id_participant' not in csv_dataframe.columns:
            csv_dataframe.loc[:, 'id_participant'] = ndar_id
        # More transformations go here.
        return csv_dataframe
    
    return transform_data

def get_ndar_id_from_zip_name(zip_name):
    return re.match(r'.*earrawdata_(NDAR_INV\w+)\.zip$', zip_name).group(1)

if __name__ == "__main__":
    main()
