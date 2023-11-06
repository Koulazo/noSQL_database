import csv
import zipfile


def unzip_file(unzipped_file_path: str, output_path: str) -> str:
    """
    Extracts the contents of a zip file to a specified output directory.

    Args:
        unzipped_file_path (str): The path to the zip file to be extracted.
        output_path (str): The path to the directory where the contents of the zip file will be extracted.

    Returns:
        str: The path to the output directory where the contents of the zip file were extracted.
    """
    with zipfile.ZipFile(unzipped_file_path, 'r') as zip_ref:
        zip_ref.extractall(output_path)
    return output_path


# file_path = output_path
def read_file(file_path):
    '''
    Reads a file from the specified file path and returns a list of dictionaries representing the file's contents.

    Args:
        file_path (str): The path to the file to be read.

    Returns:
        list: A list of dictionaries representing the file's contents.
    '''
    interactions = []
    with open(file_path, 'r') as infile:
        reader = csv.DictReader(infile, delimiter='\t')
        for row in reader:
            interactions.append(row)
    return interactions



def main():
    unzipped_file_path = 'scripps-research-test/Data/sample_interaction_data.tsv.zip'
    output_path = 'scripps-research-test/Data/'
    file_path = 'scripps-research-test/Data/sample_interaction_data.tsv'
    unzip_file(unzipped_file_path, output_path)
    read_file(file_path) 
    # print(interactions)


if __name__ == "__main__":
    main()