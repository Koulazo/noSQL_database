import csv
import zipfile



# file_path = 'scripps-research-test/sample_interaction_data.tsv.zip'
# output_path = 'scripps-research-test/'
def unzip_file(unzipped_file_path, output_path):
    with zipfile.ZipFile(unzipped_file_path, 'r') as zip_ref:
        zip_ref.extractall(output_path)
    return output_path


# file_path = output_path
def read_file(file_path):
    interactions = []
    with open(file_path, 'r') as infile:
        reader = csv.DictReader(infile, delimiter='\t')
        for row in reader:
            print(row)
            # interactions.append(row)
    return interactions



def main():
    unzipped_file_path = 'scripps-research-test/sample_interaction_data.tsv.zip'
    output_path = 'scripps-research-test/'
    file_path = 'scripps-research-test/sample_interaction_data.tsv'
    unzip_file(unzipped_file_path, output_path)
    read_file(file_path) 
    # print(interactions)


if __name__ == "__main__":
    main()