import os
import re
import sys
import logging.handlers
import base64
import gitlab
import pandas as pd
import numpy as np
import random
import datetime
import hashlib
import argparse
import logging

# Set the GitLab access token and repository URL
GITLAB_ACCESS_TOKEN = os.environ.get('GITLAB_ACCESS_TOKEN')
GITLAB_BASE_URL = 'https://gitlab.devops.telekom.de/'

# Get the directory containing the HQL files
HQL_FILES_PATH_GITLAB = 'app/CJA_hive/src/main/hive/cja/hql/db_cj_core_cdsc'
# Get the directory containing the HQL files
HQL_DIR_PATH = 'app/CJA_hive/src/main/hive/cja/hql/db_cj_core_cdsc'

# Define the patterns to match in the file names
patterns = ['hub', 'lnk', 'sat']

# Set up logging
log_file = 'data_generation_log.txt'
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1000000, backupCount=10)
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(handler)

parser = argparse.ArgumentParser(description='CDSC Application Syntactic Data Generation')
parser.add_argument('-n', '--row_count', type=int, default=10, help="Number of rows required to generate")
args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
N_ROWS = args.row_count

SOURCE = ['cai', 'crmt', 'dwhk', 'dwhm', 'idm', 'ikdb', 'ipmrs', 'kkmf', 'mfref', 'mtv', 'oneapp', 'push', 'rawvault', 'ribs', 'skswms', 'td', 'tdgsr', 'vao', 'webtrekk', 'zs_anlage_ratenplan', 'zs_deaktivierung_ratenplan', 'zs_mahndaten', 'zs_rueckbelastung']
logger.info(f"Using CDSC Source list : {','.join(SOURCE)}")

COL_TYPES = ['id', 'ps', 'hk', 'source']

logger.info(f"Using Columns in column type : {','.join(COL_TYPES)}")

hql_files_output_dir = os.path.join(os.getcwd(), 'hql_files')
os.makedirs(hql_files_output_dir, exist_ok=True)

test_data_output_dir = os.path.join(os.getcwd(), 'data_output')
os.makedirs(test_data_output_dir, exist_ok=True)

# Create a GitLab instance
gl = gitlab.Gitlab(GITLAB_BASE_URL, private_token=GITLAB_ACCESS_TOKEN)
projects = gl.projects.list(all=True, owned=True, membership=True)
cja_project = None
for project in projects:
    if project.name == 'CJA':
        cja_project = project
        break

if not cja_project:
    logger.error('CJA project not found')
    exit()

# Get the HQL files in the directory
files = cja_project.repository_tree(path=HQL_FILES_PATH_GITLAB, all=True, recursive=True)

# GET the names of the HQL files
HQLFILES = []

# Filter the HQL files based on the patterns
HQLFILES = [file['name'] for file in files if file['type'] == 'blob' and any(re.search(pattern, file['name']) for pattern in patterns)]


logger.info(f"List HQL Files: {HQLFILES}")

class DataGenerator:
    def __init__(self):
        self.n_rows = N_ROWS
        self.source = SOURCE
        self.col_types = COL_TYPES

    def get_columns(self, hql):

        matches = re.search(r'INSERT INTO \((.*?)SELECT', hql, re.IGNORECASE)
        if matches:
            extracted_string = matches.group(1)
            logger.info(f"(checks for Inset and Select: \n {extracted_string}")
        else:
            logger.info("(No match found for Inset and Select")

        insert_index = hql.upper().find('INSERT INTO')
        select_index = hql.upper().find('SELECT')
       
        if insert_index == -1 or select_index == -1:
            raise ValueError('HQL file does not contain INSERT INTO or SELECT statement')

        partition_pattern_INSERT = r'PARTITION\s*\((.*?)\)'
        partition_match_str = re.search(partition_pattern_INSERT, hql)
    
        if partition_match_str:
            columns_section = hql[insert_index + len('INSERT INTO'):select_index].strip()
           
            if columns_section and columns_section[-1].startswith('{') and columns_section[-1].endswith('}'):
                columns_section[-1] = columns_section[-1].strip('{}')

            # Remove the specific pattern from the substring
            pattern = '{target_schema}.{target_table} PARTITION({target_partitioned_by})'
            columns_section_cleaned = columns_section.replace(pattern, '')

            # Find the indices of opening and closing parentheses
            opening_parenthesis_index = columns_section_cleaned.find('(')
            closing_parenthesis_index = columns_section_cleaned.find(')')

            # Extract the substring between parentheses
            columns_section_between_parentheses = columns_section_cleaned[opening_parenthesis_index + 1:closing_parenthesis_index]

            # Split the cleaned substring into a list of columns
            columns = [col.strip() for col in columns_section_between_parentheses.split(',') if col.strip() != '']
           
            if columns and columns[0].startswith('TABLE'):
                columns[0] = columns[0].replace('TABLE \n\n(\n    ', '')
            
        else:
            
            di_pattern = r'INSERT INTO {target_schema}.{target_table}'
            table_match = re.search(di_pattern, hql)
            if table_match:
                columns_section = hql[insert_index + len('INSERT INTO'):select_index].strip()
               
            # Extract columns between INSERT INTO and SELECT
            di_pattern = re.search(r'INSERT INTO .*?\((.*?)\).*?SELECT', hql, re.IGNORECASE | re.DOTALL)
            if di_pattern:
                extracted_string = di_pattern.group(1)
                columns_section = [column.strip() for column in extracted_string.split(',')]  
            else:
                # If the table pattern is not found, treat the entire columns_section as the column list
                columns_section = columns_section.strip()
            columns = columns_section
        

        if columns and columns[-1].startswith('{') and columns[-1].endswith('}'):
            columns[-1] = columns[-1].strip('{}')
       
       
        return columns

    def generate_random_data(self, col_name, col_type):
        if col_type == 'id':
            return [f'{col_name}_{i}' for i in range(N_ROWS)]
        elif col_type == 'ps':
            return list(np.random.randint(1, 100000, size=N_ROWS))
        elif col_type == 'hk':
            return [hashlib.sha256(f'{col_type}_{i}'.encode()).hexdigest() for i in range(N_ROWS)]
        else:
            return [f"{col_type}_{i}" for i in range(N_ROWS)]

    def generate_data_frame(self, hqfile):
            logger.info(f"HQL File used  : {hqfile}")
            full_path = os.path.join(HQL_FILES_PATH_GITLAB, hqfile)
            if os.name == 'nt':  # Windows path
                full_path = full_path.replace('\\', '/')

            hql_file = cja_project.files.get(file_path=full_path, ref=cja_project.default_branch)
            hql_content = base64.b64decode(hql_file.content).decode('utf-8')
        
            # Combine the subfolder path and file name
            sub_folder_data_path_hql = f'{hql_files_output_dir}\{hqfile}'

            # Write the DataFrame of hql_content into sub_folder_data_path_hql
            logger.info(f"(HQL hql_content into\n : {sub_folder_data_path_hql}")

            # Write the HQL content to the file
            with open(sub_folder_data_path_hql, 'w') as file:
                file.write(hql_content)
            column_lst = self.get_columns(hql_content)
            logger.info(f"(column list: \n {column_lst}")
            data = {}
            for col_name in column_lst:
                col_type = col_name.split('_')[-1]
                data[col_name] = self.generate_random_data(col_name, col_type)
                data['source'] = random.choices(self.source, k=N_ROWS)
                data['load_date'] = [datetime.date.today() - datetime.timedelta(days=random.randint(1, 100)) for _ in range(N_ROWS)]
                
             # Combine the subfolder path and file name
            
            data['target_partitioned_by'] = [d.strftime('%Y-%m-%d') for d in data['load_date']]
            csv_filename = os.path.splitext(hqfile)[0] + '.csv'
            sub_folder_data_path_csv = f'{test_data_output_dir}\{csv_filename}'
            logger.info(f"CSV data output file  : \n {sub_folder_data_path_csv}")
       
            df = pd.DataFrame(data)
            df.to_csv(sub_folder_data_path_csv, index=False)  # Write DataFrame to CSV with column_lst as header
        
            return df

        
def main():
  for hqfile in HQLFILES:
        
        # for testing only one file  hqfile = 'dwhm_to_rawvault_mf_invoice_definition_mf_invoice_header_lnk.hql'
        # actual hqlfile
        hqfile = 'dwhm_to_rawvault_mf_invoice_definition_mf_invoice_header_lnk.hql'
        logger.info(f"HQL File used:\n {hqfile}")

        # Create an instance of the DataGenerator class
        generator = DataGenerator()
        try:
            # Combine the subfolder path test_data_output_dir and file name csv_filename
           generator.generate_data_frame(hqfile)
        except Exception as e:
            logging.error(f"Error generating CSV file : {e}")
        break

if __name__ == '__main__':
    main()
