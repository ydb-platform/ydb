import os
import re
import requests
import configparser
import ydb
from git import Repo
from mute_utils import mute_target, pattern_to_re
from get_file_diff import extract_diff_lines

dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
repo_path = f"{dir}/../../../"
config.read(config_file_path)

build_preset = os.environ.get("build_preset")
branch = os.environ.get("branch_to_compare")

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]

class YaMuteCheck:
    def __init__(self):
        self.regexps = set()
        self.regexps = []

    def load(self, fn):
        with open(fn, "r") as fp:
            for line in fp:
                line = line.strip()
                try:
                    testsuite, testcase = line.split(" ", maxsplit=1)
                except ValueError:
                    log_print(f"SKIP INVALID MUTE CONFIG LINE: {line!r}")
                    continue
                self.populate(testsuite, testcase)

    def populate(self, testsuite, testcase):
        check = []

        for p in (pattern_to_re(testsuite), pattern_to_re(testcase)):
            try:
                check.append(re.compile(p))
            except re.error:
                log_print(f"Unable to compile regex {p!r}")
                return

        self.regexps.append(tuple(check))

    def __call__(self, suite_name, test_name):
        for ps, pt in self.regexps:
            if ps.match(suite_name) and pt.match(test_name):
                return True
        return False

def save_tests_to_file(path):
    print(f'Getting all tests')
    

    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print(
            "Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping"
        )
        return 1
    else:
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]
    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )
        
        # settings, paths, consts
        tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=True)
        table_client = ydb.TableClient(driver, tc_settings)
            
        # geting last date from history
        tests = f"""
        select 
                                    suite_folder,
                                    test_name,

                                from  `test_results/analytics/testowners`
                                order by suite_folder,test_name
        """
        query = ydb.ScanQuery(tests, {})
        it = table_client.scan_query(query)
        results = []
        while True:
            try:
                result = next(it)
                results = results + result.result_set.rows
            except StopIteration:
                break
        lines=[]
        for row in results: 
            lines.append(row['suite_folder'] + ' '+ row['test_name'] + '\n')
            
        write_to_file(lines, path)
        return results
        
def read_file(path):
    line_list = []
    with open(path, "r") as fp:
        for line in fp:
            line = line.strip()
            try:
                testsuite, testcase = line.split(" ", maxsplit=1)
            except ValueError:
                log_print(f"SKIP INVALID MUTE CONFIG LINE: {line!r}")
                continue
            line_list.append((testsuite,testcase))
            
        return line_list
        
        

def is_git_repository(path):
    try:
        _ = Repo(path).git_dir
        return True
    except:
        return False

def download_file(url, local_filename):
    """
    Downloads a file from the given URL and saves it locally.

    :param url: URL of the file
    :param local_filename: Name to save the file locally
    :return: Name of the locally saved file
    """
    try:
        # Perform an HTTP GET request
        with requests.get(url, stream=True) as response:
            response.raise_for_status()  # Raise an exception for HTTP errors
            # Open the file for writing in binary mode
            with open(local_filename, 'wb') as file:
                # Save the file in chunks (to avoid loading the entire file into memory)
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        print(f"Downloaded file to {local_filename}")
        return local_filename
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return None


def get_diff_for_file(repo_path, filename):
    repo = Repo(repo_path)
    added_texts = []
    removed_texts = []

    diffs = repo.index.diff(None)  # Получаем diff всех измененных файлов
    
    # Найти изменения конкретного файла
    for diff in diffs:
        if diff.b_path == filename:
            if diff.change_type == 'A':  # Файл полностью новый
                with open(diff.b_path, 'r') as f:
                    added_texts.extend(f.readlines())
            else:
                # Читаем diff для данного файла
                for line in repo.git.diff(diff.b_path).split('\n'):
                    if line.startswith('+') and not line.startswith('+++'):
                        added_texts.append(line[1:] + '\n')
                    elif line.startswith('-') and not line.startswith('---'):
                        removed_texts.append(line[1:] + '\n')

    return added_texts, removed_texts

def write_to_file(text, file):
    #os.remove(file)
    os.makedirs(os.path.dirname(file), exist_ok=True)
    with open(file, 'w') as f:
        f.writelines(text)



def main():
    
    s3_all_tests_url = 'https://storage.yandexcloud.net/ydb-gh-logs/ydb-platform/ydb/Tests/all_tests.txt'

    path_in_repo = '.github/config/'
    muted_ya_path = path_in_repo + 'muted_ya.txt'  # имя файла, для которого нужно получить diff
    added_lines_file = path_in_repo + 'mute_info/added_muted_line.txt'
    added_lines_file_muted = path_in_repo + 'mute_info/added_muted_test.txt'
    removed_lines_file = path_in_repo + 'mute_info/removed_muted_line.txt'
    removed_lines_file_muted = path_in_repo + 'mute_info/removed_muted_test.txt'
    all_tests_file =  path_in_repo + 'mute_info/all_tests.txt'
    all_muted_tests_file =  path_in_repo + 'mute_info/all_muted_tests.txt'
    
    if not is_git_repository(repo_path):
        print(f"The directory {repo_path} is not a valid Git repository.")
        return
    added_texts, removed_texts = extract_diff_lines(muted_ya_path)
    #added_texts, removed_texts = get_diff_for_file(repo_path, muted_ya_path)
    write_to_file('\n'.join(added_texts), added_lines_file)
    write_to_file('\n'.join(removed_texts), removed_lines_file)

    print(f"Added lines have been written to {added_lines_file}.")
    print(f"Removed lines have been written to {removed_lines_file}.")

    
    #get all tests
    all_tests_file = download_file(s3_all_tests_url, all_tests_file)
    all_tests = read_file(os.path.join(repo_path,all_tests_file))
    if not all_tests:
        all_tests= save_tests_to_file(os.path.join(repo_path,all_tests_file))
    all_tests= save_tests_to_file(os.path.join(repo_path,all_tests_file))

    #all muted
    mute_check = YaMuteCheck()
    mute_check.load(os.path.join(repo_path,muted_ya_path))
    muted_tests = []
    print("All muted tests:")
    for test in all_tests:
        testsuite = test[0]
        testcase = test[1]
        if mute_check(testsuite, testcase):
            muted_tests.append(testsuite + ' ' + testcase + '\n')
            #print(testsuite + ' ' + testcase+ '\n')
    write_to_file(muted_tests,os.path.join(repo_path, all_muted_tests_file))
    #checking added lines
    mute_check = YaMuteCheck()
    mute_check.load(os.path.join(repo_path,added_lines_file))
    added_muted_line = read_file(os.path.join(repo_path,added_lines_file))
    added_muted_tests=[]
    print("New muted tests:")
    for test in all_tests:
        testsuite = test[0]
        testcase = test[1]
        if mute_check(testsuite, testcase):
            added_muted_tests.append(testsuite + ' '+ testcase + '\n')
            print(testsuite + ' ' + testcase)

    write_to_file(added_muted_tests,os.path.join(repo_path, added_lines_file_muted))
    #if mute_check(suite_name, test_name):
    #checking removed lines
    mute_check = YaMuteCheck()
    mute_check.load(os.path.join(repo_path,removed_lines_file))
   
    removed_muted_line = read_file(os.path.join(repo_path,removed_lines_file))
    removed_muted_tests=[]
    print("Unmuted tests:")
    for test in all_tests:
        testsuite = test[0]
        testcase = test[1]
        if mute_check(testsuite, testcase):
            removed_muted_tests.append(testsuite + ' '+ testcase + '\n')
            print(testsuite + ' ' + testcase)
            
    write_to_file(removed_muted_tests,os.path.join(repo_path, removed_lines_file_muted))


if __name__ == "__main__":
    main()