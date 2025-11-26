import ydb
import os
import argparse
import json
import enum
import time
from typing import List, Dict
from ydb import convert

class TestQuery:
    def __init__(self, name: str, query_text: str, expected_rows_count: int, expected_rows: list) -> None:
        self._name = name
        self._query_text = query_text
        self._expected_rows_count = expected_rows_count
        self._expected_rows = expected_rows

    def __str__(self) -> str:
        return f'Query name: {self._name}, query text: {self._query_text}, Expected count rows: {self._expected_rows_count}'
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def query_text(self) -> str:
        return self._query_text
    
    @property
    def expected_rows_count(self) -> int:
        return self._expected_rows_count
    
    @property
    def expected_rows(self) -> list:
        return self._expected_rows

class StatusQuery(enum.Enum):
    PASSED = 0,
    FAILED = 1,
    WRONG_ANSWER = 2

class ResultQuery:
    def __init__(self, status: StatusQuery, error_message: str = "", expected_rows: int = 0, actual_rows: int = 0) -> None:
        self.status = status
        self.message_error = error_message
        self.expected_rows = expected_rows
        self.actual_rows = actual_rows


    def is_passed(self) -> bool:
        return self.status == StatusQuery.PASSED
    
    def is_error(self) -> bool:
        return self.status == StatusQuery.FAILED
    
    def is_wrong_answer(self) -> bool:
        return self.status == StatusQuery.WRONG_ANSWER
    
    def __str__(self):
        if self.is_error():
            return f"{self.status.name} {self.message_error if self.is_error() else ''}"
        elif self.is_wrong_answer():
            if self.actual_rows != self.expected_rows:
                return f"{self.status.name} actual rows count = {self.actual_rows}, expected rows count = {self.expected_rows}"
            else:
                return f"{self.status.name} {self.message_error}"
        return self.status.name

def parse_query_from_json(path_to_json: str) -> Dict[str, TestQuery]:
    result: Dict[str, TestQuery] = dict()

    with open(path_to_json, 'r', encoding="utf-8") as json_file:
        # json_file._content.decode('utf-8')
        json_data: dict = json.load(json_file)
        for name_test, value in json_data.items():
            result[name_test] = TestQuery(name_test, value['filter_query'] + value['query_text'], int(value['expected_rows_count']), value['expected_rows'])
    return result

def run_query(pool: ydb.QuerySessionPool, queries: List[TestQuery]) -> Dict[str, StatusQuery]:
    result: Dict[str, StatusQuery] = dict()
    for query in queries:
        print(f"Run query '{query.name}' ")
        try:
            start = time.time()
            result_query: List[convert.ResultSet] = pool.execute_with_retries(query.query_text)
            elapsed_time = time.time() - start
            flag: bool = True
            count_rows: int = 0
            for res in result_query:
                    if len(query._expected_rows) == 0:
                        count_rows += len(res.rows)
                    else:
                        for i in range(len(res.rows)):
                            flag = flag and (query.expected_rows[i] == res.rows[i])
                            count_rows += 1
                        
            print(f"Query time: {round(elapsed_time, 2)} sec, count rows: {count_rows}")
            if query.expected_rows_count == count_rows:
                if flag:
                    result[query.name] = ResultQuery(status=StatusQuery.PASSED)
                else:
                    result[query.name] = ResultQuery(status=StatusQuery.WRONG_ANSWER, error_message="return rows don't match expected rows")
            else:
                result[query.name] = ResultQuery(status=StatusQuery.WRONG_ANSWER, expected_rows=query.expected_rows_count, actual_rows=count_rows)
        except ydb.Error as err:
            result[query.name] = ResultQuery(status=StatusQuery.FAILED, error_message=err.message)
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("-e", "--endpoint", help="Endpoint url to use")
    parser.add_argument("-d", "--database", help="Name of the database to use")
    parser.add_argument("-q", "--queries", help="Path to json with queries")

    args = parser.parse_args()

    if not args.endpoint:
        raise Exception("endpoint is not set")
    
    if not args.database:
        raise Exception("database is not set")
    
    iam_token = os.getenv("YDB_ACCESS_TOKEN_CREDENTIALS", None)
    if iam_token == None:
        raise Exception("set enviroment variable `YDB_ACCESS_TOKEN_CREDENTIALS`")
    
    driver = ydb.Driver(
        endpoint=args.endpoint,
        database=args.database,
        credentials=ydb.AccessTokenCredentials(iam_token),
    )

    with driver:
        try:
            driver.wait(fail_fast=True, timeout=15)
        except TimeoutError:
            print("Connect failed to YDB")
            print("Last reported errors by discovery:")
            print(driver.discovery_debug_details())
        quries: Dict[str, TestQuery] = parse_query_from_json(args.queries)
        result_queries: Dict[str, ResultQuery] = dict()
        with ydb.QuerySessionPool(driver) as pool:
            result_queries: Dict[str, StatusQuery] = run_query(pool, list(quries.values()))
            for name, res in result_queries.items():
                print(f"Query: `{name}` {res}")
