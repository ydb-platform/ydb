import sys
import ydb
import shutil
import json
import pathlib
import os
import argparse
import datetime


DATABASE_ENDPOINT = "grpcs://lb.etnvsjbk7kh1jc6bbfi8.ydb.mdb.yandexcloud.net:2135"
DATABASE_PATH = "/ru-central1/b1ggceeul2pkher8vhb6/etnvsjbk7kh1jc6bbfi8"


class RunParams:
    def __init__(self, is_spilling, variant, datasize, tasks, query):
        self.is_spilling = is_spilling
        self.variant = variant
        self.datasize = datasize
        self.tasks = tasks
        self.query = query

    def __repr__(self):
        result = []
        for key, value in self.__dict__.items():
            result.append(f"{key}: {value}")
        return "RunParams(" + ", ".join(result) + "})"


class RunResults:
    def __init__(self):
        self.exitcode = None
        self.read_bytes = None
        self.write_bytes = None
        self.user_time_ms = None
        self.system_time = None
        self.rss = None
        self.result_hash = None
        self.stdout_file_path = None
        self.stderr_file_path = None
        self.perf_file_path = None

    def from_json(self, json):
        self.exitcode = json["exitcode"]
        io_info = json["io"]
        self.read_bytes = io_info["read_bytes"]
        self.write_bytes = io_info["write_bytes"]
        resourse_usage = json["rusage"]
        self.user_time = datetime.timedelta(seconds=resourse_usage["utime"])
        self.system_time = datetime.timedelta(seconds=resourse_usage["stime"])
        self.rss = resourse_usage["maxrss"]

    def __repr__(self):
        result = []
        for key, value in self.__dict__.items():
            result.append(f"{key}: {value}")
        return "RunQueryData(" + ", ".join(result) + "})"


def pretty_print(value):
    if value is None:
        return "NULL"
    if type(value) == datetime.datetime:
        delt = value - datetime.datetime(1970, 1, 1)
        assert type(delt) == datetime.timedelta
        return f"Unwrap(DateTime::FromSeconds({int(delt.total_seconds())}))"
    if type(value) == datetime.timedelta:
        return f"DateTime::IntervalFromMicroseconds({int(value / datetime.timedelta(microseconds=1))})"
    if isinstance(value, pathlib.Path):
        return f'\"{value}\"'
    if type(value) == str:
        return f'\"{value}\"'
    if type(value) in [int, float]:
        return str(value)
    if type(value) == bool:
        return "TRUE" if value else "FALSE"

    assert False, f"unrecognized type: {type(value)}"


def upload_file_to_s3(s3_folder, result_path, file):
    # copying files to folder that will be synced with s3
    dst = file.relative_to(result_path)
    s3_file = (s3_folder / dst).resolve()
    s3_file.parent.mkdir(parents=True, exist_ok=True)
    _ = shutil.copy2(str(file.resolve()), str(s3_file))
    return dst


def upload_results(result_path, s3_folder, test_start, try_num):
    def add_try_num_to_path(path):
        if try_num:
            path = f"try_{try_num}" / path
        return path

    results_map = {}
    for entry in result_path.glob("*/*"):
        if not entry.is_dir():
            continue
        this_result = {}
        suffix = entry.relative_to(result_path)
        # {no|with}-spilling/<variant>-<datasize>-<tasks>
        is_spilling = suffix.parts[0].split("-")[0] == "with"
        variant, datasize, tasks = suffix.parts[1].split("-")
        datasize = int(datasize)
        tasks = int(tasks)

        for file in entry.iterdir():
            if not file.is_file():
                continue
            name = file.name
            if len(file.suffixes) > 0:
                name = name.rsplit(file.suffixes[0])[0]
            if name[0] == "q":
                query_num = int(name[1:].split("-")[0])
                if query_num not in this_result:
                    this_result[query_num] = RunResults()

                # q<num>.svg
                if file.suffix == ".svg":
                    this_result[query_num].perf_file_path = add_try_num_to_path(upload_file_to_s3(s3_folder, result_path, file))

                # q<num>-result.yson
                if file.stem == f"q{query_num}-result":
                    with open(file, "r") as result:
                        this_result[query_num].result_hash = str(hash(result.read().strip()))

                # q<num>-stdout.txt
                if file.stem == f"q{query_num}-stdout":
                    this_result[query_num].stdout_file_path = add_try_num_to_path(upload_file_to_s3(s3_folder, result_path, file))

                # q<num>-stderr.txt
                if file.stem == f"q{query_num}-stderr":
                    this_result[query_num].stderr_file_path = add_try_num_to_path(upload_file_to_s3(s3_folder, result_path, file))

        summary_file = entry / "summary.json"

        with open(summary_file, "r") as res_file:
            for line in res_file.readlines()[1:]:
                info = json.loads(line)
                query_num = int(info["q"][1:])
                this_result[query_num].from_json(info)

        for key, value in this_result.items():
            params = RunParams(is_spilling, variant, datasize, tasks, key)
            results_map[params] = value

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        driver.wait(timeout=5)

        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )
        for params, results in results_map.items():
            with session.transaction() as tx:
                mapping = {
                    "BenchmarkType" : params.variant,
                    "Scale" : params.datasize,
                    "QueryNum" : params.query,
                    "WithSpilling" : params.is_spilling,
                    "Timestamp" : test_start,
                    "WasSpillingInAggregation" : None,
                    "WasSpillingInJoin" : None,
                    "WasSpillingInChannels" : None,
                    "MaxTasksPerStage" : params.tasks,
                    "ExitCode" : results.exitcode,
                    "ResultHash" : results.result_hash,
                    "SpilledBytes" : results.read_bytes,
                    "UserTime" : results.user_time,
                    "SystemTime" : results.system_time,
                    "StdoutFileLink" : results.stdout_file_path,
                    "StderrFileLink" : results.stderr_file_path,
                    "PerfFileLink" : results.perf_file_path
                }
                sql = 'UPSERT INTO `perfomance/olap/dq_spilling_nightly_runs`\n\t({columns})\nVALUES\n\t({values})'.format(
                    columns=", ".join(map(str, mapping.keys())),
                    values=", ".join(map(pretty_print, mapping.values())))
                tx.execute(sql, commit_tx=True)


def main():
    upload_time = datetime.datetime.now()

    parser = argparse.ArgumentParser()

    parser.add_argument("--result-path", type=pathlib.Path)
    parser.add_argument("--s3-folder", type=pathlib.Path)
    parser.add_argument("--try-num", default=None)

    args = parser.parse_args()

    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        raise AttributeError("Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping uploading")
    os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ["CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]

    upload_results(args.result_path, args.s3_folder, upload_time, args.try_num)


if __name__ == "__main__":
    main()
