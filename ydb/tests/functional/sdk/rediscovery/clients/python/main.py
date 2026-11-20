import os
import sys
import time

import ydb


def list_directory(driver, database):
    settings = ydb.BaseRequestSettings().with_timeout(2)
    while True:
        try:
            driver.scheme_client.list_directory(database, settings=settings)
            print("OK", flush=True)
        except ydb.Error as error:
            print(error, file=sys.stderr, flush=True)
        time.sleep(0.2)


def write_topic(driver, topic):
    with driver.topic_client.writer(topic, producer_id="probe", codec=ydb.TopicCodec.RAW) as writer:
        while True:
            writer.write_with_ack(b"probe")
            print("OK", flush=True)
            time.sleep(0.2)


def read_topic(driver, topic):
    with driver.topic_client.reader(topic, consumer="probe") as reader:
        while True:
            message = reader.receive_message()
            print("OK", message.data.decode(), flush=True)


def main():
    database = os.environ["YDB_DATABASE"]
    with ydb.Driver(endpoint=os.environ["YDB_ENDPOINT"], database=database) as driver:
        driver.wait(timeout=15, fail_fast=True)
        probe = os.environ.get("YDB_PROBE", "scheme")
        if probe == "scheme":
            list_directory(driver, database)
        elif probe == "topic-write":
            write_topic(driver, f"{database}/topic")
        elif probe == "topic-read":
            read_topic(driver, f"{database}/topic")
        else:
            raise ValueError(f"Unknown probe {probe!r}")


if __name__ == "__main__":
    main()
