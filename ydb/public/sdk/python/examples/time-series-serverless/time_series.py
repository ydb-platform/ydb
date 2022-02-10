from typing import Dict
import random

import ydb

from database import ydb_client
from entry import Entry
from exception import ConnectionFailure, ValidationError
from parameters import Parameters
from response import Response, Conflict, BadRequest, Ok


def generate_time_series(parameters: Parameters):
    column_types = ydb.BulkUpsertColumns()
    column_types.add_column("timestamp", ydb.PrimitiveType.Timestamp)
    column_types.add_column("value", ydb.PrimitiveType.Double)

    rows = [
        Entry(t, random.normalvariate(parameters.mean, parameters.sigma))
        for t in range(parameters.start_us, parameters.end_us, parameters.interval_us)
    ]

    ydb_client.bulk_upsert(rows, column_types)


def do_handle(event: Dict, _) -> Response:
    if "queryStringParameters" not in event:
        return BadRequest("Incorrect function call: non HTTP request")

    try:
        query_string = event["queryStringParameters"]
        mean = query_string["mean"]
        sigma = query_string["sigma"]
        start_ms = query_string["start"]
        end_ms = query_string["end"]
        interval_ms = query_string["interval"]
    except KeyError:
        return BadRequest("Incorrect function call: required parameters missing")

    try:
        parameters = Parameters.from_strings(start_ms, end_ms, interval_ms, mean, sigma)
    except ValidationError as e:
        return BadRequest(f"Incorrect function call: {e.reason}")

    try:
        generate_time_series(parameters)
    except ConnectionFailure as e:
        return Conflict(f"Failed to connect to YDB: {e.reason}")

    return Ok()


def handler(event, context):
    return do_handle(event, context).as_dict()
