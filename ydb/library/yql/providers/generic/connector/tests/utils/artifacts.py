import json
import os
from pathlib import Path

from yt import yson
import yatest.common


def make_path(test_name: str, artifact_name: str) -> Path:
    artifact_path = yatest.common.output_path(f'{test_name}/{artifact_name}')
    os.makedirs(os.path.dirname(artifact_path), exist_ok=True)
    return artifact_path


def dump_yson(obj, test_name: str, filename: str):
    with open(make_path(test_name, filename), 'wb') as f:
        f.write(yson.dumps(obj, yson_format='pretty', indent=4))


def dump_json(obj, test_name: str, filename: str):
    with open(make_path(test_name, filename), 'w') as f:
        f.write(json.dumps(obj, indent=4))


def dump_str(obj, test_name: str, filename: str):
    with open(make_path(test_name, filename), 'w') as f:
        f.write(str(obj))
