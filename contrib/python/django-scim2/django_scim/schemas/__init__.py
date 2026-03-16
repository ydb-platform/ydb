import json
import os
import importlib.resources

SCHEMAS_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_SUB_DIRS = ('core', 'extension')


def load_schemas():
    schemas = []
    for dir_ in SCHEMA_SUB_DIRS:
        sub_dir = importlib.resources.files(__package__) / dir_
        for file_ in sub_dir.iterdir():
            if not file_.name.endswith('.json'):
                continue
            with file_.open() as fp:
                schemas.append(json.load(fp))

    return schemas


ALL = load_schemas()


def default_schemas_getter():
    return ALL
