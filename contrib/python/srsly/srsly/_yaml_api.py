from typing import Union, IO, Any
from io import StringIO
import sys

from .ruamel_yaml import YAML
from .ruamel_yaml.representer import RepresenterError
from .util import force_path, FilePath, YAMLInput, YAMLOutput


class CustomYaml(YAML):
    def __init__(self, typ="safe", pure=True):
        YAML.__init__(self, typ=typ, pure=pure)
        self.default_flow_style = False
        self.allow_unicode = True
        self.encoding = "utf-8"

    # https://yaml.readthedocs.io/en/latest/example.html#output-of-dump-as-a-string
    def dump(self, data, stream=None, **kw):
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        YAML.dump(self, data, stream, **kw)
        if inefficient:
            return stream.getvalue()


def yaml_dumps(
    data: YAMLInput,
    indent_mapping: int = 2,
    indent_sequence: int = 4,
    indent_offset: int = 2,
    sort_keys: bool = False,
) -> str:
    """Serialize an object to a YAML string. See the ruamel.yaml docs on
    indentation for more details on the expected format.
    https://yaml.readthedocs.io/en/latest/detail.html?highlight=indentation#indentation-of-block-sequences

    data: The YAML-serializable data.
    indent_mapping (int): Mapping indentation.
    indent_sequence (int): Sequence indentation.
    indent_offset (int): Indentation offset.
    sort_keys (bool): Sort dictionary keys.
    RETURNS (str): The serialized string.
    """
    yaml = CustomYaml()
    yaml.sort_base_mapping_type_on_output = sort_keys
    yaml.indent(mapping=indent_mapping, sequence=indent_sequence, offset=indent_offset)
    return yaml.dump(data)


def yaml_loads(data: Union[str, IO]) -> YAMLOutput:
    """Deserialize unicode or a file object a Python object.

    data (str / file): The data to deserialize.
    RETURNS: The deserialized Python object.
    """
    yaml = CustomYaml()
    try:
        return yaml.load(data)
    except Exception as e:
        raise ValueError(f"Invalid YAML: {e}")


def read_yaml(path: FilePath) -> YAMLOutput:
    """Load YAML from file or standard input.

    location (FilePath): The file path. "-" for reading from stdin.
    RETURNS (YAMLOutput): The loaded content.
    """
    if path == "-":  # reading from sys.stdin
        data = sys.stdin.read()
        return yaml_loads(data)
    file_path = force_path(path)
    with file_path.open("r", encoding="utf8") as f:
        return yaml_loads(f)


def write_yaml(
    path: FilePath,
    data: YAMLInput,
    indent_mapping: int = 2,
    indent_sequence: int = 4,
    indent_offset: int = 2,
    sort_keys: bool = False,
) -> None:
    """Create a .json file and dump contents or write to standard
    output.

    location (FilePath): The file path. "-" for writing to stdout.
    data (YAMLInput): The JSON-serializable data to output.
    indent_mapping (int): Mapping indentation.
    indent_sequence (int): Sequence indentation.
    indent_offset (int): Indentation offset.
    sort_keys (bool): Sort dictionary keys.
    """
    yaml_data = yaml_dumps(
        data,
        indent_mapping=indent_mapping,
        indent_sequence=indent_sequence,
        indent_offset=indent_offset,
        sort_keys=sort_keys,
    )
    if path == "-":  # writing to stdout
        print(yaml_data)
    else:
        file_path = force_path(path, require_exists=False)
        with file_path.open("w", encoding="utf8") as f:
            f.write(yaml_data)


def is_yaml_serializable(obj: Any) -> bool:
    """Check if a Python object is YAML-serializable (strict).

    obj: The object to check.
    RETURNS (bool): Whether the object is YAML-serializable.
    """
    try:
        yaml_dumps(obj)
        return True
    except RepresenterError:
        return False
