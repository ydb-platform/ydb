import yaml
import yaml.parser

from testsuite.utils import object_hook as object_hook_util


class BaseError(Exception):
    pass


class ParserError(BaseError):
    pass


if hasattr(yaml, 'CLoader'):
    _Loader = yaml.CLoader  # type: ignore
else:
    _Loader = yaml.Loader  # type: ignore


def load_file(path, encoding='utf-8', object_hook=None):
    with open(path, encoding=encoding) as fp:
        return load(fp, object_hook=object_hook)


def load(string_or_stream, object_hook=None):
    try:
        yaml_obj = yaml.load(string_or_stream, Loader=_Loader)
        hook = object_hook_util.build_object_hook(object_hook=object_hook)
        return object_hook_util.substitute(yaml_obj, hook)
    except yaml.parser.ParserError as exc:
        raise ParserError(str(exc)) from exc
