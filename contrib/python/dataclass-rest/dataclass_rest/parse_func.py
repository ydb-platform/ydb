import string
from inspect import FullArgSpec, getfullargspec, isclass
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Sequence,
    Type,
    TypeAlias,
    TypedDict,
    Union,
)

from .http_request import File
from .methodspec import MethodSpec

DEFAULT_BODY_PARAM = "body"
UrlTemplate: TypeAlias = Union[str, Callable[..., str]]


def get_url_params_from_string(url_template: str) -> List[str]:
    parsed_format = string.Formatter().parse(url_template)
    return [x[1] for x in parsed_format if x[1]]


def create_query_params_type(
    spec: FullArgSpec,
    func: Callable,
    skipped: Sequence[str],
) -> Type:
    fields = {}
    self_processed = False
    for x in spec.args + spec.kwonlyargs:
        if not self_processed:
            self_processed = True
            continue
        if x in skipped:
            continue
        fields[x] = spec.annotations.get(x, Any)
    return TypedDict(f"{func.__name__}_Params", fields)


def create_body_type(
    spec: FullArgSpec,
    body_param_name: str,
) -> Type:
    return spec.annotations.get(body_param_name, Any)


def create_response_type(
    spec: FullArgSpec,
) -> Type:
    return spec.annotations.get("return", Any)


def get_file_params(spec):
    return [
        field
        for field, field_type in spec.annotations.items()
        if isclass(field_type) and issubclass(field_type, File)
    ]


def get_url_params_from_callable(
    url_template: Callable[..., str],
) -> List[str]:
    url_template_func_arg_spec = getfullargspec(url_template)
    return url_template_func_arg_spec.args


def parse_func(
    func: Callable,
    method: str,
    url_template: UrlTemplate,
    additional_params: Dict[str, Any],
    is_json_request: bool,  # noqa: FBT001
    body_param_name: str,
) -> MethodSpec:
    spec = getfullargspec(func)
    file_params = get_file_params(spec)

    is_string_url_template = isinstance(url_template, str)
    url_template_callable = (
        url_template.format if is_string_url_template else url_template
    )

    url_params = (
        get_url_params_from_string(url_template)
        if is_string_url_template
        else get_url_params_from_callable(url_template)
    )

    skipped_params = url_params + file_params + [body_param_name]

    return MethodSpec(
        func=func,
        http_method=method,
        url_template=url_template_callable,
        url_params=url_params,
        query_params_type=create_query_params_type(spec, func, skipped_params),
        body_type=create_body_type(spec, body_param_name),
        response_type=create_response_type(spec),
        body_param_name=body_param_name,
        additional_params=additional_params,
        is_json_request=is_json_request,
        file_param_names=file_params,
    )
