import abc
import json
import typing
import sys
from inspect import signature, getmro
from json.decoder import JSONDecodeError
from types import SimpleNamespace
from typing import Callable, Tuple, Literal, Type

from aiohttp.helpers import parse_mimetype
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp.web_request import BaseRequest
from multidict import MultiDict
from pydantic import BaseModel, create_model

from .uploaded_file import UploadedFile, StrictOrderedMultipartReader
from .utils import is_pydantic_base_model, robuste_issubclass

CONTEXT = Literal["body", "headers", "path", "query string"]

if sys.version_info >= (3, 9):
    from typing import get_type_hints
else:
    # Backport: Added include_extras parameter as part of PEP: 593.
    from typing_extensions import get_type_hints


class AbstractInjector(metaclass=abc.ABCMeta):
    """
    An injector parse HTTP request and inject params to the view.
    """

    model: Type[BaseModel]

    @property
    @abc.abstractmethod
    def context(self) -> CONTEXT:
        """
        The name of part of parsed request
        i.e "HTTP header", "URL path", ...
        """

    @abc.abstractmethod
    def __init__(self, args_spec: dict, default_values: dict):
        """
        args_spec - ordered mapping: arg_name -> type
        """

    @abc.abstractmethod
    def inject(self, request: BaseRequest, args_view: list, kwargs_view: dict):
        """
        Get elements in request and inject them in args_view or kwargs_view.
        """


class MatchInfoGetter(AbstractInjector):
    """
    Validates and injects the part of URL path inside the view positional args.
    """

    context = "path"

    def __init__(self, args_spec: dict, default_values: dict):
        self.model = create_model(
            "PathModel",
            **{
                k: ((v, d) if ((d := default_values.get(k, ...)) is not ...) else v)
                for k, v in args_spec.items()
            },
        )

    def inject(self, request: BaseRequest, args_view: list, kwargs_view: dict):
        args_view.extend(self.model(**request.match_info).model_dump().values())


class BodyGetter(AbstractInjector):
    """
    Validates and injects the content of request body inside the view kwargs.
    """

    context = "body"

    def __init__(self, args_spec: dict, default_values: dict):
        self.arg_name = ""
        self._file_arg_names: list[str] = []
        for name, annotation in args_spec.items():
            if robuste_issubclass(annotation, UploadedFile):
                self._file_arg_names.append(name)
            else:
                if self.arg_name != "":
                    raise RuntimeError(
                        f'You cannot define multiple bodies arguments with pydantic.BaseModel ("{self.arg_name}" and "{name}" are annotated with a pydantic.BaseModel)'
                    )
                if self._file_arg_names:
                    raise RuntimeError(
                        f'You cannot define a pydantic.BaseModel argument after an UploadedFile argument. (The argument "{name}" must be defined before "{self._file_arg_names[0]}")'
                    )
                self.arg_name, self.model = name, annotation
                self._expect_object = (
                    self.model.model_json_schema().get("type") == "object"
                )

    async def _inject(self, json_getter, args_view: list, kwargs_view: dict):
        try:
            body = await json_getter()
        except JSONDecodeError:
            raise HTTPBadRequest(
                text='{"error": "Malformed JSON"}', content_type="application/json"
            ) from None

        # Pydantic tries to cast certain structures, such as a list of 2-tuples,
        # to a dict. Prevent this by requiring the body to be a dict for object models.
        if self._expect_object and not isinstance(body, dict):
            raise HTTPBadRequest(
                text='[{"in": "body", "loc": ["__root__"], "msg": "value is not a '
                'valid dict", "type": "type_error.dict"}]',
                content_type="application/json",
            ) from None

        kwargs_view[self.arg_name] = self.model.model_validate(body)

    async def inject(self, request: BaseRequest, args_view: list, kwargs_view: dict):
        # Standard request containing data to fill a pydantic.Basemodel.
        if self.arg_name and not self._file_arg_names:
            await self._inject(request.json, args_view, kwargs_view)
        else:
            if not parse_mimetype(request.content_type).type == "multipart":
                raise HTTPBadRequest(
                    text=json.dumps(
                        [
                            {
                                "in": "body",
                                "loc": ["__root__"],
                                "msg": "Multipart request is required",
                                "type": "type_error.multipart",
                            }
                        ]
                    ),
                    content_type="application/json",
                )

            reader = await request.multipart()

            # If the request contains a part to fill a pydantic.Basemodel.
            if self.arg_name:
                multipart_reader = StrictOrderedMultipartReader(
                    reader, [self.arg_name] + self._file_arg_names
                )
                part = await multipart_reader.next_part(self.arg_name)
                # TODO: Check the header ?
                # part.headers.get(CONTENT_TYPE) == 'application/json'
                await self._inject(part.json, args_view, kwargs_view)
            else:
                multipart_reader = StrictOrderedMultipartReader(
                    reader, self._file_arg_names
                )

            # Inject file upload utility as view kwarg.
            for file_arg_name in self._file_arg_names:
                kwargs_view[file_arg_name] = UploadedFile(
                    multipart_reader, file_arg_name
                )


class QueryGetter(AbstractInjector):
    """
    Validates and injects the query string inside the view kwargs.
    """

    context = "query string"

    def __init__(self, args_spec: dict, default_values: dict):
        args_spec = args_spec.copy()

        self._groups = {}
        for group_name, group in args_spec.items():
            if robuste_issubclass(group, Group):
                self._groups[group_name] = (group, _get_group_signature(group)[0])

        _unpack_group_in_signature(args_spec, default_values)
        self.model = create_model(
            "QueryModel",
            **{
                k: ((v, d) if ((d := default_values.get(k, ...)) is not ...) else v)
                for k, v in args_spec.items()
            },
        )
        self.args_spec = args_spec
        self._is_multiple = frozenset(
            name for name, spec in args_spec.items() if typing.get_origin(spec) is list
        )

    def inject(self, request: BaseRequest, args_view: list, kwargs_view: dict):
        data = self._query_to_dict(request.query)
        cleaned = self.model(**data).model_dump()
        for group_name, (group_cls, group_attrs) in self._groups.items():
            group = group_cls()
            for attr_name in group_attrs:
                setattr(group, attr_name, cleaned.pop(attr_name))
            cleaned[group_name] = group
        kwargs_view.update(**cleaned)

    def _query_to_dict(self, query: MultiDict):
        """
        Return a dict with list as value from the MultiDict.

        The value will be wrapped in a list if the args spec is define as a list or if
        the multiple values are sent (i.e ?foo=1&foo=2)
        """
        return {
            key: (
                values
                if len(values := query.getall(key)) > 1 or key in self._is_multiple
                else value
            )
            for key, value in query.items()
        }


class HeadersGetter(AbstractInjector):
    """
    Validates and injects the HTTP headers inside the view kwargs.
    """

    context = "headers"

    def __init__(self, args_spec: dict, default_values: dict):
        args_spec = args_spec.copy()

        self._groups = {}
        for group_name, group in args_spec.items():
            if robuste_issubclass(group, Group):
                self._groups[group_name] = (group, _get_group_signature(group)[0])

        _unpack_group_in_signature(args_spec, default_values)
        self.model = create_model(
            "HeaderModel",
            **{
                k: ((v, d) if ((d := default_values.get(k, ...)) is not ...) else v)
                for k, v in args_spec.items()
            },
        )

    def inject(self, request: BaseRequest, args_view: list, kwargs_view: dict):
        header = {k.lower().replace("-", "_"): v for k, v in request.headers.items()}
        cleaned = self.model(**header).model_dump()
        for group_name, (group_cls, group_attrs) in self._groups.items():
            group = group_cls()
            for attr_name in group_attrs:
                setattr(group, attr_name, cleaned.pop(attr_name))
            cleaned[group_name] = group
        kwargs_view.update(cleaned)


class Group(SimpleNamespace):
    """
    Class to group header or query string parameters.

    The parameter from query string or header will be set in the group
    and the group will be passed as function parameter.

    Example:

    class Pagination(Group):
        current_page: int = 1
        page_size: int = 15

    class PetView(PydanticView):
        def get(self, page: Pagination):
            ...
    """


_NOT_SET = object()


def _get_group_signature(cls) -> Tuple[dict, dict]:
    """
    Analyse Group subclass annotations and return them with default values.
    """

    sig = {}
    defaults = {}
    mro = getmro(cls)
    for base in reversed(mro[: mro.index(Group)]):
        attrs = vars(base)

        # Use __annotations__ to know if an attribute is
        # overwrite to remove the default value.
        for attr_name, type_ in base.__annotations__.items():
            if (default := attrs.get(attr_name, _NOT_SET)) is _NOT_SET:
                defaults.pop(attr_name, None)
            else:
                defaults[attr_name] = default

        # Use get_type_hints to have postponed annotations.
        for attr_name, type_ in get_type_hints(base, include_extras=True).items():
            sig[attr_name] = type_

    return sig, defaults


def _parse_func_signature(
    func: Callable, unpack_group: bool = False, ignore_params=("self",)
) -> Tuple[dict, dict, dict, dict, dict]:
    """
    Analyse function signature and returns 5-tuple:
        0 - arguments will be set from the url path
        1 - argument will be set from the request body.
        2 - argument will be set from the query string.
        3 - argument will be set from the HTTP headers.
        4 - Default value for each parameter
    """

    path_args = {}
    body_args = {}
    qs_args = {}
    header_args = {}
    defaults = {}

    annotations = get_type_hints(func, include_extras=True)
    for param_name, param_spec in signature(func).parameters.items():

        if param_name in ignore_params:
            continue

        if param_spec.annotation == param_spec.empty:
            raise RuntimeError(f"The parameter {param_name} must have an annotation")

        annotation = annotations[param_name]
        if param_spec.default is not param_spec.empty:
            defaults[param_name] = param_spec.default

        if param_spec.kind is param_spec.POSITIONAL_ONLY:
            path_args[param_name] = annotation

        elif param_spec.kind is param_spec.POSITIONAL_OR_KEYWORD:
            if is_pydantic_base_model(annotation) or robuste_issubclass(
                annotation, UploadedFile
            ):
                body_args[param_name] = annotation
            else:
                qs_args[param_name] = annotation
        elif param_spec.kind is param_spec.KEYWORD_ONLY:
            header_args[param_name] = annotation
        else:
            raise RuntimeError(f"You cannot use {param_spec.VAR_POSITIONAL} parameters")

    if unpack_group:
        try:
            _unpack_group_in_signature(qs_args, defaults)
            _unpack_group_in_signature(header_args, defaults)
        except DuplicateNames as error:
            raise TypeError(
                f"Parameters conflict in function {func},"
                f" the group {error.group} has an attribute named {error.attr_name}"
            ) from None

    return path_args, body_args, qs_args, header_args, defaults


class DuplicateNames(Exception):
    """
    Raised when a same parameter name is used in group and function signature.
    """

    group: Type[Group]
    attr_name: str

    def __init__(self, group: Type[Group], attr_name: str):
        self.group = group
        self.attr_name = attr_name
        super().__init__(
            f"Conflict with {group}.{attr_name} and function parameter name"
        )


def _unpack_group_in_signature(args: dict, defaults: dict) -> None:
    """
    Unpack in place each Group found in args.
    """
    for group_name, group in args.copy().items():
        if robuste_issubclass(group, Group):
            group_sig, group_default = _get_group_signature(group)
            for attr_name in group_sig:
                if attr_name in args and attr_name != group_name:
                    raise DuplicateNames(group, attr_name)

            del args[group_name]
            args.update(group_sig)
            defaults.update(group_default)
