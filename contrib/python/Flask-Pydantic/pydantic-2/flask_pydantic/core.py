from functools import wraps
from typing import Any, Callable, Iterable, List, Optional, Tuple, Type, Union

from flask import Response, current_app, jsonify, make_response, request
from pydantic import BaseModel, RootModel, TypeAdapter, ValidationError
from pydantic.v1 import BaseModel as V1BaseModel
from pydantic.v1.error_wrappers import ValidationError as V1ValidationError
from pydantic.v1.tools import parse_obj_as

from .converters import convert_query_params
from .exceptions import (
    InvalidIterableOfModelsException,
    JsonBodyParsingError,
    ManyModelValidationError,
)
from .exceptions import ValidationError as FailedValidation

try:
    from flask_restful import original_flask_make_response as make_response
except ImportError:
    pass

V1OrV2BaseModel = Union[BaseModel, V1BaseModel]


def _model_dump_json(model: V1OrV2BaseModel, **kwargs):
    """Adapter to dump a model to json, whether it's a Pydantic V1 or V2 model."""
    if isinstance(model, BaseModel):
        return model.model_dump_json(**kwargs)
    else:
        return model.json(**kwargs)


def _sanitize_ctx_errors(errors):
    """
    Make Pydantic `ctx["error"]` JSON-serializable by replacing
    exception instances with {type, message}.
    """
    for error in errors:
        ctx = error.get("ctx")
        if isinstance(ctx, dict) and isinstance(ctx.get("error"), Exception):
            exc = ctx["error"]
            ctx["error"] = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
    return errors


def make_json_response(
    content: Union[V1OrV2BaseModel, Iterable[V1OrV2BaseModel]],
    status_code: int,
    by_alias: bool,
    exclude_none: bool = False,
    many: bool = False,
) -> Response:
    """serializes model, creates JSON response with given status code"""
    if many:
        js = f"[{', '.join([_model_dump_json(model, exclude_none=exclude_none, by_alias=by_alias) for model in content])}]"
    else:
        js = _model_dump_json(content, exclude_none=exclude_none, by_alias=by_alias)
    response = make_response(js, status_code)
    response.mimetype = "application/json"
    return response


def unsupported_media_type_response(request_cont_type: str) -> Response:
    body = {
        "detail": f"Unsupported media type '{request_cont_type}' in request. "
        "'application/json' is required."
    }
    return make_response(jsonify(body), 415)


def is_iterable_of_models(content: Any) -> bool:
    try:
        return all(isinstance(obj, (BaseModel, V1BaseModel)) for obj in content)
    except TypeError:
        return False


def validate_many_models(
    model: Type[V1OrV2BaseModel], content: Any
) -> List[V1OrV2BaseModel]:
    try:
        return [model(**fields) for fields in content]
    except TypeError as te:
        # iteration through `content` fails
        err = [
            {
                "loc": ["root"],
                "msg": "is not an array of objects",
                "type": "type_error.array",
            }
        ]

        raise ManyModelValidationError(err) from te
    except (ValidationError, V1ValidationError) as ve:
        raise ManyModelValidationError(_sanitize_ctx_errors(ve.errors())) from ve


def validate_path_params(func: Callable, kwargs: dict) -> Tuple[dict, list]:
    errors = []
    validated = {}
    # Only validate parameters that are actual path parameters from the route
    # request.view_args contains only the path parameters extracted from the URL
    path_param_names = set(request.view_args.keys()) if request.view_args else set()

    for name, type_ in func.__annotations__.items():
        if name not in path_param_names:
            continue
        try:
            if not isinstance(type_, V1BaseModel):
                adapter = TypeAdapter(type_)
                validated[name] = adapter.validate_python(kwargs.get(name))
            else:
                value = parse_obj_as(type_, kwargs.get(name))
                validated[name] = value
        except (ValidationError, V1ValidationError) as e:
            err = e.errors()[0]
            err["loc"] = [name]
            errors.append(err)
    kwargs = {**kwargs, **validated}
    return kwargs, errors


def get_body_dict(**params):
    data = request.get_json(**params)
    if data is None and params.get("silent"):
        return {}
    return data


def validate(
    body: Optional[Type[V1OrV2BaseModel]] = None,
    query: Optional[Type[V1OrV2BaseModel]] = None,
    on_success_status: int = 200,
    exclude_none: bool = False,
    response_many: bool = False,
    request_body_many: bool = False,
    response_by_alias: bool = False,
    get_json_params: Optional[dict] = None,
    form: Optional[Type[V1OrV2BaseModel]] = None,
):
    """
    Decorator for route methods which will validate query, body and form parameters
    as well as serialize the response (if it derives from pydantic's BaseModel
    class).

    Request parameters are accessible via flask's `request` variable:
        - request.query_params
        - request.body_params
        - request.form_params

    Or directly as `kwargs`, if you define them in the decorated function.

    `exclude_none` whether to remove None fields from response
    `response_many` whether content of response consists of many objects
        (e. g. List[BaseModel]). Resulting response will be an array of serialized
        models.
    `request_body_many` whether response body contains array of given model
        (request.body_params then contains list of models i. e. List[BaseModel])
    `response_by_alias` whether Pydantic's alias is used
    `get_json_params` - parameters to be passed to Request.get_json() function

    example::

        from flask import request
        from flask_pydantic import validate
        from pydantic import BaseModel

        class Query(BaseModel):
            query: str

        class Body(BaseModel):
            color: str

        class Form(BaseModel):
            name: str

        class MyModel(BaseModel):
            id: int
            color: str
            description: str

        ...

        @app.route("/")
        @validate(query=Query, body=Body, form=Form)
        def test_route():
            query = request.query_params.query
            color = request.body_params.query

            return MyModel(...)

        @app.route("/kwargs")
        @validate()
        def test_route_kwargs(query:Query, body:Body, form:Form):

            return MyModel(...)

    -> that will render JSON response with serialized MyModel instance
    """

    def decorate(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            q, b, f, err = None, None, None, {}
            kwargs, path_err = validate_path_params(func, kwargs)
            if path_err:
                err["path_params"] = path_err
            query_in_kwargs = func.__annotations__.get("query")
            query_model = query_in_kwargs or query
            if query_model:
                query_params = convert_query_params(request.args, query_model)
                try:
                    q = query_model(**query_params)
                except (ValidationError, V1ValidationError) as ve:
                    err["query_params"] = _sanitize_ctx_errors(ve.errors())
            body_in_kwargs = func.__annotations__.get("body")
            body_model = body_in_kwargs or body
            if body_model:
                body_params = get_body_dict(**(get_json_params or {}))
                if (
                    issubclass(body_model, V1BaseModel)
                    and "__root__" in body_model.__fields__
                ):
                    try:
                        b = body_model(__root__=body_params).__root__
                    except (ValidationError, V1ValidationError) as ve:
                        err["body_params"] = _sanitize_ctx_errors(ve.errors())
                elif issubclass(body_model, RootModel):
                    try:
                        b = body_model(body_params)
                    except (ValidationError, V1ValidationError) as ve:
                        err["body_params"] = _sanitize_ctx_errors(ve.errors())
                elif request_body_many:
                    try:
                        b = validate_many_models(body_model, body_params)
                    except ManyModelValidationError as e:
                        err["body_params"] = e.errors()
                else:
                    try:
                        b = body_model(**body_params)
                    except TypeError as te:
                        content_type = request.headers.get("Content-Type", "").lower()
                        media_type = content_type.split(";")[0]
                        if media_type != "application/json":
                            return unsupported_media_type_response(content_type)
                        else:
                            raise JsonBodyParsingError() from te
                    except (ValidationError, V1ValidationError) as ve:
                        err["body_params"] = _sanitize_ctx_errors(ve.errors())
            form_in_kwargs = func.__annotations__.get("form")
            form_model = form_in_kwargs or form
            if form_model:
                form_params = request.form
                if (
                    isinstance(form, V1BaseModel)
                    and "__root__" in form_model.__fields__
                ):
                    try:
                        f = form_model(form_params)
                    except (ValidationError, V1ValidationError) as ve:
                        err["form_params"] = _sanitize_ctx_errors(ve.errors())
                elif issubclass(form_model, RootModel):
                    try:
                        f = form_model(form_params)
                    except (ValidationError, V1ValidationError) as ve:
                        err["form_params"] = _sanitize_ctx_errors(ve.errors())
                else:
                    try:
                        f = form_model(**form_params)
                    except TypeError as te:
                        content_type = request.headers.get("Content-Type", "").lower()
                        media_type = content_type.split(";")[0]
                        if media_type != "multipart/form-data":
                            return unsupported_media_type_response(content_type)
                        else:
                            raise JsonBodyParsingError from te
                    except (ValidationError, V1ValidationError) as ve:
                        err["form_params"] = _sanitize_ctx_errors(ve.errors())
            request.query_params = q
            request.body_params = b
            request.form_params = f
            if query_in_kwargs:
                kwargs["query"] = q
            if body_in_kwargs:
                kwargs["body"] = b
            if form_in_kwargs:
                kwargs["form"] = f

            if err:
                if current_app.config.get(
                    "FLASK_PYDANTIC_VALIDATION_ERROR_RAISE", False
                ):
                    raise FailedValidation(**err)
                else:
                    status_code = current_app.config.get(
                        "FLASK_PYDANTIC_VALIDATION_ERROR_STATUS_CODE", 400
                    )
                    return make_response(
                        jsonify({"validation_error": err}), status_code
                    )
            res = current_app.ensure_sync(func)(*args, **kwargs)

            if response_many:
                if is_iterable_of_models(res):
                    return make_json_response(
                        res,
                        on_success_status,
                        by_alias=response_by_alias,
                        exclude_none=exclude_none,
                        many=True,
                    )
                else:
                    raise InvalidIterableOfModelsException(res)

            if isinstance(res, (BaseModel, V1BaseModel)):
                return make_json_response(
                    res,
                    on_success_status,
                    exclude_none=exclude_none,
                    by_alias=response_by_alias,
                )

            if (
                isinstance(res, tuple)
                and len(res) in [2, 3]
                and isinstance(res[0], (BaseModel, V1BaseModel))
            ):
                headers = None
                status = on_success_status
                if isinstance(res[1], (dict, tuple, list)):
                    headers = res[1]
                elif len(res) == 3 and isinstance(res[2], (dict, tuple, list)):
                    status = res[1]
                    headers = res[2]
                else:
                    status = res[1]

                ret = make_json_response(
                    res[0],
                    status,
                    exclude_none=exclude_none,
                    by_alias=response_by_alias,
                )
                if headers:
                    ret.headers.update(headers)
                return ret

            return res

        return wrapper

    return decorate
