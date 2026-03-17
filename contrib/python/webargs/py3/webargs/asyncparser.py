"""Asynchronous request parser. Compatible with Python>=3.5."""
import asyncio
import functools
import inspect
import typing
from collections.abc import Mapping

from marshmallow import Schema, ValidationError
from marshmallow.fields import Field
import marshmallow as ma
from marshmallow.utils import missing

from webargs import core

Request = typing.TypeVar("Request")
ArgMap = typing.Union[Schema, typing.Mapping[str, Field]]
Validate = typing.Union[typing.Callable, typing.Iterable[typing.Callable]]


class AsyncParser(core.Parser):
    """Asynchronous variant of `webargs.core.Parser`, where parsing methods may be
    either coroutines or regular methods.
    """

    async def _parse_request(
        self, schema: Schema, req: Request, locations: typing.Iterable
    ) -> typing.Union[dict, list]:
        if schema.many:
            assert (
                "json" in locations
            ), "schema.many=True is only supported for JSON location"
            # The ad hoc Nested field is more like a workaround or a helper,
            # and it servers its purpose fine. However, if somebody has a desire
            # to re-design the support of bulk-type arguments, go ahead.
            parsed = await self.parse_arg(
                name="json",
                field=ma.fields.Nested(schema, many=True),
                req=req,
                locations=locations,
            )
            if parsed is missing:
                parsed = []
        else:
            argdict = schema.fields
            parsed = {}
            for argname, field_obj in argdict.items():
                if core.MARSHMALLOW_VERSION_INFO[0] < 3:
                    parsed_value = await self.parse_arg(
                        argname, field_obj, req, locations
                    )
                    # If load_from is specified on the field, try to parse from that key
                    if parsed_value is missing and field_obj.load_from:
                        parsed_value = await self.parse_arg(
                            field_obj.load_from, field_obj, req, locations
                        )
                        argname = field_obj.load_from
                else:
                    argname = field_obj.data_key or argname
                    parsed_value = await self.parse_arg(
                        argname, field_obj, req, locations
                    )
                if parsed_value is not missing:
                    parsed[argname] = parsed_value
        return parsed

    # TODO: Lots of duplication from core.Parser here. Rethink.
    async def parse(
        self,
        argmap: ArgMap,
        req: Request = None,
        locations: typing.Iterable = None,
        validate: Validate = None,
        error_status_code: typing.Union[int, None] = None,
        error_headers: typing.Union[typing.Mapping[str, str], None] = None,
    ) -> typing.Union[typing.Mapping, None]:
        """Coroutine variant of `webargs.core.Parser`.

        Receives the same arguments as `webargs.core.Parser.parse`.
        """
        self.clear_cache()  # in case someone used `parse_*()`
        req = req if req is not None else self.get_default_request()
        assert req is not None, "Must pass req object"
        data = None
        validators = core._ensure_list_of_callables(validate)
        schema = self._get_schema(argmap, req)
        try:
            parsed = await self._parse_request(
                schema=schema, req=req, locations=locations or self.locations
            )
            result = schema.load(parsed)
            data = result.data if core.MARSHMALLOW_VERSION_INFO[0] < 3 else result
            self._validate_arguments(data, validators)
        except ma.exceptions.ValidationError as error:
            await self._on_validation_error(
                error, req, schema, error_status_code, error_headers
            )
        return data

    async def _on_validation_error(
        self,
        error: ValidationError,
        req: Request,
        schema: Schema,
        error_status_code: typing.Union[int, None],
        error_headers: typing.Union[typing.Mapping[str, str], None] = None,
    ) -> None:
        error_handler = self.error_callback or self.handle_error
        await error_handler(error, req, schema, error_status_code, error_headers)

    def use_args(
        self,
        argmap: ArgMap,
        req: typing.Optional[Request] = None,
        locations: typing.Iterable = None,
        as_kwargs: bool = False,
        validate: Validate = None,
        error_status_code: typing.Optional[int] = None,
        error_headers: typing.Union[typing.Mapping[str, str], None] = None,
    ) -> typing.Callable[..., typing.Callable]:
        """Decorator that injects parsed arguments into a view function or method.

        Receives the same arguments as `webargs.core.Parser.use_args`.
        """
        locations = locations or self.locations
        request_obj = req
        # Optimization: If argmap is passed as a dictionary, we only need
        # to generate a Schema once
        if isinstance(argmap, Mapping):
            argmap = core.dict2schema(argmap, self.schema_class)()

        def decorator(func: typing.Callable) -> typing.Callable:
            req_ = request_obj

            if inspect.iscoroutinefunction(func):

                @functools.wraps(func)
                async def wrapper(*args, **kwargs):
                    req_obj = req_

                    if not req_obj:
                        req_obj = self.get_request_from_view_args(func, args, kwargs)
                    # NOTE: At this point, argmap may be a Schema, callable, or dict
                    parsed_args = await self.parse(
                        argmap,
                        req=req_obj,
                        locations=locations,
                        validate=validate,
                        error_status_code=error_status_code,
                        error_headers=error_headers,
                    )
                    if as_kwargs:
                        kwargs.update(parsed_args or {})
                        return await func(*args, **kwargs)
                    else:
                        # Add parsed_args after other positional arguments
                        new_args = args + (parsed_args,)
                        return await func(*new_args, **kwargs)

            else:

                @functools.wraps(func)  # type: ignore
                def wrapper(*args, **kwargs):
                    req_obj = req_

                    if not req_obj:
                        req_obj = self.get_request_from_view_args(func, args, kwargs)
                    # NOTE: At this point, argmap may be a Schema, callable, or dict
                    parsed_args = yield from self.parse(  # type: ignore
                        argmap,
                        req=req_obj,
                        locations=locations,
                        validate=validate,
                        error_status_code=error_status_code,
                        error_headers=error_headers,
                    )
                    if as_kwargs:
                        kwargs.update(parsed_args)
                        return func(*args, **kwargs)  # noqa: B901
                    else:
                        # Add parsed_args after other positional arguments
                        new_args = args + (parsed_args,)
                        return func(*new_args, **kwargs)

            return wrapper

        return decorator

    def use_kwargs(self, *args, **kwargs) -> typing.Callable:
        """Decorator that injects parsed arguments into a view function or method.

        Receives the same arguments as `webargs.core.Parser.use_kwargs`.

        """
        return super().use_kwargs(*args, **kwargs)

    async def parse_arg(
        self, name: str, field: Field, req: Request, locations: typing.Iterable = None
    ) -> typing.Any:
        location = field.metadata.get("location")
        if location:
            locations_to_check = self._validated_locations([location])
        else:
            locations_to_check = self._validated_locations(locations or self.locations)

        for location in locations_to_check:
            value = await self._get_value(name, field, req=req, location=location)
            # Found the value; validate and return it
            if value is not core.missing:
                return value
        return core.missing

    async def _get_value(
        self, name: str, argobj: Field, req: Request, location: str
    ) -> typing.Any:
        function = self._get_handler(location)
        if asyncio.iscoroutinefunction(function):
            value = await function(req, name, argobj)
        else:
            value = function(req, name, argobj)
        return value
