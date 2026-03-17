import json
from abc import ABCMeta, abstractmethod
from contextlib import asynccontextmanager as actxmgr
from typing import AsyncIterator, Type, TypeVar

import trafaret as t
from aiohttp import web


class APIParams(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def get_checker(cls) -> t.Trafaret:
        raise NotImplementedError

    @classmethod
    def check(cls, value):
        checker = cls.get_checker()
        data = checker.check(value)
        return cls(**data)  # assumes dataclass-like init with kwargs


T_APIParams = TypeVar("T_APIParams", bound=APIParams)


@actxmgr
async def check_params(
    request: web.Request,
    checker: Type[T_APIParams],
) -> AsyncIterator[T_APIParams]:
    try:
        if request.method in ("GET", "DELETE"):
            params = checker.check(request.query)
        else:
            body = await request.post()
            params = checker.check(body)
        yield params
    except t.DataError as e:
        error_data = e.as_dict()
        if isinstance(error_data, str):
            detail = error_data
        else:
            detail = "\n".join(v for v in error_data.values())
        raise web.HTTPBadRequest(
            content_type="application/json",
            body=json.dumps({"msg": "Invalid parameters", "detail": detail}),
        ) from None
    except Exception as e:
        raise web.HTTPInternalServerError(
            content_type="application/json",
            body=json.dumps({"msg": "Internal server error", "detail": repr(e)}),
        ) from e
