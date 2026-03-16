__all__ = ["CurioAsksSession"]

import curio
import asks
from asks import Session

from .abc import AbstractSession
from ..models import Response

asks.init("curio")


class CurioAsksSession(Session, AbstractSession):
    def __init__(self, *args, **kwargs):
        if kwargs.get("timeout"):
            del kwargs["timeout"]
            kwargs.pop("timeout", None)
        super().__init__(*args, **kwargs)

    async def send(
        self,
        *requests,
        timeout=None,
        full_res=False,
        raise_for_status=True,
        session_factory=None
    ):
        async def resolve_response(request, response):
            data = None
            json = None
            download_file = None
            upload_file = None
            pipe_from = None

            # If downloading file:
            if request.media_download:
                raise NotImplementedError(
                    "Downloading media isn't supported by this session"
                )
            else:
                if response.status_code != 204:  # If no (no content)
                    try:
                        json = response.json()
                    except:  # noqa: E722  bare-except
                        try:
                            data = response.text
                        except:  # noqa: E722  bare-except
                            try:
                                data = response.content
                            except:  # noqa: E722  bare-except
                                try:
                                    data = response.body
                                except:  # noqa: E722  bare-except
                                    data = None

            if request.media_upload:
                upload_file = request.media_upload.file_path

            return Response(
                url=str(response.url),
                headers=response.headers,
                status_code=response.status_code,
                json=json,
                data=data,
                reason=response.reason_phrase
                if getattr(response, "reason_phrase")
                else None,
                req=request,
                download_file=download_file,
                upload_file=upload_file,
                pipe_from=pipe_from,
                session_factory=session_factory,
            )

        async def fire_request(request):
            request.headers["Accept-Encoding"] = "gzip"
            request.headers["User-Agent"] = "Aiogoogle Asks Curio (gzip)"
            if request.media_upload:
                raise NotImplementedError(
                    "Uploading media isn't supported by this session"
                )
            else:
                return await self.request(
                    method=request.method,
                    url=request.url,
                    headers=request.headers,
                    data=request.data,
                    json=request.json,
                    # TODO: doesn't work with Asks
                    # verify=request._verify_ssl,
                )

        # ----------------- send sequence ------------------#
        async def get_response(request):
            response = await fire_request(request)
            response = await resolve_response(request, response)
            if raise_for_status is True:
                response.raise_for_status()
            return response

        async def get_content(request):
            response = await get_response(request)
            return response.content

        # ----------------- /send sequence ------------------#

        async def execute_tasks():
            async with curio.TaskGroup() as g:
                if full_res is True:
                    tasks = [
                        await g.spawn(get_response, request) for request in requests
                    ]
                else:
                    tasks = [
                        await g.spawn(get_content, request) for request in requests
                    ]
            return await curio.gather(tasks)

        session_factory = self.__class__ if session_factory is None else session_factory

        if timeout is not None:
            async with curio.timeout_after(timeout):
                results = await execute_tasks()
        else:
            results = await execute_tasks()

        if isinstance(results, list) and len(results) == 1:
            return results[0]
        else:
            return results
