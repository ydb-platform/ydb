import logging
import os
import time
from queue import Empty, Full, Queue
from typing import Any, Callable, Optional, TypeVar, cast

import backoff
import requests
from typing_extensions import ParamSpec

from langfuse._client.environment_variables import LANGFUSE_MEDIA_UPLOAD_ENABLED
from langfuse._utils import _get_timestamp
from langfuse.api import GetMediaUploadUrlRequest, PatchMediaBody
from langfuse.api.client import FernLangfuse
from langfuse.api.core import ApiError
from langfuse.api.resources.media.types.media_content_type import MediaContentType
from langfuse.media import LangfuseMedia

from .media_upload_queue import UploadMediaJob

T = TypeVar("T")
P = ParamSpec("P")


class MediaManager:
    _log = logging.getLogger("langfuse")

    def __init__(
        self,
        *,
        api_client: FernLangfuse,
        media_upload_queue: Queue,
        max_retries: Optional[int] = 3,
    ):
        self._api_client = api_client
        self._queue = media_upload_queue
        self._max_retries = max_retries
        self._enabled = os.environ.get(
            LANGFUSE_MEDIA_UPLOAD_ENABLED, "True"
        ).lower() not in ("false", "0")

    def process_next_media_upload(self) -> None:
        try:
            upload_job = self._queue.get(block=True, timeout=1)
            self._log.debug(
                f"Media: Processing upload for media_id={upload_job['media_id']} in trace_id={upload_job['trace_id']}"
            )
            self._process_upload_media_job(data=upload_job)

            self._queue.task_done()
        except Empty:
            pass
        except Exception as e:
            self._log.error(
                f"Media upload error: Failed to upload media due to unexpected error. Queue item marked as done. Error: {e}"
            )
            self._queue.task_done()

    def _find_and_process_media(
        self,
        *,
        data: Any,
        trace_id: str,
        observation_id: Optional[str],
        field: str,
    ) -> Any:
        if not self._enabled:
            return data

        seen = set()
        max_levels = 10

        def _process_data_recursively(data: Any, level: int) -> Any:
            if id(data) in seen or level > max_levels:
                return data

            seen.add(id(data))

            if isinstance(data, LangfuseMedia):
                self._process_media(
                    media=data,
                    trace_id=trace_id,
                    observation_id=observation_id,
                    field=field,
                )

                return data

            if isinstance(data, str) and data.startswith("data:"):
                media = LangfuseMedia(
                    obj=data,
                    base64_data_uri=data,
                )

                self._process_media(
                    media=media,
                    trace_id=trace_id,
                    observation_id=observation_id,
                    field=field,
                )

                return media

            # Anthropic
            if (
                isinstance(data, dict)
                and "type" in data
                and data["type"] == "base64"
                and "media_type" in data
                and "data" in data
            ):
                media = LangfuseMedia(
                    base64_data_uri=f"data:{data['media_type']};base64," + data["data"],
                )

                self._process_media(
                    media=media,
                    trace_id=trace_id,
                    observation_id=observation_id,
                    field=field,
                )

                copied = data.copy()
                copied["data"] = media

                return copied

            # Vertex
            if (
                isinstance(data, dict)
                and "type" in data
                and data["type"] == "media"
                and "mime_type" in data
                and "data" in data
            ):
                media = LangfuseMedia(
                    base64_data_uri=f"data:{data['mime_type']};base64," + data["data"],
                )

                self._process_media(
                    media=media,
                    trace_id=trace_id,
                    observation_id=observation_id,
                    field=field,
                )

                copied = data.copy()
                copied["data"] = media

                return copied

            if isinstance(data, list):
                return [_process_data_recursively(item, level + 1) for item in data]

            if isinstance(data, dict):
                return {
                    key: _process_data_recursively(value, level + 1)
                    for key, value in data.items()
                }

            return data

        return _process_data_recursively(data, 1)

    def _process_media(
        self,
        *,
        media: LangfuseMedia,
        trace_id: str,
        observation_id: Optional[str],
        field: str,
    ) -> None:
        if (
            media._content_length is None
            or media._content_type is None
            or media._content_sha256_hash is None
            or media._content_bytes is None
        ):
            return

        if media._media_id is None:
            self._log.error("Media ID is None. Skipping upload.")
            return

        try:
            upload_media_job = UploadMediaJob(
                media_id=media._media_id,
                content_bytes=media._content_bytes,
                content_type=media._content_type,
                content_length=media._content_length,
                content_sha256_hash=media._content_sha256_hash,
                trace_id=trace_id,
                observation_id=observation_id,
                field=field,
            )

            self._queue.put(
                item=upload_media_job,
                block=False,
            )
            self._log.debug(
                f"Queue: Enqueued media ID {media._media_id} for upload processing | trace_id={trace_id} | field={field}"
            )

        except Full:
            self._log.warning(
                f"Queue capacity: Media queue is full. Failed to process media_id={media._media_id} for trace_id={trace_id}. Consider increasing queue capacity."
            )

        except Exception as e:
            self._log.error(
                f"Media processing error: Failed to process media_id={media._media_id} for trace_id={trace_id}. Error: {str(e)}"
            )

    def _process_upload_media_job(
        self,
        *,
        data: UploadMediaJob,
    ) -> None:
        upload_url_response = self._request_with_backoff(
            self._api_client.media.get_upload_url,
            request=GetMediaUploadUrlRequest(
                contentLength=data["content_length"],
                contentType=cast(MediaContentType, data["content_type"]),
                sha256Hash=data["content_sha256_hash"],
                field=data["field"],
                traceId=data["trace_id"],
                observationId=data["observation_id"],
            ),
        )

        upload_url = upload_url_response.upload_url

        if not upload_url:
            self._log.debug(
                f"Media status: Media with ID {data['media_id']} already uploaded. Skipping duplicate upload."
            )

            return

        if upload_url_response.media_id != data["media_id"]:
            self._log.error(
                f"Media integrity error: Media ID mismatch between SDK ({data['media_id']}) and Server ({upload_url_response.media_id}). Upload cancelled. Please check media ID generation logic."
            )

            return

        headers = {"Content-Type": data["content_type"]}

        # In self-hosted setups with GCP, do not add unsupported headers that fail the upload
        is_self_hosted_gcs_bucket = "storage.googleapis.com" in upload_url

        if not is_self_hosted_gcs_bucket:
            headers["x-ms-blob-type"] = "BlockBlob"
            headers["x-amz-checksum-sha256"] = data["content_sha256_hash"]

        upload_start_time = time.time()
        upload_response = self._request_with_backoff(
            requests.put,
            upload_url,
            headers=headers,
            data=data["content_bytes"],
        )
        upload_time_ms = int((time.time() - upload_start_time) * 1000)

        self._request_with_backoff(
            self._api_client.media.patch,
            media_id=data["media_id"],
            request=PatchMediaBody(
                uploadedAt=_get_timestamp(),
                uploadHttpStatus=upload_response.status_code,
                uploadHttpError=upload_response.text,
                uploadTimeMs=upload_time_ms,
            ),
        )

        self._log.debug(
            f"Media upload: Successfully uploaded media_id={data['media_id']} for trace_id={data['trace_id']} | status_code={upload_response.status_code} | duration={upload_time_ms}ms | size={data['content_length']} bytes"
        )

    def _request_with_backoff(
        self, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs
    ) -> T:
        def _should_give_up(e: Exception) -> bool:
            if isinstance(e, ApiError):
                return (
                    e.status_code is not None
                    and 400 <= e.status_code < 500
                    and e.status_code != 429
                )
            if isinstance(e, requests.exceptions.RequestException):
                return (
                    e.response is not None
                    and e.response.status_code < 500
                    and e.response.status_code != 429
                )
            return False

        @backoff.on_exception(
            backoff.expo,
            Exception,
            max_tries=self._max_retries,
            giveup=_should_give_up,
            logger=None,
        )
        def execute_task_with_backoff() -> T:
            return func(*args, **kwargs)

        return execute_task_with_backoff()
