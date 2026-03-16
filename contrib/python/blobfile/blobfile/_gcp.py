import base64
import binascii
import concurrent.futures
import datetime
import hashlib
import json
import math
import os
import platform
import socket
import time
import urllib.parse
from typing import Any, Iterator, Mapping

import urllib3

from blobfile import _common as common
from blobfile._common import (
    GCP_BASE_URL,
    BaseStreamingReadFile,
    BaseStreamingWriteFile,
    Config,
    DirEntry,
    Error,
    FileBody,
    Request,
    RequestFailure,
    RestartableStreamingWriteFailure,
    Stat,
    TokenManager,
    path_join,
    strip_slashes,
)

MAX_EXPIRATION = 7 * 24 * 60 * 60
OAUTH_TOKEN = "oauth_token"
ANONYMOUS = "anonymous"


def _is_gce_instance() -> bool:
    try:
        socket.getaddrinfo("metadata.google.internal", 80)
    except socket.gaierror:
        return False
    return True


def _b64encode(s: bytes) -> bytes:
    return base64.urlsafe_b64encode(s)


def _sign(private_key: str, msg: bytes) -> bytes:
    from Cryptodome.Hash import SHA256
    from Cryptodome.PublicKey import RSA
    from Cryptodome.Signature import pkcs1_15

    key = RSA.import_key(private_key)
    h = SHA256.new(msg)
    return pkcs1_15.new(key).sign(h)


def _create_jwt(private_key: str, data: Mapping[str, Any]) -> bytes:
    header_b64 = _b64encode(json.dumps({"alg": "RS256", "typ": "JWT"}).encode("utf8"))
    body_b64 = _b64encode(json.dumps(data).encode("utf8"))
    to_sign = header_b64 + b"." + body_b64
    signature_b64 = _b64encode(_sign(private_key, to_sign))
    return header_b64 + b"." + body_b64 + b"." + signature_b64


def _create_token_request(client_email: str, private_key: str, scopes: list[str]) -> Request:
    # https://developers.google.com/identity/protocols/OAuth2ServiceAccount
    now = time.time()
    claim_set = {
        "iss": client_email,
        "scope": " ".join(scopes),
        "aud": "https://www.googleapis.com/oauth2/v4/token",
        "exp": now + 60 * 60,
        "iat": now,
    }
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": _create_jwt(private_key, claim_set),
    }
    return Request(
        url="https://www.googleapis.com/oauth2/v4/token",
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=urllib.parse.urlencode(data).encode("utf8"),
    )


def _refresh_access_token_request(
    client_id: str, client_secret: str, refresh_token: str
) -> Request:
    # https://developers.google.com/identity/protocols/OAuth2WebServer#offline
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    return Request(
        url="https://www.googleapis.com/oauth2/v4/token",
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=urllib.parse.urlencode(data).encode("utf8"),
    )


def _load_credentials() -> dict[str, Any]:
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        if not os.path.exists(creds_path):
            raise Error(
                f"credentials not found at {creds_path} specified by environment variable 'GOOGLE_APPLICATION_CREDENTIALS'"
            )
        with open(creds_path) as f:
            return json.load(f)
    if platform.system() == "Windows":
        # https://www.jhanley.com/google-cloud-application-default-credentials/
        default_creds_path = os.path.join(
            os.environ["APPDATA"], "gcloud/application_default_credentials.json"
        )
    else:
        default_creds_path = os.path.join(
            os.environ["HOME"], ".config/gcloud/application_default_credentials.json"
        )

    if os.path.exists(default_creds_path):
        with open(default_creds_path) as f:
            return json.load(f)
    return {}


def _create_access_token_request(creds: dict[str, Any], scopes: list[str]) -> Request:
    if "private_key" in creds:
        # looks like GCS does not support the no-oauth flow https://developers.google.com/identity/protocols/OAuth2ServiceAccount#jwt-auth
        return _create_token_request(creds["client_email"], creds["private_key"], scopes)
    elif "refresh_token" in creds:
        return _refresh_access_token_request(
            refresh_token=creds["refresh_token"],
            client_id=creds["client_id"],
            client_secret=creds["client_secret"],
        )
    else:
        raise Error("Credentials not recognized")


def build_url(template: str, **data: str) -> str:
    return common.build_url(GCP_BASE_URL, template, **data)


def create_api_request(req: Request, auth: tuple[str, str]) -> Request:
    if req.headers is None:
        headers = {}
    else:
        headers = dict(req.headers).copy()

    if req.params is None:
        params = {}
    else:
        params = dict(req.params).copy()

    kind, token = auth
    if kind == OAUTH_TOKEN:
        headers["Authorization"] = f"Bearer {token}"
    elif kind == ANONYMOUS:
        pass
    else:
        raise Error(f"unrecognized auth kind `{kind}`")

    data = req.data
    if data is not None and isinstance(data, dict):
        data = json.dumps(data).encode("utf8")
        assert "Content-Type" not in headers
        headers["Content-Type"] = "application/json"
    return Request(
        method=req.method,
        url=req.url,
        params=params,
        headers=headers,
        data=data,
        preload_content=req.preload_content,
        success_codes=tuple(req.success_codes),
        retry_codes=tuple(req.retry_codes),
    )


def generate_signed_url(
    bucket: str,
    name: str,
    expiration: float,
    method: str = "GET",
    params: Mapping[str, str] | None = None,
    headers: Mapping[str, str] | None = None,
) -> tuple[str, float | None]:
    if params is None:
        p = {}
    else:
        p = dict(params).copy()

    if headers is None:
        h = {}
    else:
        h = dict(headers).copy()

    # https://cloud.google.com/storage/docs/access-control/signing-urls-manually
    creds = _load_credentials()
    if "private_key" not in creds:
        raise Error(
            "Private key not found in credentials.  Please set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to a JSON key for a service account to use this call"
        )

    if expiration > MAX_EXPIRATION:
        raise Error(f"Expiration can't be longer than {MAX_EXPIRATION} seconds.")

    escaped_object_name = urllib.parse.quote(name, safe="")
    canonical_uri = f"/{bucket}/{escaped_object_name}"

    datetime_now = datetime.datetime.now(tz=datetime.timezone.utc)
    request_timestamp = datetime_now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = datetime_now.strftime("%Y%m%d")

    credential_scope = f"{datestamp}/auto/storage/goog4_request"
    credential = f"{creds['client_email']}/{credential_scope}"
    h["host"] = "storage.googleapis.com"

    canonical_headers = ""
    ordered_headers = sorted(h.items())
    for k, v in ordered_headers:
        lower_k = str(k).lower()
        strip_v = str(v).lower()
        canonical_headers += f"{lower_k}:{strip_v}\n"

    signed_headers_parts = []
    for k, _ in ordered_headers:
        lower_k = str(k).lower()
        signed_headers_parts.append(lower_k)
    signed_headers = ";".join(signed_headers_parts)

    p["X-Goog-Algorithm"] = "GOOG4-RSA-SHA256"
    p["X-Goog-Credential"] = credential
    p["X-Goog-Date"] = request_timestamp
    p["X-Goog-Expires"] = str(expiration)
    p["X-Goog-SignedHeaders"] = signed_headers

    canonical_query_string_parts = []
    ordered_params = sorted(p.items())
    for k, v in ordered_params:
        encoded_k = urllib.parse.quote(str(k), safe="")
        encoded_v = urllib.parse.quote(str(v), safe="")
        canonical_query_string_parts.append(f"{encoded_k}={encoded_v}")
    canonical_query_string = "&".join(canonical_query_string_parts)

    canonical_request = "\n".join(
        [
            method,
            canonical_uri,
            canonical_query_string,
            canonical_headers,
            signed_headers,
            "UNSIGNED-PAYLOAD",
        ]
    )

    canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()

    string_to_sign = "\n".join(
        ["GOOG4-RSA-SHA256", request_timestamp, credential_scope, canonical_request_hash]
    )

    signature = binascii.hexlify(_sign(creds["private_key"], string_to_sign.encode("utf8"))).decode(
        "utf8"
    )
    host_name = "https://storage.googleapis.com"
    signed_url = f"{host_name}{canonical_uri}?{canonical_query_string}&X-Goog-Signature={signature}"
    return signed_url, expiration


def isdir(conf: Config, path: str) -> bool:
    if not path.endswith("/"):
        path += "/"
    bucket, blob = split_path(path)
    if blob == "":
        req = Request(
            url=build_url("/storage/v1/b/{bucket}", bucket=bucket),
            method="GET",
            success_codes=(200, 404),
        )
        resp = execute_api_request(conf, req)
        return resp.status == 200
    else:
        req = Request(
            url=build_url("/storage/v1/b/{bucket}/o", bucket=bucket),
            method="GET",
            params=dict(prefix=blob, delimiter="/", maxResults="1"),
            success_codes=(200, 404),
        )
        resp = execute_api_request(conf, req)
        if resp.status == 404:
            return False
        result = json.loads(resp.data)
        return "items" in result or "prefixes" in result


def mkdirfile(conf: Config, path: str) -> None:
    if not path.endswith("/"):
        path += "/"
    bucket, blob = split_path(path)
    req = Request(
        url=build_url("/upload/storage/v1/b/{bucket}/o", bucket=bucket),
        method="POST",
        params=dict(uploadType="media", name=blob),
        success_codes=(200, 400),
    )
    resp = execute_api_request(conf, req)
    if resp.status == 400:
        raise Error(f"Unable to create directory, bucket does not exist: '{path}'")


def split_path(path: str) -> tuple[str, str]:
    if not path.startswith("gs://"):
        raise Error(f"Invalid path: '{path}'")
    path = path[len("gs://") :]
    bucket, _, obj = path.partition("/")
    if bucket == "":
        raise Error(f"Invalid path: '{path}'")
    return bucket, obj


def combine_path(bucket: str, obj: str) -> str:
    return f"gs://{bucket}/{obj}"


def get_md5(metadata: Mapping[str, Any]) -> str | None:
    if "md5Hash" in metadata:
        return base64.b64decode(metadata["md5Hash"]).hex()

    if "metadata" in metadata and "md5" in metadata["metadata"]:
        # fallback to our custom hash if this is a composite object that is lacking the md5Hash field
        return metadata["metadata"]["md5"]

    return None


def _parse_timestamp(text: str) -> float:
    return datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()


def make_stat(item: Mapping[str, Any]) -> Stat:
    if "metadata" in item and "blobfile-mtime" in item["metadata"]:
        mtime = float(item["metadata"]["blobfile-mtime"])
    else:
        mtime = _parse_timestamp(item["updated"])
    return Stat(
        size=int(item["size"]),
        mtime=mtime,
        ctime=_parse_timestamp(item["timeCreated"]),
        md5=get_md5(item),
        version=item["generation"],
    )


def _get_access_token(conf: Config, key: Any) -> tuple[Any, float]:
    if os.environ.get("BLOBFILE_FORCE_GOOGLE_ANONYMOUS_AUTH", "0") == "1":
        return (ANONYMOUS, ""), float("inf")

    now = time.time()

    # https://github.com/googleapis/google-auth-library-java/blob/master/README.md#application-default-credentials
    creds = _load_credentials()
    if len(creds) > 0:

        def build_req() -> Request:
            req = _create_access_token_request(
                creds=creds, scopes=["https://www.googleapis.com/auth/devstorage.full_control"]
            )
            req.success_codes = (200, 400)
            return req

        resp = common.execute_request(conf, build_req)
        result = json.loads(resp.data)
        if resp.status == 400:
            error = result["error"]
            description = result.get("error_description", "<missing description>")
            msg = f"Error with google credentials: [{error}] {description}"
            if error == "invalid_grant":
                if description.startswith("Invalid JWT:"):
                    msg += "\nPlease verify that your system clock is correct."
                elif description == "Bad Request":
                    msg += "\nYour credentials may be expired, please run the following commands: `gcloud auth application-default revoke` (this may fail but ignore the error) then `gcloud auth application-default login`"
            raise Error(msg)
        assert resp.status == 200
        return (OAUTH_TOKEN, result["access_token"]), now + float(result["expires_in"])
    elif os.environ.get("NO_GCE_CHECK", "false").lower() != "true" and _is_gce_instance():
        # see if the metadata server has a token for us
        def build_req() -> Request:
            return Request(
                method="GET",
                url="http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
                headers={"Metadata-Flavor": "Google"},
            )

        resp = common.execute_request(conf, build_req)
        result = json.loads(resp.data)
        return (OAUTH_TOKEN, result["access_token"]), now + float(result["expires_in"])
    else:
        return (ANONYMOUS, ""), float("inf")


def execute_api_request(conf: Config, req: Request) -> "urllib3.BaseHTTPResponse":
    def build_req() -> Request:
        return create_api_request(req, auth=access_token_manager.get_token(conf, key=""))

    return common.execute_request(conf, build_req)


class StreamingReadFile(BaseStreamingReadFile):
    def __init__(self, conf: Config, path: str, size: int | None) -> None:
        if size is None:
            st = maybe_stat(conf, path)
            if st is None:
                raise FileNotFoundError(f"No such file or bucket: '{path}'")
            size = st.size
        super().__init__(conf=conf, path=path, size=size)

    def _request_chunk(
        self, streaming: bool, start: int, end: int | None = None
    ) -> "urllib3.BaseHTTPResponse":
        bucket, name = split_path(self._path)
        req = Request(
            url=build_url("/storage/v1/b/{bucket}/o/{name}", bucket=bucket, name=name),
            method="GET",
            params=dict(alt="media"),
            headers={"Range": common.calc_range(start=start, end=end)},
            success_codes=(206, 416),
            # if we are streaming the data, make
            # sure we don't preload it
            preload_content=not streaming,
        )
        return execute_api_request(self._conf, req)


class StreamingWriteFile(BaseStreamingWriteFile):
    def __init__(self, conf: Config, path: str, partial_writes_on_exc: bool) -> None:
        bucket, name = split_path(path)
        req = Request(
            url=build_url("/upload/storage/v1/b/{bucket}/o?uploadType=resumable", bucket=bucket),
            method="POST",
            data=dict(name=name),
            success_codes=(200, 400, 404),
        )
        resp = execute_api_request(conf, req)
        if resp.status in (400, 404):
            raise FileNotFoundError(f"No such file or bucket: '{path}'")
        self._upload_url = resp.headers["Location"]
        # https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
        assert conf.google_write_chunk_size % (256 * 1024) == 0
        super().__init__(
            conf=conf,
            chunk_size=conf.google_write_chunk_size,
            partial_writes_on_exc=partial_writes_on_exc,
        )

    def _upload_chunk(self, chunk: memoryview, finalize: bool) -> None:
        offset = self._offset

        if len(chunk) == 0 and finalize:
            self._upload_piece(offset, chunk, finalize)
            return

        start = 0
        while start < len(chunk):
            end = start + self._conf.google_write_chunk_size
            last_piece = end >= len(chunk)
            piece = chunk[start:end]
            self._upload_piece(offset, piece, finalize and last_piece)
            start = end
            offset += len(piece)

    def _upload_piece(self, offset: int, piece: memoryview, finalize: bool) -> None:
        start = offset
        end = offset + len(piece) - 1

        total_size = "*"
        if finalize:
            total_size = offset + len(piece)

        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Range": f"bytes {start}-{end}/{total_size}",
        }
        if len(piece) == 0 and finalize:
            # this is not mentioned in the docs but appears to be allowed
            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range
            headers["Content-Range"] = f"bytes */{total_size}"

        req = Request(
            url=self._upload_url,
            data=piece,
            headers=headers,
            method="PUT",
            success_codes=(200, 201) if finalize else (308,),
        )

        try:
            execute_api_request(self._conf, req)
        except RequestFailure as e:
            # https://cloud.google.com/storage/docs/resumable-uploads#practices
            if e.response_status in (404, 410):
                raise RestartableStreamingWriteFailure(
                    message=e.message,
                    request_string=e.request_string,
                    response_status=e.response_status,
                    error=e.error,
                    error_description=e.error_description,
                )
            else:
                raise


def maybe_stat(conf: Config, path: str) -> Stat | None:
    bucket, blob = split_path(path)
    if blob == "":
        return None
    req = Request(
        url=build_url("/storage/v1/b/{bucket}/o/{object}", bucket=bucket, object=blob),
        method="GET",
        success_codes=(200, 404),
    )
    resp = execute_api_request(conf, req)
    if resp.status != 200:
        return None
    return make_stat(json.loads(resp.data))


def remove(conf: Config, path: str) -> bool:
    bucket, blob = split_path(path)
    if blob == "":
        raise FileNotFoundError(f"The system cannot find the path specified: '{path}'")
    req = Request(
        url=build_url("/storage/v1/b/{bucket}/o/{object}", bucket=bucket, object=blob),
        method="DELETE",
        success_codes=(204, 404),
    )
    resp = execute_api_request(conf, req)
    return resp.status == 204


def maybe_update_md5(conf: Config, path: str, generation: str, hexdigest: str) -> bool:
    bucket, blob = split_path(path)
    req = Request(
        url=build_url("/storage/v1/b/{bucket}/o/{object}", bucket=bucket, object=blob),
        method="PATCH",
        params=dict(ifGenerationMatch=generation),
        # it looks like we can't set the underlying md5Hash, only the metadata fields
        data=dict(metadata={"md5": hexdigest}),
        success_codes=(200, 404, 412),
    )

    resp = execute_api_request(conf, req)
    return resp.status == 200


def _upload_part(conf: Config, path: str, start: int, size: int, dst: str) -> str:
    bucket, blob = split_path(dst)
    req = Request(
        url=build_url("/upload/storage/v1/b/{bucket}/o", bucket=bucket),
        method="POST",
        params=dict(uploadType="media", name=blob),
        data=FileBody(path, start=start, end=start + size),
        success_codes=(200,),
    )
    resp = execute_api_request(conf, req)
    metadata = json.loads(resp.data)
    return metadata["generation"]


def _delete_part(conf: Config, bucket: str, name: str) -> None:
    req = Request(
        url=build_url("/storage/v1/b/{bucket}/o/{object}", bucket=bucket, object=name),
        method="DELETE",
        success_codes=(204, 404),
    )
    execute_api_request(conf, req)


def parallel_upload(
    conf: Config, executor: concurrent.futures.Executor, src: str, dst: str, return_md5: bool
) -> str | None:
    with open(src, "rb") as f:
        md5_digest = common.block_md5(f)

    hexdigest = binascii.hexlify(md5_digest).decode("utf8")

    s = os.stat(src)
    if s.st_size == 0:
        # write an empty file, the normal upload requires at least one part
        with StreamingWriteFile(conf, dst, partial_writes_on_exc=True) as f:
            pass
        return hexdigest if return_md5 else None

    dstbucket, dstname = split_path(dst)
    source_objects = []
    object_names = []
    max_workers = getattr(executor, "_max_workers", os.cpu_count() or 1)
    part_size = max(math.ceil(s.st_size / max_workers), common.PARALLEL_COPY_MINIMUM_PART_SIZE)
    i = 0
    start = 0
    futures = []
    while start < s.st_size:
        suffix = f".part.{i}"
        future = executor.submit(
            _upload_part, conf, src, start, min(part_size, s.st_size - start), dst + suffix
        )
        futures.append(future)
        object_names.append(dstname + suffix)
        i += 1
        start += part_size
    for name, future in zip(object_names, futures):
        generation = future.result()
        source_objects.append(
            {
                "name": name,
                "generation": generation,
                "objectPreconditions": {"ifGenerationMatch": generation},
            }
        )

    req = Request(
        url=build_url(
            "/storage/v1/b/{destinationBucket}/o/{destinationObject}/compose",
            destinationBucket=dstbucket,
            destinationObject=dstname,
        ),
        method="POST",
        data={"sourceObjects": source_objects},
        success_codes=(200,),
    )
    resp = execute_api_request(conf, req)
    metadata = json.loads(resp.data)
    maybe_update_md5(conf, dst, metadata["generation"], hexdigest)

    # delete parts in parallel
    delete_futures = []
    for name in object_names:
        future = executor.submit(_delete_part, conf, dstbucket, name)
        delete_futures.append(future)
    for future in delete_futures:
        future.result()

    return hexdigest if return_md5 else None


def _create_page_iterator(
    conf: Config, url: str, method: str, params: Mapping[str, str]
) -> Iterator[dict[str, Any]]:
    p = dict(params).copy()

    while True:
        req = Request(url=url, method=method, params=p, success_codes=(200, 404))
        resp = execute_api_request(conf, req)
        if resp.status == 404:
            return
        result = json.loads(resp.data)
        yield result
        if "nextPageToken" not in result:
            break
        p["pageToken"] = result["nextPageToken"]


def list_blobs(conf: Config, path: str, delimiter: str | None = None) -> Iterator[DirEntry]:
    params = {}
    if delimiter is not None:
        params["delimiter"] = delimiter

    bucket, prefix = split_path(path)
    it = _create_page_iterator(
        conf=conf,
        url=build_url("/storage/v1/b/{bucket}/o", bucket=bucket),
        method="GET",
        params=dict(prefix=prefix, **params),
    )
    for result in it:
        yield from _get_entries(bucket, result)


def _get_entries(bucket: str, result: Mapping[str, Any]) -> Iterator[DirEntry]:
    if "prefixes" in result:
        for p in result["prefixes"]:
            path = combine_path(bucket, p)
            yield entry_from_dirpath(path)
    if "items" in result:
        for item in result["items"]:
            path = combine_path(bucket, item["name"])
            if item["name"].endswith("/"):
                yield entry_from_dirpath(path)
            else:
                yield entry_from_path_stat(path, make_stat(item))


def entry_from_dirpath(path: str) -> DirEntry:
    if path.endswith("/"):
        path = path[:-1]
    _, obj = split_path(path)
    name = obj.split("/")[-1]
    return DirEntry(name=name, path=path, is_dir=True, is_file=False, stat=None)


def entry_from_path_stat(path: str, stat: Stat) -> DirEntry:
    assert not path.endswith("/")
    _, obj = split_path(path)
    name = obj.split("/")[-1]
    return DirEntry(name=name, path=path, is_dir=False, is_file=True, stat=stat)


def get_url(conf: Config, path: str) -> tuple[str, float | None]:
    bucket, blob = split_path(path)
    return generate_signed_url(bucket, blob, expiration=MAX_EXPIRATION)


def set_mtime(conf: Config, path: str, mtime: float, version: str | None = None) -> bool:
    bucket, blob = split_path(path)
    params = None
    if version is not None:
        params = dict(ifGenerationMatch=version)
    req = Request(
        url=build_url("/storage/v1/b/{bucket}/o/{object}", bucket=bucket, object=blob),
        method="PATCH",
        params=params,
        data=dict(metadata={"blobfile-mtime": str(mtime)}),
        success_codes=(200, 404, 412),
    )
    resp = execute_api_request(conf, req)
    if resp.status == 404:
        raise FileNotFoundError(f"No such file: '{path}'")
    return resp.status == 200


def dirname(conf: Config, path: str) -> str:
    bucket, obj = split_path(path)
    obj = strip_slashes(obj)
    if "/" in obj:
        obj = "/".join(obj.split("/")[:-1])
        return combine_path(bucket, obj)
    else:
        return combine_path(bucket, "")[:-1]


def remote_copy(conf: Config, src: str, dst: str, return_md5: bool) -> str | None:
    srcbucket, srcname = split_path(src)
    dstbucket, dstname = split_path(dst)
    params = {}
    while True:
        req = Request(
            url=build_url(
                "/storage/v1/b/{sourceBucket}/o/{sourceObject}/rewriteTo/b/{destinationBucket}/o/{destinationObject}",
                sourceBucket=srcbucket,
                sourceObject=srcname,
                destinationBucket=dstbucket,
                destinationObject=dstname,
            ),
            method="POST",
            params=params,
            success_codes=(200, 404),
        )
        resp = execute_api_request(conf, req)
        if resp.status == 404:
            raise FileNotFoundError(f"Source file not found: '{src}'")
        result = json.loads(resp.data)
        if result["done"]:
            if return_md5:
                return get_md5(result["resource"])
            else:
                return None
        params["rewriteToken"] = result["rewriteToken"]


def join_paths(conf: Config, a: str, b: str) -> str:
    if not a.endswith("/"):
        a += "/"

    bucket, obj = split_path(a)
    obj = path_join(obj, b)
    if obj.startswith("/"):
        obj = obj[1:]
    return combine_path(bucket, obj)


access_token_manager = TokenManager(_get_access_token, "gcp")
