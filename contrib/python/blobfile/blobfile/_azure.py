import base64
import binascii
import calendar
import concurrent.futures
import datetime
import hashlib
import hmac
import json
import math
import os
import re
import time
import urllib.parse
from typing import Any, Iterator, Mapping, Optional, Sequence

import urllib3

from blobfile import _common as common
from blobfile import _xml as xml
from blobfile._common import (
    DEFAULT_RETRY_CODES,
    INVALID_HOSTNAME_STATUS,
    BaseStreamingReadFile,
    BaseStreamingWriteFile,
    ConcurrentWriteFailure,
    Config,
    DirEntry,
    Error,
    FileBody,
    Request,
    RequestFailure,
    Stat,
    TokenManager,
    VersionMismatch,
    path_join,
    rng,
    strip_slashes,
)

SHARED_KEY = "shared_key"
OAUTH_TOKEN = "oauth_token"
ANONYMOUS = "anonymous"

# it looks like azure signed urls cannot exceed the lifetime of the token used
# to create them, so don't keep the key around too long
SAS_TOKEN_EXPIRATION_SECONDS = 60 * 60
# these seem to be expired manually, but we don't currently detect that
SHARED_KEY_EXPIRATION_SECONDS = 24 * 60 * 60

# max 100MB https://docs.microsoft.com/en-us/rest/api/storageservices/put-block#remarks
# there is a preview version of the API that allows this to be 4000MiB
MAX_BLOCK_SIZE = 100_000_000

BLOCK_COUNT_LIMIT = 50_000

RESPONSE_HEADER_TO_REQUEST_HEADER = {
    "Cache-Control": "x-ms-blob-cache-control",
    "Content-Type": "x-ms-blob-content-type",
    "Content-MD5": "x-ms-blob-content-md5",
    "Content-Encoding": "x-ms-blob-content-encoding",
    "Content-Language": "x-ms-blob-content-language",
    "Content-Disposition": "x-ms-blob-content-disposition",
}


def _load_credentials() -> dict[str, Any]:
    # When AZURE_USE_IDENTITY=1, blobfile will use the azure-identity package to retrieve AAD access tokens
    if os.getenv("AZURE_USE_IDENTITY", "0") == "1":
        return {"_azure_auth": "azure-identity"}

    # https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/identity/azure-identity#environment-variables
    # AZURE_STORAGE_KEY seems to be the environment variable mentioned by the az cli
    # AZURE_STORAGE_ACCOUNT_KEY is mentioned elsewhere on the internet
    for varname in ["AZURE_STORAGE_KEY", "AZURE_STORAGE_ACCOUNT_KEY"]:
        if varname in os.environ:
            account = {}
            if "AZURE_STORAGE_ACCOUNT" in os.environ:
                account["account"] = os.environ["AZURE_STORAGE_ACCOUNT"]
            return {"_azure_auth": "sakey", "storage_account_key": os.environ[varname], **account}

    if "AZURE_STORAGE_CONNECTION_STRING" in os.environ:
        connection_data = {}
        # technically this should be parsed according to the rules in
        # https://www.connectionstrings.com/formating-rules-for-connection-strings/
        for part in os.environ["AZURE_STORAGE_CONNECTION_STRING"].split(";"):
            key, _, val = part.partition("=")
            connection_data[key.lower()] = val
        return {
            "_azure_auth": "sakey",
            "account": connection_data["accountname"],
            "storage_account_key": connection_data["accountkey"],
        }

    if "AZURE_APPLICATION_CREDENTIALS" in os.environ:
        creds_path = os.environ["AZURE_APPLICATION_CREDENTIALS"]
        if not os.path.exists(creds_path):
            raise Error(
                f"Credentials not found at '{creds_path}' specified by environment variable 'AZURE_APPLICATION_CREDENTIALS'"
            )
        with open(creds_path) as f:
            creds = json.load(f)
            return {
                "_azure_auth": "svcact",
                "client_id": creds["appId"],
                "client_secret": creds["password"],
                "tenant_id": creds["tenant"],
            }

    if "AZURE_CLIENT_ID" in os.environ:
        return {
            "_azure_auth": "svcact",
            "client_id": os.environ["AZURE_CLIENT_ID"],
            "client_secret": os.environ["AZURE_CLIENT_SECRET"],
            "tenant_id": os.environ["AZURE_TENANT_ID"],
        }

    # look for a refresh token in the az command line credentials
    # we could also try to use any found access tokens
    msal_tokens_path = os.path.expanduser("~/.azure/msal_token_cache.json")
    if os.path.exists(msal_tokens_path):
        with open(msal_tokens_path) as f:
            tokens = json.load(f)
            for token in tokens.get("RefreshToken", {}).values():
                if token["credential_type"] != "RefreshToken":
                    continue
                return {"_azure_auth": "refresh", "refresh_token": token["secret"]}

    access_tokens_path = os.path.expanduser("~/.azure/accessTokens.json")
    if os.path.exists(access_tokens_path):
        with open(access_tokens_path) as f:
            tokens = json.load(f)
            best_token = None
            for token in tokens:
                if "refreshToken" not in token:
                    continue
                creds = {"_azure_auth": "refresh", "refresh_token": token["refreshToken"]}
                if best_token is None:
                    best_token = creds
                else:
                    # expiresOn may be missing for tokens from service principals
                    if token.get("expiresOn", "") > best_token.get("expiresOn", ""):
                        best_token = creds
            if best_token is not None:
                return best_token

    return {}


def load_subscription_ids() -> list[str]:
    """
    Return a list of subscription ids from the local azure profile
    the default subscription will appear first in the list
    """
    default_profile_path = os.path.expanduser("~/.azure/azureProfile.json")
    if not os.path.exists(default_profile_path):
        return []

    with open(default_profile_path, "rb") as f:
        # this file has a UTF-8 BOM
        profile = json.loads(f.read().decode("utf-8-sig"))

    subscriptions = profile.get("subscriptions", [])

    def key_fn(x: Mapping[str, Any]) -> bool:
        return x["isDefault"]

    subscriptions.sort(key=key_fn, reverse=True)
    return [sub["id"] for sub in subscriptions]


def build_url(account: str, template: str, **data: str) -> str:
    return common.build_url(f"https://{account}.blob.core.windows.net", template, **data)


def _create_access_token_request(
    creds: Mapping[str, str], scope: str, success_codes: Sequence[int] = (200,)
) -> Request:
    if creds["_azure_auth"] == "refresh":
        # https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-auth-code-flow#refresh-the-access-token
        data = {
            "grant_type": "refresh_token",
            "refresh_token": creds["refresh_token"],
            "scope": scope,
        }
        tenant_id = "common"
    elif creds["_azure_auth"] == "svcact":
        # https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow#first-case-access-token-request-with-a-shared-secret
        # https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-azure-active-directory#use-oauth-access-tokens-for-authentication
        # The following Azure CLI commands will create an AzureAD service principal with sufficient access to use Azure's `client_credentials` grant type:
        # az ad sp create-for-rbac --name <name>
        # az account list
        # az role assignment create --role "Storage Blob Data Contributor" --assignee <app_id> --scope "/subscriptions/<subscription_id>/resourceGroups/<resource_group_name>/providers/Microsoft.Storage/storageAccounts/<storage_account_name>"
        data = {
            "grant_type": "client_credentials",
            "client_id": creds["client_id"],
            "client_secret": creds["client_secret"],
            "scope": scope,
        }
        tenant_id = creds["tenant_id"]
    else:
        raise AssertionError
    return Request(
        url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=urllib.parse.urlencode(data).encode("utf8"),
        success_codes=success_codes,
    )


def create_api_request(req: Request, auth: tuple[str, str]) -> Request:
    if req.headers is None:
        headers = {}
    else:
        headers = dict(req.headers).copy()

    if req.params is None:
        params = {}
    else:
        params = dict(req.params).copy()

    # https://docs.microsoft.com/en-us/rest/api/storageservices/previous-azure-storage-service-versions
    headers["x-ms-version"] = "2019-02-02"
    headers["x-ms-date"] = datetime.datetime.now(tz=datetime.timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )
    data = req.data
    if data is not None and isinstance(data, dict):
        data = xml.unparse(data)

    result = Request(
        method=req.method,
        url=req.url,
        params=params,
        headers=headers,
        data=data,
        preload_content=req.preload_content,
        success_codes=tuple(req.success_codes),
        retry_codes=tuple(req.retry_codes),
    )

    kind, token = auth
    if kind == SHARED_KEY:
        # make sure we are signing the request that has the ms headers added already
        headers["Authorization"] = sign_with_shared_key(result, token)
    elif kind == OAUTH_TOKEN:
        headers["Authorization"] = f"Bearer {token}"
    elif kind == ANONYMOUS:
        pass
    return result


def generate_signed_url(key: Mapping[str, str], url: str) -> tuple[str, float]:
    # https://docs.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature
    # https://docs.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas
    # https://docs.microsoft.com/en-us/rest/api/storageservices/service-sas-examples
    params = {
        "st": key["SignedStart"],
        "se": key["SignedExpiry"],
        "sks": key["SignedService"],
        "skt": key["SignedStart"],
        "ske": key["SignedExpiry"],
        "sktid": key["SignedTid"],
        "skoid": key["SignedOid"],
        # signed key version (param name not mentioned in docs)
        "skv": key["SignedVersion"],
        "sv": "2018-11-09",  # signed version
        "sr": "b",  # signed resource
        "sp": "r",  # signed permissions
        "sip": "",  # signed ip
        "si": "",  # signed identifier
        "spr": "https,http",  # signed http protocol
        "rscc": "",  # Cache-Control header
        "rscd": "",  # Content-Disposition header
        "rsce": "",  # Content-Encoding header
        "rscl": "",  # Content-Language header
        "rsct": "",  # Content-Type header
    }
    u = urllib.parse.urlparse(url)
    storage_account = u.netloc.split(".")[0]
    canonicalized_resource = urllib.parse.unquote(f"/blob/{storage_account}/{u.path[1:]}")
    parts_to_sign = (
        params["sp"],
        params["st"],
        params["se"],
        canonicalized_resource,
        params["skoid"],
        params["sktid"],
        params["skt"],
        params["ske"],
        params["sks"],
        params["skv"],
        params["sip"],
        params["spr"],
        params["sv"],
        params["sr"],
        params["rscc"],
        params["rscd"],
        params["rsce"],
        params["rscl"],
        params["rsct"],
        # this is documented on a different page
        # https://docs.microsoft.com/en-us/rest/api/storageservices/create-service-sas#specifying-the-signed-identifier
        params["si"],
    )
    string_to_sign = "\n".join(parts_to_sign)
    params["sig"] = base64.b64encode(
        hmac.digest(base64.b64decode(key["Value"]), string_to_sign.encode("utf8"), "sha256")
    ).decode("utf8")
    query = urllib.parse.urlencode({k: v for k, v in params.items() if v != ""})
    # convert to a utc struct_time by replacing the timezone
    ts = time.strptime(key["SignedExpiry"].replace("Z", "GMT"), "%Y-%m-%dT%H:%M:%S%Z")
    t = calendar.timegm(ts)
    return url + "?" + query, t


def split_path(path: str) -> tuple[str, str, str]:
    if path.startswith("az://"):
        return split_az_path(path)
    elif path.startswith("https://"):
        return split_https_path(path)
    else:
        raise Error(f"Invalid path: '{path}'")


def split_az_path(path: str) -> tuple[str, str, str]:
    parts = path[len("az://") :].split("/")
    if len(parts) < 2:
        raise Error(f"Invalid path: '{path}'")
    account = parts[0]
    container = parts[1]
    if container == "":
        raise Error(f"Invalid path: '{path}'")
    obj = "/".join(parts[2:])
    return account, container, obj


def split_https_path(path: str) -> tuple[str, str, str]:
    parts = path[len("https://") :].split("/")
    if len(parts) < 2:
        raise Error(f"Invalid path: '{path}'")
    hostname = parts[0]
    container = parts[1]
    if not hostname.endswith(".blob.core.windows.net") or container == "":
        raise Error(f"Invalid path: '{path}'")
    obj = "/".join(parts[2:])
    account = hostname.split(".")[0]
    return account, container, obj


def combine_https_path(account: str, container: str, obj: str) -> str:
    return f"https://{account}.blob.core.windows.net/{container}/{obj}"


def combine_az_path(account: str, container: str, obj: str) -> str:
    return f"az://{account}/{container}/{obj}"


def combine_path(conf: Config, account: str, container: str, obj: str) -> str:
    if conf.output_az_paths:
        return combine_az_path(account, container, obj)
    else:
        return combine_https_path(account, container, obj)


def mkdirfile(conf: Config, path: str) -> None:
    if not path.endswith("/"):
        path += "/"
    account, container, blob = split_path(path)
    req = Request(
        url=build_url(account, "/{container}/{blob}", container=container, blob=blob),
        method="PUT",
        headers={"x-ms-blob-type": "BlockBlob"},
        success_codes=(201, 400),
    )
    resp = execute_api_request(conf, req)
    if resp.status == 400:
        raise Error(f"Unable to create directory, account/container does not exist: '{path}'")


def sign_with_shared_key(req: Request, key: str) -> str:
    # https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key
    params_to_sign = []
    if req.params is not None:
        for name, value in req.params.items():
            canonical_name = name.lower()
            params_to_sign.append(f"{canonical_name}:{value}")

    u = urllib.parse.urlparse(req.url)
    storage_account = u.netloc.split(".")[0]
    canonical_url = f"/{storage_account}/{u.path[1:]}"
    canonicalized_resource = "\n".join([canonical_url] + sorted(params_to_sign))

    if req.headers is None:
        headers = {}
    else:
        headers = dict(req.headers)

    headers_to_sign = []
    for name, value in headers.items():
        canonical_name = name.lower()
        canonical_value = re.sub(r"\s+", " ", value).strip()
        if canonical_name.startswith("x-ms-"):
            headers_to_sign.append(f"{canonical_name}:{canonical_value}")
    canonicalized_headers = "\n".join(sorted(headers_to_sign))

    content_length = headers.get("Content-Length", "")
    if req.data is not None:
        content_length = str(len(req.data))

    parts_to_sign = [
        req.method,
        headers.get("Content-Encoding", ""),
        headers.get("Content-Language", ""),
        content_length,
        headers.get("Content-MD5", ""),
        headers.get("Content-Type", ""),
        headers.get("Date", ""),
        headers.get("If-Modified-Since", ""),
        headers.get("If-Match", ""),
        headers.get("If-None-Match", ""),
        headers.get("If-Unmodified-Since", ""),
        headers.get("Range", ""),
        canonicalized_headers,
        canonicalized_resource,
    ]
    string_to_sign = "\n".join(parts_to_sign)

    signature = base64.b64encode(
        hmac.digest(base64.b64decode(key), string_to_sign.encode("utf8"), "sha256")
    ).decode("utf8")

    return f"SharedKey {storage_account}:{signature}"


def _get_md5(metadata: Mapping[str, Any]) -> str | None:
    if "Content-MD5" in metadata:
        b64_encoded = metadata["Content-MD5"]
        if b64_encoded is None:
            return None
        return base64.b64decode(b64_encoded).hex()
    else:
        return None


_MONTH_NAME_TO_INDEX = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}


def _parse_timestamp(text: str) -> float:
    # this function seems faster than using strptime
    # text="Sun, 27 Sep 2009 18:41:57 GMT"
    #       0    1  2   3    4        5
    p = text.split()
    date_string = f"{p[3]}-{_MONTH_NAME_TO_INDEX[p[2]]}-{p[1]}T{p[4]}+00:00"
    dt = datetime.datetime.fromisoformat(date_string)
    return dt.timestamp()


def make_stat(item: Mapping[str, str]) -> Stat:
    if "Creation-Time" in item:
        raw_ctime = item["Creation-Time"]
    else:
        raw_ctime = item["x-ms-creation-time"]
    if "x-ms-meta-blobfilemtime" in item:
        mtime = float(item["x-ms-meta-blobfilemtime"])
    else:
        mtime = _parse_timestamp(item["Last-Modified"])
    return Stat(
        size=int(item["Content-Length"]),
        mtime=mtime,
        ctime=_parse_timestamp(raw_ctime),
        md5=_get_md5(item),
        version=item["Etag"],
    )


def _can_access_container(
    conf: Config,
    account: str,
    container: str,
    auth: tuple[str, str],
    out_failures: list[RequestFailure],
) -> bool:
    # Skip this check and hope the credentials work if we're blind writing.
    if conf.use_blind_writes:
        return True
    # https://myaccount.blob.core.windows.net/mycontainer?restype=container&comp=list
    success_codes = [200, 403, 404, INVALID_HOSTNAME_STATUS]
    if auth[0] == ANONYMOUS:
        # some containers can produce a 409 error "PublicAccessNotPermitted" when accessed with an anonymous account
        success_codes.append(409)

    def build_req() -> Request:
        req = Request(
            method="GET",
            url=build_url(account, "/{container}", container=container),
            params={"restype": "container", "comp": "list", "maxresults": "1"},
            success_codes=success_codes,
        )
        return create_api_request(req, auth=auth)

    resp = common.execute_request(conf, build_req)
    # technically INVALID_HOSTNAME_STATUS means we can't access the account because it
    # doesn't exist, but to be consistent with how we treat this error elsewhere we
    # ignore it here
    if resp.status == INVALID_HOSTNAME_STATUS:
        return True
    # anonymous requests will for some reason get a 404 when they should get a 403
    # so treat a 404 from anon requests as a 403
    if resp.status == 404 and auth[0] == ANONYMOUS:
        out_failures.append(
            RequestFailure.create_from_request_response(
                "Could not access container", build_req(), resp
            )
        )
        return False
    # if the container list succeeds or the container doesn't exist, return success
    if resp.status in (200, 404):
        return True
    else:
        out_failures.append(
            RequestFailure.create_from_request_response(
                "Could not access container", build_req(), resp
            )
        )
        return False


def _get_storage_account_id(
    conf: Config, subscription_id: str, account: str, auth: tuple[str, str]
) -> str | None:
    # get a list of storage accounts
    url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Storage/storageAccounts"
    params = {"api-version": "2019-04-01"}
    while True:

        def build_req() -> Request:
            req = Request(method="GET", url=url, params=params, success_codes=(200, 401, 403))
            return create_api_request(req, auth=auth)

        resp = common.execute_request(conf, build_req)
        if resp.status in (401, 403):
            # we aren't allowed to query this for this subscription, skip it
            return None

        out = json.loads(resp.data)
        # check if we found the storage account we are looking for
        for obj in out["value"]:
            if obj["name"] == account:
                return obj["id"]

        if "nextLink" not in out:
            return None

        url = out["nextLink"]
        params = None  # the url will already have params on it


def _get_subscription_ids(conf: Config, auth: tuple[str, str]) -> list[str]:
    url = "https://management.azure.com/subscriptions"
    params = {"api-version": "2020-01-01"}
    result = []
    while True:

        def build_req() -> Request:
            req = Request(method="GET", url=url, params=params)
            return create_api_request(req, auth=auth)

        resp = common.execute_request(conf, build_req)
        data = json.loads(resp.data)
        result.extend([item["subscriptionId"] for item in data["value"]])
        if "nextLink" not in data:
            return result
        url = data["nextLink"]
        params = None  # the url will already have params on it


def _get_storage_account_key(
    conf: Config,
    account: str,
    container: str,
    creds: Mapping[str, Any],
    out_failures: list[RequestFailure],
) -> tuple[Any, float] | None:
    # azure resource manager has very low limits on number of requests, so we have
    # to be careful to avoid extra requests here
    # https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/azure-subscription-service-limits#storage-resource-provider-limits

    # in general, this code path should be avoided by using a service principal and
    # giving it access to the bucket

    # get an access token for the management service
    def build_req_access_token() -> Request:
        return _create_access_token_request(
            creds=creds, scope="https://management.azure.com/.default"
        )

    resp = common.execute_request(conf, build_req_access_token)
    result = json.loads(resp.data)
    auth = (OAUTH_TOKEN, result["access_token"])

    # attempt to use list of subscriptions from the azure cli tool
    stored_subscription_ids = load_subscription_ids()

    storage_account_id = None
    for subscription_id in stored_subscription_ids:
        storage_account_id = _get_storage_account_id(conf, subscription_id, account, auth)
        if storage_account_id is not None:
            break
    else:
        # if we didn't find the storage account we are looking for, check to see if there
        # are any subscriptions that we did not query
        subscription_ids = _get_subscription_ids(conf, auth)
        unchecked_subscription_ids = [
            id for id in subscription_ids if id not in stored_subscription_ids
        ]

        for subscription_id in unchecked_subscription_ids:
            storage_account_id = _get_storage_account_id(conf, subscription_id, account, auth)
            if storage_account_id is not None:
                break
        else:
            # we failed to find the storage account, give up
            return None

    def build_req_list_keys() -> Request:
        req = Request(
            method="POST",
            url=f"https://management.azure.com{storage_account_id}/listKeys",
            params={"api-version": "2019-04-01"},
        )
        return create_api_request(req, auth=auth)

    resp = common.execute_request(conf, build_req_list_keys)
    result = json.loads(resp.data)
    for key in result["keys"]:
        if key["permissions"] == "FULL":
            storage_key_auth = (SHARED_KEY, key["value"])
            if _can_access_container(
                conf, account, container, storage_key_auth, out_failures=out_failures
            ):
                return storage_key_auth
            else:
                raise Error(
                    f"Found storage account key, but it was unable to access storage account: '{account}' and container: '{container}'"
                )
    raise Error(f"Storage account was found, but storage account keys were missing: '{account}'")


def _get_access_token(conf: Config, key: Any) -> tuple[Any, float]:
    account, container = key
    now = time.time()
    creds = _load_credentials()
    azure_auth = creds.get("_azure_auth")
    access_failures: list[RequestFailure] = []
    if azure_auth == "sakey":
        if "account" in creds:
            if creds["account"] != account:
                raise Error(
                    f"Provided storage account key for account '{creds['account']}' via environment variables, "
                    f"but needed credentials for account '{account}'"
                )
        auth = (SHARED_KEY, creds["storage_account_key"])
        if _can_access_container(conf, account, container, auth, out_failures=access_failures):
            return (auth, now + SHARED_KEY_EXPIRATION_SECONDS)
    elif azure_auth == "refresh":
        # we have a refresh token, convert it into an access token for this account
        def build_req() -> Request:
            return _create_access_token_request(
                creds=creds,
                scope=f"https://{account}.blob.core.windows.net/.default",
                success_codes=(200, 400),
            )

        resp = common.execute_request(conf, build_req)
        result = json.loads(resp.data)
        if resp.status == 400:
            if (
                (
                    result["error"] == "invalid_grant"
                    and "AADSTS700082" in result["error_description"]
                )
                or (
                    result["error"] == "interaction_required"
                    and "AADSTS50078" in result["error_description"]
                )
                or (
                    result["error"] == "interaction_required"
                    and "AADSTS50076" in result["error_description"]
                )
            ):
                raise Error(
                    "Your refresh token is no longer valid, please run `az login` to get a new one"
                )
            else:
                raise Error(
                    f"Encountered an error when requesting an access token: `{result['error']}: {result['error_description']}`.  You can attempt to fix this by re-running `az login`."
                )

        auth = (OAUTH_TOKEN, result["access_token"])

        # for some azure accounts this access token does not work, check if it works
        if _can_access_container(conf, account, container, auth, out_failures=access_failures):
            return (auth, now + float(result["expires_in"]))

        if conf.use_azure_storage_account_key_fallback:
            # fall back to getting the storage keys
            storage_account_key_auth = _get_storage_account_key(
                conf=conf,
                account=account,
                container=container,
                creds=creds,
                out_failures=access_failures,
            )
            if storage_account_key_auth is not None:
                return (storage_account_key_auth, now + SHARED_KEY_EXPIRATION_SECONDS)
    elif azure_auth == "svcact":
        # we have a service principal, get an oauth token
        def build_req() -> Request:
            return _create_access_token_request(
                creds=creds, scope="https://storage.azure.com/.default"
            )

        resp = common.execute_request(conf, build_req)
        result = json.loads(resp.data)
        auth = (OAUTH_TOKEN, result["access_token"])
        if _can_access_container(conf, account, container, auth, out_failures=access_failures):
            return (auth, now + float(result["expires_in"]))

        if conf.use_azure_storage_account_key_fallback:
            # fall back to getting the storage keys
            storage_account_key_auth = _get_storage_account_key(
                conf=conf,
                account=account,
                container=container,
                creds=creds,
                out_failures=access_failures,
            )
            if storage_account_key_auth is not None:
                return (storage_account_key_auth, now + SHARED_KEY_EXPIRATION_SECONDS)

    # If opted into using azure-identity, use DefaultAzureCredential to get a token
    # This enables the use of Managed Identity, Workload Identity, and other auth methods not implemented here
    elif azure_auth == "azure-identity":
        try:
            from azure.identity import DefaultAzureCredential  # pyright: ignore
        except ImportError:
            raise RuntimeError(
                "When setting AZURE_USE_IDENTITY=1, you must also install the azure-identity package"
            )

        with DefaultAzureCredential() as cred:
            token = cred.get_token("https://storage.azure.com/.default")
        auth = (OAUTH_TOKEN, token.token)
        if _can_access_container(conf, account, container, auth, out_failures=access_failures):
            return (auth, token.expires_on)

        if conf.use_azure_storage_account_key_fallback:
            # fall back to getting the storage keys
            storage_account_key_auth = _get_storage_account_key(
                conf=conf,
                account=account,
                container=container,
                creds=creds,
                out_failures=access_failures,
            )
            if storage_account_key_auth is not None:
                return (storage_account_key_auth, now + SHARED_KEY_EXPIRATION_SECONDS)

    # oddly, it seems that if you request a public container with a valid azure account, you cannot list the bucket
    # but if you list it with no account, that works fine
    anonymous_auth = (ANONYMOUS, "")
    if _can_access_container(
        conf, account, container, anonymous_auth, out_failures=access_failures
    ):
        return (anonymous_auth, float("inf"))

    msg = f"Could not find any credentials that grant access to storage account: '{account}' and container: '{container}'"
    if len(access_failures) > 0:
        for resp_failure in access_failures:
            msg += f"\n    Access Failure: {resp_failure}"
    if len(creds) == 0:
        msg += """

No Azure credentials were found.  If the container is not marked as public, please do one of the following:

* Log in with 'az login', blobfile will use your default credentials to lookup your storage account key
* Set the environment variable 'AZURE_STORAGE_KEY' to your storage account key which you can find by following this guide: https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage
* Create an account with 'az ad sp create-for-rbac --name <name>' and set the 'AZURE_APPLICATION_CREDENTIALS' environment variable to the path of the output from that command or individually set the 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET', and 'AZURE_TENANT_ID' environment variables"""
    raise Error(msg)


def _get_sas_token(conf: Config, key: Any) -> tuple[Any, float]:
    auth = access_token_manager.get_token(conf, key=key)
    if auth[0] == ANONYMOUS:
        # for public containers, use None as the token so that this will be cached
        # and we can tell when we don't have a real SAS token for a container
        return (None, time.time() + SAS_TOKEN_EXPIRATION_SECONDS)

    account, container = key

    def build_req() -> Request:
        # https://docs.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        start = (now + datetime.timedelta(hours=-1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        expiry = (now + datetime.timedelta(days=6)).strftime("%Y-%m-%dT%H:%M:%SZ")
        req = Request(
            url=f"https://{account}.blob.core.windows.net/",
            method="POST",
            params=dict(restype="service", comp="userdelegationkey"),
            data={"KeyInfo": {"Start": start, "Expiry": expiry}},
            success_codes=(200, 403),
        )
        auth = access_token_manager.get_token(conf, key=key)
        if auth[0] != OAUTH_TOKEN:
            raise Error(
                "Only OAuth tokens can be used to get SAS tokens. You should set the Storage "
                "Blob Data Reader or Storage Blob Data Contributor IAM role. You can run "
                f"`az storage blob list --auth-mode login --account-name {account} --container {container}` "
                "to confirm that the missing role is the issue."
            )
        return create_api_request(req, auth=auth)

    resp = common.execute_request(conf, build_req)
    if resp.status == 403:
        raise Error(
            f"You do not have permission to generate an SAS token for account {account}. "
            "Try setting the Storage Blob Delegator or Storage Blob Data Contributor IAM role "
            "at the account level."
        )
    out = xml.parse(resp.data)
    t = time.time() + SAS_TOKEN_EXPIRATION_SECONDS
    return out["UserDelegationKey"], t


def execute_api_request(conf: Config, req: Request) -> "urllib3.BaseHTTPResponse":
    u = urllib.parse.urlparse(req.url)
    account = u.netloc.split(".")[0]
    path_parts = u.path.split("/")
    if len(path_parts) < 2:
        raise Error("missing container from path")
    container = u.path.split("/")[1]

    def build_req() -> Request:
        return create_api_request(
            req, auth=access_token_manager.get_token(conf, key=(account, container))
        )

    return common.execute_request(conf, build_req)


def _block_index_to_block_id(index: int, upload_id: int) -> str:
    assert index < 2**17
    id_plus_index = (upload_id << 17) + index
    assert id_plus_index < 2**64
    return base64.b64encode(id_plus_index.to_bytes(8, byteorder="big")).decode("utf8")


def _clear_uncommitted_blocks(
    conf: Config, url: str, metadata: Mapping[str, str]
) -> Optional["urllib3.BaseHTTPResponse"]:
    # to avoid leaking uncommitted blocks, we can do a Put Block List with
    # all the existing blocks for a file
    # this will change the last-modified timestamp and the etag
    req = Request(url=url, params=dict(comp="blocklist"), method="GET", success_codes=(200, 404))
    resp = execute_api_request(conf, req)
    if resp.status != 200:
        return None

    result = xml.parse(resp.data, repeated_tags={"Block"})
    if result["BlockList"]["CommittedBlocks"] is None:
        return None

    blocks = result["BlockList"]["CommittedBlocks"]["Block"]
    body = {"BlockList": {"Latest": [b["Name"] for b in blocks]}}
    # make sure to preserve metadata for the file
    headers: dict[str, str] = {k: v for k, v in metadata.items() if k.startswith("x-ms-meta-")}
    for src, dst in RESPONSE_HEADER_TO_REQUEST_HEADER.items():
        if src in metadata:
            headers[dst] = metadata[src]
    req = Request(
        url=url,
        method="PUT",
        params=dict(comp="blocklist"),
        headers={**headers, "If-Match": metadata["etag"]},
        data=body,
        success_codes=(201, 404, 412),
    )

    return execute_api_request(conf, req)


def _finalize_blob(
    conf: Config,
    path: str,
    url: str,
    block_ids: list[str],
    md5_digest: bytes | None,
    version: str | None,
) -> "urllib3.BaseHTTPResponse":
    body = {"BlockList": {"Latest": block_ids}}
    headers = {}
    if md5_digest is not None:
        # azure does not calculate md5s for us, we have to do that manually
        # https://web.archive.org/web/20190118183153/https://blogs.msdn.microsoft.com/windowsazurestorage/2011/02/17/windows-azure-blob-md5-overview/
        headers["x-ms-blob-content-md5"] = base64.b64encode(md5_digest).decode("utf8")

    if version is not None:
        headers["If-Match"] = version
    req = Request(
        url=url,
        method="PUT",
        headers=headers,
        params=dict(comp="blocklist"),
        data=body,
        success_codes=(201, 400, 404, 412, INVALID_HOSTNAME_STATUS),
    )
    resp = execute_api_request(conf, req)
    if resp.status == 400:
        result = xml.parse(resp.data)
        if result["Error"]["Code"] == "InvalidBlockList":
            # the most likely way this could happen is if the file was deleted while
            # we were uploading, so assume that is what happened
            # this could be interpreted as a sort of RestartableStreamingWriteFailure but
            # that could result in two processes fighting while uploading the file
            raise ConcurrentWriteFailure.create_from_request_response(
                f"Invalid block list, most likely a concurrent writer wrote to the same path: `{path}`",
                request=req,
                response=resp,
            )
        else:
            raise RequestFailure.create_from_request_response(
                message=f"unexpected status {resp.status}", request=req, response=resp
            )
    elif resp.status == 404 or resp.status == INVALID_HOSTNAME_STATUS:
        # this can occur when using parallel upload
        raise FileNotFoundError(f"No such file or directory: '{path}'")
    elif resp.status == 412:
        if resp.headers["x-ms-error-code"] != "ConditionNotMet":
            raise RequestFailure.create_from_request_response(
                message=f"unexpected status {resp.status}", request=req, response=resp
            )
        else:
            raise VersionMismatch.create_from_request_response(
                message="etag mismatch", request=req, response=resp
            )
    return resp


def isdir(conf: Config, path: str) -> bool:
    """
    Return true if a path is an existing directory
    """
    if not path.endswith("/"):
        path += "/"
    account, container, blob = split_path(path)
    if blob == "":
        req = Request(
            url=build_url(account, "/{container}", container=container, blob=blob),
            method="GET",
            params=dict(restype="container"),
            success_codes=(200, 404, INVALID_HOSTNAME_STATUS),
        )
        resp = execute_api_request(conf, req)
        return resp.status == 200
    else:
        # even though we're only interested in having one result, we still need to make an
        # iterator. as it happens, azure is perfectly willing to return an empty first page.
        it = create_page_iterator(
            conf,
            url=build_url(account, "/{container}", container=container),
            method="GET",
            params=dict(
                comp="list", restype="container", prefix=blob, delimiter="/", maxresults="1"
            ),
        )
        for result in it:
            if result["Blobs"] is not None:
                return "BlobPrefix" in result["Blobs"] or "Blob" in result["Blobs"]
        return False


def create_page_iterator(
    conf: Config,
    url: str,
    method: str,
    data: Mapping[str, str] | None = None,
    params: Mapping[str, str] | None = None,
) -> Iterator[dict[str, Any]]:
    if params is None:
        p = {}
    else:
        p = dict(params).copy()
    if data is None:
        d = None
    else:
        d = dict(data).copy()
    while True:
        req = Request(
            url=url,
            method=method,
            params=p,
            data=d,
            success_codes=(200, 404, INVALID_HOSTNAME_STATUS),
        )
        resp = execute_api_request(conf, req)
        if resp.status in (404, INVALID_HOSTNAME_STATUS):
            return
        result = xml.parse(resp.data, repeated_tags={"BlobPrefix", "Blob"})["EnumerationResults"]
        yield result
        if result["NextMarker"] is None:
            break
        p["marker"] = result["NextMarker"]


class StreamingReadFile(BaseStreamingReadFile):
    def __init__(self, conf: Config, path: str, size: int | None, version: str | None) -> None:
        self._version: str | None = version
        if size is None:
            st = maybe_stat(conf, path, version=version)
            if st is None:
                raise FileNotFoundError(f"No such file or directory: '{path}'")
            size = st.size
            self._version = st.version
        super().__init__(conf=conf, path=path, size=size)

    def _request_chunk(
        self, streaming: bool, start: int, end: int | None = None
    ) -> "urllib3.BaseHTTPResponse":
        account, container, blob = split_path(self._path)
        headers = {"Range": common.calc_range(start=start, end=end)}
        if self._version is not None:
            headers["If-Match"] = self._version
        req = Request(
            url=build_url(account, "/{container}/{blob}", container=container, blob=blob),
            method="GET",
            headers=headers,
            success_codes=(206, 416),
            # if we are streaming the data, make
            # sure we don't preload it
            preload_content=not streaming,
        )
        resp = execute_api_request(self._conf, req)
        self._version = self._version or resp.headers.get("ETag")
        return resp


class StreamingWriteFile(BaseStreamingWriteFile):
    def __init__(
        self, conf: Config, path: str, version: str | None, partial_writes_on_exc: bool
    ) -> None:
        self._path = path
        account, container, blob = split_path(path)
        self._url = build_url(account, "/{container}/{blob}", container=container, blob=blob)
        self._upload_id = rng.randint(0, 2**47 - 1)
        self._block_index = 0
        self._version: str | None = version  # for azure, this is an etag
        self._md5 = hashlib.md5()
        super().__init__(
            conf=conf,
            chunk_size=conf.azure_write_chunk_size,
            partial_writes_on_exc=partial_writes_on_exc,
        )
        if not conf.use_blind_writes:
            self._prepare_write()

    def _prepare_write(self) -> None:
        # block blobs let you upload up to 100,000 "uncommitted" blocks with user-chosen block ids
        # using the "Put Block" call
        # you may then call "Put Block List" with up to 50,000 block ids of the blocks you
        # want to be in the blob (50,000 is the max blocks per blob)
        # all unused uncommitted blocks will be deleted
        # uncommitted blocks also expire after a week if they are not committed
        #
        # since we use block blobs, there are a few ways we could implement this streaming write file
        #
        # method 1:
        #   upload the first chunk of the file as block id "0", the second as block id "1" etc
        #   when we are done writing the file, we call "Put Block List" using range(num_blocks) as
        #   the block ids
        #
        #   this has the advantage that if our program crashes, the same block ids will be reused
        #   for the next upload and so we'll never get more than 50,000 uncommitted blocks
        #
        #   in general, azure does not seem to support concurrent writers except maybe
        #   for writing small files (GCS does to a limited extent through resumable upload sessions)
        #
        #   with method 1, if you have two writers:
        #
        #       writer 0: write block id "0"
        #       writer 1: write block id "0"
        #       writer 1: crash
        #       writer 0: write block id "1"
        #       writer 0: put block list ["0", "1"]
        #
        #   then you will end up with block "0" from writer 1 and block "1" from writer 0, which means
        #   your file will be corrupted
        #
        #   this appears to be the method used by the azure python SDK
        #
        # method 2:
        #   generate a random session id
        #   upload the first chunk of the file as block id "<session id>-0",
        #       the second block as "<session id>-1" etc
        #   when we are done writing the file, call "Put Block List" using
        #       [f"<session id>-{i}" for i in range(num_blocks)] as the block list
        #
        #   this has the advantage that we should not observe data corruption from concurrent writers
        #       assuming that the session ids are unique, although whichever writer finishes first will
        #       win, because calling "Put Block List" will delete all uncommitted blocks
        #
        #   this has the disadvantage that we can end up hitting the uncommitted block limit
        #       1) with 100,000 concurrent writers, each one would write the first block, then all
        #           would immediately hit the block limit and get 409 errors
        #       2) with a single writer that crashes every time it writes the second block, it would
        #           retry 100,000 times, then be unable to continue due to all the uncommitted blocks
        #           it was generating
        #
        #   the workaround we use here is that whenever a file is opened for reading, we clear all
        #       uncommitted blocks by calling "Put Block List" with the list of currently committed blocks
        #
        #   this seems to be reasonably fast in practice, and means that failure #2 should not be an issue
        #
        #   failure #1 could still happen with concurrent writers, but this should result only in a
        #       confusing error message (409 error) instead of a ConcurrentWriteFailure, though we
        #       could likely raise that error if we saw a 409 with the error RequestEntityTooLargeBlockCountExceedsLimit
        #
        #   this does change the behavior slightly, now the writer that will end up succeeding on "Put Block List"
        #       is likely to be the last writer to open the file for writing, the others will fail
        #       because their uncommitted blocks have been cleared
        #
        # it would be nice to replace this with a less odd method, but it's not obvious how
        #   to do this on azure storage
        #
        # if there were upload sessions like GCS, this wouldn't be an issue
        # if there was no uncommitted block limit, method 2 would work fine
        # if blobs could automatically expire without having to add a container lifecycle rule
        #   then we could upload to a temp path, then copy to the final path (assuming copy is atomic)
        #   without automatic expiry, we'd leak temp files
        # we can use the lease system, but then we have to deal with leases
        req = Request(
            url=self._url,
            method="HEAD",
            headers={"If-Match": self._version} if self._version else {},
            success_codes=(200, 400, 404, 412, INVALID_HOSTNAME_STATUS),
        )
        resp = execute_api_request(self._conf, req)
        if resp.status == 200:
            if resp.headers["x-ms-blob-type"] == "BlockBlob":
                # because we delete all the uncommitted blocks, any concurrent writers will fail
                # but they would fail anyway since the first writer to finish would end up
                # deleting all uncommitted blocks
                # this means that the last writer to start is likely to win, the others should fail
                # with ConcurrentWriteFailure
                resp = _clear_uncommitted_blocks(self._conf, self._url, resp.headers)
                if resp:
                    self._version = resp.headers["ETag"]  # update the version according to new etag
            else:
                # if the existing blob type is not compatible with the block blob we are about to write
                # we have to delete the file before writing our block blob or else we will get a 409
                # error when putting the first block
                remove(self._conf, self._path)
        elif resp.status in (400, INVALID_HOSTNAME_STATUS) or (
            resp.status == 404 and resp.headers["x-ms-error-code"] == "ContainerNotFound"
        ):
            raise FileNotFoundError(
                f"No such file or container/account does not exist: '{self._path}'"
            )
        elif resp.status == 412:
            if resp.headers["x-ms-error-code"] != "ConditionNotMet":
                raise RequestFailure.create_from_request_response(
                    message=f"unexpected status {resp.status}", request=req, response=resp
                )
            else:
                raise VersionMismatch.create_from_request_response(
                    message="etag mismatch", request=req, response=resp
                )

    def _upload_chunk(self, chunk: memoryview, finalize: bool) -> None:
        start = 0
        while start < len(chunk):
            # premium block blob storage supports block blobs and append blobs
            # https://azure.microsoft.com/en-us/blog/azure-premium-block-blob-storage-is-now-generally-available/
            # we use block blobs because they are compatible with WASB:
            # https://docs.microsoft.com/en-us/azure/databricks/kb/data-sources/wasb-check-blob-types
            end = start + self._conf.azure_write_chunk_size
            data = chunk[start:end]
            self._md5.update(data)
            block_md5 = hashlib.md5(data)
            req = Request(
                url=self._url,
                method="PUT",
                headers={"Content-MD5": base64.b64encode(block_md5.digest()).decode("utf8")},
                params=dict(
                    comp="block",
                    blockid=_block_index_to_block_id(self._block_index, self._upload_id),
                ),
                data=data,
                success_codes=(201,),
                # retry if we get an MD5 mismatch error (unfortunately we are just guessing that it is a mismatch error
                # from the 400 status code)
                retry_codes=(400,) + DEFAULT_RETRY_CODES,
            )
            try:
                execute_api_request(self._conf, req)
            except Exception:
                del chunk, data, req.data, req
                raise

            self._block_index += 1
            if self._block_index >= BLOCK_COUNT_LIMIT:
                raise Error(
                    f"Exceeded block count limit of {BLOCK_COUNT_LIMIT} for Azure Storage.  Increase `azure_write_chunk_size` so that {BLOCK_COUNT_LIMIT} * `azure_write_chunk_size` exceeds the size of the file you are writing."
                )

            start = end

        if finalize:
            block_ids = [
                _block_index_to_block_id(i, self._upload_id) for i in range(self._block_index)
            ]
            resp = _finalize_blob(
                conf=self._conf,
                path=self._path,
                url=self._url,
                block_ids=block_ids,
                md5_digest=self._md5.digest(),
                version=self._version,
            )
            # Update the version according to new etag. The file will be closed
            # after this, but the version can still be retrieved.
            self._version = resp.headers.get("ETag") or self._version


def _upload_chunk(conf: Config, path: str, start: int, size: int, url: str, block_id: str) -> None:
    req = Request(
        url=url,
        method="PUT",
        params=dict(comp="block", blockid=block_id),
        # this needs to be specified since we use a file object for the data
        headers={"Content-Length": str(size)},
        data=FileBody(path, start=start, end=start + size),
        success_codes=(201, 404, INVALID_HOSTNAME_STATUS),
    )
    resp = execute_api_request(conf, req)
    if resp.status == 404 or resp.status == INVALID_HOSTNAME_STATUS:
        # this can happen during parallel upload
        raise FileNotFoundError(f"No such file or directory: '{url}'")


def parallel_upload(
    conf: Config,
    executor: concurrent.futures.Executor,
    src: str,
    dst: str,
    return_md5: bool,
    dst_version: str | None,
) -> str | None:
    with open(src, "rb") as f:
        md5_digest = common.block_md5(f)

    account, container, blob = split_path(dst)
    dst_url = build_url(account, "/{container}/{blob}", container=container, blob=blob)

    upload_id = rng.randint(0, 2**47 - 1)
    s = os.stat(src)
    block_ids = []
    max_workers = getattr(executor, "_max_workers", os.cpu_count() or 1)
    part_size = min(
        max(math.ceil(s.st_size / max_workers), common.PARALLEL_COPY_MINIMUM_PART_SIZE),
        MAX_BLOCK_SIZE,
    )
    i = 0
    start = 0
    futures = []
    while start < s.st_size:
        block_id = _block_index_to_block_id(i, upload_id)
        future = executor.submit(
            _upload_chunk, conf, src, start, min(part_size, s.st_size - start), dst_url, block_id
        )
        futures.append(future)
        block_ids.append(block_id)
        i += 1
        start += part_size
    for future in futures:
        future.result()

    _finalize_blob(
        conf=conf,
        path=dst,
        url=dst_url,
        block_ids=block_ids,
        md5_digest=md5_digest,
        version=dst_version,
    )
    return binascii.hexlify(md5_digest).decode("utf8") if return_md5 else None


def maybe_stat(conf: Config, path: str, *, version: str | None = None) -> Stat | None:
    account, container, blob = split_path(path)
    if blob == "":
        return None
    req = Request(
        url=build_url(account, "/{container}/{blob}", container=container, blob=blob),
        method="HEAD",
        success_codes=(200, 404, INVALID_HOSTNAME_STATUS),
    )
    resp = execute_api_request(conf, req)
    if resp.status != 200:
        return None
    stat = make_stat(resp.headers)
    if version is not None and stat.version != version:
        raise VersionMismatch.create_from_request_response(
            message="etag mismatch", request=req, response=resp
        )
    return stat


def remove(conf: Config, path: str) -> bool:
    account, container, blob = split_path(path)
    if blob == "":
        raise FileNotFoundError(f"The system cannot find the path specified: '{path}'")
    req = Request(
        url=build_url(account, "/{container}/{blob}", container=container, blob=blob),
        method="DELETE",
        success_codes=(202, 404, INVALID_HOSTNAME_STATUS),
    )
    resp = execute_api_request(conf, req)
    return resp.status == 202


def maybe_update_md5(conf: Config, path: str, etag: str, hexdigest: str) -> bool:
    account, container, blob = split_path(path)
    req = Request(
        url=build_url(account, "/{container}/{blob}", container=container, blob=blob),
        method="HEAD",
        headers={"If-Match": etag},
        success_codes=(200, 404, 412),
    )
    resp = execute_api_request(conf, req)
    if resp.status in (404, 412):
        return False

    # these will be cleared if not provided, there does not appear to be a PATCH method like for GCS
    # https://docs.microsoft.com/en-us/rest/api/storageservices/set-blob-properties#remarks
    headers: dict[str, str] = {}
    for src, dst in RESPONSE_HEADER_TO_REQUEST_HEADER.items():
        if src in resp.headers:
            headers[dst] = resp.headers[src]
    headers["x-ms-blob-content-md5"] = base64.b64encode(binascii.unhexlify(hexdigest)).decode(
        "utf8"
    )

    req = Request(
        url=build_url(account, "/{container}/{blob}", container=container, blob=blob),
        method="PUT",
        params=dict(comp="properties"),
        headers={
            **headers,
            # https://docs.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations
            "If-Match": etag,
        },
        # for a 403, we actually maybe could have updated the md5, but we don't have permission
        # for a 404, this could be due to this being some anonymous access container
        # for both cases this is potentially bad because it means cache_dir won't work, it might be nice to let the user
        # know if there's a non-spammy way to do it
        success_codes=(200, 403, 404, 412),
    )
    resp = execute_api_request(conf, req)
    return resp.status == 200


def list_blobs(conf: Config, path: str, delimiter: str | None = None) -> Iterator[DirEntry]:
    params = {}
    if delimiter is not None:
        params["delimiter"] = delimiter

    account, container, prefix = split_path(path)
    it = create_page_iterator(
        conf,
        url=build_url(account, "/{container}", container=container),
        method="GET",
        params=dict(comp="list", restype="container", prefix=prefix, **params),
    )
    for result in it:
        yield from _get_entries(conf, account, container, result)


def entry_from_dirpath(path: str) -> DirEntry:
    if path.endswith("/"):
        path = path[:-1]
    _, _, obj = split_path(path)
    name = obj.split("/")[-1]
    return DirEntry(name=name, path=path, is_dir=True, is_file=False, stat=None)


def entry_from_path_stat(path: str, stat: Stat) -> DirEntry:
    assert not path.endswith("/")
    _, _, obj = split_path(path)
    name = obj.split("/")[-1]
    return DirEntry(name=name, path=path, is_dir=False, is_file=True, stat=stat)


def _get_entries(
    conf: Config, account: str, container: str, result: Mapping[str, Any]
) -> Iterator[DirEntry]:
    blobs = result["Blobs"]
    if blobs is None:
        return
    if "BlobPrefix" in blobs:
        for bp in blobs["BlobPrefix"]:
            path = combine_path(conf, account, container, bp["Name"])
            yield entry_from_dirpath(path)
    if "Blob" in blobs:
        for b in blobs["Blob"]:
            path = combine_path(conf, account, container, b["Name"])
            if b["Name"].endswith("/"):
                yield entry_from_dirpath(path)
            else:
                props = b["Properties"]
                yield entry_from_path_stat(path, make_stat(props))


def get_url(conf: Config, path: str) -> tuple[str, float | None]:
    account, container, blob = split_path(path)
    url = build_url(account, "/{container}/{blob}", container=container, blob=blob)
    token = sas_token_manager.get_token(conf=conf, key=(account, container))
    if token is None:
        # the container has public access
        return url, float("inf")
    return generate_signed_url(key=token, url=url)


def set_mtime(conf: Config, path: str, mtime: float, version: str | None = None) -> bool:
    account, container, blob = split_path(path)
    headers = {}
    if version is not None:
        headers["If-Match"] = version
    req = Request(
        url=build_url(account, "/{container}/{blob}", container=container, blob=blob),
        method="HEAD",
        params=dict(comp="metadata"),
        headers=headers,
        success_codes=(200, 404, 412),
    )
    resp = execute_api_request(conf, req)
    if resp.status == 404:
        raise FileNotFoundError(f"No such file: '{path}'")
    if resp.status == 412:
        return False

    headers = {k: v for k, v in resp.headers.items() if k.startswith("x-ms-meta-")}
    headers["x-ms-meta-blobfilemtime"] = str(mtime)
    if version is not None:
        headers["If-Match"] = version
    req = Request(
        url=build_url(account, "/{container}/{blob}", container=container, blob=blob),
        method="PUT",
        params=dict(comp="metadata"),
        headers=headers,
        success_codes=(200, 404, 412),
    )
    resp = execute_api_request(conf, req)
    if resp.status == 404:
        raise FileNotFoundError(f"No such file: '{path}'")
    return resp.status == 200


def dirname(conf: Config, path: str) -> str:
    account, container, obj = split_path(path)
    obj = strip_slashes(obj)
    if "/" in obj:
        obj = "/".join(obj.split("/")[:-1])
        return combine_path(conf, account, container, obj)
    else:
        return combine_path(conf, account, container, "")[:-1]


def remote_copy(conf: Config, src: str, dst: str, return_md5: bool) -> str | None:
    # https://docs.microsoft.com/en-us/rest/api/storageservices/copy-blob
    dst_account, dst_container, dst_blob = split_path(dst)
    src_account, src_container, src_blob = split_path(src)

    def build_req() -> Request:
        src_url = build_url(
            src_account, "/{container}/{blob}", container=src_container, blob=src_blob
        )
        if src_account != dst_account:
            # the signed url can expire, so technically we should get the sas_token and build the signed url
            # each time we build a new request
            sas_token = sas_token_manager.get_token(conf=conf, key=(src_account, src_container))
            # if we don't get a token, it's likely we have anonymous access to the container
            # if we do get a token, the container is likely private and we need to use
            # a signed url as the source
            if sas_token is not None:
                src_url, _ = generate_signed_url(key=sas_token, url=src_url)
        req = Request(
            url=build_url(
                dst_account, "/{container}/{blob}", container=dst_container, blob=dst_blob
            ),
            method="PUT",
            headers={"x-ms-copy-source": src_url},
            success_codes=(202, 404, 409, INVALID_HOSTNAME_STATUS),
        )
        return create_api_request(
            req, auth=access_token_manager.get_token(conf=conf, key=(dst_account, dst_container))
        )

    copy_id = None
    copy_status = None
    etag = None

    # start the copy, waiting for any existing copies to finish
    backoff_generator = common.exponential_sleep_generator()
    for backoff in backoff_generator:
        resp = common.execute_request(conf, build_req)
        if resp.status == 202:
            copy_id = resp.headers["x-ms-copy-id"]
            copy_status = resp.headers["x-ms-copy-status"]
            etag = resp.headers["etag"]
            break
        elif resp.status == INVALID_HOSTNAME_STATUS:
            raise FileNotFoundError(
                f"Source container or destination container not found: src='{src}' dst='{dst}'"
            )
        elif resp.status == 404:
            if resp.headers["x-ms-error-code"] == "ContainerNotFound":
                raise FileNotFoundError(
                    f"Source container or destination container not found: src='{src}' dst='{dst}'"
                )

            raise FileNotFoundError(f"Source file not found: '{src}'")
        elif resp.status == 409:
            result = xml.parse(resp.data)
            if result["Error"]["Code"] != "PendingCopyOperation":
                raise RequestFailure.create_from_request_response(
                    message=f"unexpected status {resp.status}", request=build_req(), response=resp
                )
            # if we got a pending copy operation, continue to wait
        else:
            raise Error(f"unhandled status {resp.status}")
        time.sleep(backoff)

    # wait for potentially async copy operation to finish
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob
    # pending, success, aborted, failed
    backoff_generator = common.exponential_sleep_generator()
    while copy_status == "pending":
        time.sleep(next(backoff_generator))
        req = Request(
            url=build_url(
                dst_account, "/{container}/{blob}", container=dst_container, blob=dst_blob
            ),
            method="GET",
        )
        resp = execute_api_request(conf, req)
        if resp.headers["x-ms-copy-id"] != copy_id:
            raise Error("Copy id mismatch")
        etag = resp.headers["etag"]
        copy_status = resp.headers["x-ms-copy-status"]
    if copy_status != "success":
        raise Error(f"Invalid copy status: '{copy_status}'")
    if return_md5:
        # if the file is the same one that we just copied, return the stored MD5
        st = maybe_stat(conf, dst)
        if st is not None and st.version == etag:
            return st.md5
    return None


def join_paths(conf: Config, url: str, relpath: str) -> str:
    parsed_url = urllib.parse.urlparse(url)
    if parsed_url.scheme == "https":
        account, host = parsed_url.netloc.split(".", maxsplit=1)
        if host != "blob.core.windows.net":
            raise Error(f"Invalid URL '{url}'; unexpected host '{host}'")
    elif parsed_url.scheme == "az":
        account = parsed_url.netloc
    else:
        raise Error(f"Invalid URL '{url}'; expected 'https' or 'az' scheme")

    if not account:
        raise Error(f"Invalid URL '{url}'; expected account name")

    # split the unparsed URL
    parts = url.split("/", maxsplit=4)
    container = parts[3] if len(parts) >= 4 else ""
    blob = parts[4] if len(parts) >= 5 else ""

    if not container:
        if blob:
            raise Error(f"Invalid URL '{url}'; expected container name")
        container, _, relpath = relpath.partition("/")

    if not blob.endswith("/"):
        blob += "/"
    blob = path_join(blob, relpath)
    if blob.startswith("/"):
        blob = blob[1:]
    return combine_path(conf, account, container, blob)


def _put_block_from_url(
    conf: Config, src: str, start: int, size: int, dst: str, block_id: str
) -> None:
    src_account, src_container, src_blob = split_path(src)
    dst_account, dst_container, dst_blob = split_path(dst)

    src_url = build_url(src_account, "/{container}/{blob}", container=src_container, blob=src_blob)

    dst_url = build_url(dst_account, "/{container}/{blob}", container=dst_container, blob=dst_blob)

    def build_req() -> Request:
        # the signed url can expire, so technically we should get the sas_token and build the signed url
        # each time we build a new request
        sas_token = sas_token_manager.get_token(conf=conf, key=(src_account, src_container))
        # if we don't get a token, it's likely we have anonymous access to the container
        # if we do get a token, the container is likely private and we need to use
        # a signed url as the source
        if sas_token is None:
            copy_src_url = src_url
        else:
            copy_src_url, _ = generate_signed_url(key=sas_token, url=src_url)
        req = Request(
            url=dst_url,
            method="PUT",
            params=dict(comp="block", blockid=block_id),
            headers={
                "Content-Length": "0",
                "x-ms-copy-source": copy_src_url,
                "x-ms-source-range": common.calc_range(start=start, end=start + size),
            },
            success_codes=(201, 404, INVALID_HOSTNAME_STATUS),
        )
        return create_api_request(
            req, auth=access_token_manager.get_token(conf=conf, key=(dst_account, dst_container))
        )

    resp = common.execute_request(conf, build_req)
    if resp.status == 404:
        raise FileNotFoundError(
            f"Source file/container or destination container not found: src='{src}' dst='{dst}'"
        )
    elif resp.status == INVALID_HOSTNAME_STATUS:
        raise FileNotFoundError(
            f"Source container or destination container not found: src='{src}' dst='{dst}'"
        )


def parallel_remote_copy(
    conf: Config,
    executor: concurrent.futures.Executor,
    src: str,
    dst: str,
    return_md5: bool,
    dst_version: str | None,
) -> str | None:
    # for whatever reason, put block from url is faster than copy blob when you run multiple requests in parallel
    # https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-from-url

    st = maybe_stat(conf, src)
    if st is None:
        raise FileNotFoundError(f"The system cannot find the path specified: '{src}'")

    md5_digest = None
    if st.md5 is not None:
        md5_digest = binascii.unhexlify(st.md5)
    upload_id = rng.randint(0, 2**47 - 1)
    block_ids = []
    min_block_size = st.size // BLOCK_COUNT_LIMIT
    assert min_block_size <= MAX_BLOCK_SIZE
    part_size = max(conf.azure_write_chunk_size, min_block_size)
    i = 0
    start = 0
    futures = []
    while start < st.size:
        block_id = _block_index_to_block_id(i, upload_id)
        future = executor.submit(
            _put_block_from_url, conf, src, start, min(part_size, st.size - start), dst, block_id
        )
        futures.append(future)
        block_ids.append(block_id)
        i += 1
        start += part_size
    for future in futures:
        future.result()

    dst_account, dst_container, dst_blob = split_path(dst)
    dst_url = build_url(dst_account, "/{container}/{blob}", container=dst_container, blob=dst_blob)

    _finalize_blob(
        conf=conf,
        path=dst,
        url=dst_url,
        block_ids=block_ids,
        md5_digest=md5_digest,
        version=dst_version,
    )
    return (
        binascii.hexlify(md5_digest).decode("utf8")
        if (return_md5 and md5_digest is not None)
        else None
    )


access_token_manager = TokenManager(_get_access_token, "azure_access")

sas_token_manager = TokenManager(_get_sas_token, "azure_sas")
