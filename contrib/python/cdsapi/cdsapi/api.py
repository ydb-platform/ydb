# (C) Copyright 2018 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from __future__ import absolute_import, division, print_function, unicode_literals

import json
import logging
import os
import time
import uuid
from importlib.metadata import version

import requests

try:
    from urllib.parse import urljoin
except ImportError:
    from urlparse import urljoin

from tqdm import tqdm


def bytes_to_string(n):
    u = ["", "K", "M", "G", "T", "P"]
    i = 0
    while n >= 1024:
        n /= 1024.0
        i += 1
    return "%g%s" % (int(n * 10 + 0.5) / 10.0, u[i])


def read_config(path):
    config = {}
    with open(path) as f:
        for line in f.readlines():
            if ":" in line:
                k, v = line.strip().split(":", 1)
                if k in ("url", "key", "verify"):
                    config[k] = v.strip()
    return config


def get_url_key_verify(url, key, verify):
    if url is None:
        url = os.environ.get("CDSAPI_URL")
    if key is None:
        key = os.environ.get("CDSAPI_KEY")
    dotrc = os.environ.get("CDSAPI_RC", os.path.expanduser("~/.cdsapirc"))

    if url is None or key is None:
        if os.path.exists(dotrc):
            config = read_config(dotrc)

            if key is None:
                key = config.get("key")

            if url is None:
                url = config.get("url")

            if verify is None:
                verify = bool(int(config.get("verify", 1)))

    if url is None or key is None:
        raise Exception("Missing/incomplete configuration file: %s" % (dotrc))

    # If verify is still None, then we set to default value of True
    if verify is None:
        verify = True
    return url, key, verify


def toJSON(obj):
    to_json = getattr(obj, "toJSON", None)
    if callable(to_json):
        return to_json()

    if isinstance(obj, (list, tuple)):
        return [toJSON(x) for x in obj]

    if isinstance(obj, dict):
        r = {}
        for k, v in obj.items():
            r[k] = toJSON(v)
        return r

    return obj


class Result(object):
    def __init__(self, client, reply):
        self.reply = reply

        self._url = client.url

        self.session = client.session
        self.robust = client.robust
        self.verify = client.verify
        self.cleanup = client.delete

        self.debug = client.debug
        self.info = client.info
        self.warning = client.warning
        self.error = client.error
        self.sleep_max = client.sleep_max
        self.retry_max = client.retry_max

        self.timeout = client.timeout
        self.progress = client.progress

        self._deleted = False

    def toJSON(self):
        r = dict(
            resultType="url",
            contentType=self.content_type,
            contentLength=self.content_length,
            location=self.location,
        )
        return r

    def _download(self, url, size, target):
        if target is None:
            target = url.split("/")[-1]

        self.info("Downloading %s to %s (%s)", url, target, bytes_to_string(size))
        start = time.time()

        mode = "wb"
        total = 0
        sleep = 10
        tries = 0
        headers = None

        while tries < self.retry_max:
            r = self.robust(self.session.get)(
                url,
                stream=True,
                verify=self.verify,
                headers=headers,
                timeout=self.timeout,
            )
            try:
                r.raise_for_status()

                with tqdm(
                    total=size,
                    unit_scale=True,
                    unit_divisor=1024,
                    unit="B",
                    disable=not self.progress,
                    leave=False,
                ) as pbar:
                    pbar.update(total)
                    with open(target, mode) as f:
                        for chunk in r.iter_content(chunk_size=1024):
                            if chunk:
                                f.write(chunk)
                                total += len(chunk)
                                pbar.update(len(chunk))

            except requests.exceptions.ConnectionError as e:
                self.error("Download interupted: %s" % (e,))
            finally:
                r.close()

            if total >= size:
                break

            self.error(
                "Download incomplete, downloaded %s byte(s) out of %s" % (total, size)
            )
            self.warning("Sleeping %s seconds" % (sleep,))
            time.sleep(sleep)
            mode = "ab"
            total = os.path.getsize(target)
            sleep *= 1.5
            if sleep > self.sleep_max:
                sleep = self.sleep_max
            headers = {"Range": "bytes=%d-" % total}
            tries += 1
            self.warning("Resuming download at byte %s" % (total,))

        if total != size:
            raise Exception(
                "Download failed: downloaded %s byte(s) out of %s" % (total, size)
            )

        elapsed = time.time() - start
        if elapsed:
            self.info("Download rate %s/s", bytes_to_string(size / elapsed))

        return target

    def download(self, target=None):
        return self._download(self.location, self.content_length, target)

    @property
    def content_length(self):
        return int(self.reply["content_length"])

    @property
    def location(self):
        return urljoin(self._url, self.reply["location"])

    @property
    def content_type(self):
        return self.reply["content_type"]

    def __repr__(self):
        return "Result(content_length=%s,content_type=%s,location=%s)" % (
            self.content_length,
            self.content_type,
            self.location,
        )

    def check(self):
        self.debug("HEAD %s", self.location)
        metadata = self.robust(self.session.head)(
            self.location, verify=self.verify, timeout=self.timeout
        )
        metadata.raise_for_status()
        self.debug(metadata.headers)
        return metadata

    def update(self, request_id=None):
        if request_id is None:
            request_id = self.reply["request_id"]
        task_url = "%s/tasks/%s" % (self._url, request_id)
        self.debug("GET %s", task_url)

        result = self.robust(self.session.get)(
            task_url, verify=self.verify, timeout=self.timeout
        )
        result.raise_for_status()
        self.reply = result.json()

    def delete(self):
        if self._deleted:
            return

        if "request_id" in self.reply:
            rid = self.reply["request_id"]

            task_url = "%s/tasks/%s" % (self._url, rid)
            self.debug("DELETE %s", task_url)

            delete = self.session.delete(
                task_url, verify=self.verify, timeout=self.timeout
            )
            self.debug("DELETE returns %s %s", delete.status_code, delete.reason)

            try:
                delete.raise_for_status()
            except Exception:
                self.warning(
                    "DELETE %s returns %s %s",
                    task_url,
                    delete.status_code,
                    delete.reason,
                )

            self._deleted = True

    def __del__(self):
        try:
            if self.cleanup:
                self.delete()
        except Exception as e:
            print(e)


class Client(object):
    logger = logging.getLogger("cdsapi")

    def __new__(cls, url=None, key=None, *args, **kwargs):
        _, token, _ = get_url_key_verify(url, key, None)
        if ":" in token:
            return super().__new__(cls)

        from ecmwf.datastores.legacy_client import LegacyClient

        return super().__new__(LegacyClient)

    def __init__(
        self,
        url=None,
        key=None,
        quiet=False,
        debug=False,
        verify=None,
        timeout=60,
        progress=True,
        full_stack=False,
        delete=True,
        retry_max=500,
        sleep_max=120,
        wait_until_complete=True,
        info_callback=None,
        warning_callback=None,
        error_callback=None,
        debug_callback=None,
        metadata=None,
        forget=False,
        session=requests.Session(),
    ):
        if not quiet:
            if debug:
                level = logging.DEBUG
            else:
                level = logging.INFO

            self.logger.setLevel(level)

            # avoid duplicate handlers when creating more than one Client
            if not self.logger.handlers:
                formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
                handler = logging.StreamHandler()
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)

        url, key, verify = get_url_key_verify(url, key, verify)

        self.url = url
        self.key = key

        self.quiet = quiet
        self.progress = progress and not quiet

        self.verify = True if verify else False
        self.timeout = timeout
        self.sleep_max = sleep_max
        self.retry_max = retry_max
        self.full_stack = full_stack
        self.delete = delete
        self.last_state = None
        self.wait_until_complete = wait_until_complete

        self.debug_callback = debug_callback
        self.warning_callback = warning_callback
        self.info_callback = info_callback
        self.error_callback = error_callback

        self.session = session
        self.session.auth = tuple(self.key.split(":", 2))
        self.session.headers = {
            "User-Agent": f"cdsapi/{version('cdsapi')}",
        }

        assert len(self.session.auth) == 2, (
            "The cdsapi key provided is not the correct format, please ensure it conforms to:\n"
            "<UID>:<APIKEY>"
        )

        self.metadata = metadata
        self.forget = forget

        self.debug(
            "CDSAPI %s",
            dict(
                url=self.url,
                key=self.key,
                quiet=self.quiet,
                verify=self.verify,
                timeout=self.timeout,
                progress=self.progress,
                sleep_max=self.sleep_max,
                retry_max=self.retry_max,
                full_stack=self.full_stack,
                delete=self.delete,
                metadata=self.metadata,
                forget=self.forget,
            ),
        )

    def retrieve(self, name, request, target=None):
        result = self._api("%s/resources/%s" % (self.url, name), request, "POST")
        if target is not None:
            result.download(target)
        return result

    def service(self, name, *args, **kwargs):
        self.delete = False  # Don't delete results
        name = "/".join(name.split("."))
        mimic_ui = kwargs.pop("mimic_ui", False)
        # To mimic the CDS ui the request should be populated directly with the kwargs
        if mimic_ui:
            request = kwargs
        else:
            request = dict(args=args, kwargs=kwargs)

        if self.metadata:
            request["_cds_metadata"] = self.metadata
        request = toJSON(request)
        result = self._api(
            "%s/tasks/services/%s/clientid-%s" % (self.url, name, uuid.uuid4().hex),
            request,
            "PUT",
        )
        return result

    def workflow(self, code, *args, **kwargs):
        workflow_name = kwargs.pop("workflow_name", "application")
        params = dict(code=code, args=args, kwargs=kwargs, workflow_name=workflow_name)
        return self.service("tool.toolbox.orchestrator.run_workflow", params)

    def status(self, context=None):
        url = "%s/status.json" % (self.url,)
        r = self.session.get(url, verify=self.verify, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _status(self, url):
        try:
            status = self.status(url)

            info = status.get("info", [])
            if not isinstance(info, list):
                info = [info]
            for i in info:
                self.info("%s", i)

            warning = status.get("warning", [])
            if not isinstance(warning, list):
                warning = [warning]
            for w in warning:
                self.warning("%s", w)

        except Exception:
            pass

    def _api(self, url, request, method):
        self._status(url)

        session = self.session

        self.info("Sending request to %s", url)
        self.debug("%s %s %s", method, url, json.dumps(request))

        if method == "PUT":
            action = session.put
        else:
            action = session.post

        result = self.robust(action)(
            url, json=request, verify=self.verify, timeout=self.timeout
        )

        if self.forget:
            return result

        reply = None

        try:
            result.raise_for_status()
            reply = result.json()
        except Exception:
            if reply is None:
                try:
                    reply = result.json()
                except Exception:
                    reply = dict(message=result.text)

            self.debug(json.dumps(reply))

            if "message" in reply:
                error = reply["message"]

                if "context" in reply and "required_terms" in reply["context"]:
                    e = [error]
                    for t in reply["context"]["required_terms"]:
                        e.append(
                            "To access this resource, you first need to accept the terms"
                            "of '%s' at %s" % (t["title"], t["url"])
                        )
                    error = ". ".join(e)
                raise Exception(error)
            else:
                raise

        if not self.wait_until_complete:
            return Result(self, reply)

        sleep = 1

        while True:
            self.debug("REPLY %s", reply)

            if reply["state"] != self.last_state:
                self.info("Request is %s" % (reply["state"],))
                self.last_state = reply["state"]

            if reply["state"] == "completed":
                self.debug("Done")

                if "result" in reply:
                    return reply["result"]

                return Result(self, reply)

            if reply["state"] in ("queued", "running"):
                rid = reply["request_id"]

                self.debug("Request ID is %s, sleep %s", rid, sleep)
                time.sleep(sleep)
                sleep *= 1.5
                if sleep > self.sleep_max:
                    sleep = self.sleep_max

                task_url = "%s/tasks/%s" % (self.url, rid)
                self.debug("GET %s", task_url)

                result = self.robust(session.get)(
                    task_url, verify=self.verify, timeout=self.timeout
                )
                result.raise_for_status()
                reply = result.json()
                continue

            if reply["state"] in ("failed",):
                self.error("Message: %s", reply["error"].get("message"))
                self.error("Reason:  %s", reply["error"].get("reason"))
                for n in (
                    reply.get("error", {})
                    .get("context", {})
                    .get("traceback", "")
                    .split("\n")
                ):
                    if n.strip() == "" and not self.full_stack:
                        break
                    self.error("  %s", n)
                raise Exception(
                    "%s. %s."
                    % (reply["error"].get("message"), reply["error"].get("reason"))
                )

            raise Exception("Unknown API state [%s]" % (reply["state"],))

    def info(self, *args, **kwargs):
        if self.info_callback:
            self.info_callback(*args, **kwargs)
        else:
            self.logger.info(*args, **kwargs)

    def warning(self, *args, **kwargs):
        if self.warning_callback:
            self.warning_callback(*args, **kwargs)
        else:
            self.logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs):
        if self.error_callback:
            self.error_callback(*args, **kwargs)
        else:
            self.logger.error(*args, **kwargs)

    def debug(self, *args, **kwargs):
        if self.debug_callback:
            self.debug_callback(*args, **kwargs)
        else:
            self.logger.debug(*args, **kwargs)

    def _download(self, results, targets=None):
        if isinstance(results, Result):
            if targets:
                path = targets.pop(0)
            else:
                path = None
            return results.download(path)

        if isinstance(results, (list, tuple)):
            return [self._download(x, targets) for x in results]

        if isinstance(results, dict):
            if "location" in results and "contentLength" in results:
                reply = dict(
                    location=results["location"],
                    content_length=results["contentLength"],
                    content_type=results.get("contentType"),
                )

                if targets:
                    path = targets.pop(0)
                else:
                    path = None

                return Result(self, reply).download(path)

            r = {}
            for k, v in results.items():
                r[v] = self._download(v, targets)
            return r

        return results

    def download(self, results, targets=None):
        if targets:
            # Make a copy
            targets = [t for t in targets]
        return self._download(results, targets)

    def remote(self, url):
        r = requests.head(url)
        reply = dict(
            location=url,
            content_length=r.headers["Content-Length"],
            content_type=r.headers["Content-Type"],
        )
        return Result(self, reply)

    def robust(self, call):
        def retriable(code, reason):
            if code in [
                requests.codes.internal_server_error,
                requests.codes.bad_gateway,
                requests.codes.service_unavailable,
                requests.codes.gateway_timeout,
                requests.codes.too_many_requests,
                requests.codes.request_timeout,
            ]:
                return True

            return False

        def wrapped(*args, **kwargs):
            tries = 0
            while True:
                txt = "Error"
                try:
                    resp = call(*args, **kwargs)
                except (
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ReadTimeout,
                ) as e:
                    resp = None
                    txt = f"Connection error: [{e}]"

                if resp is not None:
                    if not retriable(resp.status_code, resp.reason):
                        break
                    try:
                        self.warning(resp.json()["reason"])
                    except Exception:
                        pass
                    txt = f"HTTP error: [{resp.status_code} {resp.reason}]"

                tries += 1
                self.warning(txt + f". Attempt {tries} of {self.retry_max}.")
                if tries < self.retry_max:
                    self.warning(f"Retrying in {self.sleep_max} seconds")
                    time.sleep(self.sleep_max)
                    self.info("Retrying now...")
                else:
                    raise Exception("Could not connect")

            return resp

        return wrapped
