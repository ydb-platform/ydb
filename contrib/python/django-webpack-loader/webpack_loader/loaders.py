import json
import os
import time
from functools import lru_cache
from io import open
from typing import Dict, Optional
from urllib.parse import urlparse
from warnings import warn

from django.conf import settings
from django.contrib.staticfiles.storage import staticfiles_storage
from django.http.request import HttpRequest

from .exceptions import (
    WebpackBundleLookupError,
    WebpackError,
    WebpackLoaderBadStatsError,
    WebpackLoaderTimeoutError,
)

_CROSSORIGIN_NO_REQUEST = (
    'The crossorigin attribute might be necessary but you did not pass a '
    'request object. django_webpack_loader needs a request object to be able '
    'to know when to emit the crossorigin attribute on link and script tags. '
    'Chunk name: {chunk_name}')
_CROSSORIGIN_NO_HOST = (
    'You have passed the request object but it does not have a "HTTP_HOST", '
    'thus django_webpack_loader can\'t know if the crossorigin header will '
    'be necessary or not. Chunk name: {chunk_name}')
_NONCE_NO_REQUEST = (
    'You have enabled the adding of nonce attributes to generated tags via '
    'django_webpack_loader, but haven\'t passed a request. '
    'Chunk name: {chunk_name}')
_NONCE_NO_CSPNONCE = (
    'django_webpack_loader can\'t generate a nonce tag for a bundle, '
    'because the passed request doesn\'t contain a "csp_nonce". '
    'Chunk name: {chunk_name}')
_LOADER_POSSIBLE_LIMBO = (
    'You are using django_webpack_loader with default timeout "None" '
    'which can cause request to hang indefinitely. '
    'Validate status of webpack-stats.json file if you experience infinite loading.'
)

@lru_cache(maxsize=100)
def _get_netloc(url: str) -> str:
    'Return a cached netloc (host:port) for the passed `url`.'
    return urlparse(url=url).netloc


class WebpackLoader:
    _assets = {}

    def __init__(self, name, config):
        self.name = name
        self.config = config

    def load_assets(self):
        try:
            with open(self.config["STATS_FILE"], encoding="utf-8") as f:
                return json.load(f)
        except IOError:
            raise IOError(
                "Error reading {0}. Are you sure webpack has generated "
                "the file and the path is correct?".format(self.config["STATS_FILE"])
            )

    def get_assets(self):
        if self.config["CACHE"]:
            if self.name not in self._assets:
                self._assets[self.name] = self.load_assets()
            return self._assets[self.name]
        return self.load_assets()

    def get_asset_by_source_filename(self, name):
        files = self.get_assets()["assets"].values()
        return next((x for x in files if x.get("sourceFilename") == name), None)

    def _add_crossorigin(
            self, request: Optional[HttpRequest], chunk: Dict[str, str],
            integrity: str, attrs_l: str) -> str:
        'Return an added `crossorigin` attribute if necessary.'
        def_value = f' integrity="{integrity}" '
        if not request:
            message = _CROSSORIGIN_NO_REQUEST.format(chunk_name=chunk['name'])
            warn(message=message, category=RuntimeWarning)
            return def_value
        if 'crossorigin' in attrs_l:
            return def_value
        host: Optional[str] = request.META.get('HTTP_HOST')
        if not host:
            message = _CROSSORIGIN_NO_HOST.format(chunk_name=chunk['name'])
            warn(message=message, category=RuntimeWarning)
            return def_value
        netloc = _get_netloc(url=chunk['url'])
        if netloc == '' or netloc == host:
            # Crossorigin not necessary
            return def_value
        cfgval: str = self.config.get('CROSSORIGIN')
        if cfgval == '':
            return f'{def_value}crossorigin '
        return f'{def_value}crossorigin="{cfgval}" '

    def get_integrity_attr(
            self, chunk: Dict[str, str], request: Optional[HttpRequest],
            attrs_l: str) -> str:
        if not self.config.get('INTEGRITY'):
            # Crossorigin only necessary when integrity is used
            return ' '

        integrity = chunk.get('integrity')
        if not integrity:
            raise WebpackLoaderBadStatsError(
                'The stats file does not contain valid data: INTEGRITY is set '
                'to True, but chunk does not contain "integrity" key. Maybe '
                'you forgot to add integrity: true in your '
                'BundleTrackerPlugin configuration?'
            )
        return self._add_crossorigin(
            request=request,
            chunk=chunk,
            integrity=integrity,
            attrs_l=attrs_l,
        )

    def get_nonce_attr(self, chunk: Dict[str, str], request: Optional[HttpRequest], attrs: str) -> str:
        'Return an added nonce for CSP when available.'
        if not self.config.get('CSP_NONCE'):
            return ''
        if request is None:
            message = _NONCE_NO_REQUEST.format(chunk_name=chunk['name'])
            warn(message=message, category=RuntimeWarning)
            return ''
        nonce = getattr(request, 'csp_nonce', None)
        if nonce is None:
            message = _NONCE_NO_CSPNONCE.format(chunk_name=chunk['name'])
            warn(message=message, category=RuntimeWarning)
            return ''
        if 'nonce=' in attrs.lower():
            return ''
        return f'nonce="{nonce}" '

    def filter_chunks(self, chunks):
        filtered_chunks = []

        for chunk in chunks:
            ignore = any(regex.match(chunk) for regex in self.config["ignores"])
            if not ignore:
                filtered_chunks.append(chunk)

        return filtered_chunks

    def map_chunk_files_to_url(self, chunks):
        assets = self.get_assets()
        files = assets["assets"]

        add_integrity = self.config.get("INTEGRITY")

        for chunk in chunks:
            url = self.get_chunk_url(files[chunk])

            if add_integrity:
                yield {
                    "name": chunk,
                    "url": url,
                    "integrity": files[chunk].get("integrity"),
                }
            else:
                yield {"name": chunk, "url": url}

    def get_chunk_url(self, chunk_file):
        public_path = chunk_file.get("publicPath")
        if public_path and public_path != "auto":
            return public_path

        # Use os.path.normpath for Windows paths
        relpath = os.path.normpath(
            os.path.join(self.config["BUNDLE_DIR_NAME"], chunk_file["name"])
        )
        return staticfiles_storage.url(relpath)

    def get_bundle(self, bundle_name):
        assets = self.get_assets()

        # poll when debugging and block request until bundle is compiled
        # or the build times out
        if settings.DEBUG:
            timeout = self.config["TIMEOUT"] or 0
            timed_out = False
            start = time.time()
            while assets["status"] == "compile" and not timed_out:
                time.sleep(self.config["POLL_INTERVAL"])
                if timeout and (time.time() - timeout > start):
                    timed_out = True
                if not timeout:
                    warn(
                        message=_LOADER_POSSIBLE_LIMBO, category=RuntimeWarning
                    )
                assets = self.get_assets()

            if timed_out:
                raise WebpackLoaderTimeoutError(
                    "Timed Out. Bundle `{0}` took more than {1} seconds "
                    "to compile.".format(bundle_name, timeout)
                )

        if assets.get("status") == "done":
            chunks = assets["chunks"].get(bundle_name, None)
            if chunks is None:
                raise WebpackBundleLookupError(
                    "Cannot resolve bundle {0}.".format(bundle_name)
                )

            filtered_chunks = self.filter_chunks(chunks)

            for chunk in filtered_chunks:
                asset = assets["assets"][chunk]
                if asset is None:
                    raise WebpackBundleLookupError(
                        "Cannot resolve asset {0}.".format(chunk)
                    )

            return self.map_chunk_files_to_url(filtered_chunks)

        elif assets.get("status") == "error":
            if "file" not in assets:
                assets["file"] = ""
            if "error" not in assets:
                assets["error"] = "Unknown Error"
            if "message" not in assets:
                assets["message"] = ""
            error = """
            {error} in {file}
            {message}
            """.format(**assets)
            raise WebpackError(error)

        raise WebpackLoaderBadStatsError(
            "The stats file does not contain valid data. Make sure "
            "webpack-bundle-tracker plugin is enabled and try to run "
            "webpack again."
        )


class FakeWebpackLoader(WebpackLoader):
    """
    A fake loader to help run Django tests.

    For running tests where `render_bundle` is used but assets aren't built.
    """

    def get_assets(self):
        return {}

    def get_bundle(self, _bundle_name):
        return [
            {
                "name": "test.bundle.js",
                "url": "http://localhost/static/webpack_bundles/test.bundle.js",
            }
        ]
