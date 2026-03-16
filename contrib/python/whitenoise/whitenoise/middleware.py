from __future__ import annotations

import os
from posixpath import basename
from urllib.parse import urlparse

from django.conf import settings
from django.contrib.staticfiles import finders
from django.contrib.staticfiles.storage import staticfiles_storage
from django.http import FileResponse

from whitenoise.base import WhiteNoise
from whitenoise.string_utils import ensure_leading_trailing_slash

__all__ = ["WhiteNoiseMiddleware"]


class WhiteNoiseFileResponse(FileResponse):
    """
    Wrap Django's FileResponse to prevent setting any default headers. For the
    most part these just duplicate work already done by WhiteNoise but in some
    cases (e.g. the content-disposition header introduced in Django 3.0) they
    are actively harmful.
    """

    def set_headers(self, *args, **kwargs):
        pass


class WhiteNoiseMiddleware(WhiteNoise):
    """
    Wrap WhiteNoise to allow it to function as Django middleware, rather
    than WSGI middleware.
    """

    def __init__(self, get_response=None, settings=settings):
        self.get_response = get_response

        try:
            autorefresh: bool = settings.WHITENOISE_AUTOREFRESH
        except AttributeError:
            autorefresh = settings.DEBUG
        try:
            max_age = settings.WHITENOISE_MAX_AGE
        except AttributeError:
            if settings.DEBUG:
                max_age = 0
            else:
                max_age = 60
        try:
            allow_all_origins = settings.WHITENOISE_ALLOW_ALL_ORIGINS
        except AttributeError:
            allow_all_origins = True
        try:
            charset = settings.WHITENOISE_CHARSET
        except AttributeError:
            charset = "utf-8"
        try:
            mimetypes = settings.WHITENOISE_MIMETYPES
        except AttributeError:
            mimetypes = None
        try:
            add_headers_function = settings.WHITENOISE_ADD_HEADERS_FUNCTION
        except AttributeError:
            add_headers_function = None
        try:
            index_file = settings.WHITENOISE_INDEX_FILE
        except AttributeError:
            index_file = None
        try:
            immutable_file_test = settings.WHITENOISE_IMMUTABLE_FILE_TEST
        except AttributeError:
            immutable_file_test = None

        super().__init__(
            application=None,
            autorefresh=autorefresh,
            max_age=max_age,
            allow_all_origins=allow_all_origins,
            charset=charset,
            mimetypes=mimetypes,
            add_headers_function=add_headers_function,
            index_file=index_file,
            immutable_file_test=immutable_file_test,
        )

        try:
            self.use_finders = settings.WHITENOISE_USE_FINDERS
        except AttributeError:
            self.use_finders = settings.DEBUG

        try:
            self.static_prefix = settings.WHITENOISE_STATIC_PREFIX
        except AttributeError:
            self.static_prefix = urlparse(settings.STATIC_URL or "").path
            if settings.FORCE_SCRIPT_NAME:
                script_name = settings.FORCE_SCRIPT_NAME.rstrip("/")
                if self.static_prefix.startswith(script_name):
                    self.static_prefix = self.static_prefix[len(script_name) :]
        self.static_prefix = ensure_leading_trailing_slash(self.static_prefix)

        self.static_root = settings.STATIC_ROOT
        if self.static_root:
            self.add_files(self.static_root, prefix=self.static_prefix)

        try:
            root = settings.WHITENOISE_ROOT
        except AttributeError:
            root = None
        if root:
            self.add_files(root)

        if self.use_finders and not self.autorefresh:
            self.add_files_from_finders()

    def __call__(self, request):
        if self.autorefresh:
            static_file = self.find_file(request.path_info)
        else:
            static_file = self.files.get(request.path_info)
        if static_file is not None:
            return self.serve(static_file, request)
        return self.get_response(request)

    @staticmethod
    def serve(static_file, request):
        response = static_file.get_response(request.method, request.META)
        status = int(response.status)
        http_response = WhiteNoiseFileResponse(response.file or (), status=status)
        # Remove default content-type
        del http_response["content-type"]
        for key, value in response.headers:
            http_response[key] = value
        return http_response

    def add_files_from_finders(self):
        files = {}
        for finder in finders.get_finders():
            for path, storage in finder.list(None):
                prefix = (getattr(storage, "prefix", None) or "").strip("/")
                url = "".join(
                    (
                        self.static_prefix,
                        prefix,
                        "/" if prefix else "",
                        path.replace("\\", "/"),
                    )
                )
                # Use setdefault as only first matching file should be used
                files.setdefault(url, storage.path(path))
        stat_cache = {path: os.stat(path) for path in files.values()}
        for url, path in files.items():
            self.add_file_to_dictionary(url, path, stat_cache=stat_cache)

    def candidate_paths_for_url(self, url):
        if self.use_finders and url.startswith(self.static_prefix):
            path = finders.find(url[len(self.static_prefix) :])
            if path:
                yield path
        paths = super().candidate_paths_for_url(url)
        for path in paths:
            yield path

    def immutable_file_test(self, path, url):
        """
        Determine whether given URL represents an immutable file (i.e. a
        file with a hash of its contents as part of its name) which can
        therefore be cached forever
        """
        if not url.startswith(self.static_prefix):
            return False
        name = url[len(self.static_prefix) :]
        name_without_hash = self.get_name_without_hash(name)
        if name == name_without_hash:
            return False
        static_url = self.get_static_url(name_without_hash)
        # If the static_url function maps the name without hash
        # back to the original name, then we know we've got a
        # versioned filename
        return bool(static_url and basename(static_url) == basename(url))

    def get_name_without_hash(self, filename):
        """
        Removes the version hash from a filename e.g, transforms
        'css/application.f3ea4bcc2.css' into 'css/application.css'

        Note: this is specific to the naming scheme used by Django's
        CachedStaticFilesStorage. You may have to override this if
        you are using a different static files versioning system
        """
        name_with_hash, ext = os.path.splitext(filename)
        name = os.path.splitext(name_with_hash)[0]
        return name + ext

    def get_static_url(self, name):
        try:
            return staticfiles_storage.url(name)
        except ValueError:
            return None
