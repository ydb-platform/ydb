# coding: utf-8

import os

from django.contrib import messages
from django.core.exceptions import ImproperlyConfigured
try:
    from django.urls import reverse
except ImportError:
    from django.core.urlresolvers import reverse
from django.http import HttpResponseRedirect
from django.utils.encoding import smart_str
from django.utils.translation import gettext as _

from filebrowser.templatetags.fb_tags import query_helper


def get_path(path, site):
    converted_path = smart_str(os.path.join(site.directory, path))
    if not path.startswith('.') and not os.path.isabs(converted_path):
        if site.storage.isdir(converted_path):
            return path


def get_file(path, filename, site):
    # Files and directories are valid
    converted_path = smart_str(os.path.join(site.directory, path, filename))
    if not path.startswith('.') and not filename.startswith('.') and not os.path.isabs(converted_path):
        if site.storage.isfile(converted_path) or site.storage.isdir(converted_path):
            return filename


def path_exists(site, function):
    "Check if the given path exists."

    def decorator(request, *args, **kwargs):
        # TODO: This check should be moved to a better location than a decorator
        if get_path('', site=site) is None:
            # The storage location does not exist, raise an error to prevent eternal redirecting.
            raise ImproperlyConfigured(_("Error finding Upload-Folder (site.storage.location + site.directory). Maybe it does not exist?"))
        if get_path(request.GET.get('dir', ''), site=site) is None:
            msg = _('The requested Folder does not exist.')
            messages.add_message(request, messages.ERROR, msg)
            redirect_url = reverse("filebrowser:fb_browse", current_app=site.name) + query_helper(request.GET, u"", "dir")
            return HttpResponseRedirect(redirect_url)
        return function(request, *args, **kwargs)
    return decorator


def file_exists(site, function):
    "Check if the given file exists."

    def decorator(request, *args, **kwargs):
        file_path = get_file(request.GET.get('dir', ''), request.GET.get('filename', ''), site=site)
        if file_path is None:
            msg = _('The requested File does not exist.')
            messages.add_message(request, messages.ERROR, msg)
            redirect_url = reverse("filebrowser:fb_browse", current_app=site.name) + query_helper(request.GET, u"", "dir")
            return HttpResponseRedirect(redirect_url)
        return function(request, *args, **kwargs)
    return decorator
