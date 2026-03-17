# coding: utf-8

import os
import re
import json
from time import gmtime, strftime, localtime, time

from django import forms
from django import VERSION as DJANGO_VERSION
from django.contrib import messages
from django.contrib.admin.sites import site as admin_site
from django.utils.module_loading import import_string 
from django.contrib.admin.views.decorators import staff_member_required
from django.core.files.storage import DefaultStorage, default_storage, FileSystemStorage
from django.core.paginator import Paginator, InvalidPage, EmptyPage
try:
    from django.urls import reverse, get_urlconf, get_resolver
except ImportError:
    from django.core.urlresolvers import reverse, get_urlconf, get_resolver
from django.http import HttpResponseRedirect, HttpResponseBadRequest
from django.shortcuts import render, HttpResponse
from django.template import RequestContext as Context
from django.template.response import TemplateResponse
from django.utils.encoding import smart_str
from django.utils.translation import gettext as _
from django.views.decorators.cache import never_cache
from django.views.decorators.clickjacking import xframe_options_sameorigin
from django.views.decorators.csrf import csrf_exempt

from filebrowser import signals
from filebrowser.base import FileListing, FileObject
from filebrowser.decorators import path_exists, file_exists
from filebrowser.storage import FileSystemStorageMixin
from filebrowser.templatetags.fb_tags import query_helper
from filebrowser.utils import convert_filename
from filebrowser.settings import (DIRECTORY, EXTENSIONS, SELECT_FORMATS, ADMIN_VERSIONS, ADMIN_THUMBNAIL, 
    MAX_UPLOAD_SIZE, NORMALIZE_FILENAME, CONVERT_FILENAME, SEARCH_TRAVERSE, EXCLUDE, VERSIONS, 
    VERSIONS_BASEDIR, EXTENSION_LIST, DEFAULT_SORTING_BY, DEFAULT_SORTING_ORDER, LIST_PER_PAGE,
    OVERWRITE_EXISTING, DEFAULT_PERMISSIONS, UPLOAD_TEMPDIR, ADMIN_CUSTOM
)



# This use admin_custom and not admin.sites.site of Django.
admin_site = import_string(ADMIN_CUSTOM) if ADMIN_CUSTOM else admin_site


# Add some required methods to FileSystemStorage
if FileSystemStorageMixin not in FileSystemStorage.__bases__:
    FileSystemStorage.__bases__ += (FileSystemStorageMixin,)


# This cache contains all *instantiated* FileBrowser sites
_sites_cache = {}


def get_site_dict(app_name='filebrowser'):
    """
    Return a dict with all *deployed* FileBrowser sites that have
    a given app_name.
    """
    if app_name not in _sites_cache:
        return {}
    # Get names of all deployed filebrowser sites with a give app_name
    deployed = get_resolver(get_urlconf()).app_dict[app_name]
    # Get the deployed subset from the cache
    return dict((k, v) for k, v in _sites_cache[app_name].items() if k in deployed)


def register_site(app_name, site_name, site):
    """
    Add a site into the site dict.
    """
    if app_name not in _sites_cache:
        _sites_cache[app_name] = {}
    _sites_cache[app_name][site_name] = site


def get_default_site(app_name='filebrowser'):
    """
    Returns the default site. This function uses Django's url resolution method to
    obtain the name of the default site.
    """
    # Get the name of the default site:
    resolver = get_resolver(get_urlconf())
    name = 'filebrowser'

    # Django's default name resolution method (see django.core.urlresolvers.reverse())
    app_list = resolver.app_dict[app_name]
    if name not in app_list:
        name = app_list[0]

    return get_site_dict()[name]


def get_breadcrumbs(query, path):
    """
    Get breadcrumbs.
    """

    breadcrumbs = []
    dir_query = ""
    if path:
        for item in path.split(os.sep):
            dir_query = os.path.join(dir_query, item)
            breadcrumbs.append([item, dir_query])
    return breadcrumbs


def get_filterdate(filter_date, date_time):
    """
    Get filterdate.
    """

    returnvalue = ''
    date_year = strftime("%Y", gmtime(date_time))
    date_month = strftime("%m", gmtime(date_time))
    date_day = strftime("%d", gmtime(date_time))
    if filter_date == 'today' and int(date_year) == int(localtime()[0]) and int(date_month) == int(localtime()[1]) and int(date_day) == int(localtime()[2]):
        returnvalue = 'true'
    elif filter_date == 'thismonth' and date_time >= time() - 2592000:
        returnvalue = 'true'
    elif filter_date == 'thisyear' and int(date_year) == int(localtime()[0]):
        returnvalue = 'true'
    elif filter_date == 'past7days' and date_time >= time() - 604800:
        returnvalue = 'true'
    elif filter_date == '':
        returnvalue = 'true'
    return returnvalue


def get_settings_var(directory=DIRECTORY):
    """
    Get settings variables used for FileBrowser listing.
    """

    settings_var = {}
    # Main
    # Extensions/Formats (for FileBrowseField)
    settings_var['EXTENSIONS'] = EXTENSIONS
    settings_var['SELECT_FORMATS'] = SELECT_FORMATS
    # Versions
    settings_var['ADMIN_VERSIONS'] = ADMIN_VERSIONS
    settings_var['ADMIN_THUMBNAIL'] = ADMIN_THUMBNAIL
    # FileBrowser Options
    settings_var['MAX_UPLOAD_SIZE'] = MAX_UPLOAD_SIZE
    # Normalize Filenames
    settings_var['NORMALIZE_FILENAME'] = NORMALIZE_FILENAME
    # Convert Filenames
    settings_var['CONVERT_FILENAME'] = CONVERT_FILENAME
    # Traverse directories when searching
    settings_var['SEARCH_TRAVERSE'] = SEARCH_TRAVERSE
    return settings_var


def handle_file_upload(path, file, site):
    """
    Handle File Upload.
    """

    uploadedfile = None
    try:
        file_path = os.path.join(path, file.name)
        uploadedfile = site.storage.save(file_path, file)
    except Exception as inst:
        raise inst
    return uploadedfile


def filebrowser_view(view):
    "Only let staff browse the files"
    return staff_member_required(never_cache(xframe_options_sameorigin(view)))


class FileBrowserSite(object):
    """
    A filebrowser.site defines admin views for browsing your servers media files.
    """
    filelisting_class = FileListing

    def __init__(self, name=None, app_name='filebrowser', storage=default_storage):
        self.name = name
        self.app_name = app_name
        self.storage = storage

        self._actions = {}
        self._global_actions = self._actions.copy()

        # Register this site in the global site cache
        register_site(self.app_name, self.name, self)

        # Per-site settings:
        self.directory = DIRECTORY

    def _directory_get(self):
        "Set directory"
        return self._directory

    def _directory_set(self, val):
        "Get directory"
        self._directory = val

    directory = property(_directory_get, _directory_set)

    def get_urls(self):
        "URLs for a filebrowser.site"
        from django.urls import re_path

        # filebrowser urls (views)
        urlpatterns = [
            re_path(r'^browse/$', path_exists(self, filebrowser_view(self.browse)), name="fb_browse"),
            re_path(r'^createdir/', path_exists(self, filebrowser_view(self.createdir)), name="fb_createdir"),
            re_path(r'^upload/', path_exists(self, filebrowser_view(self.upload)), name="fb_upload"),
            re_path(r'^delete_confirm/$', file_exists(self, path_exists(self, filebrowser_view(self.delete_confirm))), name="fb_delete_confirm"),
            re_path(r'^delete/$', file_exists(self, path_exists(self, filebrowser_view(self.delete))), name="fb_delete"),
            re_path(r'^detail/$', file_exists(self, path_exists(self, filebrowser_view(self.detail))), name="fb_detail"),
            re_path(r'^version/$', file_exists(self, path_exists(self, filebrowser_view(self.version))), name="fb_version"),
            re_path(r'^upload_file/$', staff_member_required(csrf_exempt(self._upload_file)), name="fb_do_upload"),
        ]
        return urlpatterns

    def add_action(self, action, name=None):
        """
        Register an action to be available globally.
        """
        name = name or action.__name__
        # Check/create short description
        if not hasattr(action, 'short_description'):
            action.short_description = action.__name__.replace("_", " ").capitalize()
        # Check/create applies-to filter
        if not hasattr(action, 'applies_to'):
            action.applies_to = lambda x: True
        self._actions[name] = action
        self._global_actions[name] = action

    def disable_action(self, name):
        """
        Disable a globally-registered action. Raises KeyError for invalid names.
        """
        del self._actions[name]

    def get_action(self, name):
        """
        Explicitally get a registered global action wheather it's enabled or
        not. Raises KeyError for invalid names.
        """
        return self._global_actions[name]

    def applicable_actions(self, fileobject):
        """
        Return a list of tuples (name, action) of actions applicable to a given fileobject.
        """
        res = []
        for name, action in self.actions:
            if action.applies_to(fileobject):
                res.append((name, action))
        return res

    @property
    def actions(self):
        """
        Get all the enabled actions as a list of (name, func). The list
        is sorted alphabetically by actions names
        """
        res = list(self._actions.items())
        res.sort(key=lambda name_func: name_func[0])
        return res

    @property
    def urls(self):
        "filebrowser.site URLs"
        return self.get_urls(), self.app_name, self.name

    def browse(self, request):
        "Browse Files/Directories."
        filter_re = []
        for exp in EXCLUDE:
            filter_re.append(re.compile(exp))

        # do not filter if VERSIONS_BASEDIR is being used
        if not VERSIONS_BASEDIR:
            for k, v in VERSIONS.items():
                exp = (r'_%s(%s)$') % (k, '|'.join(EXTENSION_LIST))
                filter_re.append(re.compile(exp, re.IGNORECASE))

        def filter_browse(item):
            "Defining a browse filter"
            filtered = item.filename.startswith('.')
            for re_prefix in filter_re:
                if re_prefix.search(item.filename):
                    filtered = True
            if filtered:
                return False
            return True

        query = request.GET.copy()
        path = u'%s' % os.path.join(self.directory, query.get('dir', ''))

        filelisting = self.filelisting_class(
            path,
            filter_func=filter_browse,
            sorting_by=query.get('o', DEFAULT_SORTING_BY),
            sorting_order=query.get('ot', DEFAULT_SORTING_ORDER),
            site=self)

        files = []
        if SEARCH_TRAVERSE and query.get("q"):
            listing = filelisting.files_walk_filtered()
        else:
            listing = filelisting.files_listing_filtered()

        # If we do a search, precompile the search pattern now
        do_search = query.get("q")
        if do_search:
            re_q = re.compile(query.get("q").lower(), re.M)

        filter_type = query.get('filter_type')
        filter_date = query.get('filter_date')

        for fileobject in listing:
            # date/type filter
            append = False
            if (not filter_type or fileobject.filetype == filter_type) and (not filter_date or get_filterdate(filter_date, fileobject.date or 0)):
                append = True
            # search
            if do_search and not re_q.search(fileobject.filename.lower()):
                append = False
            # append
            if append:
                files.append(fileobject)

        filelisting.results_total = len(listing)
        filelisting.results_current = len(files)

        p = Paginator(files, LIST_PER_PAGE)
        page_nr = request.GET.get('p', '1')
        try:
            page = p.page(page_nr)
        except (EmptyPage, InvalidPage):
            page = p.page(p.num_pages)

        request.current_app = self.name
        return TemplateResponse(request, 'filebrowser/index.html', dict(
            admin_site.each_context(request),
            **{
                'p': p,
                'page': page,
                'filelisting': filelisting,
                'query': query,
                'title': _(u'FileBrowser'),
                'is_popup': "pop" in request.GET,   # ChangeList uses "pop"
                'settings_var': get_settings_var(directory=self.directory),
                'breadcrumbs': get_breadcrumbs(query, query.get('dir', '')),
                'breadcrumbs_title': "",
                'filebrowser_site': self,
            }
        ))

    def createdir(self, request):
        "Create Directory"
        from filebrowser.forms import CreateDirForm
        query = request.GET
        path = u'%s' % os.path.join(self.directory, query.get('dir', ''))

        if request.method == 'POST':
            form = CreateDirForm(path, request.POST, filebrowser_site=self)
            if form.is_valid():
                server_path = os.path.join(path, form.cleaned_data['name'])
                try:
                    signals.filebrowser_pre_createdir.send(sender=request, path=server_path, name=form.cleaned_data['name'], site=self)
                    self.storage.makedirs(server_path)
                    signals.filebrowser_post_createdir.send(sender=request, path=server_path, name=form.cleaned_data['name'], site=self)
                    messages.add_message(request, messages.SUCCESS, _('The Folder %s was successfully created.') % form.cleaned_data['name'])
                    redirect_url = reverse("filebrowser:fb_browse", current_app=self.name) + query_helper(query, "ot=desc,o=date", "ot,o,filter_type,filter_date,q,p")
                    return HttpResponseRedirect(redirect_url)
                except OSError as e:
                    errno = e.args[0]
                    if errno == 13:
                        form.errors['name'] = forms.util.ErrorList([_('Permission denied.')])
                    else:
                        form.errors['name'] = forms.util.ErrorList([_('Error creating folder.')])
        else:
            form = CreateDirForm(path, filebrowser_site=self)

        request.current_app = self.name
        return TemplateResponse(request, 'filebrowser/createdir.html', dict(
            admin_site.each_context(request),
            **{
                'form': form,
                'query': query,
                'title': _(u'New Folder'),
                'is_popup': "pop" in request.GET,
                'settings_var': get_settings_var(directory=self.directory),
                'breadcrumbs': get_breadcrumbs(query, query.get('dir', '')),
                'breadcrumbs_title': _(u'New Folder'),
                'filebrowser_site': self
            }
        ))

    def upload(self, request):
        "Multipe File Upload."
        query = request.GET

        request.current_app = self.name
        return TemplateResponse(request, 'filebrowser/upload.html', dict(
            admin_site.each_context(request),
            **{
                'query': query,
                'title': _(u'Select files to upload'),
                'is_popup': "pop" in request.GET,
                'settings_var': get_settings_var(directory=self.directory),
                'breadcrumbs': get_breadcrumbs(query, query.get('dir', '')),
                'breadcrumbs_title': _(u'Upload'),
                'filebrowser_site': self
            }
        ))

    def delete_confirm(self, request):
        "Delete existing File/Directory."
        query = request.GET
        path = u'%s' % os.path.join(self.directory, query.get('dir', ''))
        fileobject = FileObject(os.path.join(path, query.get('filename', '')), site=self)
        if fileobject.filetype == "Folder":
            filelisting = self.filelisting_class(
                os.path.join(path, fileobject.filename),
                sorting_by=query.get('o', 'filename'),
                sorting_order=query.get('ot', DEFAULT_SORTING_ORDER),
                site=self)
            filelisting = filelisting.files_walk_total()
            if len(filelisting) > 100:
                additional_files = len(filelisting) - 100
                filelisting = filelisting[:100]
            else:
                additional_files = None
        else:
            filelisting = None
            additional_files = None

        request.current_app = self.name
        return TemplateResponse(request, 'filebrowser/delete_confirm.html', dict(
            admin_site.each_context(request),
            **{
                'fileobject': fileobject,
                'filelisting': filelisting,
                'additional_files': additional_files,
                'query': query,
                'title': _(u'Confirm delete'),
                'is_popup': "pop" in request.GET,
                'settings_var': get_settings_var(directory=self.directory),
                'breadcrumbs': get_breadcrumbs(query, query.get('dir', '')),
                'breadcrumbs_title': _(u'Confirm delete'),
                'filebrowser_site': self
            }
        ))

    def delete(self, request):
        "Delete existing File/Directory."
        query = request.GET
        path = u'%s' % os.path.join(self.directory, query.get('dir', ''))
        fileobject = FileObject(os.path.join(path, query.get('filename', '')), site=self)

        if request.GET:
            try:
                signals.filebrowser_pre_delete.send(sender=request, path=fileobject.path, name=fileobject.filename, site=self)
                fileobject.delete_versions()
                fileobject.delete()
                signals.filebrowser_post_delete.send(sender=request, path=fileobject.path, name=fileobject.filename, site=self)
                messages.add_message(request, messages.SUCCESS, _('Successfully deleted %s') % fileobject.filename)
            except OSError:
                # TODO: define error-message
                pass
        redirect_url = reverse("filebrowser:fb_browse", current_app=self.name) + query_helper(query, "", "filename,filetype")
        return HttpResponseRedirect(redirect_url)

    def detail(self, request):
        """
        Show detail page for a file.
        Rename existing File/Directory (deletes existing Image Versions/Thumbnails).
        """
        from filebrowser.forms import ChangeForm
        query = request.GET
        path = u'%s' % os.path.join(self.directory, query.get('dir', ''))
        fileobject = FileObject(os.path.join(path, query.get('filename', '')), site=self)

        if request.method == 'POST':
            form = ChangeForm(request.POST, path=path, fileobject=fileobject, filebrowser_site=self)
            if form.is_valid():
                new_name = form.cleaned_data['name']
                action_name = form.cleaned_data['custom_action']
                try:
                    action_response = None
                    if action_name:
                        action = self.get_action(action_name)
                        # Pre-action signal
                        signals.filebrowser_actions_pre_apply.send(sender=request, action_name=action_name, fileobject=[fileobject], site=self)
                        # Call the action to action
                        action_response = action(request=request, fileobjects=[fileobject])
                        # Post-action signal
                        signals.filebrowser_actions_post_apply.send(sender=request, action_name=action_name, fileobject=[fileobject], result=action_response, site=self)
                    if new_name != fileobject.filename:
                        signals.filebrowser_pre_rename.send(sender=request, path=fileobject.path, name=fileobject.filename, new_name=new_name, site=self)
                        fileobject.delete_versions()
                        self.storage.move(fileobject.path, os.path.join(fileobject.head, new_name))
                        signals.filebrowser_post_rename.send(sender=request, path=fileobject.path, name=fileobject.filename, new_name=new_name, site=self)
                        messages.add_message(request, messages.SUCCESS, _('Renaming was successful.'))
                    if isinstance(action_response, HttpResponse):
                        return action_response
                    if "_continue" in request.POST:
                        redirect_url = reverse("filebrowser:fb_detail", current_app=self.name) + query_helper(query, "filename=" + new_name, "filename")
                    else:
                        redirect_url = reverse("filebrowser:fb_browse", current_app=self.name) + query_helper(query, "", "filename")
                    return HttpResponseRedirect(redirect_url)
                except OSError:
                    form.errors['name'] = forms.util.ErrorList([_('Error.')])
        else:
            form = ChangeForm(initial={"name": fileobject.filename}, path=path, fileobject=fileobject, filebrowser_site=self)

        request.current_app = self.name
        return TemplateResponse(request, 'filebrowser/detail.html', dict(
            admin_site.each_context(request),
            **{
                'form': form,
                'fileobject': fileobject,
                'query': query,
                'title': u'%s' % fileobject.filename,
                'is_popup': "pop" in request.GET,
                'settings_var': get_settings_var(directory=self.directory),
                'breadcrumbs': get_breadcrumbs(query, query.get('dir', '')),
                'breadcrumbs_title': u'%s' % fileobject.filename,
                'filebrowser_site': self
            }
        ))

    def version(self, request):
        """
        Version detail.
        This just exists in order to select a version with a filebrowser–popup.
        """
        query = request.GET
        path = u'%s' % os.path.join(self.directory, query.get('dir', ''))
        fileobject = FileObject(os.path.join(path, query.get('filename', '')), site=self)

        request.current_app = self.name
        return TemplateResponse(request, 'filebrowser/version.html', dict(
            admin_site.each_context(request),
            **{
                'fileobject': fileobject,
                'query': query,
                'settings_var': get_settings_var(directory=self.directory),
                'filebrowser_site': self
            }
        ))

    def _upload_file(self, request):
        """
        Upload file to the server.

        If temporary is true, we upload to UPLOAD_TEMPDIR, otherwise
        we upload to site.directory
        """
        if request.method == "POST":
            folder = request.GET.get('folder', '')
            temporary = request.GET.get('temporary', '')
            temp_filename = None

            if len(request.FILES) == 0:
                return HttpResponseBadRequest('Invalid request! No files included.')
            if len(request.FILES) > 1:
                return HttpResponseBadRequest('Invalid request! Multiple files included.')

            filedata = list(request.FILES.values())[0]

            fb_uploadurl_re = re.compile(r'^.*(%s)' % reverse("filebrowser:fb_upload", current_app=self.name))
            folder = fb_uploadurl_re.sub('', folder)

            # temporary upload folder should be outside self.directory
            if folder == UPLOAD_TEMPDIR and temporary == "true":
                path = folder
            else:
                path = os.path.join(self.directory, folder)
            # we convert the filename before uploading in order
            # to check for existing files/folders
            file_name = convert_filename(filedata.name)
            filedata.name = file_name
            file_path = os.path.join(path, file_name)
            file_already_exists = self.storage.exists(file_path)

            # construct temporary filename by adding the upload folder, because
            # otherwise we don't have any clue if the file has temporary been
            # uploaded or not
            if folder == UPLOAD_TEMPDIR and temporary == "true":
                temp_filename = os.path.join(folder, file_name)

            # Check for name collision with a directory
            if file_already_exists and self.storage.isdir(file_path):
                ret_json = {'success': False, 'filename': file_name}
                return HttpResponse(json.dumps(ret_json))

            signals.filebrowser_pre_upload.send(sender=request, path=folder, file=filedata, site=self)
            uploadedfile = handle_file_upload(path, filedata, site=self)

            if file_already_exists and OVERWRITE_EXISTING:
                old_file = smart_str(file_path)
                new_file = smart_str(uploadedfile)
                self.storage.move(new_file, old_file, allow_overwrite=True)
                full_path = FileObject(smart_str(old_file), site=self).path_full
            else:
                file_name = smart_str(uploadedfile)
                filedata.name = os.path.relpath(file_name, path)
                full_path = FileObject(smart_str(file_name), site=self).path_full

            # set permissions
            if DEFAULT_PERMISSIONS is not None:
                os.chmod(full_path, DEFAULT_PERMISSIONS)

            f = FileObject(smart_str(file_name), site=self)
            signals.filebrowser_post_upload.send(sender=request, path=folder, file=f, site=self)

            # let Ajax Upload know whether we saved it or not
            ret_json = {
                'success': True,
                'filename': f.filename,
                'temp_filename': temp_filename,
                'url': f.url,
            }
            return HttpResponse(json.dumps(ret_json), content_type="application/json")

storage = DefaultStorage()
# Default FileBrowser site
site = FileBrowserSite(name='filebrowser', storage=storage)

# Default actions
from filebrowser.actions import flip_horizontal, flip_vertical, rotate_90_clockwise, rotate_90_counterclockwise, rotate_180
site.add_action(flip_horizontal)
site.add_action(flip_vertical)
site.add_action(rotate_90_clockwise)
site.add_action(rotate_90_counterclockwise)
site.add_action(rotate_180)
