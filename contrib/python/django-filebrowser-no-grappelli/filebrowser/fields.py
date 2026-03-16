# coding: utf-8
import os

from django import forms
try:
    from django.urls import reverse
except ImportError:
    from django.core.urlresolvers import reverse
from django.db.models.fields import CharField
from django.forms.widgets import Input
from django.template.loader import render_to_string
from django.templatetags.static import static
from django.utils.translation import gettext_lazy as _
from django.contrib.admin.options import FORMFIELD_FOR_DBFIELD_DEFAULTS
from six import string_types

from filebrowser.base import FileObject
from filebrowser.settings import ADMIN_THUMBNAIL, EXTENSIONS, UPLOAD_TEMPDIR
from filebrowser.sites import site


class FileBrowseWidget(Input):
    input_type = 'text'

    class Media:
        js = ('filebrowser/js/AddFileBrowser.js',)

    def __init__(self, attrs={}):
        super(FileBrowseWidget, self).__init__(attrs)
        self.site = attrs.get('filebrowser_site', None)
        self.directory = attrs.get('directory', '')
        self.extensions = attrs.get('extensions', '')
        self.format = attrs.get('format', '')
        if attrs is not None:
            self.attrs = attrs.copy()
        else:
            self.attrs = {}
        super(FileBrowseWidget, self).__init__(attrs)

    def render(self, name, value, attrs=None, renderer=None):
        url = reverse(self.site.name + ":fb_browse")
        if value is None:
            value = ""
        if value != "" and not isinstance(value, FileObject):
            value = FileObject(value, site=self.site)
        final_attrs = self.build_attrs(attrs, type=self.input_type, name=name)
        final_attrs['search_icon'] = static('filebrowser/img/filebrowser_icon_show.gif')
        final_attrs['url'] = url
        final_attrs['directory'] = self.directory
        final_attrs['extensions'] = self.extensions
        final_attrs['format'] = self.format
        final_attrs['ADMIN_THUMBNAIL'] = ADMIN_THUMBNAIL
        final_attrs['data_attrs'] = {k: v for k, v in final_attrs.items() if k.startswith('data-')}
        filebrowser_site = self.site
        if value != "":
            try:
                final_attrs['directory'] = os.path.split(value.original.path_relative_directory)[0]
            except:
                pass
        return render_to_string("filebrowser/custom_field.html", locals())

    def build_attrs(self, extra_attrs=None, **kwargs):
        "Helper function for building an attribute dictionary."
        attrs = dict(self.attrs, **kwargs)
        if extra_attrs:
            attrs.update(extra_attrs)
        return attrs


class FileBrowseFormField(forms.CharField):

    default_error_messages = {
        'extension': _(u'Extension %(ext)s is not allowed. Only %(allowed)s is allowed.'),
    }

    def __init__(self, max_length=None, min_length=None, site=None, directory=None, extensions=None, format=None, *args, **kwargs):
        self.max_length, self.min_length = max_length, min_length
        self.site = kwargs.pop('filebrowser_site', site)
        self.directory = directory
        self.extensions = extensions
        if format:
            self.format = format or ''
            self.extensions = extensions or EXTENSIONS.get(format)
        super(FileBrowseFormField, self).__init__(*args, **kwargs)

    def clean(self, value):
        value = super(FileBrowseFormField, self).clean(value)
        if not value:
            return value
        file_extension = os.path.splitext(value)[1].lower()
        if self.extensions and file_extension not in self.extensions:
            raise forms.ValidationError(self.error_messages['extension'] % {'ext': file_extension, 'allowed': ", ".join(self.extensions)})
        return value


class FileBrowseField(CharField):
    description = "FileBrowseField"

    def __init__(self, *args, **kwargs):
        self.site = kwargs.pop('filebrowser_site', site)
        self.directory = kwargs.pop('directory', '')
        self.extensions = kwargs.pop('extensions', '')
        self.format = kwargs.pop('format', '')
        return super(FileBrowseField, self).__init__(*args, **kwargs)

    def to_python(self, value):
        if not value or isinstance(value, FileObject):
            return value
        return FileObject(value, site=self.site)

    def from_db_value(self, value, *args, **kwargs):
        return self.to_python(value)

    def get_prep_value(self, value):
        if not value or isinstance(value, string_types):
            return value
        return value.path

    def value_to_string(self, obj):
        value = self.value_from_object(obj)
        if not value:
            return value
        return value.path

    def formfield(self, **kwargs):
        widget_class = kwargs.get('widget', FileBrowseWidget)
        attrs = {}
        attrs["filebrowser_site"] = self.site
        attrs["directory"] = self.directory
        attrs["extensions"] = self.extensions
        attrs["format"] = self.format
        defaults = {
            'form_class': FileBrowseFormField,
            'widget': widget_class(attrs=attrs),
            'filebrowser_site': self.site,
            'directory': self.directory,
            'extensions': self.extensions,
            'format': self.format
        }
        return super(FileBrowseField, self).formfield(**defaults)

FORMFIELD_FOR_DBFIELD_DEFAULTS[FileBrowseField] = {'widget': FileBrowseWidget}


class FileBrowseUploadWidget(Input):
    input_type = 'text'

    class Media:
        js = ('filebrowser/js/AddFileBrowser.js', 'filebrowser/js/fileuploader.js',)
        css = {
            'all': (os.path.join('/static/filebrowser/css/uploadfield.css'),)
        }

    def __init__(self, attrs=None):
        super(FileBrowseUploadWidget, self).__init__(attrs)
        self.site = attrs.get('site', '')
        self.directory = attrs.get('directory', '')
        self.extensions = attrs.get('extensions', '')
        self.format = attrs.get('format', '')
        self.upload_to = attrs.get('upload_to', '')
        self.temp_upload_dir = attrs.get('temp_upload_dir', '')
        if attrs is not None:
            self.attrs = attrs.copy()
        else:
            self.attrs = {}
        super(FileBrowseUploadWidget, self).__init__(attrs)

    def render(self, name, value, attrs=None, renderer=None):
        url = reverse(self.site.name + ":fb_browse")
        if value is None:
            value = ""
        if value != "" and not isinstance(value, FileObject):
            value = FileObject(value, site=self.site)
        final_attrs = self.build_attrs(attrs, type=self.input_type, name=name)
        final_attrs['search_icon'] = '/static/filebrowser/img/filebrowser_icon_show.gif'
        final_attrs['url'] = url
        final_attrs['directory'] = self.directory
        final_attrs['extensions'] = self.extensions
        final_attrs['format'] = self.format
        final_attrs['upload_to'] = self.upload_to
        final_attrs['temp_upload_dir'] = UPLOAD_TEMPDIR
        final_attrs['ADMIN_THUMBNAIL'] = ADMIN_THUMBNAIL
        if value != "":
            try:
                final_attrs['directory'] = os.path.split(value.original.path_relative_directory)[0]
            except:
                pass
        return render_to_string("filebrowser/custom_upload_field.html", locals())

    def build_attrs(self, extra_attrs=None, **kwargs):
        "Helper function for building an attribute dictionary."
        attrs = dict(self.attrs, **kwargs)
        if extra_attrs:
            attrs.update(extra_attrs)
        return attrs


class FileBrowseUploadFormField(forms.CharField):

    default_error_messages = {
        'extension': _(u'Extension %(ext)s is not allowed. Only %(allowed)s is allowed.'),
    }

    def __init__(self, max_length=None, min_length=None, site=None, directory=None, extensions=None, format=None, upload_to=None, temp_upload_dir=None, *args, **kwargs):
        self.max_length, self.min_length = max_length, min_length
        self.site = site
        self.directory = directory
        self.extensions = extensions
        if format:
            self.format = format or ''
            self.extensions = extensions or EXTENSIONS.get(format)
        self.upload_to = upload_to
        self.temp_upload_dir = temp_upload_dir
        super(FileBrowseUploadFormField, self).__init__(*args, **kwargs)

    def clean(self, value):
        value = super(FileBrowseUploadFormField, self).clean(value)
        if value == '':
            return value
        file_extension = os.path.splitext(value)[1].lower()
        if self.extensions and file_extension not in self.extensions:
            raise forms.ValidationError(self.error_messages['extension'] % {'ext': file_extension, 'allowed': ", ".join(self.extensions)})
        return value


class FileBrowseUploadField(CharField):
    """
    Model field which renders with an option to browse site.directory as well
    as upload a file to a temporary folder (you still need to somehow move that
    temporary file to an actual location when the model is being saved).
    """

    description = "FileBrowseUploadField"

    def __init__(self, *args, **kwargs):
        self.site = kwargs.pop('site', site)
        self.directory = kwargs.pop('directory', '')
        self.extensions = kwargs.pop('extensions', '')
        self.format = kwargs.pop('format', '')
        self.upload_to = kwargs.pop('upload_to', '')
        self.temp_upload_dir = kwargs.pop('temp_upload_dir', '')
        return super(FileBrowseUploadField, self).__init__(*args, **kwargs)

    def from_db_value(self, value, *args, **kwargs):
        return self.to_python(value)

    def to_python(self, value):
        if not value or isinstance(value, FileObject):
            return value
        return FileObject(value, site=self.site)

    def get_prep_value(self, value):
        if not value or isinstance(value, string_types):
            return value
        return value.path

    def value_to_string(self, obj):
        value = self.value_from_object(obj)
        if not value:
            return value
        return value.path

    def formfield(self, **kwargs):
        attrs = {}
        attrs["site"] = self.site
        attrs["directory"] = self.directory
        attrs["extensions"] = self.extensions
        attrs["format"] = self.format
        attrs["upload_to"] = self.upload_to
        attrs["temp_upload_dir"] = self.temp_upload_dir
        defaults = {
            'form_class': FileBrowseUploadFormField,
            'widget': FileBrowseUploadWidget(attrs=attrs),
            'site': self.site,
            'directory': self.directory,
            'extensions': self.extensions,
            'format': self.format,
            'upload_to': self.upload_to,
            'temp_upload_dir': self.temp_upload_dir
        }
        return super(FileBrowseUploadField, self).formfield(**defaults)


try:
    from south.modelsinspector import add_introspection_rules
    add_introspection_rules([], [r"^filebrowser\.fields\.FileBrowseField"])
    add_introspection_rules([], [r"^filebrowser\.fields\.FileBrowseUploadField"])
except:
    pass
