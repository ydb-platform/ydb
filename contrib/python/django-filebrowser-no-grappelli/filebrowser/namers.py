from __future__ import unicode_literals
import re

import six
from django.utils.encoding import force_str
from django.utils.module_loading import import_string

from .settings import VERSIONS, VERSION_NAMER


def get_namer(**kwargs):
    namer_cls = import_string(VERSION_NAMER)
    return namer_cls(**kwargs)


class VersionNamer(object):
    "Base namer only for reference"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        for k, v in kwargs.items():
            setattr(self, k, v)

    def get_version_name(self):
        return self.file_object.filename_root + "_" + self.version_suffix + self.extension

    def get_original_name(self):
        tmp = self.file_object.filename_root.split("_")
        if tmp[len(tmp) - 1] in VERSIONS:
            return "%s%s" % (
                self.file_object.filename_root.replace("_%s" % tmp[len(tmp) - 1], ""),
                self.file_object.extension)


class OptionsNamer(VersionNamer):

    def get_version_name(self):
        name = "{root}_{options}{extension}".format(
            root=force_str(self.file_object.filename_root),
            options=self.options_as_string,
            extension=self.file_object.extension,
        )
        return name

    def get_original_name(self):
        """
        Restores the original file name wipping out the last
        `_version_suffix--plus-any-configs` block entirely.
        """
        root = self.file_object.filename_root
        tmp = root.split("_")
        options_part = tmp[len(tmp) - 1]
        name = re.sub('_%s$' % options_part, '', root)
        return "%s%s" % (name, self.file_object.extension)

    @property
    def options_as_string(self):
        """
        The options part should not contain `_` (underscore) on order to get
        original name back.
        """
        name = '--'.join(self.options_list).replace(',', 'x')
        name = re.sub(r'[_\s]', '-', name)
        return re.sub(r'[^\w-]', '', name).strip()

    @property
    def options_list(self):
        opts = []
        if not self.options:
            return opts

        if 'version_suffix' in self.kwargs:
            opts.append(self.kwargs['version_suffix'])

        if 'size' in self.options:
            opts.append('%sx%s' % tuple(self.options['size']))
        elif 'width' in self.options or 'height' in self.options:
            width = float(self.options.get('width') or 0)
            height = float(self.options.get('height') or 0)
            opts.append('%dx%d' % (width, height))

        for k, v in sorted(self.options.items()):
            if not v or k in ('size', 'width', 'height',
                              'quality', 'subsampling', 'verbose_name'):
                continue
            if v is True:
                opts.append(k)
                continue
            if not isinstance(v, six.string_types):
                try:
                    v = 'x'.join([six.text_type(v) for item in v])
                except TypeError:
                    v = six.text_type(v)
            opts.append('%s-%s' % (k, v))

        return opts
