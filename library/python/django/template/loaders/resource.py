# coding: utf8
from __future__ import unicode_literals, absolute_import, division, print_function

import os

from django.template import Origin, TemplateDoesNotExist
from django.template.loaders.filesystem import Loader as FileSystemLoader
try:
    from django.utils.encoding import force_text
except ImportError:
    from django.utils.encoding import force_str as force_text

from library.python import resource


class Loader(FileSystemLoader):

    def __init__(self, engine, dirs=None):
        super(Loader, self).__init__(engine)
        self.dirs = dirs

    def get_contents(self, origin):
        res = resource.resfs_read(origin.name)
        if res is None:
            raise TemplateDoesNotExist(origin)
        return res.decode(self.engine.file_charset)

    def get_template_sources(self, template_name, template_dirs=None):
        """
        Return an Origin object pointing to an absolute path in each directory
        in template_dirs. For security reasons, if a path doesn't lie inside
        one of the template_dirs it is excluded from the result set.
        """
        if not template_dirs:
            template_dirs = self.get_dirs()

        for template_dir in template_dirs:
            name = os.path.join(force_text(template_dir), force_text(template_name))
            yield Origin(
                name=name,
                template_name=template_name,
                loader=self,
            )
