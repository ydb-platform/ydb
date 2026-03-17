# coding: utf-8

import os
import re

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from six.moves import input

from filebrowser.base import FileListing
from filebrowser.settings import EXTENSION_LIST, EXCLUDE, DIRECTORY, VERSIONS


filter_re = []
for exp in EXCLUDE:
    filter_re.append(re.compile(exp))
for k, v in VERSIONS.items():
    exp = (r'_%s(%s)') % (k, '|'.join(EXTENSION_LIST))
    filter_re.append(re.compile(exp))


class Command(BaseCommand):
    help = "(Re)Generate image versions."

    def add_arguments(self, parser):
        parser.add_argument('media_path', nargs='?', default=DIRECTORY)

    def handle(self, *args, **options):
        path = options['media_path']

        if not os.path.isdir(os.path.join(settings.MEDIA_ROOT, path)):
            raise CommandError('<media_path> must be a directory in MEDIA_ROOT (If you don\'t add a media_path the default path is DIRECTORY).\n"%s" is no directory.' % path)

        # get version name
        while 1:
            self.stdout.write('\nSelect a version you want to generate:\n')
            for version in VERSIONS:
                self.stdout.write(' * %s\n' % version)

            version_name = input('(leave blank to generate all versions): ')

            if version_name == "":
                selected_version = None
                break
            else:
                try:
                    tmp = VERSIONS[version_name]
                    selected_version = version_name
                    break
                except:
                    self.stderr.write('Error: Version "%s" doesn\'t exist.\n' % version_name)
                    version_name = None
                    continue

        # filelisting
        filelisting = FileListing(path, filter_func=self.filter_images)  # FIXME filterfunc: no hidden files, exclude list, no versions, just images!
        for fileobject in filelisting.files_walk_filtered():
            if fileobject.filetype == "Image":
                if selected_version:
                    self.stdout.write('generating version "%s" for: %s\n' % (selected_version, fileobject.path))
                    versionobject = fileobject.version_generate(selected_version)  # FIXME force?
                else:
                    self.stdout.write('generating all versions for: %s\n' % fileobject.path)
                    for version in VERSIONS:
                        versionobject = fileobject.version_generate(version)  # FIXME force?

        # # walkt throu the filebrowser directory
        # # for all/new files (except file versions itself and excludes)
        # for dirpath,dirnames,filenames in os.walk(path, followlinks=True):
        #     rel_dir = os.path.relpath(dirpath, os.path.realpath(settings.MEDIA_ROOT))
        #     for filename in filenames:
        #         filtered = False
        #         # no "hidden" files (stating with ".")
        #         if filename.startswith('.'):
        #             continue
        #         # check the exclude list
        #         for re_prefix in filter_re:
        #             if re_prefix.search(filename):
        #                 filtered = True
        #         if filtered:
        #             continue
        #         (tmp, extension) = os.path.splitext(filename)
        #         if extension in EXTENSIONS["Image"]:
        #             self.createVersions(os.path.join(rel_dir, filename), selected_version)

    def filter_images(self, item):
        filtered = item.filename.startswith('.')
        for re_prefix in filter_re:
            if re_prefix.search(item.filename):
                filtered = True
        if filtered:
            return False
        return True
