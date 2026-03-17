# coding: utf-8
import os
import re
import sys

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from six.moves import input

from filebrowser.settings import EXCLUDE, EXTENSIONS


class Command(BaseCommand):
    args = '<media_path>'
    help = "Remove Image-Versions within FILEBROWSER_DIRECTORY/MEDIA_ROOT."

    def handle(self, *args, **options):

        media_path = ""

        if len(args):
            media_path = args[0]

        path = os.path.join(settings.MEDIA_ROOT, media_path)

        if not os.path.isdir(path):
            raise CommandError('<media_path> must be a directory in MEDIA_ROOT. "%s" is no directory.' % path)

        self.stdout.write("\n%s\n" % self.help)
        self.stdout.write("in this case: %s\n" % path)

        # get suffix or prefix
        default_prefix_or_suffix = "s"
        while 1:
            self.stdout.write('\nOlder versions of the FileBrowser used to prefix the filename with the version name.\n')
            self.stdout.write('Current version of the FileBrowser adds the version name as suffix.\n')
            prefix_or_suffix = input('"p" for prefix or "s" for suffix (leave blank for "%s"): ' % default_prefix_or_suffix)

            if default_prefix_or_suffix and prefix_or_suffix == '':
                prefix_or_suffix = default_prefix_or_suffix
            if prefix_or_suffix != "s" and prefix_or_suffix != "p":
                sys.stderr.write('Error: "p" and "s" are the only valid inputs.\n')
                prefix_or_suffix = None
                continue
            break

        # get version name
        while 1:
            version_name = input('\nversion name as defined with VERSIONS: ')

            if version_name == "":
                self.stderr.write('Error: You have to enter a version name.\n')
                version_name = None
                continue
            else:
                break

        # get list of all matching files
        files = self.get_files(path, version_name, (prefix_or_suffix == "p"))

        # output (short version) of files to be deleted
        if len(files) > 15:
            self.stdout.write('\nFirst/Last 5 files to remove:\n')
            for current_file in files[:5]:
                self.stdout.write('%s\n' % current_file)
            self.stdout.write('...\n')
            self.stdout.write('...\n')
            for current_file in files[len(files) - 5:]:
                self.stdout.write('%s\n' % current_file)
        else:
            self.stdout.write('\nFiles to remove:\n')
            for current_file in files:
                self.stdout.write('%s\n' % current_file)

        # no files...done
        if len(files) == 0:
            self.stdout.write('0 files removed.\n\n')
            return
        else:
            self.stdout.write('%d file(s) will be removed.\n\n' % len(files))

        # ask to make sure
        do_remove = ""
        self.stdout.write('Are Sure you want to delete these files?\n')
        do_remove = input('"y" for Yes or "n" for No (leave blank for "n"): ')

        # if "yes" we delete. any different case we finish without removing anything
        if do_remove == "y":
            for current_file in files:
                os.remove(current_file)
            self.stdout.write('%d file(s) removed.\n\n' % len(files))
        else:
            self.stdout.write('No files removed.\n\n')
        return

    # get files mathing:
    # path: search recoursive in this path (os.walk)
    # version_name: string is pre/suffix of filename
    # search_for_prefix: if true we match against the start of the filename (default is the end)
    def get_files(self, path, version_name, search_for_prefix):
        file_list = []
        # Precompile regular expressions
        filter_re = []
        for exp in EXCLUDE:
            filter_re.append(re.compile(exp))

        # walkt throu the filebrowser directory
        # for all/new files (except file versions itself and excludes)
        for dirpath, dirnames, filenames in os.walk(path, followlinks=True):
            for filename in filenames:
                filtered = False
                # no "hidden" files (stating with ".")
                if filename.startswith('.'):
                    continue
                # check the exclude list
                for re_prefix in filter_re:
                    if re_prefix.search(filename):
                        filtered = True
                if filtered:
                    continue
                (filename_noext, extension) = os.path.splitext(filename)
                # images only
                if extension in EXTENSIONS["Image"]:
                    # if image matches with version_name we add it to the file_list
                    if search_for_prefix:
                        if filename_noext.startswith(version_name + "_"):
                            file_list.append(os.path.join(dirpath, filename))
                    elif filename_noext.endswith("_" + version_name):
                        file_list.append(os.path.join(dirpath, filename))

        return file_list
