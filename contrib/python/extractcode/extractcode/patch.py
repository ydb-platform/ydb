#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/extractcode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import posixpath
import logging
import os.path

from commoncode import paths
from commoncode import fileutils
from commoncode import text
import typecode.contenttype

import extractcode
from extractcode import ExtractErrorFailedToExtract

"""
Low level utilities to parse patch files and treat them as if they were
archives containing files. Patches do not really contain files but changes to
files that would patched.

This way of dealing with patches helps handling patches with mixed origins
more conveniently.
"""

LOG = logging.getLogger(__name__)


def extract(location, target_dir):
    """
    Extract each patch of a patch file at `location` as files in a target_dir
    directory tree mimicking the directory in which the patches would be
    applied with the patch command.

    This treats a patch file as if it were an archive containing one file for
    each patch applied to a file to be patched.

    Return a list of warning messages. Raise Exceptionon errors.
    """
    for source, target, text in patch_info(location):
        # prefer the target path for writing the patch text to a subfile
        # unless target is /dev/null (a deletion)
        if '/dev/null' in target:
            patch_subfile_path = source
        else:
            patch_subfile_path = target

        # make the path safe to use as a subfile path
        # ensure this a good and clean posix relative path
        patch_subfile_path = paths.safe_path(patch_subfile_path, preserve_spaces=True)

        # create directories
        parent_dir = posixpath.dirname(patch_subfile_path)
        parent_target_dir = os.path.join(target_dir, parent_dir)
        fileutils.create_dir(parent_target_dir)

        # find a unique name using a simple counter
        base_subfile_path = os.path.join(target_dir, patch_subfile_path)
        counter = 0
        fp = base_subfile_path
        while os.path.exists(fp + extractcode.EXTRACT_SUFFIX):
            fp = base_subfile_path + '_%d' % counter
            counter += 1
        base_subfile_path = fp

        # write the location proper, with a suffix extension to avoid
        # recursive extraction
        subfile_path = base_subfile_path + extractcode.EXTRACT_SUFFIX
        with open(subfile_path, 'w') as subfile:
            subfile.write('\n'.join(text))

        return []


def is_patch(location, include_extracted=False):
    """
    Test if a file is a possible patch file. May return True for some files
    that are not patches. Extracted patch files are ignored by default.
    """
    T = typecode.contenttype.get_type(location)
    file_name = fileutils.file_name(location)
    patch_like = (
        'diff ' in T.filetype_file.lower()
        or '.diff' in file_name
        or '.patch' in file_name
    )

    if not patch_like:
        return False

    if extractcode.is_extraction_path(file_name) and not include_extracted:
        return False

    return True


def patch_text(ptch):
    """
    Return the patch text content as an iterable of lines given a ptch 'patch
    item'.

    The content is re-formatted as unified diff.
    """
    for head in ptch.header:
        yield head
    yield '--- ' + fileutils.as_posixpath(ptch.source)
    yield '+++ ' + fileutils.as_posixpath(ptch.target)
    hk = '@@ -%(startsrc)d,%(linessrc)d +%(starttgt)d,%(linestgt)d @@ %(desc)s'

    def hunk_data(hnk):
        return dict(
            startsrc=hnk.startsrc,
            linessrc=hnk.linessrc,
            starttgt=hnk.starttgt,
            linestgt=hnk.linestgt,
            desc=text.as_unicode(hnk.desc),
        )

    for hunk in ptch.hunks:
        yield hk % hunk_data(hunk)
        for line in hunk.text:
            yield line


def patch_info(location):
    """
    Yield an iterable of tuples of (src_path, target_path, patch_text)
    for each patch segment of a patch file at location.

    Raise an exception if the file is not a patch file or cannot be parsed.
    """
    import patch as pythonpatch
    patchset = pythonpatch.fromfile(location)
    if not patchset:
        msg = 'Unable to parse patch file: %(location)s' % locals()
        raise ExtractErrorFailedToExtract(msg)

    for ptch in patchset.items:
        src = fileutils.as_posixpath(ptch.source.strip())
        tgt = fileutils.as_posixpath(ptch.target.strip())
        text = [l.strip() for l in patch_text(ptch) if l]
        yield src, tgt, text
