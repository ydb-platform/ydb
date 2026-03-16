#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/extractcode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import logging
import traceback

from collections import namedtuple
from functools import partial
from os.path import abspath
from os.path import expanduser
from os.path import join

from commoncode import fileutils
from commoncode import ignore

import extractcode  # NOQA
import extractcode.archive

logger = logging.getLogger(__name__)
TRACE = False

if TRACE:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

"""
Extract archives and compressed files recursively to get the file content
available for further processing. This the high level extraction entry point.

This is NOT a general purpose un-archiver. The code tries hard to do the right
thing, BUT the extracted files are not meant to be something that can be
faithfully re-archived to get an equivalent archive. The purpose instead is to
extract the content of the archives as faithfully and safely as possible to make
this content available for scanning: some paths may be altered. Some files may
be altered or skipped entirely.

In particular:

 - Permissions and owners stored in archives are ignored entirely: The extracted
   content is always owned and readable by the user who ran the extraction.

 - Special files are never extracted (such as FIFO, character devices, etc)

 - Symlinks may be replaced by plain file copies as if they were regular files.
   Hardlinks may be recreated as regular files, not as hardlinks to the original
   files.

 - Files and directories may be renamed when their name is a duplicate. And a
   name may be considered a duplicate ignore upper and lower case mixes even
   on case-sensitive file systems. In particular when an archive contains the
   same file path several times, every paths will be extracted with different
   files names, even though using a regular tool for extraction would have
   overwritten previous paths with the last path.

 - Paths may be converted to a safe ASCII alternative that is portable across
   OSes.

 - Symlinks, relative paths and absolute paths pointing outside of the archive
   are replaced and renamed in such a way that all the extract content of an
   archive exist under a single target extraction directory. This process
   includes eventually creating "synthetic" or dummy paths that did not exist in
   the original archive.
"""

"""
An ExtractEvent contains data about an archive extraction progress:
 - `source` is the location of the archive being extracted
 - `target` is the target location where things are extracted
 - `done` is a boolean set to True when the extraction is done (even if failed).
 - `warnings` is a mapping of extracted paths to a list of warning messages.
 - `errors` is a list of error messages.
"""
ExtractEvent = namedtuple('ExtractEvent', 'source target done warnings errors')


def extract(
    location,
    kinds=extractcode.default_kinds,
    recurse=False,
    replace_originals=False,
    ignore_pattern=(),
):
    """
    Walk and extract any archives found at ``location`` (either a file or
    directory). Extract only archives of a kind listed in the ``kinds`` kind
    tuple.

    Return an iterable of ExtractEvent for each extracted archive. This can be
    used to track extraction progress:

     - one event is emitted just before extracting an archive. The ExtractEvent
       warnings and errors are empty. The "done" flag is False.

     - one event is emitted right after extracting an archive. The ExtractEvent
       warnings and errors contains warnings and errors if any. The "done" flag
       is True.

    If ``recurse`` is True, extract recursively archives nested inside other
    archives. If ``recurse`` is false, then do not extract further an already
    extracted archive identified by the corresponding extract suffix location.

    If ``replace_originals`` is True, the extracted archives are replaced by the
    extracted content, only if the extraction was successful.

    ``ignore_pattern`` is a list of glob patterns to ignore.

    Note that while the original filesystem is walked top-down, breadth-first,
    if ``recurse`` and a nested archive is found, it is extracted first
    recursively and at full depth-first before resuming the filesystem walk.
    """

    extract_events = extract_files(
        location=location,
        kinds=kinds,
        recurse=recurse,
        ignore_pattern=ignore_pattern,
    )

    processed_events = []
    processed_events_append = processed_events.append
    for event in extract_events:
        yield event
        if replace_originals:
            processed_events_append(event)

    # move files around when done, unless there are errors
    if replace_originals:
        for xevent in reversed(processed_events):
            if xevent.done and not (event.warnings or event.errors):
                source = xevent.source
                target = xevent.target
                if TRACE:
                    logger.debug(
                        f'extract:replace_originals: replacing '
                        f'{source!r} by {target!r}'
                    )
                fileutils.delete(source)
                fileutils.copytree(target, source)
                fileutils.delete(target)


def extract_files(
    location,
    kinds=extractcode.default_kinds,
    recurse=False,
    ignore_pattern=(),
):
    """
    Extract the files found at `location`.

    Extract only archives of a kind listed in the `kinds` kind tuple.

    If `recurse` is True, extract recursively archives nested inside other
    archives.

    If `recurse` is false, then do not extract further an already
    extracted archive identified by the corresponding extract suffix location.

    ``ignore_pattern`` is a list of glob patterns to ignore.
    """
    ignored = partial(ignore.is_ignored, ignores=ignore.default_ignores, unignores={})
    if TRACE:
        logger.debug('extract:start: %(location)r recurse: %(recurse)r\n' % locals())

    abs_location = abspath(expanduser(location))
    for top, dirs, files in fileutils.walk(abs_location, ignored):
        if TRACE:
            logger.debug(
                'extract:walk: top: %(top)r dirs: %(dirs)r files: r(files)r' % locals())

        if not recurse:
            if TRACE:
                drs = set(dirs)
            for d in dirs[:]:
                if extractcode.is_extraction_path(d):
                    dirs.remove(d)
            if TRACE:
                rd = repr(drs.symmetric_difference(set(dirs)))
                logger.debug(f'extract:walk: not recurse: removed dirs: {rd}')

        for f in files:
            loc = join(top, f)
            if not recurse and extractcode.is_extraction_path(loc):
                if TRACE:
                    logger.debug(
                        'extract:walk not recurse: skipped  file: %(loc)r' % locals())
                continue

            if not extractcode.archive.should_extract(
                location=loc,
                kinds=kinds,
                ignore_pattern=ignore_pattern
            ):
                if TRACE:
                    logger.debug(
                        'extract:walk: skipped file: not should_extract: %(loc)r' % locals())
                continue

            target = join(abspath(top), extractcode.get_extraction_path(loc))
            if TRACE:
                logger.debug('extract:target: %(target)r' % locals())

            # extract proper
            for xevent in extract_file(
                location=loc,
                target=target,
                kinds=kinds,
            ):
                if TRACE:
                    logger.debug('extract:walk:extraction event: %(xevent)r' % locals())
                yield xevent

            if recurse:
                if TRACE:
                    logger.debug('extract:walk: recursing on target: %(target)r' % locals())
                for xevent in extract(
                    location=target,
                    kinds=kinds,
                    recurse=recurse,
                    ignore_pattern=ignore_pattern,
                ):
                    if TRACE:
                        logger.debug('extract:walk:recurse:extraction event: %(xevent)r' % locals())
                    yield xevent


def extract_file(
    location,
    target,
    kinds=extractcode.default_kinds,
    verbose=False,
    *args,
    **kwargs,
):
    """
    Extract a single archive file at ``location`` to the ``target`` directory if
    this file is of a kind supported in the ``kinds`` kind tuple. Yield
    ExtractEvents. Does not extract recursively.
    """
    warnings = []
    errors = []
    extractor = extractcode.archive.get_extractor(
        location=location,
        kinds=kinds,
    )

    if TRACE:
        emodule = getattr(extractor, '__module__', '')
        ename = getattr(extractor, '__name__', '')
        logger.debug(
            f'extract_file: extractor: for: {location} with kinds: '
            f'{kinds}: {emodule}.{ename}'
        )

    if extractor:
        yield ExtractEvent(
            source=location,
            target=target,
            done=False,
            warnings=[],
            errors=[],
        )

        try:
            # Extract first to a temp directory: if there is an error, the
            # extracted files will not be moved to the target.
            tmp_tgt = fileutils.get_temp_dir(prefix='extractcode-extract-')
            abs_location = abspath(expanduser(location))
            warns = extractor(abs_location, tmp_tgt) or []
            warnings.extend(warns)
            fileutils.copytree(tmp_tgt, target)
            fileutils.delete(tmp_tgt)

        except Exception as e:
            errors = [str(e).strip(' \'"')]
            if verbose:
                errors.append(traceback.format_exc())
            if TRACE:
                tb = traceback.format_exc()
                logger.debug(
                    f'extract_file: ERROR: {location}: {errors}\n{e}\n{tb}')

        finally:
            yield ExtractEvent(
                source=location,
                target=target,
                done=True,
                warnings=warnings,
                errors=errors,
            )
