#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/extractcode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import logging
import os
from collections import namedtuple

from commoncode import fileutils
from commoncode import filetype
from commoncode import functional
from commoncode.ignore import is_ignored
from typecode import contenttype

from extractcode import all_kinds
from extractcode import regular
from extractcode import package
from extractcode import docs
from extractcode import regular_nested
from extractcode import file_system
from extractcode import patches
from extractcode import special_package

from extractcode import libarchive2
from extractcode import patch
from extractcode import sevenzip
from extractcode import vmimage

from extractcode.uncompress import uncompress_gzip
from extractcode.uncompress import uncompress_bzip2

logger = logging.getLogger(__name__)
TRACE = False
TRACE_DEEP = False

if TRACE:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

"""
Archive formats handling. The purpose of this module is to select an extractor
suitable for the accurate extraction of a given kind of archive. An extractor is
a function that can read an archive and extract it to a directory. Multiple
extractors functions can be called in sequence to handle nested archives such
as tar.gz.

A handler contains selection criteria and a list of extractors.
We select an extraction handler based on these croiteria:
 - file type,
 - mime type,
 - file extension,
 - kind of archive.

Several handlers may be suitable candidates for extraction of a given archive.
Candidates are scored and the best one picked which is typically the most
specific and the one capable of doing the deepest extraction of a given archive.

At the lowest level, archives are processed by standard library code (sometimes
patched) or native code (libarchive, 7zip).

For background on archive and compressed file formats see:
 - http://en.wikipedia.org/wiki/List_of_archive_formats
 - http://en.wikipedia.org/wiki/List_of_file_formats#Archive_and_compressed
"""

# if strict, all handlers criteria must be matched for a handler to be selected
Handler = namedtuple(
    'Handler',
    [
        'name',
        'filetypes',
        'mimetypes',
        'extensions',
        'kind',
        'extractors',
        'strict',
    ]
)


def can_extract(location):
    """
    Return True if this location can be extracted by some handler.
    """
    handlers = list(get_handlers(location))
    if handlers:
        return True
    else:
        return False


def should_extract(location, kinds, ignore_pattern=()):
    """
    Return True if this ``location`` should be extracted based on the provided
    ``kinds`` tuple and an ``ignore_pattern`` list of glob patterns.
    """
    location = os.path.abspath(os.path.expanduser(location))
    ignore_pattern = {
        extension : 'User ignore: Supplied by --ignore'
        for extension in ignore_pattern
    }
    should_ignore = is_ignored(location, ignore_pattern)
    extractor = get_extractor(location, kinds=kinds)

    if TRACE_DEEP:
        logger.debug(
            f'  should_extract: extractor: {extractor}, '
            f'should_ignore: {should_ignore}'
        )

    if extractor and not should_ignore:
        return True


def get_extractor(location, kinds=all_kinds):
    """
    Return an extraction callable that can extract the file at ``location`` or
    None if no extraction callable function is found.
    Limit the search for an extractor to the ``kinds`` list of archive kinds.
    See  extractcode.all_kinds for details.

    An extraction callable should accept these arguments:
    - location of the file to extract
    - target_dir where to extract
    It should extract files from the `location` in the `target_dir` directory.
    It must return a list of warning messages if any or an empty list.
    It must raise Exceptions on errors.
    """
    assert location
    location = os.path.abspath(os.path.expanduser(location))
    extractors = get_extractors(location, kinds=kinds)
    if not extractors:
        if TRACE_DEEP:
            logger.debug(f'  get_extractor: not extractors: {extractors}')
        return None

    if len(extractors) == 2:
        extractor1, extractor2 = extractors
        nested_extractor = functional.partial(
            extract_twice,
            extractor1=extractor1,
            extractor2=extractor2,
        )
        return nested_extractor
    elif len(extractors) == 1:
        return extractors[0]
    else:
        return None


def get_extractors(location, kinds=all_kinds):
    """
    Return a list of extractors that can extract the file at
    location or an empty list.
    """
    handler = get_best_handler(location, kinds)
    if TRACE_DEEP:
        logger.debug(f'  get_extractors: handler: {handler}')

    return handler and handler.extractors or []


def get_best_handler(location, kinds=all_kinds):
    """
    Return the best handler for the file at `location` or None .
    """
    location = os.path.abspath(os.path.expanduser(location))
    if not filetype.is_file(location):
        return

    handlers = list(get_handlers(location))
    if TRACE_DEEP:
        logger.debug(f'    get_best_handler: handlers: {handlers}')
    if not handlers:
        return

    candidates = list(score_handlers(handlers))
    if TRACE_DEEP:
        logger.debug(f'    get_best_handler: candidates: {candidates}')
    if not candidates:
        if TRACE_DEEP:
            logger.debug(f'    get_best_handler: candidates: {candidates}')
        return

    picked = pick_best_handler(candidates, kinds=kinds)
    if TRACE_DEEP:
        logger.debug(f'    get_best_handler: picked: {picked}')
    return picked


def get_handlers(location):
    """
    Return an iterable of (handler, type_matched, mime_matched,
    extension_matched,) for this `location`.
    """
    if filetype.is_file(location):

        T = contenttype.get_type(location)
        ftype = T.filetype_file.lower()
        mtype = T.mimetype_file

        if TRACE_DEEP:
            logger.debug(
                'get_handlers: processing %(location)s: '
                'ftype: %(ftype)s, mtype: %(mtype)s ' % locals())
        for handler in archive_handlers:
            if not handler.extractors:
                continue

            extractor_count = len(handler.extractors)
            if extractor_count > 2:
                raise Exception('Maximum level of archive nesting is two.')

            # default to False
            type_matched = handler.filetypes and any(t in ftype for t in handler.filetypes)
            if TRACE_DEEP:
                logger.debug(f'    get_handlers: handler.filetypes={handler.filetypes}')
            mime_matched = handler.mimetypes and any(m in mtype for m in handler.mimetypes)
            exts = handler.extensions
            if exts:
                extension_matched = exts and location.lower().endswith(exts)

            if TRACE_DEEP:
                print(
                    f'  get_handlers: matched type: {type_matched}, '
                    f'mime: {mime_matched}, ext: {extension_matched}' % locals()
                  )

            if (
                handler.strict
                and not (
                    type_matched
                    and mime_matched
                    and extension_matched
                )
            ):
                if TRACE_DEEP:
                    print(f'  get_handlers: skip strict: {handler.name}')
                continue

            if type_matched or mime_matched or extension_matched:
                if TRACE_DEEP:
                    handler_name = handler.name
                    logger.debug('     get_handlers: yielding handler: %(handler_name)r' % locals())
                yield handler, type_matched, mime_matched, extension_matched


def score_handlers(handlers):
    """
    Score candidate handlers. Higher score is better.
    """
    for handler, type_matched, mime_matched, extension_matched in handlers:
        if TRACE_DEEP:
            logger.debug(
                f'     score_handlers: handler={handler}, '
                f'type_matched={type_matched}, '
                f'mime_matched={mime_matched}, '
                f'extension_matched={extension_matched}'
            )
        score = 0
        # increment kind value: higher kinds numerical values are more
        # specific by design
        score += handler.kind
        if TRACE_DEEP: logger.debug(f'     score_handlers: score += handler.kind {score}')

        # increment score based on matched criteria
        if type_matched and mime_matched and extension_matched:
            # bump for matching all criteria
            score += 10

        if type_matched:
            # type is more specific than mime
            score += 8

        if mime_matched:
            score += 6

        if extension_matched:
            # extensions have little power
            score += 2

        if extension_matched and not (type_matched or mime_matched):
            # extension matched alone should not be extracted
            score -= 100

        # increment using the number of extractors: higher score means that we
        # have some kind of nested archive that we can extract in one
        # operation, therefore more this is a more specific extraction that we
        # should prefer. For instance we prefer uncompressing and extracting a
        # tgz at once, rather than uncompressing in a first operation then
        # later extracting the plain tar in a second operation
        score += len(handler.extractors)

        if TRACE_DEEP:
            handler_name = handler.name
            logger.debug(
                '     score_handlers: yielding handler: %(handler_name)r, '
                'score: %(score)d, extension_matched: %(extension_matched)r' % locals())

        if score > 0:
            yield score, handler, extension_matched


def pick_best_handler(candidates, kinds):
    """
    Return the best handler with the highest score.
    In case of ties, look at the top two handlers and keep:
    - the handler with the most extractors (i.e. a handler that does deeper
      nested extractions),
    - OR the handler that has matched extensions,
    - OR finally the first handler in the list.
    """
    # sort by increasing scores
    scored = sorted(candidates, reverse=True)

    if TRACE_DEEP:
        logger.debug(f'  pick_best_handler: scored: {scored}')

    if not scored:
        return

    top_score, top, top_ext = scored[0]
    # logger.debug('pick_best_handler: top: %(top)r\n' % locals())
    # single candidate case
    if len(scored) == 1:
        return top if top.kind in kinds else None

    # else: here we have 2 or more candidates: look at the runner up.
    runner_up_score, runner_up, runner_up_ext = scored[1]
    # logger.debug('pick_best_handler: runner_up: %(runner_up)r\n' % locals())

    # return the top scoring if there is score ties.
    if top_score > runner_up_score:
        return top if top.kind in kinds else None

    # else: with sorting top_score == runner_up_score by construction here
    # break ties based on number of extractors
    if len(top.extractors) > len(runner_up.extractors):
        return top if top.kind in kinds else None

    elif len(top.extractors) < len(runner_up.extractors):
        return runner_up if runner_up.kind in kinds else None

    # else: here len(top.extractors) == len(runner_up.extractors)
    # now, break ties based on extensions being matched
    if top_ext and not runner_up_ext:
        return top if top.kind in kinds else None

    elif runner_up_ext and not top_ext:
        return runner_up if runner_up.kind in kinds else None

    # else: we could not break ties. finally return the top
    return top if top.kind in kinds else None


def extract_twice(location, target_dir, extractor1, extractor2):
    """
    Extract a nested compressed archive at `location` to `target_dir` using
    the `extractor1` function to a temporary directory then the `extractor2`
    function on the extracted payload of `extractor1`.

    Return a list of warning messages. Raise exceptions on errors.

    Typical nested archives include compressed tarballs and RPMs (containing a
    compressed cpio).

    Note: it would be easy to support deeper extractor chains, but this gets
    hard to trace and debug very quickly. A depth of two is simple and sane and
    covers most common cases.
    """
    abs_location = os.path.abspath(os.path.expanduser(location))
    abs_target_dir = str(os.path.abspath(os.path.expanduser(target_dir)))
    # extract first the intermediate payload to a temp dir
    temp_target = str(fileutils.get_temp_dir(prefix='extractcode-extract-'))
    warnings = extractor1(abs_location, temp_target)
    if TRACE:
        logger.debug('extract_twice: temp_target: %(temp_target)r' % locals())

    # extract this intermediate payload to the final target_dir
    try:
        inner_archives = list(fileutils.resource_iter(temp_target, with_dirs=False))
        if not inner_archives:
            warnings.append(location + ': No files found in archive.')
        else:
            for extracted1_loc in inner_archives:
                if TRACE:
                    logger.debug('extract_twice: extractor2: %(extracted1_loc)r' % locals())
                warnings.extend(extractor2(extracted1_loc, abs_target_dir))
    finally:
        # cleanup the temporary output from extractor1
        fileutils.delete(temp_target)
    return warnings


def extract_with_fallback(location, target_dir, extractor1, extractor2):
    """
    Extract archive at `location` to `target_dir` trying first the primary
    `extractor1` function. If extract fails with this function, attempt
    extraction again with the fallback `extractor2` function.
    Return a list of warning messages. Raise exceptions on errors.

    Note: there are a few cases where the primary extractor for a type may fail
    and a fallback extractor will succeed.
    """
    abs_location = os.path.abspath(os.path.expanduser(location))
    abs_target_dir = str(os.path.abspath(os.path.expanduser(target_dir)))
    # attempt extract first to a temp dir
    temp_target1 = str(fileutils.get_temp_dir(prefix='extractcode-extract1-'))
    try:
        warnings = extractor1(abs_location, temp_target1)
        if TRACE:
            logger.debug('extract_with_fallback: temp_target1: %(temp_target1)r' % locals())
        fileutils.copytree(temp_target1, abs_target_dir)
    except:
        try:
            temp_target2 = str(fileutils.get_temp_dir(prefix='extractcode-extract2-'))
            warnings = extractor2(abs_location, temp_target2)
            if TRACE:
                logger.debug('extract_with_fallback: temp_target2: %(temp_target2)r' % locals())
            fileutils.copytree(temp_target2, abs_target_dir)
        finally:
            fileutils.delete(temp_target2)
    finally:
        fileutils.delete(temp_target1)
    return warnings


def try_to_extract(location, target_dir, extractor):
    """
    Extract archive at `location` to `target_dir` trying the `extractor` function.
    If extract fails, just return without returning warnings nor raising exceptions.

    Note: there are a few cases where we want to attempt extracting something
    but do not care if this fails.
    """
    abs_location = os.path.abspath(os.path.expanduser(location))
    abs_target_dir = str(os.path.abspath(os.path.expanduser(target_dir)))
    temp_target = str(fileutils.get_temp_dir(prefix='extractcode-extract1-'))
    warnings = []
    try:
        warnings = extractor(abs_location, temp_target)
        if TRACE:
            logger.debug('try_to_extract: temp_target: %(temp_target)r' % locals())
        fileutils.copytree(temp_target, abs_target_dir)
    except:
        return warnings
    finally:
        fileutils.delete(temp_target)
    return warnings

# High level aliases to lower level extraction functions
########################################################


extract_tar = libarchive2.extract
extract_patch = patch.extract

extract_deb = libarchive2.extract

# sevenzip is best for windows lib formats and works fine otherwise. libarchive
# works on standard ar formats.
extract_ar = functional.partial(
    extract_with_fallback,
    extractor1=libarchive2.extract,
    extractor2=sevenzip.extract,
)

extract_msi = sevenzip.extract
extract_cpio = libarchive2.extract

# sevenzip should be best at extracting 7zip but most often libarchive is better first
extract_7z = functional.partial(
    extract_with_fallback,
    extractor1=libarchive2.extract,
    extractor2=sevenzip.extract,
)

# libarchive is best for the run of the mill zips, but sevenzip sometimes is better
extract_zip = functional.partial(
    extract_with_fallback,
    extractor1=libarchive2.extract,
    extractor2=sevenzip.extract,
)

extract_springboot = functional.partial(try_to_extract, extractor=extract_zip)

extract_lzip = libarchive2.extract
extract_zstd = libarchive2.extract
extract_lz4 = libarchive2.extract

extract_iso = sevenzip.extract
extract_rar = libarchive2.extract
extract_rpm = sevenzip.extract
extract_xz = sevenzip.extract
extract_lzma = sevenzip.extract
extract_squashfs = sevenzip.extract
extract_vm_image = vmimage.extract
extract_cab = sevenzip.extract
extract_nsis = sevenzip.extract
extract_ishield = sevenzip.extract
extract_Z = sevenzip.extract
extract_xarpkg = sevenzip.extract

# Archive handlers.
####################

TarHandler = Handler(
    name='Tar',
    filetypes=('.tar', 'tar archive',),
    mimetypes=('application/x-tar',),
    extensions=('.tar',),
    kind=regular,
    extractors=[extract_tar],
    strict=False
)

RubyGemHandler = Handler(
    name='Ruby Gem package',
    filetypes=('.tar', 'tar archive',),
    mimetypes=('application/x-tar',),
    extensions=('.gem',),
    kind=package,
    extractors=[extract_tar],
    strict=True
)

ZipHandler = Handler(
    name='Zip',
    filetypes=('zip archive',),
    mimetypes=('application/zip',),
    extensions=('.zip', '.zipx',),
    kind=regular,
    extractors=[extract_zip],
    strict=False
)

OfficeDocHandler = Handler(
    name='Office doc',
    filetypes=(
        'zip archive',
        'microsoft word 2007+',
        'microsoft excel 2007+',
        'microsoft powerpoint 2007+',
    ),
    mimetypes=('application/zip', 'application/vnd.openxmlformats',),
    # Extensions of office documents that are zip files too
    extensions=(
        # ms doc
        '.docx', '.dotx', '.docm',
        # ms xls
        '.xlsx', '.xltx', '.xlsm', '.xltm',
        # ms ppt
        '.pptx', '.ppsx', '.potx', '.pptm', '.potm', '.ppsm',
        # oo write
        '.odt', '.odf', '.sxw', '.stw',
        # oo calc
        '.ods', '.ots', '.sxc', '.stc',
        # oo pres and draw
        '.odp', '.otp', '.odg', '.otg', '.sxi', '.sti', '.sxd',
        '.sxg', '.std',
        # star office
        '.sdc', '.sda', '.sdd', '.smf', '.sdw', '.sxm', '.stw',
        '.oxt', '.sldx',

        '.epub',
    ),
    kind=docs,
    extractors=[extract_zip],
    strict=True
)

AndroidAppHandler = Handler(
    name='Android app',
    filetypes=('zip archive',),
    mimetypes=('application/zip',),
    extensions=('.apk',),
    kind=package,
    extractors=[extract_zip],
    strict=True
)

# see http://tools.android.com/tech-docs/new-build-system/aar-formats
AndroidLibHandler = Handler(
    name='Android library',
    filetypes=('zip archive',),
    mimetypes=('application/zip',),
    # note: Apache Axis also uses AAR extensions for plain Jars
    extensions=('.aar',),
    kind=package,
    extractors=[extract_zip],
    strict=True
)

MozillaExtHandler = Handler(
    name='Mozilla extension',
    filetypes=('zip archive',),
    mimetypes=('application/zip',),
    extensions=('.xpi',),
    kind=package,
    extractors=[extract_zip],
    strict=True
)

# see https://developer.chrome.com/extensions/crx
# not supported for now
ChromeExtHandler = Handler(
    name='Chrome extension',
    filetypes=('data',),
    mimetypes=('application/octet-stream',),
    extensions=('.crx',),
    kind=package,
    extractors=[extract_7z],
    strict=True
)

IosAppHandler = Handler(
    name='iOS app',
    filetypes=('zip archive',),
    mimetypes=('application/zip',),
    extensions=('.ipa',),
    kind=package,
    extractors=[extract_zip],
    strict=True
)

JavaJarHandler = Handler(
    name='Java Jar package',
    filetypes=('java archive',),
    mimetypes=('application/java-archive',),
    extensions=('.jar', '.zip',),
    kind=package,
    extractors=[extract_zip],
    strict=False
)

JavaJarZipHandler = Handler(
    name='Java Jar package',
    filetypes=('zip archive',),
    mimetypes=('application/zip',),
    extensions=('.jar',),
    kind=package,
    extractors=[extract_zip],
    strict=False
)

# See https://projects.spring.io/spring-boot/
# this is a ZIP with a shell header (e.g. a self-executing zip of sorts)
# internalyl the zip is really a war rather than a jar
SpringBootShellJarHandler = Handler(
    name='Springboot Java Jar package',
    filetypes=('bourne-again shell script executable (binary data)',),
    mimetypes=('text/x-shellscript',),
    extensions=('.jar',),
    kind=package,
    extractors=[extract_springboot],
    strict=True
)

JavaWebHandler = Handler(
    name='Java archive',
    filetypes=('zip archive',),
    mimetypes=('application/zip', 'application/java-archive',),
    extensions=('.war', '.sar', '.ear',),
    kind=regular,
    extractors=[extract_zip],
    strict=True
)

PythonHandler = Handler(
    name='Python package',
    filetypes=('zip archive',),
    mimetypes=('application/zip',),
    extensions=('.egg', '.whl', '.pyz', '.pex',),
    kind=package,
    extractors=[extract_zip],
    strict=True
)

XzHandler = Handler(
    name='xz',
    filetypes=('xz compressed',),
    mimetypes=('application/x-xz',) ,
    extensions=('.xz',),
    kind=regular,
    extractors=[extract_xz],
    strict=False
)

LzmaHandler = Handler(
    name='lzma',
    filetypes=('lzma compressed',),
    mimetypes=('application/x-xz',) ,
    extensions=('.lzma',),
    kind=regular,
    extractors=[extract_lzma],
    strict=False
)

TarXzHandler = Handler(
    name='Tar xz',
    filetypes=('xz compressed',),
    mimetypes=('application/x-xz',) ,
    extensions=('.tar.xz', '.txz', '.tarxz',),
    kind=regular_nested,
    extractors=[extract_xz, extract_tar],
    strict=False
)

TarLzmaHandler = Handler(
    name='Tar lzma',
    filetypes=('lzma compressed',),
    mimetypes=('application/x-lzma',) ,
    extensions=('tar.lzma', '.tlz', '.tarlz', '.tarlzma',),
    kind=regular_nested,
    extractors=[extract_lzma, extract_tar],
    strict=False
)

TarGzipHandler = Handler(
    name='Tar gzip',
    filetypes=('gzip compressed',),
    mimetypes=('application/gzip',),
    extensions=('.tgz', '.tar.gz', '.tar.gzip', '.targz', '.targzip', '.tgzip',),
    kind=regular_nested,
    extractors=[extract_tar],
    strict=False
)

TarLzipHandler = Handler(
    name='Tar lzip',
    filetypes=('lzip compressed',),
    mimetypes=('application/x-lzip',) ,
    extensions=('.tar.lz', '.tar.lzip',),
    kind=regular_nested,
    extractors=[extract_lzip, extract_tar],
    strict=False
)

TarZstdHandler = Handler(
    name='Tar zstd',
    filetypes=('zstandard compressed',),
    mimetypes=('application/x-zstd',) ,
    extensions=('.tar.zst', '.tar.zstd',),
    kind=regular_nested,
    extractors=[extract_zstd, extract_tar],
    strict=True
)

TarLz4Handler = Handler(
    name='Tar lz4',
    filetypes=('lz4 compressed',),
    mimetypes=('application/x-lz4',) ,
    extensions=('.tar.lz4',),
    kind=regular_nested,
    extractors=[extract_lz4, extract_tar],
    strict=True
)

# https://wiki.openwrt.org/doc/techref/opkg: ipk
# http://downloads.openwrt.org/snapshots/trunk/x86/64/packages/base/

OpkgHandler = Handler(
    name='OPKG package',
    filetypes=('gzip compressed',),
    mimetypes=('application/gzip',),
    extensions=('.ipk',),
    kind=regular_nested,
    extractors=[extract_tar],
    strict=False
)

GzipHandler = Handler(
    name='Gzip',
    filetypes=('gzip compressed', 'gzip compressed data'),
    mimetypes=('application/gzip',),
    extensions=('.gz', '.gzip', '.wmz', '.arz',),
    kind=regular,
    extractors=[uncompress_gzip],
    strict=False
)

LzipHandler = Handler(
    name='lzip',
    filetypes=('lzip compressed',),
    mimetypes=('application/x-lzip',) ,
    extensions=('.lzip',),
    kind=regular,
    extractors=[extract_lzip],
    strict=False
)

ZstdHandler = Handler(
    name='zstd',
    filetypes=('zstandard compressed',),
    mimetypes=('application/x-zstd',) ,
    extensions=('.zst', '.zstd',),
    kind=regular_nested,
    extractors=[extract_zstd],
    strict=False
)

Lz4Handler = Handler(
    name='lz4',
    filetypes=('lz4 compressed',),
    mimetypes=('application/x-lz4',) ,
    extensions=('.lz4',),
    kind=regular_nested,
    extractors=[extract_lz4],
    strict=False
)

DiaDocHandler = Handler(
    name='Dia diagram doc',
    filetypes=('gzip compressed',),
    mimetypes=('application/gzip',),
    extensions=('.dia',),
    kind=docs,
    extractors=[uncompress_gzip],
    strict=True
)

GraffleDocHandler = Handler(
    name='Graffle diagram doc',
    filetypes=('gzip compressed',),
    mimetypes=('application/gzip',),
    extensions=('.graffle',),
    kind=docs,
    extractors=[uncompress_gzip],
    strict=True
)

SvgGzDocHandler = Handler(
    name='SVG Compressed doc',
    filetypes=('gzip compressed',),
    mimetypes=('application/gzip',),
    extensions=('.svgz',),
    kind=docs,
    extractors=[uncompress_gzip],
    strict=True
)

BzipHandler = Handler(
    name='bzip2',
    filetypes=('bzip2 compressed',),
    mimetypes=('application/x-bzip2',),
    extensions=('.bz', '.bz2', 'bzip2',),
    kind=regular,
    extractors=[uncompress_bzip2],
    strict=False
)

TarBzipHandler = Handler(
    name='Tar bzip2',
    filetypes=('bzip2 compressed',),
    mimetypes=('application/x-bzip2',),
    extensions=(
        '.tar.bz2',
        '.tar.bz',
        '.tar.bzip',
        '.tar.bzip2',
        '.tbz',
        '.tbz2',
        '.tb2',
        '.tarbz2',
    ),
    kind=regular_nested,
    extractors=[extract_tar],
    strict=False
)

RarHandler = Handler(
    name='RAR',
    filetypes=('rar archive',),
    mimetypes=('application/x-rar',),
    extensions=('.rar',),
    kind=regular,
    extractors=[extract_rar],
    strict=True
)

CabHandler = Handler(
    name='Microsoft cab',
    filetypes=('microsoft cabinet',),
    mimetypes=('application/vnd.ms-cab-compressed',),
    extensions=('.cab',),
    kind=package,
    extractors=[extract_cab],
    strict=True
)

MsiInstallerHandler = Handler(
    name='Microsoft MSI Installer',
    filetypes=('msi installer',),
    mimetypes=('application/x-msi',),
    extensions=('.msi',),
    kind=package,
    extractors=[extract_msi],
    strict=True
)

InstallShieldHandler = Handler(
    name='InstallShield Installer',
    filetypes=('installshield',),
    mimetypes=('application/x-dosexec',),
    extensions=('.exe',),
    kind=special_package,
    extractors=[extract_ishield],
    strict=True
)

NugetHandler = Handler(
    name='Nuget',
    # TODO: file a bug upstream
    # Weirdly enough the detection by libmagic is sometimes wrong
    # this is due to this issue:
    # being recognized by libmagic as an OOXML file
    # https://en.wikipedia.org/wiki/Open_Packaging_Conventions#File_formats_using_the_OPC
    filetypes=('zip archive', 'microsoft ooxml',),
    mimetypes=('application/zip', 'application/octet-stream',),
    extensions=('.nupkg',),
    kind=package,
    extractors=[extract_zip],
    strict=True
)

NSISInstallerHandler = Handler(
    name='Nullsoft Installer',
    filetypes=('nullsoft installer',),
    mimetypes=('application/x-dosexec',),
    extensions=('.exe',),
    kind=special_package,
    extractors=[extract_nsis],
    strict=True
)

ArHandler = Handler(
    name='ar archive',
    filetypes=('current ar archive',),
    mimetypes=('application/x-archive',),
    extensions=('.ar',),
    kind=regular,
    extractors=[extract_ar],
    strict=False
)

StaticLibHandler = Handler(
    name='Static Library',
    filetypes=('current ar archive', 'current ar archive random library',),
    mimetypes=('application/x-archive',),
    extensions=('.a', '.lib', '.out', '.ka',),
    kind=package,
    extractors=[extract_ar],
    strict=True
)

DebHandler = Handler(
    name='Debian package',
    filetypes=('debian binary package',),
    mimetypes=(
        'application/vnd.debian.binary-package',
        'application/x-archive',
    ),
    extensions=('.deb', '.udeb',),
    kind=package,
    extractors=[extract_deb],
    strict=True
)

RpmHandler = Handler(
    name='RPM package',
    filetypes=('rpm ',),
    mimetypes=('application/x-rpm',),
    extensions=('.rpm', '.srpm', '.mvl', '.vip',),
    kind=package,
    extractors=[extract_rpm, extract_cpio],
    strict=False
)

SevenZipHandler = Handler(
    name='7zip',
    filetypes=('7-zip archive',),
    mimetypes=('application/x-7z-compressed',),
    extensions=('.7z',),
    kind=regular,
    extractors=[extract_7z],
    strict=False
)

TarSevenZipHandler = Handler(
    name='Tar 7zip',
    filetypes=('7-zip archive',),
    mimetypes=('application/x-7z-compressed',),
    extensions=('.tar.7z', '.tar.7zip', '.t7z',),
    kind=regular_nested,
    extractors=[extract_7z, extract_tar],
    strict=True
)

SharHandler = Handler(
    name='shar shell archive',
    filetypes=('posix shell script',),
    mimetypes=('text/x-shellscript',),
    extensions=('.sha', '.shar', '.bin',),
    kind=special_package,
    extractors=[],
    strict=True
)

CpioHandler = Handler(
    name='cpio',
    filetypes=('cpio archive',),
    mimetypes=('application/x-cpio',),
    extensions=('.cpio',),
    kind=regular,
    extractors=[extract_cpio],
    strict=False
)

ZHandler = Handler(
    name='Z',
    filetypes=("compress'd data",),
    mimetypes=('application/x-compress',),
    extensions=('.z',),
    kind=regular,
    extractors=[extract_Z],
    strict=False
)

TarZHandler = Handler(
    name='Tar Z',
    filetypes=("compress'd data",),
    mimetypes=('application/x-compress',),
    extensions=('.tz', '.tar.z', '.tarz',),
    kind=regular_nested,
    extractors=[extract_Z, extract_tar],
    strict=False
)

AppleDmgHandler = Handler(
    name='Apple dmg',
    filetypes=('zlib compressed',),
    mimetypes=('application/zlib',),
    extensions=('.dmg', '.sparseimage',),
    kind=package,
    extractors=[extract_iso],
    strict=True
)

ApplePkgHandler = Handler(
    name='Apple pkg or mpkg package installer',
    filetypes=('xar archive',),
    mimetypes=('application/octet-stream',),
    extensions=('.pkg', '.mpkg',),
    kind=package,
    extractors=[extract_xarpkg],
    strict=True
)

XarHandler = Handler(
    name='Xar archive v1',
    filetypes=('xar archive',),
    mimetypes=('application/octet-stream', 'application/x-xar',),
    extensions=('.xar',),
    kind=package,
    extractors=[extract_xarpkg],
    strict=True
)

IsoImageHandler = Handler(
    name='ISO CD image',
    filetypes=('iso 9660 cd-rom', 'high sierra cd-rom',),
    mimetypes=('application/x-iso9660-image',),
    extensions=('.iso', '.udf', '.img',),
    kind=file_system,
    extractors=[extract_iso],
    strict=True
)

SquashfsHandler = Handler(
    name='SquashFS disk image',
    filetypes=('squashfs',),
    mimetypes=(),
    extensions=(),
    kind=file_system,
    extractors=[extract_squashfs],
    strict=False
)

QCOWHandler = Handler(
    # note that there are v1, v2 and v3 formats.
    name='QEMU QCOW2 disk image',
    filetypes=('qemu qcow2 image', 'qemu qcow image',),
    mimetypes=('application/octet-stream',),
    extensions=('.qcow2', '.qcow', '.qcow2c', '.img',),
    kind=file_system,
    extractors=[extract_vm_image],
    strict=True,
)

VMDKHandler = Handler(
    name='VMDK disk image',
    filetypes=('vmware4 disk image',),
    mimetypes=('application/octet-stream',),
    extensions=('.vmdk',),
    kind=file_system,
    extractors=[extract_vm_image],
    strict=True,
)

VirtualBoxHandler = Handler(
    name='VirtualBox disk image',
    filetypes=('virtualbox disk image',),
    mimetypes=('application/octet-stream',),
    extensions=('.vdi',),
    kind=file_system,
    extractors=[extract_vm_image],
    strict=True,
)

PatchHandler = Handler(
    name='Patch',
    filetypes=('diff', 'patch',),
    mimetypes=('text/x-diff',),
    extensions=('.diff', '.patch',),
    kind=patches,
    extractors=[extract_patch],
    strict=True
)

# Actual list of handlers

archive_handlers = [
    TarHandler,
    RubyGemHandler,
    ZipHandler,
    OfficeDocHandler,
    AndroidAppHandler,
    AndroidLibHandler,
    MozillaExtHandler,
    # not supported for now
    # ChromeExtHandler,
    IosAppHandler,
    SpringBootShellJarHandler,
    JavaJarHandler,
    JavaJarZipHandler,
    JavaWebHandler,
    PythonHandler,
    XzHandler,
    LzmaHandler,
    TarXzHandler,
    TarLzmaHandler,
    TarGzipHandler,
    TarLzipHandler,
    TarLz4Handler,
    TarZstdHandler,
    DiaDocHandler,
    GraffleDocHandler,
    SvgGzDocHandler,
    GzipHandler,
    BzipHandler,
    TarBzipHandler,
    LzipHandler,
    Lz4Handler,
    ZstdHandler,
    RarHandler,
    CabHandler,
    MsiInstallerHandler,
    ApplePkgHandler,
    XarHandler,
    # notes: this may catch all exe and fails too often
    InstallShieldHandler,
    NSISInstallerHandler,
    NugetHandler,
    ArHandler,
    StaticLibHandler,
    DebHandler,
    RpmHandler,
    SevenZipHandler,
    TarSevenZipHandler,
    # not supported for now
    # SharHandler,
    CpioHandler,
    ZHandler,
    TarZHandler,
    AppleDmgHandler,
    IsoImageHandler,
    SquashfsHandler,
    QCOWHandler,
    VMDKHandler,
    VirtualBoxHandler,
]

# only support extracting patches if patch is installed. This is not a default
try:
    import patch as _pythonpatch
    archive_handlers.append(PatchHandler)
except:
    pass
