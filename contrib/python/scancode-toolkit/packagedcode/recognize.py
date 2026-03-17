#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import fnmatch
import os
import sys

from commoncode import filetype
from commoncode.fileutils import file_name
from commoncode.fileutils import splitext_name
from packagedcode import PACKAGE_TYPES
from typecode import contenttype

SCANCODE_DEBUG_PACKAGE_API = os.environ.get('SCANCODE_DEBUG_PACKAGE_API', False)

TRACE = False or SCANCODE_DEBUG_PACKAGE_API


def logger_debug(*args):
    pass


if TRACE:
    import logging

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))

    logger_debug = print

"""
Recognize package manifests in files.
"""


def recognize_packages(location):
    """
    Return a list of Package object if any packages were recognized for this
    `location`, or None if there were no Packages found. Raises Exceptions on errors.
    """

    if not filetype.is_file(location):
        return

    T = contenttype.get_type(location)
    ftype = T.filetype_file.lower()
    mtype = T.mimetype_file

    _base_name, extension = splitext_name(location, is_file=True)
    filename = file_name(location)
    extension = extension.lower()

    if TRACE:
        logger_debug(
            'recognize_packages: ftype:', ftype, 'mtype:', mtype,
            'pygtype:', T.filetype_pygment,
            'fname:', filename, 'ext:', extension,
        )

    recognized_packages = []
    for package_type in PACKAGE_TYPES:
        # Note: default to True if there is nothing to match against
        metafiles = package_type.metafiles
        if any(fnmatch.fnmatchcase(filename, metaf) for metaf in metafiles):
            for recognized in package_type.recognize(location):
                if TRACE:
                    logger_debug(
                        'recognize_packages: metafile matching: recognized:',
                        recognized,
                    )
                if recognized and not recognized.license_expression:
                    # compute and set a normalized license expression
                    recognized.license_expression = recognized.compute_normalized_license()
                    if TRACE:
                        logger_debug(
                            'recognize_packages: recognized.license_expression:',
                            recognized.license_expression,
                        )
                recognized_packages.append(recognized)
            return recognized_packages

        type_matched = False
        if package_type.filetypes:
            type_matched = any(t in ftype for t in package_type.filetypes)

        mime_matched = False
        if package_type.mimetypes:
            mime_matched = any(m in mtype for m in package_type.mimetypes)

        extension_matched = False
        extensions = package_type.extensions
        if extensions:
            extensions = (e.lower() for e in extensions)
            extension_matched = any(
                fnmatch.fnmatchcase(extension, ext_pat)
                for ext_pat in extensions
            )

        if type_matched and mime_matched and extension_matched:
            if TRACE:
                logger_debug(f'recognize_packages: all matching for {package_type}')

            try:
                for recognized in package_type.recognize(location):
                    # compute and set a normalized license expression
                    if recognized and not recognized.license_expression:
                        try:
                            recognized.license_expression = recognized.compute_normalized_license()
                        except Exception:
                            if SCANCODE_DEBUG_PACKAGE_API:
                                raise
                            recognized.license_expression = 'unknown'

                    if TRACE:
                        logger_debug('recognize_packages: recognized', recognized)

                    recognized_packages.append(recognized)

            except NotImplementedError:
                # build a plain package if recognize is not yet implemented
                recognized = package_type()
                if TRACE:
                    logger_debug('recognize_packages: recognized', recognized)

                recognized_packages.append(recognized)

                if SCANCODE_DEBUG_PACKAGE_API:
                    raise

            return recognized_packages

        if TRACE: logger_debug('recognize_packages: no match for type:', package_type)
