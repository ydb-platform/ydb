#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/extractcode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

"""
Note: this API is unstable and still evolving.
"""


def extract_archives(
    location,
    recurse=True,
    replace_originals=False,
    ignore_pattern=(),
    all_formats=False,
):
    """
    Yield ExtractEvent while extracting archive(s) and compressed files at
    ``location``.

    If ``recurse`` is True, extract nested archives-in-archives recursively.
    If ``all_formats`` is True, extract all supported archives formats. The
    default is to only extract the common "extractcode.default_kinds"

    Archives and compressed files are extracted in a new directory named
    "<file_name>-extract" created in the same directory as each extracted
    archive.

    If ``replace_originals`` is True, the extracted archives are replaced by the
    extracted content and removed when extraction is complete

    ``ignore_pattern`` is a list of glob patterns to ignore.

    Note: this API is returning an iterable and NOT a sequence.
    """

    from extractcode.extract import extract
    from extractcode import default_kinds
    from extractcode import all_kinds

    kinds = all_kinds if all_formats else default_kinds

    for xevent in extract(
        location=location,
        kinds=kinds,
        recurse=recurse,
        replace_originals=replace_originals,
        ignore_pattern=ignore_pattern,
    ):
        yield xevent


def extract_archive(location, target, verbose=False):
    """
    Yield ExtractEvent while extracting a single archive or compressed file at
    ``location`` to the ``target`` directory if the file is of any supported
    archive format. Note: this API is returning an iterable and NOT a sequence
    and does not extract recursively.

    If ``verbose`` is True, ExtractEvent.errors will contain a full error
    traceback if any.
    """

    from extractcode.extract import extract_file
    from extractcode import all_kinds
    return extract_file(
        location=location,
        target=target,
        kinds=all_kinds,
        verbose=verbose,
    )
