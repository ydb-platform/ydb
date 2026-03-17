#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from contextlib import closing

import attr
from ftfy import fix_text
import pefile

from commoncode import filetype
from commoncode import text
from typecode import contenttype

from packagedcode import models
from packagedcode.models import Party
from packagedcode.models import party_org

TRACE = False


def logger_debug(*args):
    pass


if TRACE:
    import logging
    import sys
    logger = logging.getLogger(__name__)
    # logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(
            isinstance(a, str) and a or repr(a) for a in args))

"""
Extract data from windows PE DLLs and executable.

Note that the extraction may not be correct for all PE in particular
older legacy PEs. See tests and:
    http://msdn.microsoft.com/en-us/library/aa381058%28v=VS.85%29.aspx

PE stores data in a "VarInfo" structure for "variable information".
VarInfo are by definition variable key/value pairs:
    http://msdn.microsoft.com/en-us/library/ms646995%28v=vs.85%29.aspx

Therefore we use a list of the most common and useful key names with
an eye on origin and license related information and return a value
when there is one present.
"""

"""
https://docs.microsoft.com/en-us/windows/win32/menurc/versioninfo-resource
Name                Description

Comments            Additional information that should be displayed for
                    diagnostic purposes.

CompanyName         Company that produced the file?for example, "Microsoft
                    Corporation" or "Standard Microsystems Corporation, Inc."
                    This string is required.

FileDescription     File description to be presented to users. This string may
                    be displayed in a list box when the user is choosing files
                    to install?for example, "Keyboard Driver for AT-Style
                    Keyboards". This string is required.

FileVersion         Version number of the file?for example, "3.10" or
                    "5.00.RC2". This string is required.

InternalName        Internal name of the file, if one exists?for example, a
                    module name if the file is a dynamic-link library. If the
                    file has no internal name, this string should be the
                    original filename, without extension. This string is
                    required.

LegalCopyright      Copyright notices that apply to the file. This should
                    include the full text of all notices, legal symbols,
                    copyright dates, and so on. This string is optional.

LegalTrademarks     Trademarks and registered trademarks that apply to the file.
                    This should include the full text of all notices, legal
                    symbols, trademark numbers, and so on. This string is
                    optional.

OriginalFilename    Original name of the file, not including a path. This
                    information enables an application to determine whether a
                    file has been renamed by a user. The format of the name
                    depends on the file system for which the file was created.
                    This string is required.

ProductName         Name of the product with which the file is distributed. This
                    string is required.

ProductVersion      Version of the product with which the file is
                    distributed?for example, "3.10" or "5.00.RC2". This string
                    is required.
"""

# List of common info keys found in PE.
PE_INFO_KEYS = (
    'Full Version',  # rare and used only by Java exe
    'ProductVersion',  # the actual version
    'FileVersion',  # another common version
    'Assembly Version',  # a version common in MSFT, redundant when present with ProductVersion

    'BuildDate',  # rare but useful when there 2013/02/04-18:07:46 2018-11-10 14:38

    'ProductName',  # often present often localized, that's a component name
    'OriginalFilename',  # name or the original DLL
    'InternalName',  # often present: sometimes a package name or a .dll or .exe

    'License',  # rare, seen only in CURL
    'LegalCopyright',  # copyright notice, sometimes a license tag or URL. Use it for license detection
    'LegalTrademarks',  # may sometimes contains license or copyright. Ignore a single ".". Treat as part of the declared license
    'LegalTrademarks1',  # mostly MSFT
    'LegalTrademarks2',  # mostly MSFT
    'LegalTrademarks3',  # mostly MSFT

    'FileDescription',  # description, often localized
    'Comments',  # random data. Append to a description

    'CompanyName',  # the company e.g a party, sometimes localized
    'Company',  # rare, use a fallback if present and CCompanyName missing
    'URL',  # rarely there but good if there
    'WWW',  # rarely there but good if there
)

PE_INFO_KEYSET = set(PE_INFO_KEYS)


def pe_info(location):
    """
    Return a mapping of common data available for a Windows dll or exe PE
    (portable executable).
    Return None for non-Windows PE files.
    Return an empty mapping for PE from which we could not collect data.

    Also collect extra data found if any, returned as a dictionary under the
    'extra_data' key in the returned mapping.
    """
    if not location:
        return {}

    T = contenttype.get_type(location)

    if not T.is_winexe:
        return {}

    result = dict([(k, None,) for k in PE_INFO_KEYS])
    extra_data = result['extra_data'] = {}

    with closing(pefile.PE(location)) as pe:
        if not hasattr(pe, 'FileInfo'):
            # No fileinfo section: we return just empties
            return result

        # >>> pe.FileInfo: this is a list of list of Structure objects:
        # [[<Structure: [VarFileInfo] >,  <Structure: [StringFileInfo]>]]
        file_info = pe.FileInfo
        if not file_info or not isinstance(file_info, list):
            if TRACE:
                logger.debug('pe_info: not file_info')
            return result

        # here we have a non-empty list
        file_info = file_info[0]
        if TRACE:
            logger.debug('pe_info: file_info:', file_info)

        string_file_info = [x for x in file_info
               if type(x) == pefile.Structure
               and hasattr(x, 'name')
               and x.name == 'StringFileInfo']

        if not string_file_info:
            # No stringfileinfo section: we return just empties
            if TRACE:
                logger.debug('pe_info: not string_file_info')
            return result

        string_file_info = string_file_info[0]

        if not hasattr(string_file_info, 'StringTable'):
            # No fileinfo.StringTable section: we return just empties
            if TRACE:
                logger.debug('pe_info: not StringTable')
            return result

        string_table = string_file_info.StringTable
        if not string_table or not isinstance(string_table, list):
            return result

        string_table = string_table[0]

        if TRACE:
            logger.debug(
                'pe_info: Entries keys: ' + str(set(k for k in string_table.entries)))

            logger.debug('pe_info: Entry values:')
            for k, v in string_table.entries.items():
                logger.debug('  ' + str(k) + ': ' + repr(type(v)) + repr(v))

        for k, v in string_table.entries.items():
            # convert unicode to a safe ASCII representation
            key = text.as_unicode(k).strip()
            value = text.as_unicode(v).strip()
            value = fix_text(value)
            if key in PE_INFO_KEYSET:
                result[key] = value
            else:
                extra_data[key] = value

    return result


@attr.s()
class WindowsExecutable(models.Package):
    metafiles = ()
    extensions = ('.exe', '.dll',)
    filetypes = ('pe32', 'for ms windows',)
    mimetypes = ('application/x-dosexec',)

    default_type = 'winexe'

    default_web_baseurl = None
    default_download_baseurl = None
    default_api_baseurl = None

    @classmethod
    def recognize(cls, location):
        yield parse(location)


def get_first(mapping, *keys):
    """
    Return the first value of the `keys` that is found in the `mapping`.
    """
    for key in keys:
        value = mapping.get(key)
        if value:
            return value


def concat(mapping, *keys):
    """
    Return a concatenated string of all unique values of the `keys found in the
    `mapping`.
    """
    values = []
    for key in keys:
        val = mapping.get(key)
        if val and val not in values:
            values.append(val)
    return '\n'.join(values)


def parse(location):
    """
    Return a WindowsExecutable package from the file at `location` or None.
    """
    if not filetype.is_file(location):
        return

    T = contenttype.get_type(location)
    if not T.is_winexe:
        return

    infos = pe_info(location)

    version = get_first(
        infos, 
        'Full Version', 
        'ProductVersion', 
        'FileVersion', 
        'Assembly Version',
    )
    release_date = get_first(infos, 'BuildDate')
    if release_date:
        if len(release_date) >= 10:
            release_date = release_date[:10]
        release_date = release_date.replace('/', '-')

    name = get_first(
        infos, 
        'ProductName', 
        'OriginalFilename', 
        'InternalName',
    )
    copyr = get_first(infos, 'LegalCopyright')

    LegalCopyright = copyr,

    LegalTrademarks = concat(
        infos,
        'LegalTrademarks',
        'LegalTrademarks1',
        'LegalTrademarks2',
        'LegalTrademarks3')

    License = get_first(infos, 'License')

    declared_license = {}
    if LegalCopyright or LegalTrademarks or License:
        declared_license = dict(
            LegalCopyright=copyr,
            LegalTrademarks=LegalTrademarks,
            License=License
        )

    description = concat(infos, 'FileDescription', 'Comments')

    parties = []
    cname = get_first(infos, 'CompanyName', 'Company')

    if cname:
        parties = [Party(type=party_org, role='author', name=cname)]
    homepage_url = get_first(infos, 'URL', 'WWW')

    return WindowsExecutable(
        name=name,
        version=version,
        release_date=release_date,
        copyright=copyr,
        declared_license=declared_license,
        description=description,
        parties=parties,
        homepage_url=homepage_url,
    )
