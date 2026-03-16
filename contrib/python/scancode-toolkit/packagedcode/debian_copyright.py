#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import io
import sys
from os import environ
from os import path

import attr

from debian_inspector.copyright import DebianCopyright
from debian_inspector.copyright import CatchAllParagraph
from debian_inspector.copyright import CopyrightFilesParagraph
from debian_inspector.copyright import CopyrightLicenseParagraph
from debian_inspector.copyright import CopyrightHeaderParagraph
from debian_inspector.copyright import is_machine_readable_copyright

from license_expression import Licensing
from license_expression import ExpressionError
from license_expression import LicenseSymbolLike

from packagedcode.licensing import get_normalized_expression
from packagedcode.utils import combine_expressions

from licensedcode.models import Rule
from licensedcode.match import LicenseMatch
from licensedcode.query import Query
from licensedcode.spans import Span
from licensedcode.cache import get_index

from textcode.analysis import unicode_text

"""
Detect licenses in Debian copyright files. Can handle dep-5 machine-readable
copyright files, pre-dep-5 mostly machine-readable copyright files and
unstructured copyright files.
"""

TRACE = environ.get('SCANCODE_DEBUG_PACKAGE', False) or False

MATCHER_UNKNOWN = '5-unknown'

def logger_debug(*args):
    pass


if TRACE:
    import logging

    logger = logging.getLogger(__name__)
    # logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(" ".join(isinstance(a, str) and a or repr(a) for a in args))


def parse_copyright_file(location):
    """
    Return an Obj
    extracted from the debain copyright file at `location`.
    """
    if not location or not location.endswith('copyright'):
        return

    if is_machine_readable_copyright(unicode_text(location)):
        dc = StructuredCopyrightProcessor.from_file(location=location)
    else:
        dc = UnstructuredCopyrightProcessor.from_file(location=location)

    if TRACE:
        logger_debug(f'parse_copyright_file: {dc}')

    return dc


class DebianDetector:
    # Absolute location of this copyright file
    location = attr.ib()

    @classmethod
    def from_file(cls, *args, **kwargs):
        return NotImplementedError
    
    def get_copyright(self, *args, **kwargs):
        """
        Return a copyright string suitable to use as a DebianPackage.copyright.
        """
        return NotImplementedError
    
    def get_license_expression(self, *args, **kwargs):
        """
        Return a license expression string suitable to use as a DebianPackage.license_expression.
        """
        return NotImplementedError
    
    def get_declared_license(self, *args, **kwargs):
        """
        Return a list of declared license string suitable to use as a DebianPackage.declared_license.
        """
        return NotImplementedError


@attr.s
class UnstructuredCopyrightProcessor(DebianDetector):
    # Absolute location of this copyright file
    location = attr.ib()
    
    # List of LicenseMatches in an unstructured file
    license_matches = attr.ib(default=attr.Factory(list))
    
    # List of detected copyrights in an unstructured file
    detected_copyrights = attr.ib(default=attr.Factory(list))

    @classmethod
    def from_file(cls, location):
        
        dc = cls(location=location)
        
        dc.detected_copyrights = copyright_detector(location)
        
        content = unicode_text(location)
        dc.license_matches = get_license_matches(query_string=content)
        
        return dc

    def get_declared_license(self, *args, **kwargs):
        return None
    
    def get_license_expression(self, *args, **kwargs):
        matches = self.license_matches
        if not matches:
            # we have no match: return an unknown key
            return ['unknown']

        detected_expressions = [match.rule.license_expression for match in matches]
        return combine_expressions(detected_expressions)

    def get_copyright(self, *args, **kwargs):
        return '\n'.join(self.detected_copyrights)


@attr.s
class StructuredCopyrightProcessor(DebianDetector):
    # Absolute location of this copyright file
    location = attr.ib()

    # List of LicenseDetection objects having license matches from files/header paragraphs
    license_detections = attr.ib(default=attr.Factory(list))
    
    # List of CopyrightDetection objects having copyrights from files/header paragraphs
    copyright_detections = attr.ib(default=attr.Factory(list))
    
    # A cached DebianCopyright object built from the copyright file at location
    debian_copyright = attr.ib(default=None)

    @classmethod
    def from_file(cls, location):
        """
        Return a DebianCopyrightFileProcessor object built from debian copyright file at ``location``,
        or None if this is not a debian copyright file.
        Optionally detect copyright statements, if ``with_copyright`` is True.
        Filter licenses based on header/primary license, and occurances, if `with_details` is
        False, else reports everything as detected exactly.
        """
        if not location or not location.endswith('copyright'):
            return

        debian_copyright=DebianCopyright.from_file(location)
        dc = cls(location=location, debian_copyright=debian_copyright)
        dc.detect_license()
        dc.detect_copyrights()

        return dc

    def get_declared_license(
        self, filter_licenses=False, skip_debian_packaging=False, *args, **kwargs
    ):
        """
        Return a list of declared license strings built from the available license detections.
        """
        # TODO: Also Report CopyrightLicenseParagraph
        declarable_paragraphs = [
            para
            for para in self.debian_copyright.paragraphs
            if hasattr(para, 'license') and para.license.name
        ]

        if skip_debian_packaging:
            declarable_paragraphs = [
                para
                for para in declarable_paragraphs
                if not is_paragraph_debian_packaging(para)
            ]

        if filter_licenses:
            declarable_paragraphs = self.filter_duplicate_declared_license(declarable_paragraphs)

        return [paragraph.license.name for paragraph in declarable_paragraphs]

    def get_copyright(self, skip_debian_packaging=False, unique_copyrights=False, *args, **kwarg):
        """
        Return copyrights collected from a structured file.
        """
        declarable_copyrights = []
        seen_copyrights = set()
        # TODO: Only Unique Holders (copyright without years) should be reported
        
        for copyright_detection in self.copyright_detections:
            if is_paragraph_debian_packaging(copyright_detection.paragraph) and skip_debian_packaging:
                continue
            if not isinstance(
                copyright_detection.paragraph, (CopyrightHeaderParagraph, CopyrightFilesParagraph)
            ):
                continue
            
            if unique_copyrights:
                for copyright in copyright_detection.copyrights:
                    if copyright not in seen_copyrights:
                        seen_copyrights.add(copyright)
                        declarable_copyrights.append(copyright)
                continue
            
            declarable_copyrights.extend(copyright_detection.copyrights)
                    

        return '\n'.join(declarable_copyrights)
    
    def detect_copyrights(self):
        """
        Return a list of CopyrightDetection objects from a ``debian_copyright``
        DebianCopyright object.
        # TODO: We should also track line numbers in the file where a license was found
        """
        copyright_detections = []

        for paragraph in self.debian_copyright.paragraphs:
            copyrights = []
            if isinstance(paragraph, (CopyrightHeaderParagraph, CopyrightFilesParagraph)):
                pcs = paragraph.copyright.statements or []
                for p in pcs:
                    p = p.dumps()
                    copyrights.append(p)
                    
            copyright_detections.append(
                CopyrightDetection(paragraph=paragraph, copyrights=copyrights)
            )

        # We detect plain copyrights  if we didn't find any
        if not copyrights:
            copyrights = copyright_detector(self.location)
            copyright_detections.append(
                CopyrightDetection(paragraph=None, copyrights=copyrights)
            )
            
        self.copyright_detections = copyright_detections

    def get_license_expression(
        self, filter_licenses=False, skip_debian_packaging=False, *args, **kwargs
    ):
        """
        Return a license expression string as built from available license detections.
        """
        if not self.license_detections:
            return 'unknown'

        license_detections = [
            license_detection
            for license_detection in self.license_detections
            if license_detection.is_detection_declarable()
        ]

        if skip_debian_packaging:
            license_detections = [
                license_detection
                for license_detection in license_detections
                if not is_paragraph_debian_packaging(license_detection.paragraph)
            ]

        if filter_licenses:
            license_detections = self.filter_duplicate_license_detections(license_detections)

        expressions = [
            license_detection.license_expression_object
            for license_detection in license_detections
        ]

        return str(combine_expressions(expressions, unique=False))

    @staticmethod
    def filter_duplicate_declared_license(paragraphs):
        """
        Return a list of paragraphs without declared license repetitions.
        """
        filtered = []
        seen = set()

        for paragraph in paragraphs:
            license_name = paragraph.license.name
            if license_name not in seen:
                seen.add(license_name)
                filtered.append(paragraph)

        return filtered
    
    @staticmethod
    def filter_duplicate_license_detections(license_detections):
        """
        Return a list of paragraphs without declared license repetitions.
        """
        filtered_license_detection = []
        seen = set()

        for license_detection in license_detections:
            if hasattr(license_detection.paragraph, 'license'):
                license_name = license_detection.paragraph.license.name
                if license_name not in seen:
                    seen.add(license_name)
                    filtered_license_detection.append(license_detection)
            elif isinstance(license_detection.paragraph, CatchAllParagraph):
                filtered_license_detection.append(license_detection)

        return filtered_license_detection

    def detect_license(self):
        """
        Return a list of LicenseDetection objects from a ``debian_copyright``
        DebianCopyright object.
        # TODO: We should also track line numbers in the file where a license was found
        """
        license_detections = []
        edebian_copyright = EnhancedDebianCopyright(debian_copyright=self.debian_copyright)

        debian_licensing = DebianLicensing.from_license_paragraphs(
            paras_with_license=edebian_copyright.paragraphs_with_license_text
        )

        license_detections.extend(debian_licensing.license_detections)

        header_paragraph = edebian_copyright.header_paragraph
        if header_paragraph.license.name:
            license_detection = self.get_license_detection(
                paragraph=header_paragraph,
                debian_licensing=debian_licensing,
            )
            license_detections.append(license_detection)

        for file_paragraph in edebian_copyright.file_paragraphs:
            license_detection = self.get_license_detection(
                paragraph=file_paragraph,
                debian_licensing=debian_licensing,
            )
            license_detections.append(license_detection)

        license_detections.extend(
            self.detect_license_in_other_paras(other_paras=edebian_copyright.other_paragraphs)
        )

        self.license_detections = license_detections

    @staticmethod
    def get_license_detection(paragraph, debian_licensing):
        """
        Return a LicenseDetection object from a header/files paras in structured debian copyright file.
        """
        name = paragraph.license.name
        if not name:
            return get_license_detection_from_nameless_paragraph(paragraph=paragraph)

        normalized_expression = debian_licensing.get_normalized_expression(name)

        return LicenseDetection(
            paragraph=paragraph,
            license_expression_object=normalized_expression,
            license_matches=None,
        )

    @staticmethod
    def detect_license_in_other_paras(other_paras):
        """
        Run license Detection on the entire paragraph text and return result in a
        License Detection object.
        """
        license_detections = []

        for other_para in other_paras:

            extra_data = other_para.to_dict()

            for _field_name, field_value in extra_data.items():

                license_matches = get_license_matches(query_string=field_value)
                normalized_expression = get_license_expression_from_matches(
                    license_matches=license_matches,
                )
                license_detections.append(
                    LicenseDetection(
                        paragraph=other_para,
                        license_expression_object=normalized_expression,
                        license_matches=license_matches,
                    )
                )

        return license_detections


class NoLicenseFoundError(Exception):
    """
    Raised when some license is expected to be found, but is not found.
    """


class DebianCopyrightStructureError(Exception):
    """
    Raised when a structured debian copyright file has discrepancies.
    """


# These are based on `/usr/share/common-license/`
common_licenses = {
    'apache-2.0': 'apache-2.0',
    'apache-2.0+': 'apache-2.0',
    'artistic': 'artistic-perl-1.0',
    'bsd': 'bsd-new',
    'cc0-1.0': 'cc0-1.0',
    'gfdl+': 'gfdl-1.1-plus',
    'gfdl-1.2+': 'gfdl-1.2-plus',
    'gfdl-1.3+': 'gfdl-1.3-plus',
    'gpl+': 'gpl-1.0-plus',
    'gpl-1+': 'gpl-1.0-plus',
    'gpl-2+': 'gpl-2.0-plus',
    'gpl-3+': 'gpl-3.0-plus',
    'lgpl+': 'lgpl-2.0-plus',
    'lgpl-2+': 'lgpl-2.0-plus',
    'lgpl-2.1+': 'lgpl-2.1-plus',
    'lgpl-3+': 'lgpl-3.0-plus',
    'gfdl': 'gfdl-1.1-plus',
    'gfdl-1.2': 'gfdl-1.2',
    'gfdl-1.3': 'gfdl-1.3',
    'gpl': 'gpl-1.0-plus',
    'gpl-1': 'gpl-1.0',
    'gpl-2': 'gpl-2.0',
    'gpl-3': 'gpl-3.0',
    'lgpl': 'lgpl-2.0-plus',
    'lgpl-2': 'lgpl-2.0',
    'lgpl-2.1': 'lgpl-2.1',
    'lgpl-3': 'lgpl-3.0',
    'mpl-1.1': 'mpl-1.1',
    'mpl-2.0': 'mpl-2.0',
    'mpl-1.1+': 'mpl-1.1',
    'mpl-2.0+': 'mpl-2.0',
}


@attr.s
class DebianLicenseSymbol:
    """
    This represents a license key as found in a License: field of a
    Debian Copyright File and the corresponding list of LicenseMatch objects.
    This object is suitable to be used as a license_expression.LicenseSymbolLike object.
    """

    key = attr.ib()
    is_exception = attr.ib(default=False)
    matches = attr.ib(default=attr.Factory(list))

    def get_matched_expression(self, licensing=Licensing()):
        """
        Return a single license_expression.LicenseExpression object crafted
        from the list of LicenseMatch objects.
        """
        assert self.matches, f'Cannot build expression from empty matches: {self}'
        if len(self.matches) > 1:
            expression = licensing.AND(
                *[match.rule.license_expression_object for match in self.matches]
            )
        else:
            expression = self.matches[0].rule.license_expression_object

        return expression


@attr.s
class CopyrightDetection:
    """
    Represent copyright detections for a single paragraph in a debian copyright file.
    """
    paragraph = attr.ib()
    copyrights = attr.ib()


@attr.s
class LicenseDetection:
    """
    Represent license detections for a single paragraph in a debian copyright file.
    """
    paragraph = attr.ib()
    license_expression_object = attr.ib()
    license_matches = attr.ib()

    def is_detection_declarable(self):
        """
        LicenseDetection objects contain both license texts detection in ficense/file/other
        paragraphs, and only license detections which are in files paragraph has existing
        `license_expression_object` to report.
        Also, in the case of reporting declared licenses other paragraphs are not reported.
        """
        if isinstance(self.paragraph, CopyrightLicenseParagraph):
            return False
        elif isinstance(self.paragraph, CatchAllParagraph):
            return True

        elif self.paragraph.license.text:
            if self.license_expression_object is None:
                return False

        return True


@attr.s
class DebianLicensing:
    """
    This object exposes license expression parsing that is aware of the specific context
    of a copyright file.
    Within a copyright file we have a set of custom license symbols; in general
    we also have common debian licenses. These two combined form the set of symbols
    we can use to parse the license declaration in each of the paragraphs.
    """
    licensing = attr.ib()
    license_matches_by_symbol = attr.ib()
    # A mapping of 
    substitutions = attr.ib()

    # List of license name strings that could not be parsed
    unparsable_expressions = attr.ib()

    # List of LicenseDetection objects
    license_detections = attr.ib()

    @classmethod
    def from_license_paragraphs(cls, paras_with_license):
        """
        Return a DebianLicensing object built from a list of DebianCopyright paragraph
        objects.
        """
        # rename to plural in case  list
        result = DebianLicensing.parse_paras_with_license_text(
            paras_with_license=paras_with_license
        )
        license_matches_by_symbol, license_detections = result

        expression_symbols, unparsable_expressions = DebianLicensing.build_symbols(
            license_matches_by_symbol=license_matches_by_symbol
        )

        substitutions = {}
        for sym in expression_symbols:
            ds = sym.wrapped
            dse = ds.get_matched_expression()
            substitutions[sym] = dse

        licensing = Licensing(symbols=expression_symbols)

        return cls(
            licensing=licensing,
            license_matches_by_symbol=license_matches_by_symbol,
            substitutions=substitutions,
            unparsable_expressions=unparsable_expressions,
            license_detections=license_detections,
        )
        
    def get_normalized_expression(self, exp):
        """
        Return a LicenseExpression object build from an ``exp`` license expression string.
        """
        #TODO: Also return matches
        cleaned = clean_expression(exp)
        normalized_expression = None

        try:
            debian_expression = self.licensing.parse(cleaned)
            normalized_expression = debian_expression.subs(self.substitutions)

        except ExpressionError:
            # If Expression fails to parse we lookup exact string matches in License paras
            # which also failed to parse
            if cleaned in self.unparsable_expressions:
                matches_unparsable_expression = (
                    self.license_matches_by_symbol.get(cleaned)
                )
                normalized_expression = get_license_expression_from_matches(
                    license_matches=matches_unparsable_expression,
                )

            else:
                # Case where expression is not parsable and the same expression is not present in
                # the license paragraphs
                unknown_matches = add_unknown_matches(name=exp, text=None)
                normalized_expression = get_license_expression_from_matches(
                    license_matches=unknown_matches
                )
                
        return normalized_expression
        

    @staticmethod
    def parse_paras_with_license_text(paras_with_license):
        """
        Return a dictionary `license_matches_by_symbol` with declared licenses as keys and a list
        of License Detections from parsing paras with license text.
        """
        license_detections = []
        license_matches_by_symbol = {}

        for license_paragraph in paras_with_license:
            name = license_paragraph.license.name.lower()
            # Clean should also lower
            cleaned = clean_expression(name)
            text = license_paragraph.license.text
            text_matches = get_license_matches(query_string=text)
            matches = []

            common_license = common_licenses.get(name)
            if common_license:
                # For common license the name has a meaning, so create a synthetic match on that
                common_license_tag = f'License: {name}'
                common_license_matches = get_license_matches(
                    query_string=common_license_tag
                )
                if len(common_license_matches) != 1:
                    raise Exception(
                        'Rules for common License is missing: {common_license_tag}'
                    )

                common_license_match = common_license_matches[0]
                matches.append(common_license_match)

                # Raise Exception when all the license expressions of the matches are not
                # consistent with the common_license_match
                if not have_consistent_licenses_with_match(
                    matches=text_matches, reference_match=common_license_match
                ):
                    #TODO: Add unknown matches if matches are not consistent
                    # raise Exception(f'Inconsistent Licenses: {common_license_match} {matches}')
                    pass

                #TODO: Add unknown matches if matches are weak
                matches.extend(text_matches)
            else:
                #TODO: Add unknown matches if matches are weak
                if text_matches:
                    matches.extend(text_matches)
                else:
                    matches.extend(add_unknown_matches(name=name, text=text))

            if license_paragraph.comment:
                comment = license_paragraph.comment.text
                comment_matches = get_license_matches(query_string=comment)

                # If license detected in the comments are not consistent with the license
                # detected in text, add the license matches detected in the comment to be reported
                if comment_matches:
                    if not have_consistent_licenses(
                        matches=text_matches, reference_matches=comment_matches
                    ):
                        matches.extend(comment_matches)

            license_matches_by_symbol[cleaned] = matches

            license_detections.append(
                LicenseDetection(
                    paragraph=license_paragraph,
                    license_expression_object=None,
                    license_matches=matches,
                )
            )

        return license_matches_by_symbol, license_detections

    @staticmethod
    def build_symbols(license_matches_by_symbol, common_licenses=common_licenses):
        """
        Return a list of LicenseSymbolLike objects, built from known and common licenses.
        It is expected that license_matches_by_symbol keys are in lowercase.
        Also return a list of unparsable expressions.
        """
        symbols = []
        unparsable_expressions = []
        seen_keys = set()
        for key, matches in license_matches_by_symbol.items():
            sym = DebianLicenseSymbol(key=key, matches=matches)
            try:
                lsym = LicenseSymbolLike(symbol_like=sym)
                symbols.append(lsym)
            except ExpressionError:
                unparsable_expressions.append(key)
            seen_keys.add(key)

        for debian_key, _scancode_key in common_licenses.items():
            if debian_key in seen_keys:
                continue

            common_license_tag = f'License: {debian_key}'
            matches = get_license_matches(query_string=common_license_tag)
            sym = DebianLicenseSymbol(key=debian_key, matches=matches)
            lsym = LicenseSymbolLike(symbol_like=sym)
            symbols.append(lsym)

        return symbols, unparsable_expressions


@attr.s
class EnhancedDebianCopyright:    
    debian_copyright = attr.ib()

    def get_paragraphs_by_type(self, paragraph_type):
        return [
            paragraph
            for paragraph in self.debian_copyright.paragraphs
            if isinstance(paragraph, paragraph_type)
        ]

    @property
    def header_paragraph(self):
        header_paras = self.get_paragraphs_by_type(CopyrightHeaderParagraph)

        if not header_paras:
            return

        if len(header_paras) != 1:
            raise Exception(
                f'Multiple Header paragraphs in copyright file', *header_paras
            )

        return header_paras[0]

    @property
    def file_paragraphs(self):
        return self.get_paragraphs_by_type(CopyrightFilesParagraph)
    
    @property
    def license_paragraphs(self):
        return self.get_paragraphs_by_type(CopyrightLicenseParagraph)

    @property
    def paragraphs_with_license_text(self):
        paras_with_license_text = []

        for paragraph in self.debian_copyright.paragraphs:
            if isinstance(
                paragraph,
                (CopyrightHeaderParagraph, CopyrightFilesParagraph, CopyrightLicenseParagraph)
            ):
                if paragraph.license.text:
                    paras_with_license_text.append(paragraph)

        return paras_with_license_text

    @property
    def other_paragraphs(self):
        other_paras = []

        for paragraph in self.debian_copyright.paragraphs:
            if isinstance(paragraph, CopyrightLicenseParagraph) and not paragraph.license.name:
                other_paras.append(paragraph)

            elif isinstance(paragraph, CatchAllParagraph):
                other_paras.append(paragraph)

        return other_paras

    @property
    def duplicate_license_paragraphs(self):

        seen_license_names = set()
        duplicate_license_paras = []

        for paragraph in self.debian_copyright.paragraphs:
            if not isinstance(paragraph, CopyrightLicenseParagraph):
                continue

            lic_name = paragraph.license.name
            if not lic_name:
                continue
            
            if lic_name not in seen_license_names:
                seen_license_names.add(lic_name)
            else:
                duplicate_license_paras.append(paragraph)

        return duplicate_license_paras


def copyright_detector(location):
    """
    Return lists of detected copyrights, authors & holders in file at location.
    """
    if location:
        from cluecode.copyrights import detect_copyrights

        copyrights = []

        for dtype, value, _start, _end in detect_copyrights(location):
            if dtype == 'copyrights':
                copyrights.append(value)
        return copyrights


def get_license_matches(location=None, query_string=None):
    """
    Return a sequence of LicenseMatch objects.
    """
    if not query_string:
        return []
    from licensedcode import cache

    idx = cache.get_index()
    return idx.match(location=location, query_string=query_string)


def clean_debian_comma_logic(exp):
    """
    Convert Debian specific logic regarding comma to parsable license expression.
    Example:   `lgpl-3 or gpl-2, and apache-2.0` -> `(lgpl-3 or gpl-2) and apache-2.0`
    """
    subexps = []
    while ', and' in exp:
        exp, and_op, right = exp.rpartition(', and')
        subexps.insert(0, right)
    subexps.insert(0, exp)
    wrapped = [f'({i.strip()})' for i in subexps]
    cleaned = ' and '.join(wrapped)
    return cleaned


def clean_expression(text):
    """
    Return a cleaned license expression text by normalizing the syntax so it can be parsed.
    This substitution table has been derived from a large collection of most copyright files
    from Debian (about 320K files from circa 2019-11) and Ubuntu (about 200K files from circa
    2020-06)
    """
    if not text:
        return text

    text = text.lower()
    text = ' '.join(text.split())
    if ',' in text:
        text = clean_debian_comma_logic(text)

    transforms = {
        "at&t": "at_and_t",
        "ruby's": "rubys",
        "vixie's": "vixie",
        "bsd-3-clause~o'brien": "bsd-3-clause_o_brien",
        "core security technologies-pysmb": "core-security-technologies-pysmb",
        "|": " or ",
        "{fonts-open-sans}": "fonts-open-sans",
        "{texlive-fonts-extra}": "texlive-fonts-extra",
        " [ref": "_ref",
        "]": "_",
        "zpl 2.1": "zpl-2.1",
        "public domain": "public-domain",
        "gpl-2 (with openssl and foss license exception)": "(gpl-2 with openssl) and (gpl-2 with foss_license_exception)",
        "font embedding": "font_embedding",
        "special exception for compressed executables": "special_exception_for_compressed_executables",
        "exception, origin admission": "exception_origin_admission",
        "gcc runtime library exception": "gcc_runtime_library_exception",
        "special font exception": "special_font_exception",
        "the qt company gpl exception 1.0": "the_qt_company_gpl_exception_1.0",
        "section 7 exception": "section_7_exception",
        "g++ use exception": "g_use_exception",
        "gstreamer link exception": "gstreamer_link_exception",
        "additional permission": "additional_permission",
        "autoconf ": "autoconf_",
        "(lgpl-2.1 or lgpl-3) with qt exception": "lgpl-2.1 with qt_exception or lgpl-3 with qt_exception",
        "font exception (musescore)": "font_exception_musescore",
        "font exception (freefont)": "font_exception_freefont",
        "font exception (lilypond)": "font_exception_lilypond",
        "warranty disclaimer": "warranty_disclaimer",
        "bison exception 2.2": "bison_exception_2.2",
        "zlib/libpng": "zlib_libpng",
        "zlib/png": "zlib_png",
        "mit/x11": "mit_x11",
        "x/mit": "x_mit",
        "mit/x": "mit_x",
        "x11/mit": "x11_mit",
        "-clause/": "-clause_",
        "bsd/superlu": "bsd_superlu",
        "expat/mit": "expat_mit",
        "mit/expat": "mit_expat",
        "openssl/ssleay": "openssl_ssleay",
        "cc0/publicdomain": "cc0_publicdomain",
        "univillinois/ncsa": "univillinois_ncsa",
        " linking exception": "_linking_exception",
        " qt ": "_qt_",
        " exception": "_exception",
    }

    for source, target in transforms.items():
        cleaned_text = text.replace(source, target)

    return cleaned_text


def get_license_expression_from_matches(license_matches):
    """
    Craft a license expression from a list of LicenseMatch objects.
    """
    license_expressions = [match.rule.license_expression for match in license_matches]
    return combine_expressions(license_expressions, unique=False)


def add_unknown_matches(name, text):
    """
    Return a list of LicenseMatch objects created for an unknown license match.
    Return an empty list if both name and text are empty.
    """
    if not name and not text:
        return []

    name = name or ''
    text = text or ''
    license_text = f'License: {name}\n {text}'.strip()
    expression_str = 'unknown-license-reference'

    idx = get_index()
    query = Query(query_string=license_text, idx=idx)

    query_run = query.whole_query_run()

    match_len = len(query_run)
    match_start = query_run.start
    matched_tokens = query_run.tokens

    qspan = Span(range(match_start, query_run.end + 1))
    ispan = Span(range(0, match_len))
    len_legalese = idx.len_legalese
    hispan = Span(p for p, t in enumerate(matched_tokens) if t < len_legalese)

    rule = UnknownRule(
        license_expression=expression_str,
        stored_text=license_text,
        length=match_len,
    )

    match = LicenseMatch(
        rule=rule,
        qspan=qspan,
        ispan=ispan,
        hispan=hispan,
        query_run_start=match_start,
        matcher=MATCHER_UNKNOWN,
        query=query_run.query,
    )

    return [match]

@attr.s(slots=True, repr=False)
class UnknownRule(Rule):
    """
    A specialized rule object that is used for the special case of unknown matches in
    debian copyright files.

    Since there can be a lot of unknown licenses in a debian copyright file,
    the rule and the LicenseMatch objects for these are built at matching time.
    """

    def __attrs_post_init__(self, *args, **kwargs):
        self.identifier = 'debian-unknown-' + self.license_expression
        expression = self.licensing.parse(self.license_expression)

        self.license_expression = expression.render()
        self.license_expression_object = expression
        self.is_license_tag = True
        self.is_small = False
        self.relevance = 100
        self.has_stored_relevance = True

    def load(self):
        raise NotImplementedError

    def dump(self):
        raise NotImplementedError


def get_license_detection_from_nameless_paragraph(paragraph):
    """
    Return a LicenseDetection object built from any paragraph without a license name.
    """
    assert not paragraph.license.name
    matches = get_license_matches(paragraph.license.text)

    if not matches:
        unknown_matches = add_unknown_matches(name=None, text=paragraph.license.text)
        normalized_expression = get_license_expression_from_matches(
            license_matches=unknown_matches
        )
    else:
        #TODO: Add unknown matches if matches are weak
        normalized_expression = get_license_expression_from_matches(
            license_matches=matches
        )

    return LicenseDetection(
        paragraph=paragraph,
        license_expression_object=normalized_expression,
        license_matches=matches,
    )


def have_consistent_licenses(matches, reference_matches):
    """
    Return true if all the license of the matches list of LicenseMatch have the
    same license as all the licenses of the reference_matches list of LicenseMatch.
    """
    for reference_match in reference_matches:
        if not have_consistent_licenses_with_match(
            matches=matches,
            reference_match=reference_match,
        ):
            return False
    return True


def have_consistent_licenses_with_match(matches, reference_match):
    """
    Return true if all the license of the matches list of LicenseMatch have the
    same license as the reference_match LicenseMatch.
    """
    for match in matches:
        if not reference_match.same_licensing(match):
            return False
    return True


def is_paragraph_debian_packaging(paragraph):
    """
    Return True if the `paragraph` is a CopyrightFilesParagraph that applies
    only to the Debian packaging
    """
    return isinstance(
        paragraph, CopyrightFilesParagraph
    ) and paragraph.files.values == ['debian/*']


def is_paragraph_primary_license(paragraph):
    """
    Return True if the `paragraph` is a CopyrightFilesParagraph that contains
    the primary license.
    """
    return isinstance(
        paragraph, CopyrightFilesParagraph
    ) and paragraph.files.values == ['*']


def parse_structured_copyright_file(
    location,
    skip_debian_packaging=False,
):
    """
    Return a tuple of (list of declared license strings, list of detected license matches)
    collected from the debian copyright file at `location`.

    If `skip_debian_packaging` is False, the Debian packaging license is skipped if detected.

    Note: This was the older structured file parsing method which is now discontinued.
    """
    if not location:
        return None, None

    deco = DebianCopyright.from_file(location)

    declared_licenses = []
    detected_licenses = []

    # TODO: Revisit: is this really needed
    deco = refine_debian_copyright(deco)

    licensing = Licensing()
    for paragraph in deco.paragraphs:

        if is_paragraph_debian_packaging(paragraph) and not skip_debian_packaging:
            # Skipping packaging license and copyrights since they are not
            # relevant to the effective package license
            continue

        # rare case where we have not a structured file
        if isinstance(paragraph, CatchAllParagraph):
            text = paragraph.dumps()
            if text:
                detected = get_normalized_expression(
                    text,
                    try_as_expression=False,
                    approximate=False,
                )
                if not detected:
                    detected = 'unknown'
                detected_licenses.append(detected)
        else:
            plicense = paragraph.license
            if not plicense:
                continue

            declared, detected = detect_declared_license(plicense.name)
            if declared:
                declared_licenses.append(declared)
            if detected:
                detected_licenses.append(detected)

            # also detect in text
            text = paragraph.license.text
            if text:
                detected = get_normalized_expression(
                    text,
                    try_as_expression=False,
                    approximate=True,
                )
                if not detected:
                    detected = 'unknown'

                detected_licenses.append(detected)

    declared_license = '\n'.join(declared_licenses)

    if detected_licenses:
        detected_license = str(combine_expressions(detected_licenses))

    return declared_license, detected_license


def detect_declared_license(declared):
    """
    Return a tuple of (declared license, detected license expression) from a
    declared license. Both can be None.
    """
    declared = normalize_and_cleanup_declared_license(declared)

    if TRACE:
        logger_debug(f'detect_declared_license: {declared}')

    if not declared:
        return None, None

    # apply multiple license detection in sequence
    detected = detect_using_name_mapping(declared)
    if detected:
        return declared, detected

    from packagedcode import licensing

    try:
        detected = licensing.get_normalized_expression(
            declared,
            try_as_expression=False,
            approximate=False,
        )
    except Exception:
        # FIXME: add logging
        # we never fail just for this
        return 'unknown'

    return declared, detected


def normalize_and_cleanup_declared_license(declared):
    """
    Return a cleaned and normalized declared license.
    """
    declared = declared or ''
    # there are few odd cases of license fileds starting with a colon or #
    declared = declared.strip(': \t#')
    # normalize spaces
    declared = ' '.join(declared.split())
    return declared


def detect_using_name_mapping(declared):
    """
    Return a license expression detected from a declared_license.
    """
    declared = declared.lower()
    detected = get_declared_to_detected().get(declared)
    if detected:
        licensing = Licensing()
        return str(licensing.parse(detected, simple=True))


def refine_debian_copyright(debian_copyright):
    """
    Update in place the `debian_copyright` DebianCopyright object based on
    issues found in a large collection of Debian copyright files.
    """
    for paragraph in debian_copyright.paragraphs:
        if not hasattr(paragraph, 'license'):
            continue
        plicense = paragraph.license
        if not plicense:
            continue

        license_name = plicense.name
        if not license_name:
            continue

        if license_name.startswith('200'):
            # these are copyrights and not actual licenses, such as:
            # - 2005 Sergio Costas
            # - 2006-2010 by The HDF Group.

            if isinstance(
                paragraph, (CopyrightHeaderParagraph, CopyrightFilesParagraph)
            ):
                pcs = paragraph.copyright.statements or []
                pcs.append(license_name)
                paragraph.copyright.statements = pcs
                paragraph.license.name = None

        license_name_low = license_name.lower()
        NOT_A_LICENSE_NAME = (
            'according to',
            'by obtaining',
            'distributed under the terms of the gnu',
            'gnu general public license version 2 as published by the free',
            'gnu lesser general public license 2.1 as published by the',
        )
        if license_name_low.startswith(NOT_A_LICENSE_NAME):
            text = plicense.text
            if text:
                text = '\n'.join([license_name, text])
            else:
                text = license_name
            paragraph.license.name = None
            paragraph.license.text = text

    return debian_copyright


_DECLARED_TO_DETECTED = None


def get_declared_to_detected(data_file=None):
    """
    Return a mapping of declared to detected license expression cached and
    loaded from a tab-separated text file, all lowercase.

    Each line has this form:
        some license name<tab>scancode license expression

    For instance:
        2-clause bsd    bsd-simplified

    This data file is about license keys used in copyright files and has been
    derived from a large collection of most copyright files from Debian (about
    320K files from circa 2019-11) and Ubuntu (about 200K files from circa
    2020-06)
    """
    global _DECLARED_TO_DETECTED
    if _DECLARED_TO_DETECTED:
        return _DECLARED_TO_DETECTED

    _DECLARED_TO_DETECTED = {}
    if not data_file:
        data_file = path.join(path.dirname(__file__), 'debian_licenses.txt')
    with io.open(data_file, encoding='utf-8') as df:
        for line in df:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            decl, _, detect = line.strip().partition('\t')
            if detect and detect.strip():
                decl = decl.strip()
                _DECLARED_TO_DETECTED[decl] = detect
    return _DECLARED_TO_DETECTED
