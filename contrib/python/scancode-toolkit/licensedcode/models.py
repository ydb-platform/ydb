#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import io
import shutil
import traceback
from collections import Counter
from collections import defaultdict
from functools import partial
from itertools import chain
from operator import itemgetter
from os.path import abspath
from os.path import dirname
from os.path import exists
from os.path import join
import sys

import attr
import saneyaml
from license_expression import Licensing

from commoncode.fileutils import copyfile
from commoncode.fileutils import file_base_name
from commoncode.fileutils import file_name
from commoncode.fileutils import resource_iter
from licensedcode import MIN_MATCH_HIGH_LENGTH
from licensedcode import MIN_MATCH_LENGTH
from licensedcode import SMALL_RULE
from licensedcode.tokenize import index_tokenizer
from textcode.analysis import numbered_text_lines

"""
Reference License and license Rule structures persisted as a combo of a YAML
data file and one or more text files containing license or notice texts.
"""

# Set to True to print more detailed representations of objects when tracing
TRACE_REPR = False

# these are globals but always side-by-side with the code so do no not move them around
data_dir = join(abspath(dirname(sys.executable)), 'data')
licenses_data_dir = join(data_dir, 'licenses')
rules_data_dir = join(data_dir, 'rules')

FOSS_CATEGORIES = set([
    'Copyleft',
    'Copyleft Limited',
    'Patent License',
    'Permissive',
    'Public Domain',
])

OTHER_CATEGORIES = set([
    'Commercial',
    'Proprietary Free',
    'Free Restricted',
    'Source-available',
    'Unstated License',
])

CATEGORIES = FOSS_CATEGORIES | OTHER_CATEGORIES


@attr.s(slots=True)
class License(object):
    """
    A license consists of these files, where <key> is the license key:
        - <key>.yml : the license data in YAML
        - <key>.LICENSE: the license text

    A License object is identified by a unique `key` and its data stored in the
    `src_dir` directory. Key is a lower-case unique ascii string.
    """

    __attrib = partial(attr.ib, repr=False)

    # mandatory unique key: lower case ASCII characters, digits, underscore and dots.
    key = attr.ib(repr=True)

    src_dir = __attrib(default=licenses_data_dir)

    # if this is a deprecated license, add also notes explaining why
    is_deprecated = __attrib(default=False)

    # if this license text is not in English, set this field to a two letter
    # ISO 639-1 language code https://en.wikipedia.org/wiki/ISO_639-1
    # NOTE: this is not yet supported.
    # NOTE: each translation of a license text MUST have a different license key
    language = __attrib(default='en')

    # commonly used short name, often abbreviated.
    short_name = __attrib(default=None)
    # full name.
    name = __attrib(default=None)

    # Permissive, Copyleft, etc
    category = __attrib(default=None)

    owner = __attrib(default=None)
    homepage_url = __attrib(default=None)
    notes = __attrib(default=None)

    # if this is a license exception, the license key this exception applies to
    is_exception = __attrib(default=False)

    # SPDX key for SPDX licenses
    spdx_license_key = __attrib(default=None)
    # list of other keys, such as deprecated ones
    other_spdx_license_keys = __attrib(default=attr.Factory(list))
    # OSI License Key
    osi_license_key = __attrib(default=None)
    # Various URLs for info
    text_urls = __attrib(default=attr.Factory(list))
    osi_url = __attrib(default=None)
    faq_url = __attrib(default=None)
    other_urls = __attrib(default=attr.Factory(list))

    # various alternate keys for this license
    key_aliases = __attrib(default=attr.Factory(list))

    minimum_coverage = __attrib(default=0)
    standard_notice = __attrib(default=None)

    # lists of copuyrights, emails and URLs that can be ignored when detected
    # in this license as they are part of the license or rule text itself
    ignorable_copyrights = __attrib(default=attr.Factory(list))
    ignorable_authors = __attrib(default=attr.Factory(list))
    ignorable_holders = __attrib(default=attr.Factory(list))
    ignorable_urls = __attrib(default=attr.Factory(list))
    ignorable_emails = __attrib(default=attr.Factory(list))

    # data file paths and known extensions
    data_file = __attrib(default=None)
    text_file = __attrib(default=None)

    def __attrs_post_init__(self, *args, **kwargs):

        if self.src_dir:
            self.set_file_paths()

            if exists(self.data_file):
                self.load()

    def set_file_paths(self):
        self.data_file = join(self.src_dir, f'{self.key}.yml')
        self.text_file = join(self.src_dir, f'{self.key}.LICENSE')

    def relocate(self, target_dir, new_key=None):
        """
        Return a copy of this License object relocated to a new ``target_dir``
        with data and license text files saved to the new ``target_dir``.
        Use the ``new_key`` as  license key if provided.
        """
        if not target_dir:
            raise ValueError(
                f'Cannot relocate {self.key} License to empty directory '
            )
        if target_dir == self.src_dir:
            raise ValueError(
                f'Cannot relocate {self.key} License to its current directory.'
            )

        if new_key:
            key = new_key
        else:
            key = self.key

        newl = License(key=key, src_dir=target_dir)

        # copy fields
        excluded_fields = ('key', 'src_dir', 'data_file', 'text_file',)
        all_fields = attr.fields(self.__class__)
        attrs = [f.name for f in all_fields if f.name not in excluded_fields]
        for name in attrs:
            setattr(newl, name, getattr(self, name))

        # save it all to files
        if self.text:
            copyfile(self.text_file, newl.text_file)
        newl.dump()
        return newl

    def update(self, mapping):
        for k, v in mapping.items():
            setattr(self, k, v)

    def __copy__(self):
        oldl = self.to_dict()
        newl = License(key=self.key)
        newl.update(oldl)
        return newl

    @property
    def text(self):
        """
        License text, re-loaded on demand.
        """
        return self._read_text(self.text_file)

    def to_dict(self):
        """
        Return an ordered mapping of license data (excluding texts).
        Fields with empty values are not included.
        """

        # do not dump false, empties and paths
        def dict_fields(attr, value):
            if not value:
                return False

            if attr.name in ('data_file', 'text_file', 'src_dir',):
                return False

            # default to English
            if attr.name == 'language' and value == 'en':
                return False

            if attr.name == 'minimum_coverage' and value == 100:
                return False
            return True

        data = attr.asdict(self, filter=dict_fields, dict_factory=dict)
        cv = data.get('minimum_coverage', 0)
        if cv:
            data['minimum_coverage'] = as_int(cv)
        return data

    def dump(self):
        """
        Dump a representation of this license as two files:
         - <key>.yml : the license data in YAML
         - <key>.LICENSE: the license text
        """

        def write(location, byte_string):
            # we write as binary because rules and licenses texts and data are UTF-8-encoded bytes
            with io.open(location, 'wb') as of:
                of.write(byte_string)

        as_yaml = saneyaml.dump(self.to_dict(), indent=4, encoding='utf-8')
        write(self.data_file, as_yaml)
        if self.text:
            write(self.text_file, self.text.encode('utf-8'))

    def load(self):
        """
        Populate license data from a YAML file stored in of self.src_dir.
        Does not load text files yet.
        Unknown fields are ignored and not bound to the License object.
        """
        try:
            with io.open(self.data_file, encoding='utf-8') as f:
                data = saneyaml.load(f.read())

            for k, v in data.items():
                if k == 'minimum_coverage':
                    v = as_int(v)

                if k == 'key':
                    assert self.key == v, (
                        'The license "key" attribute in the .yml file MUST ' +
                        'be the same as the base name of this license .LICENSE ' +
                        'and .yml data files license files. ' +
                        f'Yet file name = {self.key} and license key = {v}'
                    )

                setattr(self, k, v)

        except Exception as e:
            # this is a rare case: fail loudly
            print()
            print('#############################')
            print('INVALID LICENSE YAML FILE:', f'file://{self.data_file}')
            print('#############################')
            print(e)
            print('#############################')
            raise e

    def _read_text(self, location):
        if not exists(location):
            text = ''
        else:
            with io.open(location, encoding='utf-8') as f:
                text = f.read()
        return text

    def spdx_keys(self):
        """
        Yield SPDX keys for this license.
        """
        if self.spdx_license_key:
            yield self.spdx_license_key
        for key in self.other_spdx_license_keys:
            yield key

    @staticmethod
    def validate(licenses, verbose=False, no_dupe_urls=False):
        """
        Check that the ``licenses`` a mapping of {key: License} are valid.
        Return dictionaries of infos, errors and warnings mapping a license key
        to validation issue messages. Print messages if ``verbose`` is True.

        NOTE: we DO NOT run this validation as part of loading or constructing
        License objects. Instead this is invoked ONLY as part of the test suite.
        """
        infos = defaultdict(list)
        warnings = defaultdict(list)
        errors = defaultdict(list)

        # used for global dedupe of texts
        by_spdx_key = defaultdict(list)
        by_text = defaultdict(list)
        by_short_name = defaultdict(list)
        by_name = defaultdict(list)

        for key, lic in licenses.items():
            warn = warnings[key].append
            info = infos[key].append
            error = errors[key].append
            by_name[lic.name].append(lic)
            by_short_name[lic.short_name].append(lic)

            if not lic.short_name:
                error('No short name')
            elif len(lic.short_name) > 50:
                error('short name must be under 50 characters.')
            if not lic.name:
                error('No name')
            if not lic.category:
                error('No category')
            if lic.category and lic.category not in CATEGORIES:
                cats = '\n'.join(sorted(CATEGORIES))
                error(
                    f'Unknown license category: {lic.category}.\n' +
                    f'Use one of these valid categories:\n{cats}'
                )
            if not lic.owner:
                error('No owner')

            # URLS dedupe and consistency
            if no_dupe_urls:
                if lic.text_urls and not all(lic.text_urls):
                    warn('Some empty text_urls values')

                if lic.other_urls and not all(lic.other_urls):
                    warn('Some empty other_urls values')

                # redundant URLs used multiple times
                if lic.homepage_url:
                    if lic.homepage_url in lic.text_urls:
                        warn('Homepage URL also in text_urls')
                    if lic.homepage_url in lic.other_urls:
                        warn('Homepage URL also in other_urls')
                    if lic.homepage_url == lic.faq_url:
                        warn('Homepage URL same as faq_url')
                    if lic.homepage_url == lic.osi_url:
                        warn('Homepage URL same as osi_url')

                if lic.osi_url or lic.faq_url:
                    if lic.osi_url == lic.faq_url:
                        warn('osi_url same as faq_url')

                all_licenses = lic.text_urls + lic.other_urls
                for url in lic.osi_url, lic.faq_url, lic.homepage_url:
                    if url:
                        all_licenses.append(url)

                if not len(all_licenses) == len(set(all_licenses)):
                    warn('Some duplicated URLs')

            # local text consistency
            text = lic.text

            license_qtokens = tuple(index_tokenizer(text))
            if not license_qtokens:
                info('No license text')
            else:
                # for global dedupe
                by_text[license_qtokens].append(f'{key}: TEXT')

            # SPDX consistency
            if lic.spdx_license_key:
                by_spdx_key[lic.spdx_license_key].append(key)
            else:
                # SPDX license key is now mandatory
                error('No SPDX license key')
            for oslk in lic.other_spdx_license_keys:
                by_spdx_key[oslk].append(key)

        # global SPDX consistency
        multiple_spdx_keys_used = {
            k: v for k, v in by_spdx_key.items()
            if len(v) > 1
        }
        if multiple_spdx_keys_used:
            for k, lkeys in multiple_spdx_keys_used.items():
                errors['GLOBAL'].append(
                    f'SPDX key: {k} used in multiple licenses: ' +
                    ', '.join(sorted(lkeys)))

        # global text dedupe
        multiple_texts = {k: v for k, v in by_text.items() if len(v) > 1}
        if multiple_texts:
            for k, msgs in multiple_texts.items():
                errors['GLOBAL'].append(
                    'Duplicate texts in multiple licenses: ' +
                    ', '.join(sorted(msgs))
                )

        # global short_name dedupe
        for short_name, licenses in by_short_name.items():
            if len(licenses) == 1:
                continue
            errors['GLOBAL'].append(
                f'Duplicate short name: {short_name} in licenses: ' +
                ', '.join(l.key for l in licenses)
            )

        # global name dedupe
        for name, licenses in by_name.items():
            if len(licenses) == 1:
                continue
            errors['GLOBAL'].append(
                f'Duplicate name: {name} in licenses: ' +
                ', '.join(l.key for l in licenses)
            )

        errors = {k: v for k, v in errors.items() if v}
        warnings = {k: v for k, v in warnings.items() if v}
        infos = {k: v for k, v in infos.items() if v}

        if verbose:
            print('Licenses validation errors:')
            for key, msgs in sorted(errors.items()):
                print(f'ERRORS for: {key}:', '\n'.join(msgs))

            print('Licenses validation warnings:')
            for key, msgs in sorted(warnings.items()):
                print(f'WARNINGS for: {key}:', '\n'.join(msgs))

            print('Licenses validation infos:')
            for key, msgs in sorted(infos.items()):
                print(f'INFOS for: {key}:', '\n'.join(msgs))

        return errors, warnings, infos


def ignore_editor_tmp_files(location):
    return location.endswith('.swp')


def load_licenses(licenses_data_dir=licenses_data_dir , with_deprecated=False):
    """
    Return a mapping of {key: License} loaded from license data and text files
    found in ``licenses_data_dir``. Raise Exceptions if there are dangling or
    orphaned files. Optionally include deprecated license if ``with_deprecated``
    is True.
    """
    licenses = {}
    used_files = set()
    all_files = set(resource_iter(
        licenses_data_dir,
        ignored=ignore_editor_tmp_files,
        with_dirs=False,
        follow_symlinks=True,
    ))

    for data_file in sorted(all_files):
        if data_file.endswith('.yml'):
            key = file_base_name(data_file)
            lic = License(key=key, src_dir=licenses_data_dir)
            used_files.add(data_file)
            if exists(lic.text_file):
                used_files.add(lic.text_file)
            if not with_deprecated and lic.is_deprecated:
                continue
            licenses[key] = lic

    dangling = all_files.difference(used_files)
    if dangling:
        msg = (
            f'Some License files are orphaned in {licenses_data_dir!r}.\n' +
            '\n'.join(f'file://{f}' for f in sorted(dangling))
        )
        raise Exception(msg)

    if not licenses:
        msg = (
            'No licenses were loaded. Check to see if the license data files '
            f'are available at "{licenses_data_dir}".'
        )
        raise Exception(msg)

    return licenses


def get_rules(
    licenses_db=None,
    licenses_data_dir=licenses_data_dir,
    rules_data_dir=rules_data_dir
):
    """
    Yield Rule objects loaded from a ``licenses_db`` and license files found in
    ``licenses_data_dir`` and rule files found in `rules_data_dir`. Raise an
    Exception if a rule is inconsistent or incorrect.
    """
    licenses_db = licenses_db or load_licenses(licenses_data_dir=licenses_data_dir)
    rules = list(load_rules(rules_data_dir=rules_data_dir))
    validate_rules(rules, licenses_db)
    licenses_as_rules = build_rules_from_licenses(licenses_db)
    return chain(licenses_as_rules, rules)


class InvalidRule(Exception):
    pass


def _validate_all_rules(rules, licenses_by_key):
    """
    Return a mapping of {error message: [list of Rule]} from validating a list
    of ``rules`` Rule integrity and correctness using known licenses from a
    mapping of ``licenses_by_key`` {key: License}`.
    """
    licensing = Licensing(licenses_by_key.values())
    errors = defaultdict(list)

    for rule in rules:
        for err_msg in rule.validate(licensing):
            errors[err_msg].append(rule)
    return errors


def validate_rules(rules, licenses_by_key, with_text=False):
    """
    Return a mapping of {error message: [list of Rule]) from validating a list
    of ``rules`` Rule integrity and correctness using known licenses from a
    mapping of ``licenses_by_key`` {key: License}`.
    """
    errors = _validate_all_rules(rules, licenses_by_key)
    if errors:
        message = ['Errors while validating rules:']
        for msg, rules in errors.items():
            message.append('')
            message.append(msg)
            for rule in rules:
                message.append(f'  {rule!r}')
                if rule.text_file:
                    message.append(f'    file://{rule.text_file}')
                if rule.data_file:
                    message.append(f'    file://{rule.data_file}')
                if with_text:
                    txt = rule.text()[:50].strip()
                    message.append(f'       {txt}...')
        raise InvalidRule('\n'.join(message))


def build_rules_from_licenses(licenses):
    """
    Return an iterable of rules built from each license text from a ``licenses``
    iterable of License objects.
    """
    for license_key, license_obj in licenses.items():
        text_file = join(license_obj.src_dir, license_obj.text_file)
        if exists(text_file):
            minimum_coverage = license_obj.minimum_coverage or 0
            yield Rule(
                text_file=text_file,
                license_expression=license_key,

                has_stored_relevance=False,
                relevance=100,

                has_stored_minimum_coverage=bool(minimum_coverage),
                minimum_coverage=minimum_coverage,

                is_from_license=True,
                is_license_text=True,

                ignorable_copyrights=license_obj.ignorable_copyrights,
                ignorable_holders=license_obj.ignorable_holders,
                ignorable_authors=license_obj.ignorable_authors,
                ignorable_urls=license_obj.ignorable_urls,
                ignorable_emails=license_obj.ignorable_emails,
            )


def get_all_spdx_keys(licenses_db):
    """
    Return an iterable of SPDX license keys collected from a `licenses_db`
    mapping of {key: License} objects.
    """
    for lic in licenses_db.values():
        for spdx_key in lic.spdx_keys():
            yield spdx_key


def get_essential_spdx_tokens():
    """
    Yield essential SPDX tokens.
    """
    yield 'spdx'
    yield 'license'
    yield 'licence'
    yield 'identifier'
    yield 'licenseref'


def get_all_spdx_key_tokens(licenses_db):
    """
    Yield SPDX token strings collected from a ``licenses_db`` mapping of {key:
    License} objects.
    """
    for tok in get_essential_spdx_tokens():
        yield tok

    for spdx_key in get_all_spdx_keys(licenses_db):
        for token in index_tokenizer(spdx_key):
            yield token


def load_rules(rules_data_dir=rules_data_dir):
    """
    Return an iterable of rules loaded from rule files in ``rules_data_dir``.
    """
    # TODO: OPTIMIZE: create a graph of rules to account for containment and
    # similarity clusters?
    seen_files = set()
    processed_files = set()
    lower_case_files = set()
    case_problems = set()
    space_problems = []
    model_errors = []
    for data_file in resource_iter(rules_data_dir, with_dirs=False):
        if data_file.endswith('.yml'):
            base_name = file_base_name(data_file)
            if ' ' in base_name:
                space_problems.append(data_file)
            rule_file = join(rules_data_dir, f'{base_name}.RULE')
            try:
                rule = Rule(data_file=data_file, text_file=rule_file)
                yield rule
            except Exception as re:
                model_errors.append(str(re))
            # accumulate sets to ensures we do not have illegal names or extra
            # orphaned files
            data_lower = data_file.lower()
            if data_lower in lower_case_files:
                case_problems.add(data_lower)
            else:
                lower_case_files.add(data_lower)

            rule_lower = rule_file.lower()
            if rule_lower in lower_case_files:
                case_problems.add(rule_lower)
            else:
                lower_case_files.add(rule_lower)

            processed_files.update([data_file, rule_file])

        if not data_file.endswith('~'):
            seen_files.add(data_file)

    unknown_files = seen_files - processed_files
    if unknown_files or case_problems or model_errors or space_problems:
        msg = ''

        if model_errors:
            errors = '\n'.join(model_errors)
            msg += (
                '\nInvalid rule YAML file in directory: '
                f'{rules_data_dir!r}\n{errors}'
            )

        if unknown_files:
            files = '\n'.join(sorted(f'f"ile://{f}"' for f in unknown_files))
            msg += (
                '\nOrphaned files in rule directory: '
                f'{rules_data_dir!r}\n{files}'
            )

        if case_problems:
            files = '\n'.join(sorted(f'"file://{f}"' for f in case_problems))
            msg += (
                '\nRule files with non-unique name in rule directory: '
                f'{rules_data_dir!r}\n{files}'
            )

        if space_problems:
            files = '\n'.join(sorted(f'"file://{f}"' for f in space_problems))
            msg += (
                '\nRule filename cannot contain spaces: '
                f'{rules_data_dir!r}\n{files}'
            )

        raise InvalidRule(msg)


@attr.s(slots=True)
class BasicRule(object):
    """
    A detection rule object is a text to use for detection and corresponding
    detected licenses and metadata. This is a basic metadata object that does
    not have specific support for data and text files.
    """
    licensing = Licensing()

    ###########
    # FIXME: !!! TWO RULES MAY DIFFER BECAUSE THEY ARE UPDATED BY INDEXING
    ###########

    # optional rule id int typically assigned at indexing time
    rid = attr.ib(default=None, repr=TRACE_REPR)

    # unique identifier
    identifier = attr.ib(default=None)

    # License expression string
    license_expression = attr.ib(default=None)

    # License expression object, created at build time
    license_expression_object = attr.ib(default=None, repr=False)

    # An indication of what this rule importance is (e.g. how important is its
    # text when detected as a licensing clue) as one of several "is_license_xxx"
    # flags. These flags are mutually exclusive and a license can only have one
    # of these as "yes"/True.

    # A license full text: this provides the highest level of confidence wrt
    # detection
    is_license_text = attr.ib(default=False, repr=False)

    # A license notice: this provides a strong confidence wrt detection
    is_license_notice = attr.ib(default=False, repr=False)

    # A reference is for a mere short license reference such as its bare name or
    # a URL that provides a weaker clue when detected
    is_license_reference = attr.ib(default=False, repr=False)

    # A tag is for a structured licensing tag such as a package manifest
    # metadata or an SPDX license identifier or similar package manifest tags. A
    # tag provides a strong clue with high confidence when detected even though
    # it may be very short.
    is_license_tag = attr.ib(default=False, repr=False)

    # An intro is a short introductory statment that may be placed before an
    # actual license text, notice or reference. For instance "This file is
    # licensed under ...". An intro is a weak clue that there some license
    # afterwards. It should be ignored or merged with the following license
    # detected immediately after.
    is_license_intro = attr.ib(default=False, repr=False)

    # Is this rule text a false positive when matched exactly? If yes, it will
    # filtered out at the end if matched (unless part of a large match)
    is_false_positive = attr.ib(default=False, repr=False)

    # Is this rule text only to be matched with a minimum coverage e.g. a
    # minimum proportion of tokens as a float between 0 and 100 where 100 means
    # all tokens must be matched and a smaller value means a smaller propertion
    # of matched tokens is acceptable. this is computed unless this is provided
    # here.
    minimum_coverage = attr.ib(default=0)
    has_stored_minimum_coverage = attr.ib(default=False, repr=False)
    # same as minimum_coverage but divided/100
    _minimum_containment = attr.ib(default=0, repr=False)

    # Can this rule be matched if there are unknown words in its matched range?
    # The default is to allow known and unknown words. Unknown words are words
    # that do not exist in the text of any indexed license or license detection
    # rule.
    only_known_words = attr.ib(default=False)

    # what is the relevance of a match to this rule text? a float between 0 and
    # 100 where 100 means highly relevant and 0 menas not relevant at all.
    # For instance a match to the "gpl" or the "cpol" words have a fairly low
    # relevance as they are a weak indication of an actual license and could be
    # a false positive. In somce cases, this may even be used to discard obvious
    # false positive matches automatically.
    relevance = attr.ib(default=100)
    has_stored_relevance = attr.ib(default=False, repr=False)

    # The rule contains a reference to some file name that comtains the text
    referenced_filenames = attr.ib(default=attr.Factory(list), repr=False)

    # optional, free text
    notes = attr.ib(default=None, repr=False)

    # set to True if the rule is built from a .LICENSE full text
    is_from_license = attr.ib(default=False, repr=False)

    # lists of copuyrights, emails and URLs that can be ignored when detected
    # in this license as they are part of the license or rule text itself
    ignorable_copyrights = attr.ib(default=attr.Factory(list), repr=False)
    ignorable_holders = attr.ib(default=attr.Factory(list), repr=False)
    ignorable_authors = attr.ib(default=attr.Factory(list), repr=False)
    ignorable_urls = attr.ib(default=attr.Factory(list), repr=False)
    ignorable_emails = attr.ib(default=attr.Factory(list), repr=False)

    ###########################################################################

    # path to the YAML data file for this rule
    data_file = attr.ib(default=None, repr=False)

    # path to the rule text file
    text_file = attr.ib(default=None, repr=False)

    # text of this rule for special cases where the rule is not backed by a file:
    # for SPDX license expression dynamic rules or testing
    stored_text = attr.ib(default=None, repr=False)

    # These attributes are computed upon text loading or setting the thresholds
    ###########################################################################

    # lengths in tokens
    length = attr.ib(default=0)
    min_matched_length = attr.ib(default=0, repr=TRACE_REPR)

    high_length = attr.ib(default=0, repr=TRACE_REPR)
    min_high_matched_length = attr.ib(default=0, repr=TRACE_REPR)

    # lengths in unique token.
    length_unique = attr.ib(default=0, repr=TRACE_REPR)
    min_matched_length_unique = attr.ib(default=0, repr=TRACE_REPR)

    high_length_unique = attr.ib(default=0, repr=TRACE_REPR)
    min_high_matched_length_unique = attr.ib(default=0, repr=TRACE_REPR)

    is_small = attr.ib(default=False, repr=TRACE_REPR)

    has_computed_thresholds = attr.ib(default=False, repr=False)

    def __attrs_post_init__(self, *args, **kwargs):
        self.setup()

    def setup(self):
        """
        Setup a few basic computed attributes after instance creation.
        """
        self.relevance = as_int(float(self.relevance or 100))
        self.minimum_coverage = as_int(float(self.minimum_coverage or 0))

        if self.license_expression:
            try:
                expression = self.licensing.parse(self.license_expression)
            except:
                exp = self.license_expression
                trace = traceback.format_exc()
                raise InvalidRule(
                    f'Unable to parse rule License expression: {exp!r} '
                    f'for: file://{self.data_file}\n{trace}'
                )

            if expression is None:
                raise InvalidRule(
                    f'Invalid rule License expression parsed to empty: '
                    f'{self.license_expression!r} for: file://{self.data_file}'
                )

            self.license_expression = expression.render()
            self.license_expression_object = expression

    def validate(self, licensing=None):
        """
        Validate this rule using the provided ``licensing`` Licensing and yield
        one error message for each type of error detected.
        """
        is_false_positive = self.is_false_positive

        license_flags = (
            self.is_license_notice,
            self.is_license_text,
            self.is_license_reference,
            self.is_license_tag,
            self.is_license_intro,
        )

        has_license_flags = any(license_flags)
        has_many_license_flags = len([l for l in license_flags if l]) != 1

        license_expression = self.license_expression

        ignorables = (
            self.ignorable_copyrights,
            self.ignorable_holders,
            self.ignorable_authors,
            self.ignorable_urls,
            self.ignorable_emails,
        )

        if is_false_positive:
            if not self.notes:
                yield 'is_false_positive rule must have notes.'

            if has_license_flags:
                yield 'is_false_positive rule cannot have is_license_* flags.'

            if license_expression:
                yield 'is_false_positive rule cannot have a license_expression.'

            if self.has_stored_relevance:
                yield 'is_false_positive rule cannot have a stored relevance.'

            if self.referenced_filenames:
                yield 'is_false_positive rule cannot have referenced_filenames.'

            if any(ignorables):
                yield 'is_false_positive rule cannot have ignorable_* attributes.'

        if not (0 <= self.minimum_coverage <= 100):
            yield 'Invalid rule minimum_coverage. Should be between 0 and 100.'

        if not is_false_positive:
            if not (0 <= self.relevance <= 100):
                yield 'Invalid rule relevance. Should be between 0 and 100.'

            if has_many_license_flags:
                yield 'Invalid rule is_license_* flags. Only one allowed.'

            if not has_license_flags:
                yield 'At least one is_license_* flag is needed.'

            if not check_is_list_of_strings(self.referenced_filenames):
                yield 'referenced_filenames must be a list of strings'

            if not all(check_is_list_of_strings(i) for i in ignorables):
                yield 'ignorables must be a list of strings'

            if not license_expression:
                yield 'Missing license_expression.'
            else:
                if licensing:
                    try:
                        licensing.parse(license_expression, validate=True, simple=True)
                    except InvalidRule as e:
                        yield f'Failed to parse and validate license_expression: {e}'

    def license_keys(self, unique=True):
        """
        Return a list of license keys for this rule.
        """
        if not self.license_expression:
            return []
        return self.licensing.license_keys(
            self.license_expression_object,
            unique=unique,
        )

    def same_licensing(self, other):
        """
        Return True if the other rule has the same licensing as this rule.
        """
        if self.license_expression and other.license_expression:
            return self.licensing.is_equivalent(
                self.license_expression_object,
                other.license_expression_object,
            )

    def licensing_contains(self, other):
        """
        Return True if this rule licensing contains the other rule licensing.
        """
        if self.license_expression and other.license_expression:
            return self.licensing.contains(
                self.license_expression_object,
                other.license_expression_object,
            )

    def spdx_license_expression(self, licensing=None):
        if not licensing:
            from licensedcode.cache import get_licensing
            licensing = get_licensing()
        parsed = licensing.parse(self.license_expression)
        return parsed.render(template='{symbol.spdx_license_key}')

    def get_length(self, unique=False):
        return self.length_unique if unique else self.length

    def get_min_matched_length(self, unique=False):
        return (self.min_matched_length_unique if unique
                else self.min_matched_length)

    def get_high_length(self, unique=False):
        return self.high_length_unique if unique else self.high_length

    def get_min_high_matched_length(self, unique=False):
        return (self.min_high_matched_length_unique if unique
                else self.min_high_matched_length)

    def to_dict(self):
        """
        Return an ordered mapping of self, excluding texts. Used for
        serialization. Empty values are not included.
        """
        data = {}

        is_false_positive = self.is_false_positive

        if self.license_expression:
            data['license_expression'] = self.license_expression

        flags = (
            'is_false_positive',
            'is_license_text',
            'is_license_notice',
            'is_license_reference',
            'is_license_tag',
            'is_license_intro',
            'only_known_words',
        )

        for flag in flags:
            tag_value = getattr(self, flag, False)
            if tag_value:
                data[flag] = tag_value

        if self.has_stored_relevance and self.relevance and not is_false_positive:
            data['relevance'] = as_int(self.relevance)

        if self.has_stored_minimum_coverage and self.minimum_coverage > 0 and not is_false_positive:
            data['minimum_coverage'] = as_int(self.minimum_coverage)

        if self.referenced_filenames and not is_false_positive:
            data['referenced_filenames'] = self.referenced_filenames

        if self.notes:
            data['notes'] = self.notes

        if not is_false_positive:
            ignorables = (
                'ignorable_copyrights',
                'ignorable_holders',
                'ignorable_authors',
                'ignorable_urls',
                'ignorable_emails',
            )

            for igno in ignorables:
                tag_value = getattr(self, igno, False)
                if tag_value:
                    data[igno] = tag_value

        return data

    def text(self):
        """
        Return the rule text loaded from its text file.
        """
        if self.text_file and exists(self.text_file):
            # IMPORTANT: use the same process as query text loading for symmetry
            numbered_lines = numbered_text_lines(
                self.text_file,
                demarkup=False,
                plain_text=True,
            )
            return ''.join(l for _, l in numbered_lines)

        # used for non-file backed rules
        elif self.stored_text:
            return self.stored_text

        else:
            raise InvalidRule(
                f'Inconsistent rule text for: {self.identifier}\n'
                f'file://{self.text_file}'
            )


def check_is_list_of_strings(l):
    """
    Return True if `l` is a list of strings or an empty list, False otherwise.
    """
    if isinstance(l, list):
        if l:
            return all(isinstance(i, str) for i in l)
        return True
    return False


def as_int(num):
    """
    Convert ``num`` to int if ``num`` is not an int and this would not lead to
    loss of information, e.g. when ``num`` is an int stored as a float type.
    """
    if isinstance(num, str):
        num = float(num)
    if isinstance(num, float):
        n = int(num)
        if n == num:
            return n
    return num


@attr.s(slots=True)
class Rule(BasicRule):
    """
    A detection rule object with support for data and text files.
    """

    def __attrs_post_init__(self, *args, **kwargs):
        self.load_data()
        self.setup()

    def load_data(self):
        """
        Load data from data file. Check presence of text file.
        """
        if not self.text_file:
            # for SPDX or tests only
            if not self.stored_text :
                raise InvalidRule(
                    f'Invalid rule without its corresponding text file: {self}')
            self.identifier = '_tst_' + str(len(self.stored_text))
        else:
            self.identifier = file_name(self.text_file)

        if self.data_file:
            try:
                self.load()
            except Exception:
                data_file = self.data_file
                trace = traceback.format_exc()
                raise InvalidRule(f'While loading: file://{data_file}\n{trace}')

    def tokens(self):
        """
        Return an iterable of token strings for this rule. Length, relevance and
        minimum_coverage may be recomputed as a side effect.
        """
        length = 0
        text = self.text()
        text = text.strip()

        # We tag this rule as being a bare URL if it starts with a scheme and is
        # on one line: this is used to determine a matching approach

        if (
            text.startswith(('http://', 'https://', 'ftp://'))
            and '\n' not in text[:1000]
        ):
            self.minimum_coverage = 100

        for token in index_tokenizer(self.text()):
            length += 1
            yield token

        self.length = length
        self.compute_relevance()

    def compute_thresholds(self, small_rule=SMALL_RULE):
        """
        Compute and set thresholds either considering the occurrence of all
        tokens or the occurence of unique tokens.
        """
        min_cov, self.min_matched_length, self.min_high_matched_length = (
            compute_thresholds_occurences(
                self.minimum_coverage,
                self.length,
                self.high_length,
            )
        )
        if not self.has_stored_minimum_coverage:
            self.minimum_coverage = min_cov

        self._minimum_containment = self.minimum_coverage / 100

        self.min_matched_length_unique, self.min_high_matched_length_unique = (
            compute_thresholds_unique(
                self.minimum_coverage,
                self.length,
                self.length_unique, self.high_length_unique,
            )
        )

        self.is_small = self.length < small_rule

    def dump(self):
        """
        Dump a representation of this rule as two files:
         - a .yml for the rule data in YAML (self.data_file)
         - a .RULE: the rule text as a UTF-8 file (self.text_file)
        Does nothing if this rule was created from a License (e.g.
        `is_from_license` is True)
        """
        if self.is_from_license:
            return

        def write(location, byte_string):
            # we write as binary because rules and licenses texts and data are
            # UTF-8-encoded bytes
            with io.open(location, 'wb') as of:
                of.write(byte_string)

        if self.data_file:
            as_yaml = saneyaml.dump(self.to_dict(), indent=4, encoding='utf-8')
            write(self.data_file, as_yaml)
            write(self.text_file, self.text().encode('utf-8'))

    def load(self):
        """
        Load self from a .RULE YAML file stored in self.data_file.
        Does not load the rule text file.
        Unknown fields are ignored and not bound to the Rule object.
        """
        try:
            with io.open(self.data_file, encoding='utf-8') as f:
                data = saneyaml.load(f.read())
        except Exception as e:
            print('#############################')
            print('INVALID LICENSE RULE FILE:', f'file://{self.data_file}')
            print('#############################')
            print(e)
            print('#############################')
            # this is a rare case, but yes we abruptly stop.
            raise e

        known_attributes = set(attr.fields_dict(self.__class__))
        data_file_attributes = set(data)
        unknown_attributes = data_file_attributes.difference(known_attributes)
        if unknown_attributes:
            unknown_attributes = ', '.join(sorted(unknown_attributes))
            msg = 'License rule {} data file has unknown attributes: {}'
            raise InvalidRule(msg.format(self, unknown_attributes))

        self.license_expression = data.get('license_expression')

        self.is_false_positive = data.get('is_false_positive', False)

        relevance = as_int(float(data.get('relevance') or 0))
        # Keep track if we have a stored relevance of not.
        if relevance:
            self.relevance = relevance
            self.has_stored_relevance = True
        else:
            self.relevance = 100
            self.has_stored_relevance = False

        minimum_coverage = as_int(float(data.get('minimum_coverage') or 0))
        self._minimum_containment = minimum_coverage / 100
        if minimum_coverage:
            # Keep track if we have a stored minimum_coverage of not.
            self.minimum_coverage = minimum_coverage
            self.has_stored_minimum_coverage = True
        else:
            self.minimum_coverage = 0
            self.has_stored_minimum_coverage = False

        self.is_license_text = data.get('is_license_text', False)
        self.is_license_notice = data.get('is_license_notice', False)
        self.is_license_tag = data.get('is_license_tag', False)
        self.is_license_reference = data.get('is_license_reference', False)
        self.is_license_intro = data.get('is_license_intro', False)
        self.only_known_words = data.get('only_known_words', False)

        self.referenced_filenames = data.get('referenced_filenames', []) or []

        # these are purely informational and not used at run time
        notes = data.get('notes')
        if notes:
            self.notes = notes.strip()

        self.ignorable_copyrights = data.get('ignorable_copyrights', [])
        self.ignorable_holders = data.get('ignorable_holders', [])
        self.ignorable_authors = data.get('ignorable_authors', [])
        self.ignorable_urls = data.get('ignorable_urls', [])
        self.ignorable_emails = data.get('ignorable_emails', [])

        return self

    def compute_relevance(self, _threshold=18.0):
        """
        Compute and set the `relevance` attribute for this rule. The relevance
        is a float between 0 and 100 where 100 means highly relevant and 0 means
        not relevant at all.

        For instance a match to the "gpl" or the "cpol" words have a fairly low
        relevance as they are a weak indication of an actual license and could
        be a false positive and should therefore be assigned a low relevance. In
        contrast a match to most or all of the apache-2.0 license text is highly
        relevant. The Rule relevance is used as the basis to compute a match
        score.

        The relevance is either pre-defined in the rule YAML data file with the
        "relevance" attribute or computed base on the rule length here using
        this approach:

        - false positive rule has 100 relevance.
        - rule length equal or larger than threshold has 100 relevance
        - rule length smaller than threshold has 100/threshold relevance rounded
          down.

        The current threshold is 18 words.
        """

        if self.has_stored_relevance:
            return

        if (isinstance(self, SpdxRule)
            # false positive rules with no license: they do not
            # have licenses and their matches are never returned
            or self.is_false_positive
        ):
            # use the default max relevance of 100
            self.relevance = 100

        relevance_of_one_word = round((1 / _threshold) * 100, 2)
        length = self.length
        if length >= _threshold:
            # general case
            self.relevance = 100
        else:
            computed = int(length * relevance_of_one_word)
            self.relevance = min([100, computed])

    def rule_dir(self):
        """
        Return the directory of this rule.
        """
        if not (self.text_file and self.data_file):
            raise Exception(f'Cannot obtain rule directory for: {self!r}')
        return dirname(self.data_file)

    def rename_and_relocate(self, name_prefix):
        """
        Generate a new rule name and relocate the rule files to this new name
        using the ``name_prefix`` prefix. The new rule name is guaranteed to be
        unique and not conflicting with any existing rule name.
        """
        new_base_loc = find_rule_base_location(
            name_prefix=name_prefix,
            rules_directory=self.rule_dir()
        )

        new_data_file = f'{new_base_loc}.yml'
        shutil.move(self.data_file, new_data_file)
        self.data_file = new_data_file

        new_text_file = f'{new_base_loc}.RULE'
        shutil.move(self.text_file, new_text_file)
        self.text_file = new_text_file


def compute_thresholds_occurences(
    minimum_coverage,
    length,
    high_length,
    _MIN_MATCH_HIGH_LENGTH=MIN_MATCH_HIGH_LENGTH,
    _MIN_MATCH_LENGTH=MIN_MATCH_LENGTH,
):
    """
    Compute and return thresholds considering the occurrence of all tokens.
    """
    if minimum_coverage == 100:
        min_matched_length = length
        min_high_matched_length = high_length
        return minimum_coverage, min_matched_length, min_high_matched_length

    if length < 3:
        min_high_matched_length = high_length
        min_matched_length = length
        minimum_coverage = 100

    elif length < 10:
        min_matched_length = length
        min_high_matched_length = high_length
        minimum_coverage = 80

    elif length < 30:
        min_matched_length = length // 2
        min_high_matched_length = min(high_length, _MIN_MATCH_HIGH_LENGTH)
        minimum_coverage = 50

    elif length < 200:
        min_matched_length = _MIN_MATCH_LENGTH
        min_high_matched_length = min(high_length, _MIN_MATCH_HIGH_LENGTH)
        # minimum_coverage = max(15, int(length//10))

    else:  # if length >= 200:
        min_matched_length = length // 10
        min_high_matched_length = high_length // 10
        # minimum_coverage = int(length//10)

    return minimum_coverage, min_matched_length, min_high_matched_length


def compute_thresholds_unique(
    minimum_coverage,
    length,
    length_unique,
    high_length_unique,
    _MIN_MATCH_HIGH_LENGTH=MIN_MATCH_HIGH_LENGTH,
    _MIN_MATCH_LENGTH=MIN_MATCH_LENGTH,
):
    """
    Compute and set thresholds considering the occurrence of only unique tokens.
    """
    if minimum_coverage == 100:
        min_matched_length_unique = length_unique
        min_high_matched_length_unique = high_length_unique
        return min_matched_length_unique, min_high_matched_length_unique

    if length > 200:
        min_matched_length_unique = length // 10
        min_high_matched_length_unique = high_length_unique // 10

    elif length < 5:
        min_matched_length_unique = length_unique
        min_high_matched_length_unique = high_length_unique

    elif length < 10:
        if length_unique < 2:
            min_matched_length_unique = length_unique
        else:
            min_matched_length_unique = length_unique - 1
        min_high_matched_length_unique = high_length_unique

    elif length < 20:
        min_matched_length_unique = high_length_unique
        min_high_matched_length_unique = high_length_unique

    else:
        min_matched_length_unique = _MIN_MATCH_LENGTH
        highu = (int(high_length_unique // 2)) or high_length_unique
        min_high_matched_length_unique = min(highu, _MIN_MATCH_HIGH_LENGTH)

    return min_matched_length_unique, min_high_matched_length_unique


@attr.s(slots=True, repr=False)
class SpdxRule(Rule):
    """
    A specialized rule object that is used for the special case of SPDX license
    expressions.

    Since we may have an infinite possible number of SPDX expressions and these
    are not backed by a traditional rule text file, we use this class to handle
    the specifics of these how rules that are built at matching time: one rule
    is created for each detected SPDX license expression.
    """

    def __attrs_post_init__(self, *args, **kwargs):
        self.identifier = f'spdx-license-identifier: {self.license_expression}'
        expression = None
        try:
            expression = self.licensing.parse(self.license_expression)
        except:
            raise InvalidRule(
                'Unable to parse License rule expression: '
                f'{self.license_expression!r} for: SPDX rule: '
                f'{self.stored_text}\n' + traceback.format_exc()
            )

        if expression is None:
            raise InvalidRule(
                'Unable to parse License rule expression: '
                f'{self.license_expression!r} for: {self.data_file!r}'
            )

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


def _print_rule_stats():
    """
    Print rules statistics.
    """
    from licensedcode.cache import get_index
    idx = get_index()
    rules = idx.rules_by_rid
    sizes = Counter(r.length for r in rules)
    print('Top 15 lengths: ', sizes.most_common(15))
    print(
        '15 smallest lengths: ',
        sorted(sizes.items(),
        key=itemgetter(0))[:15],
    )

    high_sizes = Counter(r.high_length for r in rules)
    print('Top 15 high lengths: ', high_sizes.most_common(15))
    print(
        '15 smallest high lengths: ',
        sorted(high_sizes.items(),
        key=itemgetter(0))[:15],
    )


def update_ignorables(licensish, verbose=False):
    """
    Update ignorables and return the ``licensish`` Rule or License using the
    latest values detected in its text.

    Display progress messages if ``verbose`` is True.
    """

    if verbose:
        print(f'Processing: file://{licensish.text_file}')

    if not exists(licensish.text_file):
        return licensish

    ignorables = get_ignorables(text_file=licensish.text_file, verbose=verbose)
    set_ignorables(licensish, ignorables, verbose=verbose)
    return licensish


def set_ignorables(licensish, ignorables, verbose=False):
    """
    Update ``licensish`` Rule or License using the mapping of ``ignorables``
    attributes.

    Display progress messages if ``verbose`` is True.
    """
    for key, value in ignorables.items():
        if verbose:
            existing = getattr(licensish, key, None)
            print(f'Updating ignorable: {key} from: {existing!r} to: {value!r}')
        setattr(licensish, key, value)
    return licensish


def get_ignorables(text_file, verbose=False):
    """
    Return a mapping of ignorable clues lists found in a ``text_file`` for
    copyrights, holders, authors, urls, emails. Do not include items with empty
    values.

    Display progress messages if ``verbose`` is True.
    """
    from cluecode.copyrights import detect_copyrights
    from cluecode.finder import find_urls
    from cluecode.finder import find_emails

    # redundant clues found in a license or rule text
    # collect and set ignorable copyrights, holders and authors
    copyrights = set()
    holders = set()
    authors = set()

    for dtype, value, _start, _end in detect_copyrights(text_file):
        if verbose:
            print(f'  Found {dtype}: {value}')

        if dtype == 'copyrights':
            copyrights.add(value)
        elif dtype == 'holders':
            holders.add(value)
        elif dtype == 'authors':
            authors.add(value)

    # collect and set ignorable emails and urls
    urls = set(u for (u, _ln) in find_urls(text_file) if u)
    if verbose:
        print(f'  Found urls: {urls}')

    emails = set(e for (e, _ln) in find_emails(text_file) if e)
    if verbose:
        print(f'  Found emails: {emails}')

    ignorables = dict(
        ignorable_copyrights=sorted(copyrights),
        ignorable_holders=sorted(holders),
        ignorable_authors=sorted(authors),
        ignorable_urls=sorted(urls),
        ignorable_emails=sorted(emails),
    )

    ignorables = {k: v for k, v in sorted(ignorables.items()) if v}
    if verbose:
        print(f'  Found ignorables: {ignorables}')
    return ignorables


def find_rule_base_location(name_prefix, rules_directory=rules_data_dir):
    """
    Return a new, unique and non-existing base location in ``rules_directory``
    with a file name but without an extension suitable to create a new rule
    without overwriting any existing rule. Use the ``name_prefix`` string as a
    prefix for this name.
    """

    cleaned = (
        name_prefix
        .lower()
        .strip()
        .replace(' ', '_')
        .replace('(', '')
        .replace(')', '')
        .strip('_-')
    )
    template = cleaned + '_{idx}'

    idx = 1
    while True:
        base_name = template.format(idx=idx)
        base_loc = join(rules_directory, base_name)
        if not exists(f'{base_loc}.RULE'):
            return base_loc
        idx += 1
