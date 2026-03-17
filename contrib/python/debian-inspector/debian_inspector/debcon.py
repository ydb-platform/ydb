#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0 AND MIT
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/debian-inspector for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
# Copyright (c) 2018 Peter Odding
# Author: Peter Odding <peter@peterodding.com>
# URL: https://github.com/xolox/python-deb-pkg-tools

from collections.abc import Mapping
from collections.abc import MutableMapping
from collections.abc import Sequence
import email
import io
import re
import textwrap

from attr import attrs
from attr import attrib
import chardet

from debian_inspector import unsign

"""
Utilities to parse Debian-style control files aka. deb822 format.

See https://salsa.debian.org/dpkg-team/dpkg/blob/0c9dc4493715ff3b37262528055943c52fdfb99c/man/deb822.man

https://www.debian.org/doc/debian-policy/ch-controlfields#s-f-Description

This is an alternative to a subset of python-debian library with these
characteristics:

 - We use a lenient parsing accepting things that would not be considered
   strictly Debian-compliant.
 - The focus is first on reading Debian files and writing them second.
 - Copyright, package, control and status files are the most interesting formats.
   Changelog and other Debian file types are mostly ignored for now.
 - There is no attention paid to compatibility and support for older formats
   and older Python versions before 3.6.
"""


@attrs
class FieldMixin(object):
    """
    Base mixin for attrs-based fields.
    """

    @classmethod
    def attrib(cls, **kwargs):
        """
        Return an attrib class
        """
        return attrib(converter=cls.from_value, **kwargs)

    @classmethod
    def from_value(cls, value):
        if isinstance(value, cls):
            return value
        return cls(value)

    def dumps(self):
        """
        Return a string in Debian format for this field.
        """
        return NotImplementedError

    def __str__(self, *args, **kwargs):
        return self.dumps()


@attrs
class SingleLineField(FieldMixin):
    """
    https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/#single-line
    """

    value = attrib()

    @classmethod
    def from_value(cls, value):
        return cls(value=value and value.strip())

    def dumps(self):
        return self.value or ""


@attrs
class LineSeparatedField(FieldMixin):
    """
    https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/#line-based-lists
    """

    values = attrib()

    @classmethod
    def from_value(cls, value):
        values = []
        if value:
            for val in line_separated(value):
                values.append(val.strip())
        return cls(values=values)

    def dumps(self, **kwargs):
        return "\n ".join(self.values or [])


@attrs
class LineAndSpaceSeparatedField(FieldMixin):
    """
    LineAndSpaceSeparatedField is a list of values where each item is itself a space-separated list.
    """

    values = attrib()

    @classmethod
    def from_value(cls, value):
        values = []
        if value:
            for val in line_separated(value):
                values.append(tuple(space_separated(val)))
        return cls(values=values)

    def dumps(self, **kwargs):
        return "\n ".join(" ".join(v) for v in self.values or [])


@attrs
class AnyWhiteSpaceSeparatedField(FieldMixin):
    """
    https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/#white-space-lists
    This is a list of values where each item is itself a space-separated list.
    """

    values = attrib()

    @classmethod
    def from_value(cls, value):
        values = []
        if value:
            values = [val for val in value.split()]
        return cls(values=values)

    def dumps(self, **kwargs):
        return "\n ".join(self.values or [])


@attrs
class FormattedTextField(FieldMixin):
    """
    https://www.debian.org/doc/debian-policy/ch-controlfields#description
    Like Description, but there is no special meaning for the first line.
    """

    text = attrib()

    @classmethod
    def from_value(cls, value):
        if value:
            value = from_formatted_text(value)
        return cls(text=value)

    def dumps(self, **kwargs):
        lines = line_separated(self.text)
        if not lines:
            return ""
        return as_formatted_lines(lines)


def as_formatted_lines(lines):
    """
    Return a text formatted for use in a Debian control file with proper
    continuation for multilines.
    """
    if not lines:
        return ""
    formatted = []
    for line in lines:
        is_blank = not line.strip()
        if is_blank:
            formatted.append(".")
        else:
            formatted.append(f"{line}")
    return "\n ".join(formatted)


def as_formatted_text(text):
    """
    Return a text formatted for use in a Debian control file with proper
    continuation for multilines.
    """
    if not text:
        return text
    lines = text.splitlines(False)
    return as_formatted_lines(lines)


def from_formatted_text(text):
    """
    Return cleaned text from a Debian formatted description text
    using rules for handling line prefixes and continuations.
    """
    if not text:
        return text
    return from_formatted_lines(line_separated(text))


def from_formatted_lines(lines):
    """
    Return text from a list of `lines` strings using the Debian
    Description rules for handling line prefixes and continuations.
    """
    if not lines:
        return lines

    # first line is always "stripped"
    text = [lines[0].strip()]
    for line in lines[1:]:
        line = line.rstrip()
        if line.startswith("  "):
            # starting with two or more spaces: displayed verbatim.
            text.append(line[1:])
        elif line == (" ."):
            # containing a single space followed by a single full stop
            # character: rendered as blank lines.
            text.append("")
        elif line.startswith(" ."):
            # containing a space, a full stop and some more characters:  for
            # future expansion.... but we keep them for now
            text.append(line[2:])
        elif line.startswith(" "):
            # starting with a single space. kept stripped
            text.append(line.strip())
        else:
            # this should never happen!!!
            # but we keep it too
            text.append(line.strip())
    return "\n".join(text)


@attrs
class DescriptionField(FieldMixin):
    """
    https://www.debian.org/doc/debian-policy/ch-controlfields#description
    5.6.13. Description
    """

    synopsis = attrib(default=None)
    text = attrib(default=None)

    @classmethod
    def from_value(cls, value):
        value = value or ""
        lines = line_separated(value)
        if lines:
            synopsis = lines[0].strip()
            text = from_formatted_lines(lines[1:])
            return cls(synopsis=synopsis, text=text)
        else:
            return cls(synopsis="")

    def dumps(self, **kwargs):
        """
        Return a string representation of self.
        """
        syn = self.synopsis or ""
        syn = syn.strip()
        dumped = [syn]
        text = self.text or ""
        if text:
            if text.startswith(" "):
                text = text[1:]
            dumped.append(as_formatted_text(text))
        return "\n ".join(dumped)


@attrs
class File(object):
    name = attrib(default=None)
    size = attrib(default=None)
    md5 = attrib(default=None)
    sha1 = attrib(default=None)
    sha256 = attrib(default=None)
    sha512 = attrib(default=None)


@attrs
class FileField(FieldMixin):
    name = attrib(default=None)
    size = attrib(default=None)
    checksum = attrib(default=None)

    @classmethod
    def from_value(cls, value):
        checksum = size = name = None
        if value:
            checksum, size, name = space_separated(value)
        return cls(checksum=checksum, size=size, name=name)

    def dumps(self, **kwargs):
        return "{} {} {}".format(self.checksum, self.size, self.name)


@attrs
class FilesField(FieldMixin):
    """
    FilesField is a list of FileField
    """

    values = attrib()

    @classmethod
    def from_value(cls, value):
        values = []
        if value:
            for val in line_separated(value):
                values.append(FileField.from_value(val))
        return cls(values=values)

    def dumps(self, **kwargs):
        return "\n ".join(v.dumps(**kwargs) for v in self.values or [])


def collect_files(data):
    """
    Return a mapping of {name: File} from a Debian data mapping.

    Note: the Files and Checksums-* fields have the same structure and
    contain redundant data.
    """
    files = {}
    for name, size, md5 in collect_file(data.get("files", [])):
        f = File(md5, size, name)
        files[name] = f

    for name, size, sha1 in collect_file(data.get("checksums-sha1", [])):
        f = files[name]
        assert f.size == size
        f.sha1 = sha1

    for name, size, sha256 in collect_file(data.get("checksums-sha256", [])):
        f = files[name]
        assert f.size == size
        f.sha256 = sha256

    for name, size, sha512 in collect_file(data.get("checksums-v", [])):
        f = files[name]
        assert f.size == size
        f.sha512 = sha512

    return files


def collect_file(value):
    """
    Yield tuples of (name, size, digest) given a Debian "Files" value string
    which contains digest, size and name.
    """
    for line in line_separated(value):
        digest, size, name = space_separated(line)
        yield name, size, digest


@attrs
class MaintainerField(FieldMixin):
    """
    https://www.debian.org/doc/debian-policy/ch-controlfields#s-f-maintainer
    5.6.2. Maintainer
    """

    name = attrib()
    email_address = attrib(default=None)

    @classmethod
    def from_value(cls, value):
        name = email_address = None
        if value:
            value = value.strip()
            name, email_address = email.utils.parseaddr(value)  # NOQA
            if not name:
                name = value
                email_address = None
            return cls(name=name, email_address=email_address)

    def dumps(self, **kwargs):
        name = self.name
        if self.email_address:
            name = "{} <{}>".format(name, self.email_address)
        return name.strip()


def get_paragraphs_data_from_file(location):
    """
    Yield paragraph data mappings from the Debian control file at `location`
    that contains multiple paragraphs (e.g. Package, status, copyright file,
    etc.).
    """
    if not location:
        return []
    return get_paragraphs_data(read_text_file(location))


def split_in_paragraphs(text):
    """
    Yield paragraphs from a `text` string that contains one or more paragraph
    separated by empty lines. Each paragraph is a string.
    """
    for p in re.split(r"\n\n(?:[ \t]*\n)*", text or ""):
        if p:
            yield p


def get_paragraphs_data(text):
    """
    Yield paragraph data mappings from the Debian control `text` string that
    contains multiple paragraphs (e.g. Package, status, copyright file, etc.).
    """
    for para in split_in_paragraphs(text or ""):
        yield get_paragraph_data(para)


def get_paragraph_data_from_file(location, remove_pgp_signature=False):
    """
    Return paragraph data from the Debian control file at `location` that
    contains a single paragraph (e.g. a dsc file).

    Optionally remove a wrapping PGP signature if `remove_pgp_signature` is
    True.
    """
    if not location:
        return []
    return get_paragraph_data(
        read_text_file(location),
        remove_pgp_signature=remove_pgp_signature,
    )


def get_paragraph_data(text, remove_pgp_signature=False):
    """
    Return paragraph data from the Debian control `text`. The paragraph data is
    an ordered mapping of {name: value} fields. If there is data that is not
    parsable or not attached to a field name, this will be added to a field
    named "unknown".

    If there are duplicates field names, the string values of duplicates field
    names are merged together with a new line in the first occurence of that
    field.

    Optionally remove a wrapping PGP signature if `remove_pgp_signature` is
    True.
    """
    if not text:
        return {"unknown": text}

    if remove_pgp_signature:
        text = unsign.remove_signature(text)

    try:
        mls = email.message_from_string(text)
    except UnicodeEncodeError:
        t = text.encode("utf-8")
        mls = email.message_from_string(t)

    items = list(mls.items())

    if not items or mls.defects:
        return {"unknown": text}

    # in a header-only email we should not have a payload. Yet when this happens
    # we should no ignore it either, so let's treat this as "unknown"
    payload = mls.get_payload()
    if payload:
        items.append(("unknown", payload))

    data = {}
    for name, value in items:
        # we do not preserve case: debian field names are case-insensitive AND
        # we use a normalized lowercase version throughout.
        name = name.lower().strip()
        value = value.strip()
        if name in data:
            existing_values = data.get(name, "").splitlines()
            if value not in existing_values:
                value = "\n".join(existing_values + [value])
        data[name] = value

    return data


def line_separated(value):
    """
    Return a list of values from a `value` string using line as list delimiters.
    """
    if not value:
        return []
    return list(value.splitlines(False))


def _splitter(value, separator):
    """
    Return a list of values from a `value` string using `separator` as list delimiters.
    Empty values are NOT returned.
    """
    if not value:
        return []
    return [v.strip() for v in value.split(separator) if v.strip()]


def comma_separated(value):
    return _splitter(value, ",")


def comma_space_separated(value):
    return _splitter(value, ", ")


def space_separated(value):
    """
    Return a list of values from a `value` string using one or more whitespace
    as list items delimiter. Empty values are NOT returned.
    """
    if not value:
        return []
    return list(value.split())


def read_text_file(location):
    """
    Return the content of the file at `location` as text or None.
    """
    if not location:
        return
    try:
        with io.open(location, "r", encoding="utf-8") as tc:
            return tc.read()
    except UnicodeDecodeError:
        with open(location, "rb") as tc:
            content = tc.read()
        enc = chardet.detect(content)["encoding"]
        return content.decode(enc)


class Debian822(MutableMapping):
    """
    A mapping-like class that corresponds to a single deb822 paragraph like a
    whole .dsc file.
    """

    def __init__(self, data=None):
        """
        Build a new instance from `data` that is either a file-like object with
        a read() method, a text, a sequence of (key/values) or a mapping. Note
        that the keys are always lowercased.
        """
        if data:
            text = None
            if isinstance(data, Mapping):
                paragraph = {k.lower(): v for k, v in data.items()}

            elif isinstance(data, str):
                text = data

            elif hasattr(data, "read"):
                text = data.read()

            elif isinstance(data, Sequence):
                # a sequence should be a sequence of items or sequence of string
                # (before the : split)
                seq = list(data)
                first = seq[0]
                if isinstance(first, str):
                    seq = (s.partition(": ") for s in seq)
                    paragraph = {k.lower(): v for k, _, v in seq}
                else:
                    # seq of (k, v) items
                    paragraph = {k.lower(): v for k, v in data}

            else:
                raise TypeError(
                    "Invalid argument type. Should be one of a file-like object, "
                    "a text string, a sequence of items or a mapping but is "
                    "instead:".format(type(data))
                )
            if text:
                # we parse in a sequence of items
                paragraph = get_paragraph_data(text, remove_pgp_signature=True)

            self.data = paragraph
        else:
            self.data = {}

    def __getitem__(self, key):
        return self.data.__getitem__(key.lower())

    def __setitem__(self, key, value):
        self.data.__setitem__(key.lower(), value)

    def __delitem__(self, key):
        return self.data.__delitem__(key.lower())

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return self.data.__len__()

    @classmethod
    def from_file(cls, location, remove_pgp_signature=True):
        data = get_paragraph_data_from_file(
            location=location, remove_pgp_signature=remove_pgp_signature
        )
        if not data:
            raise ValueError("Location has no parsable data: {}".format(location))
        return Debian822(data)

    @classmethod
    def from_string(cls, text):
        return Debian822(textwrap.dedent(text).strip())

    def to_dict(self, normalize_names=False):
        if normalize_names:
            return {normalize_control_field_name(key): value for key, value in self.data.items()}
        else:
            return dict(self.data)

    def __repr__(self):
        return self.dumps()

    def dumps(self, **kwargs):
        """
        Return a text that resembles the original Debian822 format. This is not
        meant to be a high fidelity rendering and not meant to be used as-is in
        control files.
        """
        items = self.items()

        lines = []
        for key, value in items:
            key = normalize_control_field_name(key)
            lines.append("{}: {}".format(key, value))
        text = "\n".join(lines) + "\n"
        return text

    def dump(self, file_like=None, **kwargs):
        text = self.dumps(**kwargs)
        if file_like:
            file_like.write(text.encode("utf-8"))
        else:
            return text


DEFAULT_CONTROL_FIELDS = {
    "Architecture": "all",
    "Priority": "optional",
    "Section": "misc",
}


def load_control_file(control_file):
    """
    Load a control file and return the parsed control fields.

    :param control_file: The filename of the control file to load (a string).
    :returns: A dictionary created by :func:`parse_control_fields()`.
    """
    with open(control_file) as inp:
        return parse_control_fields(Debian822(inp))


DEPS_FIELDS = frozenset(
    [
        # Binary control file fields.
        "Breaks",
        "Conflicts",
        "Depends",
        "Enhances",
        "Pre-Depends",
        "Provides",
        "Recommends",
        "Replaces",
        "Suggests",
        # Source control file fields.
        "Build-Conflicts",
        "Build-Conflicts-Arch",
        "Build-Conflicts-Indep",
        "Build-Depends",
        "Build-Depends-Arch",
        "Build-Depends-Indep",
        "Built-Using",
    ]
)


def parse_control_fields(input_fields, deps_fields=DEPS_FIELDS):
    """
    Return an ordered mapping from parsing an`input_fields` mapping of Debian
    control file fields. This applies a few conversions such as:

    - The values of the fields that contain dependencies are parsed
      into Python data structures.

    - The value of some fields such as `Installed-Size` from a string to a
      native type (here an integer).
    """
    from debian_inspector import deps

    output_fields = {}
    for name, unparsed_value in input_fields.items():
        name = normalize_control_field_name(name)
        if name in deps_fields:
            parsed_value = deps.parse_depends(unparsed_value)
        elif name == "Installed-Size":
            parsed_value = int(unparsed_value)
        else:
            parsed_value = unparsed_value
        output_fields[name] = parsed_value
    return output_fields


def normalize_control_field_name(name):
    """
    Return a case-normalized field name string.

    Normalization of control file field names is not really needed when reading
    as we lowercase everything and replace dash to underscore internally, but it
    can help to compare the parsing results to the original file while testing.

    According to the Debian Policy Manual field names are not case-sensitive,
    however a conventional capitalization is most common and not using it may
    break hings.

    http://www.debian.org/doc/debian-policy/ch-controlfields.html#s-controlsyntax
    """
    special_cases = dict(md5sum="MD5sum", sha1="SHA1", sha256="SHA256")
    return "-".join(special_cases.get(w.lower(), w.capitalize()) for w in name.split("-"))
