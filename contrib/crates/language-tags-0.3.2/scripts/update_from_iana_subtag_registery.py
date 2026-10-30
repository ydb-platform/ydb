import os
import re
import urllib.request
from collections import defaultdict
from typing import Dict, Iterable, List

registry_url = 'https://www.iana.org/assignments/language-subtag-registry/language-subtag-registry'

struct_definitions = """use std::ops::Deref;
use std::str;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LanguageSubtag([u8; 3]);

impl LanguageSubtag {
    const fn new(subtag: [char; 3]) -> Self {
        LanguageSubtag([subtag[0] as u8, subtag[1] as u8, subtag[2] as u8])
    }
}

impl Deref for LanguageSubtag {
    type Target = str;

    fn deref(&self) -> &str {
        let mut end = 3;
        while self.0[end - 1] == b' ' {
            end -= 1;
        }
        unsafe { str::from_utf8_unchecked(&self.0[..end]) }
    }
}

impl str::FromStr for LanguageSubtag {
    type Err = ();

    fn from_str(input: &str) -> Result<Self, ()> {
        if 2 <= input.len() && input.len() <= 3 {
            let mut value = [b' '; 3];
            value[..input.len()].copy_from_slice(input.as_bytes());
            Ok(LanguageSubtag(value))
        } else {
            Err(())
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScriptSubtag([u8; 4]);

impl ScriptSubtag {
    const fn new(subtag: [char; 4]) -> Self {
        ScriptSubtag([
            subtag[0] as u8,
            subtag[1] as u8,
            subtag[2] as u8,
            subtag[3] as u8,
        ])
    }
}

impl Deref for ScriptSubtag {
    type Target = str;

    fn deref(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.0) }
    }
}

impl str::FromStr for ScriptSubtag {
    type Err = ();

    fn from_str(input: &str) -> Result<Self, ()> {
        if input.len() == 4 {
            let mut value = [b' '; 4];
            value.copy_from_slice(input.as_bytes());
            Ok(ScriptSubtag(value))
        } else {
            Err(())
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RegionSubtag([u8; 3]);

impl RegionSubtag {
    const fn new(subtag: [char; 3]) -> Self {
        RegionSubtag([subtag[0] as u8, subtag[1] as u8, subtag[2] as u8])
    }
}

impl Deref for RegionSubtag {
    type Target = str;

    fn deref(&self) -> &str {
        let mut end = 3;
        while self.0[end - 1] == b' ' {
            end -= 1;
        }
        unsafe { str::from_utf8_unchecked(&self.0[..end]) }
    }
}

impl str::FromStr for RegionSubtag {
    type Err = ();

    fn from_str(input: &str) -> Result<Self, ()> {
        if 2 <= input.len() && input.len() <= 3 {
            let mut value = [b' '; 3];
            value[..input.len()].copy_from_slice(input.as_bytes());
            Ok(RegionSubtag(value))
        } else {
            Err(())
        }
    }
}
"""


def parse_registry(url: str) -> Iterable[Dict[str, List[str]]]:
    """
    Parses the registry content from url and returns records as dict
    """
    field_regex = re.compile('^([a-zA-Z0-9-]+) *: *(.*)$')
    current_record = defaultdict(list)
    current_field = None
    for line in urllib.request.urlopen(url):
        line = line.decode().strip()
        if line == '%%':
            yield current_record
            current_record = defaultdict(list)
            continue

        match = field_regex.match(line)
        if match:
            current_field = match.group(1)
            current_record[current_field].append(match.group(2))
        else:
            current_record[current_field][-1] += ' ' + line
    if current_record:
        yield current_record


def serialize_string(l: List[str]):
    if len(l) == 1:
        return '"{}"'.format(l[0])
    else:
        raise ValueError('multiple values: {}'.format(l))


def serialize_byte_array(inputs: List[str], name: str, max_length: int):
    if len(inputs) != 1:
        raise ValueError('multiple values: {}'.format(l))
    chars = list(inputs[0])
    if len(chars) > max_length:
        raise ValueError('Too long string: {}'.format(input))
    while len(chars) < max_length:
        chars.append(' ')
    return '{}::new([{}])'.format(name, ', '.join('\'{}\''.format(c) for c in chars))


def serialize_tuple(*values: str):
    return '({})'.format(', '.join(values))


def serialize_static_array(name, elements: List[str], type: str):
    return 'pub const {}: [{}; {}] = [\n{}\n];\n\n'.format(
        name, type, len(elements), ',\n'.join(elements))


file_date = None
values = defaultdict(list)

for record in parse_registry(registry_url):
    if 'Type' not in record:
        if 'File-Date' in record:
            file_date = record['File-Date'][0]
        else:
            print('Unexpected record: {}'.format(record))
    elif record['Type'] == ['language']:
        if record['Subtag'] == ["qaa..qtz"]:
            continue  # private use range
        values['LANGUAGES'].append(serialize_byte_array(record['Subtag'], 'LanguageSubtag', 3))
        if record['Preferred-Value'] and record['Preferred-Value'] != record['Subtag']:
            values['LANGUAGES_PREFERRED_VALUE'].append(serialize_tuple(
                serialize_byte_array(record['Subtag'], 'LanguageSubtag', 3),
                serialize_byte_array(record['Preferred-Value'], 'LanguageSubtag', 3)
            ))
        if record['Suppress-Script']:
            values['LANGUAGES_SUPPRESS_SCRIPT'].append(serialize_tuple(
                serialize_byte_array(record['Subtag'], 'LanguageSubtag', 3),
                serialize_byte_array(record['Suppress-Script'], 'ScriptSubtag', 4)
            ))
    elif record['Type'] == ['extlang']:
        values['EXTLANGS'].append(serialize_tuple(
            serialize_byte_array(record['Subtag'], 'LanguageSubtag', 3),
            serialize_string(record['Prefix'])[:-1] + '-"'  # TODO: better storage
        ))
        if record['Preferred-Value']:
            values['EXTLANGS_PREFERRED_VALUE'].append(serialize_tuple(
                serialize_byte_array(record['Subtag'], 'LanguageSubtag', 3),
                serialize_byte_array(record['Preferred-Value'], 'LanguageSubtag', 3)
            ))
    elif record['Type'] == ['script']:
        if record['Subtag'] == ["Qaaa..Qabx"]:
            continue  # private use range
        values['SCRIPTS'].append(serialize_byte_array(record['Subtag'], 'ScriptSubtag', 4))
        if record['Preferred-Value'] and record['Preferred-Value'] != record['Subtag']:
            values['SCRIPTS_PREFERRED_VALUE'].append(serialize_tuple(
                serialize_byte_array(record['Subtag'], 'ScriptSubtag', 4),
                serialize_byte_array(record['Preferred-Value'], 'ScriptSubtag', 4)
            ))
    elif record['Type'] == ['region']:
        if record['Subtag'] == ["QM..QZ"] or record['Subtag'] == ["XA..XZ"]:
            continue  # private use range
        values['REGIONS'].append(serialize_byte_array(record['Subtag'], 'RegionSubtag', 3))
        if record['Preferred-Value'] and record['Preferred-Value'] != record['Subtag']:
            values['REGIONS_PREFERRED_VALUE'].append(serialize_tuple(
                serialize_byte_array(record['Subtag'], 'RegionSubtag', 3),
                serialize_byte_array(record['Preferred-Value'], 'RegionSubtag', 3)
            ))
    elif record['Type'] == ['variant']:
        values['VARIANTS'].append(serialize_tuple(
            serialize_string(record['Subtag']),
            '"{}"'.format(' '.join(p + '-' for p in record['Prefix'] if p))
        ))
        if record['Preferred-Value'] and record['Preferred-Value'] != record['Subtag']:
            values['VARIANTS_PREFERRED_VALUE'].append(serialize_tuple(
                serialize_string(record['Subtag']),
                serialize_string(record['Preferred-Value'])
            ))
    elif record['Type'] == ['grandfathered']:
        values['GRANDFATHEREDS'].append(serialize_string(record['Tag']))
        if record['Preferred-Value'] and record['Preferred-Value'] != record['Tag']:
            values['GRANDFATHEREDS_PREFERRED_VALUE'].append(serialize_tuple(
                serialize_string(record['Tag']),
                serialize_string(record['Preferred-Value'])
            ))
    elif record['Type'] == ['redundant']:
        if record['Preferred-Value'] and record['Preferred-Value'] != record['Tag']:
            values['REDUNDANTS_PREFERRED_VALUE'].append(serialize_tuple(
                serialize_string(record['Tag']),
                serialize_string(record['Preferred-Value'])
            ))
    else:
        print('Unexpected record: {}'.format(record))

lists_with_type = {
    'LANGUAGES': 'LanguageSubtag',
    'LANGUAGES_PREFERRED_VALUE': '(LanguageSubtag, LanguageSubtag)',
    'LANGUAGES_SUPPRESS_SCRIPT': '(LanguageSubtag, ScriptSubtag)',
    'EXTLANGS': '(LanguageSubtag, &str)',
    'EXTLANGS_PREFERRED_VALUE': '(LanguageSubtag, LanguageSubtag)',
    'SCRIPTS': 'ScriptSubtag',
    'SCRIPTS_PREFERRED_VALUE': '(ScriptSubtag, ScriptSubtag)',
    'REGIONS': 'RegionSubtag',
    'REGIONS_PREFERRED_VALUE': '(RegionSubtag, RegionSubtag)',
    'VARIANTS': '(&str, &str)',
    'VARIANTS_PREFERRED_VALUE': '(&str, &str)',
    'GRANDFATHEREDS': '&str',
    'GRANDFATHEREDS_PREFERRED_VALUE': '(&str, &str)',
    'REDUNDANTS_PREFERRED_VALUE': '(&str, &str)'

}

target_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../src/iana_registry.rs')
with open(target_path, 'wt') as fp:
    fp.write(struct_definitions)
    for name, type in lists_with_type.items():
        vals = values[name]
        vals.sort()
        fp.write(serialize_static_array(name, vals, type))
