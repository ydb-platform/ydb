import codecs


FORMAT_SUFFIX = 'FORMAT TabSeparatedWithNamesAndTypes'


def unescape(value, errors=None):
    if errors is None:
        errors = 'replace'
    return codecs.escape_decode(value)[0].decode('utf-8', errors=errors)


def parse_tsv(line, errors=None):
    return [
        (unescape(x, errors) if x != b'\\N' else None)
        for x in line.split(b'\t')
    ]
