![mf2py banner](https://microformats.github.io/mf2py/banner.png)

[![version](https://badge.fury.io/py/mf2py.svg?)](https://badge.fury.io/py/mf2py)
[![downloads](https://img.shields.io/pypi/dm/mf2py)](https://pypistats.org/packages/mf2py)
[![license](https://img.shields.io/pypi/l/mf2py?)](https://github.com/microformats/mf2py/blob/main/LICENSE)
[![python-version](https://img.shields.io/pypi/pyversions/mf2py)](https://badge.fury.io/py/mf2py)

## Welcome 👋

`mf2py` is a Python [microformats](https://microformats.org/wiki/microformats) parser with full support for `microformats2`, backwards-compatible support for `microformats1` and experimental support for `metaformats`.

## Installation 💻

To install `mf2py` run the following command:

```bash
$ pip install mf2py

```

## Quickstart 🚀

Import the library:

```pycon
>>> import mf2py

```

### Parse an HTML Document from a file or string

```pycon
>>> with open("test/examples/eras.html") as fp:
...     mf2json = mf2py.parse(doc=fp)
>>> mf2json
{'items': [{'type': ['h-entry'],
            'properties': {'name': ['Excited for the Taylor Swift Eras Tour'],
                           'author': [{'type': ['h-card'],
                                       'properties': {'name': ['James'],
                                                      'url': ['https://example.com/']},
                                       'value': 'James',
                                       'lang': 'en-us'}],
                           'published': ['2023-11-30T19:08:09'],
                           'featured': [{'value': 'https://example.com/eras.jpg',
                                         'alt': 'Eras tour poster'}],
                           'content': [{'value': "I can't decide which era is my favorite.",
                                        'lang': 'en-us',
                                        'html': "<p>I can't decide which era is my favorite.</p>"}],
                           'category': ['music', 'Taylor Swift']},
            'lang': 'en-us'}],
 'rels': {'webmention': ['https://example.com/mentions']},
 'rel-urls': {'https://example.com/mentions': {'text': '',
                                               'rels': ['webmention']}},
 'debug': {'description': 'mf2py - microformats2 parser for python',
           'source': 'https://github.com/microformats/mf2py',
           'version': '2.0.0',
           'markup parser': 'html5lib'}}

```

```pycon
>>> mf2json = mf2py.parse(doc="<a class=h-card href=https://example.com>James</a>")
>>> mf2json["items"]
[{'type': ['h-card'],
  'properties': {'name': ['James'],
                 'url': ['https://example.com']}}]

```

### Parse an HTML Document from a URL

```pycon
>>> mf2json = mf2py.parse(url="https://events.indieweb.org")
>>> mf2json["items"][0]["type"]
['h-feed']
>>> mf2json["items"][0]["children"][0]["type"]
['h-event']

```

## Experimental Options

The following options can be invoked via keyword arguments to `parse()` and `Parser()`.

### `expose_dom`

Use `expose_dom=True` to expose the DOM of embedded properties.

### `metaformats`

Use `metaformats=True` to include any [metaformats](https://microformats.org/wiki/metaformats)
found.

### `filter_roots`

Use `filter_roots=True` to filter known conflicting user names (e.g. Tailwind).
Otherwise provide a custom list to filter instead.

## Advanced Usage

`parse` is a convenience function for `Parser`. More sophisticated behaviors are
available by invoking the parser object directly.

```pycon
>>> with open("test/examples/festivus.html") as fp:
...     mf2parser = mf2py.Parser(doc=fp)

```

#### Filter by Microformat Type

```pycon
>>> mf2json = mf2parser.to_dict()
>>> len(mf2json["items"])
7
>>> len(mf2parser.to_dict(filter_by_type="h-card"))
3
>>> len(mf2parser.to_dict(filter_by_type="h-entry"))
4

```

#### JSON Output

```pycon
>>> json = mf2parser.to_json()
>>> json_cards = mf2parser.to_json(filter_by_type="h-card")

```

## Breaking Changes in `mf2py` 2.0

- Image `alt` support is now on by default.

## Notes 📝

- If you pass a BeautifulSoup document it may be modified.
- A hosted version of `mf2py` is available at [python.microformats.io](https://python.microformats.io).

## Contributing 🛠️

We welcome contributions and bug reports via GitHub.

This project follows the [IndieWeb code of conduct](https://indieweb.org/code-of-conduct). Please be respectful of other contributors and forge a spirit of positive co-operation without discrimination or disrespect.

## License 🧑‍⚖️

`mf2py` is licensed under an MIT License.
