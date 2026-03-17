# 𝓜𝓪𝓻𝓴𝓸

> A markdown parser with high extensibility.

[![PyPI](https://img.shields.io/pypi/v/marko.svg?logo=python&logoColor=white)](https://pypi.org/project/marko/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/marko.svg?logo=python&logoColor=white)](https://pypi.org/project/marko/)
[![Documentation Status](https://img.shields.io/readthedocs/marko-py.svg?logo=readthedocs)](https://marko-py.readthedocs.io/en/latest/?badge=latest)
[![CommonMark Spec](https://img.shields.io/badge/CommonMark-0.31.2-blue.svg)][spec]

![Build Status](https://github.com/frostming/marko/workflows/Tests/badge.svg)
[![codecov](https://codecov.io/gh/frostming/marko/branch/master/graph/badge.svg)](https://codecov.io/gh/frostming/marko)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/b785f5b3fa7c4d93a02372d31b3f73b1)](https://www.codacy.com/app/frostming/marko?utm_source=github.com&utm_medium=referral&utm_content=frostming/marko&utm_campaign=Badge_Grade)

Marko is a pure Python markdown parser that adheres to the specifications of [CommonMark's spec v0.31.2][spec]. It has been designed with high extensibility in mind, as detailed in the [Extensions](#extensions) section.

Marko requires Python 3.9 or higher.

## Why Marko

Of all the Python markdown parsers available, a common issue is the difficulty for users to add their own features. Additionally, both [Python-Markdown][pymd] and [mistune][mistune] do not comply with CommonMark specifications. This has prompted me to develop a new markdown parser.

Marko's compliance with the complex CommonMark specification can impact its performance. However, using a parser that does not adhere to this spec may result in unexpected rendering outcomes. According to benchmark results, Marko is three times slower than Python-Markdown but slightly faster than Commonmark-py and significantly slower than mistune. If prioritizing performance over spec compliance is crucial for you, it would be best to opt for another parser.

[spec]: https://spec.commonmark.org/0.31.2/
[pymd]: https://github.com/waylan/Python-Markdown
[mistune]: https://github.com/lepture/mistune
[cmpy]: https://github.com/rtfd/CommonMark-py

## Use Marko

The installation is very simple:

    $ pip install marko

And to use it:

```python
import marko

print(marko.convert(text))
```

Marko also provides a simple CLI, for example, to render a document and output to a html file:

    $ cat my_article.md | marko > my_article.html

## Extensions

It is super easy to use an extension:

```python
from marko import Markdown
from marko.ext.footnote import make_extension
# Add footnote extension
markdown = Markdown(extensions=[make_extension()])
# Or you can just:
markdown = Markdown(extensions=['footnote'])
# Alternatively you can register an extension later
markdown.use(make_extension())
```

An example of using an extension with the command-line version of Marko:

```
$ cat this_has_footnote.txt | marko -e footnote > hi_world.html
```

Marko is shipped with 4 extensions: `'footnote', 'toc' 'pangu', 'codehilite'`.
They are not included in CommonMark's spec but are common in other markdown parsers.

Marko also provides a Github flavored markdown parser which can be found at `marko.ext.gfm.gfm`.

Please refer to [Extend Marko](https://marko-py.readthedocs.io/en/latest/extend.html) about how to
write your own extension.

## License

Marko is released under [MIT License](LICENSE)

## [Change Log](CHANGELOG.md)
