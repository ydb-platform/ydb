[![Current PyPI packages](https://badge.fury.io/py/mecab-python3.svg)](https://pypi.org/project/mecab-python3/)
![Test Status](https://github.com/SamuraiT/mecab-python3/workflows/test-manylinux/badge.svg)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/mecab-python3)](https://pypi.org/project/mecab-python3/)
![Supported Platforms](https://img.shields.io/badge/platforms-linux%20macosx%20windows-blue)

# mecab-python3

This is a Python wrapper for the [MeCab][] morphological analyzer for Japanese
text. It works with Python 3.6 and greater; if you need to use Python 2.7, use
v1.0.2. 

**Note:** If using MacOS Big Sur, you'll need to upgrade pip to version 20.3 or
higher to use wheels due to a pip issue.

**issueを英語で書く必要はありません。**

[MeCab]: https://taku910.github.io/mecab/

Note that Windows wheels require a [Microsoft Visual C++
Redistributable][msvc], so be sure to install that.

[msvc]: https://support.microsoft.com/en-us/help/2977003/the-latest-supported-visual-c-downloads

# Basic usage

```py
>>> import MeCab
>>> wakati = MeCab.Tagger("-Owakati")
>>> wakati.parse("pythonが大好きです").split()
['python', 'が', '大好き', 'です']

>>> tagger = MeCab.Tagger()
>>> print(tagger.parse("pythonが大好きです"))
python  python  python  python  名詞-普通名詞-一般
が      ガ      ガ      が      助詞-格助詞
大好き  ダイスキ        ダイスキ        大好き  形状詞-一般
です    デス    デス    です    助動詞  助動詞-デス     終止形-一般
EOS
```

The API for `mecab-python3` closely follows the API for MeCab itself,
even when this makes it not very “Pythonic.”  Please consult the [official MeCab
documentation][mecab-docs] for more information.

[mecab-docs]: https://taku910.github.io/mecab/

# Installation

Binary wheels are available for MacOS X, Linux, and Windows (64bit) are
installed by default when you use `pip`:

```sh
pip install mecab-python3
```

These wheels include a copy of the MeCab library, but not a dictionary. In
order to use MeCab you'll need to install a dictionary. `unidic-lite` is a good
one to start with:

```sh
pip install unidic-lite
```

To build from source using pip,

```sh
pip install --no-binary :all: mecab-python3
```

## Dictionaries

In order to use MeCab, you must install a dictionary. There are many different dictionaries available for MeCab. These UniDic packages, which include slight modifications for ease of use, are recommended:

- [unidic](https://github.com/polm/unidic-py): The latest full UniDic.
- [unidic-lite](https://github.com/polm/unidic-lite): A slightly modified UniDic 2.1.2, chosen for its small size.

The dictionaries below are not recommended due to being unmaintained for many years, but they are available for use with legacy applications.

- [ipadic](https://github.com/polm/ipadic-py)
- [jumandic](https://github.com/polm/jumandic-py)

For more details on the differences between dictionaries see [here](https://www.dampfkraft.com/nlp/japanese-tokenizer-dictionaries.html). 

# Common Issues

If you get a `RuntimeError` when you try to run MeCab, here are some things to check:

## Windows Redistributable

You have to install [this][msvc] to use this package on Windows.

## Installing a Dictionary

Run `pip install unidic-lite` and confirm that works. If that fixes your
problem, you either don't have a dictionary installed, or you need to specify
your dictionary path like this:

    tagger = MeCab.Tagger('-r /dev/null -d /usr/local/lib/mecab/dic/mydic')

Note: on Windows, use `nul` instead of `/dev/null`. Alternately, if you have a
`mecabrc` you can use the path after `-r`.

## Specifying a mecabrc

If you get this error:

    error message: [ifs] no such file or directory: /usr/local/etc/mecabrc

You need to specify a `mecabrc` file. It's OK to specify an empty file, it just
has to exist. You can specify a `mecabrc` with `-r`. This may be necessary on
Debian or Ubuntu, where the `mecabrc` is in `/etc/mecabrc`.

You can specify an empty `mecabrc` like this:

    tagger = MeCab.Tagger('-r/dev/null -d/home/hoge/mydic')

## Using Unsupported Output Modes like `-Ochasen`

Chasen output is not a built-in feature of MeCab, you must specify it in your
`dicrc` or `mecabrc`. Notably, Unidic does not include Chasen output format.
Please see [the MeCab documentation](https://taku910.github.io/mecab/#format).

# Alternatives

- [fugashi](https://github.com/polm/fugashi) is a Cython wrapper for MeCab with a Pythonic interface, by the current maintainer of this library
- [SudachiPy](https://github.com/WorksApplications/sudachi.rs) is a modern tokenizer with an actively maintained dictionary
- [pymecab-ko](https://github.com/NoUnique/pymecab-ko) is a wrapper of the Korean MeCab fork [mecab-ko](https://bitbucket.org/eunjeon/mecab-ko/src/master/) based on mecab-python3
- [KoNLPy](https://konlpy.org/en/latest/) is a library for Korean NLP that includes a MeCab wrapper

# Licensing

Like MeCab itself, `mecab-python3` is copyrighted free software by
Taku Kudo <taku@chasen.org> and Nippon Telegraph and Telephone Corporation,
and is distributed under a 3-clause BSD license (see the file `BSD`).
Alternatively, it may be redistributed under the terms of the
GNU General Public License, version 2 (see the file `GPL`) or the
GNU Lesser General Public License, version 2.1 (see the file `LGPL`). 
