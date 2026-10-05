<h1 align="center">Charset Detection, for Everyone 👋</h1>

<p align="center">
  <sup>The Real First Universal Charset Detector</sup><br>
  <a href="https://pypi.org/project/charset-normalizer">
    <img src="https://img.shields.io/pypi/pyversions/charset_normalizer.svg?orange=blue" />
  </a>
  <a href="https://pepy.tech/project/charset-normalizer/">
    <img alt="Download Count Total" src="https://static.pepy.tech/badge/charset-normalizer/month" />
  </a>
  <a href="https://bestpractices.coreinfrastructure.org/projects/7297">
    <img src="https://bestpractices.coreinfrastructure.org/projects/7297/badge">
  </a>
</p>
<p align="center">
  <sup><i>Featured Packages</i></sup><br>
  <a href="https://github.com/jawah/niquests">
   <img alt="Static Badge" src="https://img.shields.io/badge/Niquests-Most_Advanced_HTTP_Client-cyan">
  </a>
  <a href="https://github.com/jawah/wassima">
   <img alt="Static Badge" src="https://img.shields.io/badge/Wassima-Certifi_Replacement-cyan">
  </a>
</p>
<p align="center">
  <sup><i>In other language (unofficial port - by the community)</i></sup><br>
  <a href="https://github.com/nickspring/charset-normalizer-rs">
   <img alt="Static Badge" src="https://img.shields.io/badge/Rust-red">
  </a>
</p>

> A library that helps you read text from an unknown charset encoding.<br /> Motivated by `chardet`,
> I'm trying to resolve the issue by taking a new approach.
> All IANA character set names for which the Python core library provides codecs are supported.
> You can also register your own set of codecs, and yes, it would work as-is.

This project offers you an alternative to **Universal Charset Encoding Detector**, also known as **Chardet**.

| Feature                                          | [Chardet](https://github.com/chardet/chardet) |                                       Charset Normalizer                                        | [cChardet](https://github.com/PyYoshi/cChardet) |
|--------------------------------------------------|:---------------------------------------------:|:-----------------------------------------------------------------------------------------------:|:-----------------------------------------------:|
| `Fast`                                           |                       ✅                       |                                                ✅                                                |                        ✅                        |
| `Universal`[^1]                                  |                       ❌                       |                                                ✅                                                |                        ❌                        |
| `Reliable` **without** distinguishable standards |                       ✅                       |                                                ✅                                                |                        ✅                        |
| `Reliable` **with** distinguishable standards    |                       ✅                       |                                                ✅                                                |                        ✅                        |
| `License`                                        |        _Disputed_[^2]<br>_restrictive_        |                                               MIT                                               |            MPL-1.1<br>_restrictive_             |
| `Native Python`                                  |                       ✅                       |                                                ✅                                                |                        ❌                        |
| `Detect spoken language`                         |                       ✅                       |                                                ✅                                                |                       N/A                       |
| `UnicodeDecodeError Safety`                      |                       ✅                       |                                                ✅                                                |                        ❌                        |
| `Whl Size (min)`                                 |                    500 kB                     |                                             150 kB                                              |                     ~200 kB                     |
| `Supported Encoding`                             |                      99                       | [99](https://charset-normalizer.readthedocs.io/en/latest/user/support.html#supported-encodings) |                       40                        |
| `Can register custom encoding`                   |                       ❌                       |                                                ✅                                                |                        ❌                        |

<p align="center">
<img src="https://i.imgflip.com/373iay.gif" alt="Reading Normalized Text" width="226"/><img src="https://media.tenor.com/images/c0180f70732a18b4965448d33adba3d0/tenor.gif" alt="Cat Reading Text" width="200"/>
</p>

[^1]: They are clearly using specific code for a specific encoding even if covering most of used one.
[^2]: Chardet 7.0+ was relicensed from LGPL-2.1 to MIT following an AI-assisted rewrite. This relicensing is disputed on two independent grounds: **(a)** the original author [contests](https://github.com/chardet/chardet/issues/327) that the maintainer had the right to relicense, arguing the rewrite is a derivative work of the LGPL-licensed codebase since it was not a clean room implementation; **(b)** the copyright claim itself is [questionable](https://github.com/chardet/chardet/issues/334) given the code was primarily generated by an LLM, and AI-generated output may not be copyrightable under most jurisdictions. Either issue alone could undermine the MIT license. Beyond licensing, the rewrite raises questions about responsible use of AI in open source: key architectural ideas pioneered by charset-normalizer - notably decode-first validity filtering (our foundational approach since v1) and encoding pairwise similarity with the same algorithm and threshold — surfaced in chardet 7 without acknowledgment. The project also imported test files from charset-normalizer to train and benchmark against it, then claimed superior accuracy on those very files. Charset-normalizer has always been MIT-licensed, encoding-agnostic by design, and built on a verifiable human-authored history.

## ⚡ Performance

This package offer better performances against Chardet. Here are some numbers.

| Package                                           | Accuracy | Mean per file (ms) | File per sec (est) |
|---------------------------------------------------|:--------:|:------------------:|:------------------:|
| [chardet 7.4](https://github.com/chardet/chardet) |   89 %   |        3 ms        |    333 file/sec    |
| charset-normalizer                                | **97 %** |        1 ms        |   1000 file/sec    |

| Package                                           | 99th percentile | 95th percentile | 50th percentile |
|---------------------------------------------------|:---------------:|:---------------:|:---------------:|
| [chardet 7.4](https://github.com/chardet/chardet) |      28 ms      |      16 ms      |     < 1 ms      |
| charset-normalizer                                |      8 ms       |      5 ms       |      1 ms       |

_updated as of July 2026 using CPython 3.12, Charset-Normalizer 3.4.8, and Chardet 7.4.3_

~Chardet's performance on larger file (1MB+) are very poor. Expect huge difference on large payload.~ No longer the case since Chardet 7.0+

> Stats are generated using 400+ files using default parameters. More details on used files, see GHA workflows.
> And yes, these results might change at any time. The dataset can be updated to include more files.
> The actual delays heavily depends on your CPU capabilities. The factors should remain the same.
> Chardet claims on his documentation to have a greater accuracy than us based on the dataset they trained Chardet on(...)
> Well, it's normal, the opposite would have been worrying. Whereas charset-normalizer don't train on anything, our solution
> is based on a completely different algorithm, still heuristic through, it does not need weights across every encoding tables.

## ✨ Installation

Using pip:

```sh
pip install charset-normalizer -U
```

## 🚀 Basic Usage

### CLI
This package comes with a CLI.

```
usage: normalizer [-h] [-v] [-a] [-n] [-m] [-r] [-f] [-t THRESHOLD]
                  file [file ...]

The Real First Universal Charset Detector. Discover originating encoding used
on text file. Normalize text to unicode.

positional arguments:
  files                 File(s) to be analysed

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         Display complementary information about file if any.
                        Stdout will contain logs about the detection process.
  -a, --with-alternative
                        Output complementary possibilities if any. Top-level
                        JSON WILL be a list.
  -n, --normalize       Permit to normalize input file. If not set, program
                        does not write anything.
  -m, --minimal         Only output the charset detected to STDOUT. Disabling
                        JSON output.
  -r, --replace         Replace file when trying to normalize it instead of
                        creating a new one.
  -f, --force           Replace file without asking if you are sure, use this
                        flag with caution.
  -t THRESHOLD, --threshold THRESHOLD
                        Define a custom maximum amount of chaos allowed in
                        decoded content. 0. <= chaos <= 1.
  --version             Show version information and exit.
```

```bash
normalizer ./data/sample.1.fr.srt
```

or

```bash
python -m charset_normalizer ./data/sample.1.fr.srt
```

🎉 Since version 1.4.0 the CLI produce easily usable stdout result in JSON format.

```json
{
    "path": "/home/default/projects/charset_normalizer/data/sample.1.fr.srt",
    "encoding": "cp1252",
    "encoding_aliases": [
        "1252",
        "windows_1252"
    ],
    "alternative_encodings": [
        "cp1254",
        "cp1256",
        "cp1258",
        "iso8859_14",
        "iso8859_15",
        "iso8859_16",
        "iso8859_3",
        "iso8859_9",
        "latin_1",
        "mbcs"
    ],
    "language": "French",
    "alphabets": [
        "Basic Latin",
        "Latin-1 Supplement"
    ],
    "has_sig_or_bom": false,
    "chaos": 0.149,
    "coherence": 97.152,
    "unicode_path": null,
    "is_preferred": true
}
```

### Python
*Just print out normalized text*
```python
from charset_normalizer import from_path

results = from_path('./my_subtitle.srt')

print(str(results.best()))
```

*Upgrade your code without effort*
```python
from charset_normalizer import detect
```

The above code will behave the same as **chardet**. We ensure that we offer the best (reasonable) BC result possible.

See the docs for advanced usage : [readthedocs.io](https://charset-normalizer.readthedocs.io/en/latest/)

## 😇 Why

When I started using Chardet, I noticed that it was not suited to my expectations, and I wanted to propose a
reliable alternative using a completely different method. Also! I never back down on a good challenge!

I **don't care** about the **originating charset** encoding, because **two different tables** can
produce **two identical rendered string.**
What I want is to get readable text, the best I can.

In a way, **I'm brute forcing text decoding.** How cool is that ? 😎

Don't confuse package **ftfy** with charset-normalizer or chardet. ftfy goal is to repair Unicode string whereas charset-normalizer to convert raw file in unknown encoding to unicode.

## 🍰 How

  - Discard all charset encoding table that could not fit the binary content.
  - Measure noise, or the mess once opened (by chunks) with a corresponding charset encoding.
  - Extract matches with the lowest mess detected.
  - Additionally, we measure coherence / probe for a language.

**Wait a minute**, what is noise/mess and coherence according to **YOU ?**

*Noise :* I opened hundred of text files, **written by humans**, with the wrong encoding table. **I observed**, then
**I established** some ground rules about **what is obvious** when **it seems like** a mess (aka. defining noise in rendered text).
 I know that my interpretation of what is noise is probably incomplete, feel free to contribute in order to
 improve or rewrite it.

*Coherence :* For each language there is on earth, we have computed ranked letter appearance occurrences (the best we can). So I thought
that intel is worth something here. So I use those records against decoded text to check if I can detect intelligent design.

## ⚡ Known limitations

  - Language detection is unreliable when text contains two or more languages sharing identical letters. (eg. HTML (english tags) + Turkish content (Sharing Latin characters))
  - Every charset detector heavily depends on sufficient content. In common cases, do not bother run detection on very tiny content.

## ⚠️ About Python EOLs

**If you are running:**

- Python >=2.7,<3.5: Unsupported
- Python 3.5: charset-normalizer < 2.1
- Python 3.6: charset-normalizer < 3.1

Upgrade your Python interpreter as soon as possible.

## 👤 Contributing

Contributions, issues and feature requests are very much welcome.<br />
Feel free to check [issues page](https://github.com/ousret/charset_normalizer/issues) if you want to contribute.

## 📝 License

Copyright © [Ahmed TAHRI @Ousret](https://github.com/Ousret).<br />
This project is [MIT](https://github.com/Ousret/charset_normalizer/blob/master/LICENSE) licensed.

Characters frequencies used in this project © 2012 [Denny Vrandečić](http://simia.net/letters/)

## 💼 For Enterprise

Professional support for charset-normalizer is available as part of the [Tidelift
Subscription][1]. Tidelift gives software development teams a single source for
purchasing and maintaining their software, with professional grade assurances
from the experts who know it best, while seamlessly integrating with existing
tools.

[1]: https://tidelift.com/subscription/pkg/pypi-charset-normalizer?utm_source=pypi-charset-normalizer&utm_medium=readme

[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/7297/badge)](https://www.bestpractices.dev/projects/7297)
