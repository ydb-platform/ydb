[![PyPI version](https://badge.fury.io/py/sacrebleu.svg)](https://badge.fury.io/py/sacrebleu)
[![GitHub issues](https://img.shields.io/github/issues/mjpost/sacreBLEU.svg)](https://github.com/awslabs/sockeye/issues)

SacreBLEU ([Post, 2018](http://aclweb.org/anthology/W18-6319)) provides hassle-free computation of shareable, comparable, and reproducible BLEU scores.
Inspired by Rico Sennrich's `multi-bleu-detok.perl`, it produces the official WMT scores but works with plain text.
It also knows all the standard test sets and handles downloading, processing, and tokenization for you.

Why use this version of BLEU?
- It automatically downloads common WMT test sets and processes them to plain text
- It produces a short version string that facilitates cross-paper comparisons
- It properly computes scores on detokenized outputs, using WMT ([Conference on Machine Translation](http://statmt.org/wmt17)) standard tokenization
- It produces the same values as official script (`mteval-v13a.pl`) used by WMT
- It outputs the BLEU score without the comma, so you don't have to remove it with `sed` (Looking at you, `multi-bleu.perl`)

# QUICK START

Install the Python module (Python 3 only)

    pip3 install sacrebleu

In order to install Japanese tokenizer support through `mecab-python3`, you need to run the
following command instead, to perform a full installation with dependencies:

    pip3 install sacrebleu[ja]

Alternately, you can install from the source:

    python3 setup.py install

This installs a shell script, `sacrebleu`.
(You can also run `python3 -m sacrebleu`, so long as this root directory is in your `$PYTHONPATH`).

Get a list of available test sets:

    sacrebleu --list

Download the source for one of the pre-defined test sets:

    sacrebleu -t wmt14 -l de-en --echo src > wmt14-de-en.src

(you can also use long parameter names for readability):

    sacrebleu --test-set wmt14 --language-pair de-en --echo src > wmt14-de-en.src

After tokenizing, translating, and detokenizing it, you can score your decoder output easily:

    cat output.detok.txt | sacrebleu -t wmt14 -l de-en

SacreBLEU knows about common WMT test sets, but you can also use it to score system outputs with arbitrary references.
It also works in backwards compatible model where you manually specify the reference(s), similar to the format of `multi-bleu.txt`:

    cat output.detok.txt | sacrebleu REF1 [REF2 ...]

Note that the system output and references will all be tokenized internally.

SacreBLEU generates version strings like the following.
Put them in a footnote in your paper!
Use `--short` for a shorter hash if you like.

    BLEU+case.mixed+lang.de-en+test.wmt17 = 32.97 66.1/40.2/26.6/18.1 (BP = 0.980 ratio = 0.980 hyp_len = 63134 ref_len = 64399)

If you are interested in the translationese effect, you can evaluate BLEU on a subset of sentences
with a given original language (identified based on the origlang tag in the raw SGM files).
E.g., to evaluate only against originally German sentences translated to English use:

    sacrebleu -t wmt13 -l de-en --origlang=de < my-wmt13-output.txt

and to evaluate against the complement (in this case origlang en, fr, cs, ru, de) use:

    sacrebleu -t wmt13 -l de-en --origlang=non-de < my-wmt13-output.txt

*Please note* that the evaluator will return a BLEU score only on the requested subset,
but it expects that you pass through the entire translated test set.

## Using SacreBLEU from Python

For evaluation, it may be useful to compute BLEU inside a script. This is how you can do it:

```python
import sacrebleu
refs = [['The dog bit the man.', 'It was not unexpected.', 'The man bit him first.'],
        ['The dog had bit the man.', 'No one was surprised.', 'The man had bitten the dog.']]
sys = ['The dog bit the man.', "It wasn't surprising.", 'The man had just bitten him.']
bleu = sacrebleu.corpus_bleu(sys, refs)
print(bleu.score)
```

# MOTIVATION

Comparing BLEU scores is harder than it should be.
Every decoder has its own implementation, often borrowed from Moses, but maybe with subtle changes.
Moses itself has a number of implementations as standalone scripts, with little indication of how they differ (note: they mostly don't, but `multi-bleu.pl` expects tokenized input).
Different flags passed to each of these scripts can produce wide swings in the final score.
All of these may handle tokenization in different ways.
On top of this, downloading and managing test sets is a moderate annoyance.
Sacre bleu!
What a mess.

SacreBLEU aims to solve these problems by wrapping the original Papineni reference implementation together with other useful features.
The defaults are set the way that BLEU should be computed, and furthermore, the script outputs a short version string that allows others to know exactly what you did.
As an added bonus, it automatically downloads and manages test sets for you, so that you can simply tell it to score against 'wmt14', without having to hunt down a path on your local file system.
It is all designed to take BLEU a little more seriously.
After all, even with all its problems, BLEU is the default and---admit it---well-loved metric of our entire research community.
Sacre BLEU.

# LICENSE

SacreBLEU is licensed under the Apache 2.0 License.

# CREDITS

This was all Rico Sennrich's idea.
Originally written by Matt Post.
The official version can be found at <https://github.com/mjpost/sacrebleu>.

If you use SacreBLEU, please cite the following:

```
@inproceedings{post-2018-call,
  title = "A Call for Clarity in Reporting {BLEU} Scores",
  author = "Post, Matt",
  booktitle = "Proceedings of the Third Conference on Machine Translation: Research Papers",
  month = oct,
  year = "2018",
  address = "Belgium, Brussels",
  publisher = "Association for Computational Linguistics",
  url = "https://www.aclweb.org/anthology/W18-6319",
  pages = "186--191",
}
```
