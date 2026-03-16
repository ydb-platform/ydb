Subword Neural Machine Translation
==================================

This repository contains preprocessing scripts to segment text into subword
units. The primary purpose is to facilitate the reproduction of our experiments
on Neural Machine Translation with subword units (see below for reference).

INSTALLATION
------------

install via pip (from PyPI):

    pip install subword-nmt

install via pip (from Github):

    pip install https://github.com/rsennrich/subword-nmt/archive/master.zip

alternatively, clone this repository; the scripts are executable stand-alone.


USAGE INSTRUCTIONS
------------------

Check the individual files for usage instructions.

To apply byte pair encoding to word segmentation, invoke these commands:

    subword-nmt learn-bpe -s {num_operations} < {train_file} > {codes_file}
    subword-nmt apply-bpe -c {codes_file} < {test_file} > {out_file}

To segment rare words into character n-grams, do the following:

    subword-nmt get-vocab --train_file {train_file} --vocab_file {vocab_file}
    subword-nmt segment-char-ngrams --vocab {vocab_file} -n {order} --shortlist {size} < {test_file} > {out_file}

The original segmentation can be restored with a simple replacement:

    sed -r 's/(@@ )|(@@ ?$)//g'

If you cloned the repository and did not install a package, you can also run the individual commands as scripts:

    ./subword_nmt/learn_bpe.py -s {num_operations} < {train_file} > {codes_file}

BEST PRACTICE ADVICE FOR BYTE PAIR ENCODING IN NMT
--------------------------------------------------

We found that for languages that share an alphabet, learning BPE on the
concatenation of the (two or more) involved languages increases the consistency
of segmentation, and reduces the problem of inserting/deleting characters when
copying/transliterating names.

However, this introduces undesirable edge cases in that a word may be segmented
in a way that has only been observed in the other language, and is thus unknown
at test time. To prevent this, `apply_bpe.py` accepts a `--vocabulary` and a
`--vocabulary-threshold` option so that the script will only produce symbols
which also appear in the vocabulary (with at least some frequency).

To use this functionality, we recommend the following recipe (assuming L1 and L2
are the two languages):

Learn byte pair encoding on the concatenation of the training text, and get resulting vocabulary for each:

    cat {train_file}.L1 {train_file}.L2 | subword-nmt learn-bpe -s {num_operations} -o {codes_file}
    subword-nmt apply-bpe -c {codes_file} < {train_file}.L1 | subword-nmt get-vocab > {vocab_file}.L1
    subword-nmt apply-bpe -c {codes_file} < {train_file}.L2 | subword-nmt get-vocab > {vocab_file}.L2

more conventiently, you can do the same with with this command:

    subword-nmt learn-joint-bpe-and-vocab --input {train_file}.L1 {train_file}.L2 -s {num_operations} -o {codes_file} --write-vocabulary {vocab_file}.L1 {vocab_file}.L2

re-apply byte pair encoding with vocabulary filter:

    subword-nmt apply-bpe -c {codes_file} --vocabulary {vocab_file}.L1 --vocabulary-threshold 50 < {train_file}.L1 > {train_file}.BPE.L1
    subword-nmt apply-bpe -c {codes_file} --vocabulary {vocab_file}.L2 --vocabulary-threshold 50 < {train_file}.L2 > {train_file}.BPE.L2

as a last step, extract the vocabulary to be used by the neural network. Example with Nematus:

    nematus/data/build_dictionary.py {train_file}.BPE.L1 {train_file}.BPE.L2

[you may want to take the union of all vocabularies to support multilingual systems]

for test/dev data, re-use the same options for consistency:

    subword-nmt apply-bpe -c {codes_file} --vocabulary {vocab_file}.L1 --vocabulary-threshold 50 < {test_file}.L1 > {test_file}.BPE.L1

ADVANCED FEATURES
-----------------

On top of the basic BPE implementation, this repository supports:

- BPE dropout (Provilkov, Emelianenko and Voita, 2019): https://arxiv.org/abs/1910.13267
  use the argument `--dropout 0.1` for `subword-nmt apply-bpe` to randomly drop out possible merges.
  Doing this on the training corpus can improve quality of the final system; at test time, use BPE without dropout.
  In order to obtain reproducible results, argument `--seed` can be used to set the random seed.
  
  **Note:** In the original paper, the authors used BPE-Dropout on each new batch separately. You can copy the training corpus several times to get similar behavior to obtain multiple segmentations for the same sentence.

- support for glossaries:
  use the argument `--glossaries` for `subword-nmt apply-bpe` to provide a list of words and/or regular expressions
  that should always be passed to the output without subword segmentation

PUBLICATIONS
------------

The segmentation methods are described in:

Rico Sennrich, Barry Haddow and Alexandra Birch (2016):
    Neural Machine Translation of Rare Words with Subword Units
    Proceedings of the 54th Annual Meeting of the Association for Computational Linguistics (ACL 2016). Berlin, Germany.

HOW IMPLEMENTATION DIFFERS FROM Sennrich et al. (2016)
------------------------------------------------------

This repository implements the subword segmentation as described in Sennrich et al. (2016),
but since version 0.2, there is one core difference related to end-of-word tokens.

In Sennrich et al. (2016), the end-of-word token `</w>` is initially represented as a separate token, which can be merged with other subwords over time:

```
u n d </w>
f u n d </w>
```

Since 0.2, end-of-word tokens are initially concatenated with the word-final character:

```
u n d</w>
f u n d</w>
```

The new representation ensures that when BPE codes are learned from the above examples and then applied to new text, it is clear that a subword unit `und` is unambiguously word-final, and `un` is unambiguously word-internal, preventing the production of up to two different subword units from each BPE merge operation.

`apply_bpe.py` is backward-compatible and continues to accept old-style BPE files. New-style BPE files are identified by having the following first line: `#version: 0.2`

ACKNOWLEDGMENTS
---------------
This project has received funding from Samsung Electronics Polska sp. z o.o. - Samsung R&D Institute Poland, and from the European Union’s Horizon 2020 research and innovation programme under grant agreement 645452 (QT21).
