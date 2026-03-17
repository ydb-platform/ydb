#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Rico Sennrich

"""Use byte pair encoding (BPE) to learn a variable-length encoding of the vocabulary in a text.
Unlike the original BPE, it does not compress the plain text, but can be used to reduce the vocabulary
of a text to a configurable number of symbols, with only a small increase in the number of tokens.
This is an (inefficient) toy implementation that shows the algorithm. For processing large datasets,
indexing and incremental updates can be used to speed up the implementation (see learn_bpe.py).

Reference:
Rico Sennrich, Barry Haddow and Alexandra Birch (2016). Neural Machine Translation of Rare Words with Subword Units.
Proceedings of the 54th Annual Meeting of the Association for Computational Linguistics (ACL 2016). Berlin, Germany.
"""


import re
import sys
import collections

def get_stats(vocab):
  pairs = collections.defaultdict(int)
  for word, freq in vocab.items():
    symbols = word.split()
    for i in range(len(symbols)-1):
      pairs[symbols[i],symbols[i+1]] += freq
  return pairs

def merge_vocab(pair, v_in):
  v_out = {}
  bigram_pattern = re.escape(' '.join(pair))
  p = re.compile(r'(?<!\S)' + bigram_pattern + r'(?!\S)')
  for word in v_in:
    w_out = p.sub(''.join(pair), word)
    v_out[w_out] = v_in[word]
  return v_out

vocab = {'l o w</w>' : 5, 'l o w e r</w>' : 2,
         'n e w e s t</w>' : 6, 'w i d e s t</w>' : 3}
num_merges = 15
for i in range(num_merges):
  pairs = get_stats(vocab)
  try:
    best = max(pairs, key=pairs.get)
  except ValueError:
    break
  if pairs[best] < 2:
     sys.stderr.write('no pair has frequency > 1. Stopping\n')
     break
  vocab = merge_vocab(best, vocab)
  print(best)
