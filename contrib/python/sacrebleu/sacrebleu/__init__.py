#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2017--2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
# use this file except in compliance with the License. A copy of the License
# is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

__version__ = '1.4.14'
__description__ = 'Hassle-free computation of shareable, comparable, and reproducible BLEU, chrF, and TER scores'


from .utils import smart_open, SACREBLEU_DIR, download_test_set
from .utils import get_source_file, get_reference_files
from .utils import get_available_testsets, get_langpairs_for_testset
from .dataset import DATASETS
from .tokenizers import TOKENIZERS, DEFAULT_TOKENIZER
from .metrics import BLEU, CHRF
from .sacrebleu import main

# Backward compatibility functions for old style API access (<= 1.4.10)
from .compat import *

# Other shorthands for backward-compatibility with <= 1.4.10
extract_ngrams = BLEU.extract_ngrams
extract_char_ngrams = CHRF.extract_char_ngrams
ref_stats = BLEU.reference_stats
compute_bleu = BLEU.compute_bleu
