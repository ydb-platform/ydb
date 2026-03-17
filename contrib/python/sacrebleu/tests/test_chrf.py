# Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

# -*- coding: utf-8 -*-

import pytest
import sacrebleu

EPSILON = 1e-8

test_cases = [(["Niemand hat die Absicht, eine Mauer zu errichten"], ["Niemand hat die Absicht, eine Mauer zu errichten"], 1.0),
              (["abcdefg"], ["hijklmnop"], 0.0),
              (["a"], ["a"], 1.0),
              ([""], [""], 0.0),
              ([""], ["reference"], 0.0),
              (["a b c"], ["a b c"], 1.0),
              (["a b c"], ["abc"], 1.0),
              ([""], ["c"], 0.0),
              (["a", "b"], ["a", "c"], 0.5),
              (["source"], [""], 0.0),
              (["aa"], ["ab"], 0.25),
              ([" Die    Beziehung zwischen  Obama und Netanjahu ist nicht gerade  freundlich. "], ["Das Verhältnis zwischen Obama und Netanyahu ist nicht gerade freundschaftlich."], 0.64130269831561459),
              ([" risk assessment must be made of those who are qualified and expertise in the sector - these are the scientists ."], ["risk assessment has to be undertaken by those who are qualified and expert in that area - that is the scientists ."], 0.63361730303214769)]

test_cases_keep_whitespace = [
              (["Die Beziehung zwischen Obama und Netanjahu ist nicht gerade freundlich."], ["Das Verhältnis zwischen Obama und Netanyahu ist nicht gerade freundschaftlich."], 0.67348160629772402),
              (["risk assessment must be made of those who are qualified and expertise in the sector - these are the scientists ."], ["risk assessment has to be undertaken by those who are qualified and expert in that area - that is the scientists ."], 0.652414427449)]

@pytest.mark.parametrize("hypotheses, references, expected_score", test_cases)
def test_chrf(hypotheses, references, expected_score):
    score = sacrebleu.corpus_chrf(hypotheses, [references], 6, 3).score
    assert abs(score - expected_score) < EPSILON

@pytest.mark.parametrize("hypotheses, references, expected_score", test_cases_keep_whitespace)
def test_chrf_keep_whitespace(hypotheses, references, expected_score):
    score = sacrebleu.corpus_chrf(hypotheses, [references], 6, 3, remove_whitespace=False).score
    assert abs(score - expected_score) < EPSILON
