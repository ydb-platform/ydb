# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .loaders import WebAnnotatorLoader, GateLoader, HtmlLoader, load_trees
from .sequence_encoding import IobEncoder, InputTokenProcessor
from .feature_extraction import HtmlFeatureExtractor
from .html_tokenizer import HtmlTokenizer, HtmlToken
from .wapiti import WapitiCRF, create_wapiti_pipeline
from .crfsuite import create_crfsuite_pipeline
from .model import NER
from .utils import smart_join
