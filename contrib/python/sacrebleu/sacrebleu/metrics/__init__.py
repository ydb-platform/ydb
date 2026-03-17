# -*- coding: utf-8 -*-

from .bleu import BLEU, BLEUScore
from .chrf import CHRF, CHRFScore
from .ter import TER, TERScore

METRICS = {
    'bleu': BLEU,
    'chrf': CHRF,
    'ter': TER,
}
