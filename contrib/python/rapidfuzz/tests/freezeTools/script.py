from __future__ import annotations

import rapidfuzz
from rapidfuzz.distance import metrics_cpp, metrics_py

rapidfuzz.distance.Levenshtein.distance("test", "teste")
metrics_py.levenshtein_distance("test", "teste")
metrics_cpp.levenshtein_distance("test", "teste")
