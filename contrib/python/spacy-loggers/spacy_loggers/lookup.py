"""
A utility logger that looks up specific statistics and prints them to stdout.
"""
from typing import Dict, Any, Optional, IO, List
import sys

from spacy import Language
from .util import dict_to_dot, LoggerT, matcher_for_regex_patterns


def lookup_logger_v1(patterns: List[str]) -> LoggerT:
    def setup_logger(nlp: Language, stdout: IO = sys.stdout, stderr: IO = sys.stderr):
        if len(patterns) == 0:
            raise ValueError("Lookup logger should receive at least one pattern")
        match_stat = matcher_for_regex_patterns(patterns)

        def log_step(info: Optional[Dict[str, Any]]):
            if info is None:
                return
            config_dot = dict_to_dot(info)
            for k, v in config_dot.items():
                if match_stat(k):
                    stdout.writelines([k, " -> ", str(v), "\n"])

        def finalize():
            pass

        return log_step, finalize

    return setup_logger
