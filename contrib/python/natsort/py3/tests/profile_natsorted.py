# -*- coding: utf-8 -*-
"""\
This file contains functions to profile natsorted with different
inputs and different settings.
"""

import cProfile
import locale
import sys
from typing import List, Union

try:
    from natsort import ns, natsort_keygen
except ImportError:
    sys.path.insert(0, ".")
    from natsort import ns, natsort_keygen

from natsort.natsort import NatsortKeyType

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")

# Samples to parse
number = 14695498
int_string = "43493"
float_string = "-434.93e7"
plain_string = "hello world"
fancy_string = "7abba9342fdab"
a_path = "/p/Folder (1)/file (1).tar.gz"
some_bytes = b"these are bytes"
a_list = ["hello", "goodbye", "74"]

basic_key = natsort_keygen()
real_key = natsort_keygen(alg=ns.REAL)
path_key = natsort_keygen(alg=ns.PATH)
locale_key = natsort_keygen(alg=ns.LOCALE)


def prof_time_to_generate() -> None:
    print("*** Generate Plain Key ***")
    for _ in range(100000):
        natsort_keygen()


cProfile.run("prof_time_to_generate()", sort="time")


def prof_parsing(
    a: Union[str, int, bytes, List[str]], msg: str, key: NatsortKeyType = basic_key
) -> None:
    print(msg)
    for _ in range(100000):
        key(a)


cProfile.run(
    'prof_parsing(int_string, "*** Basic Call, Int as String ***")', sort="time"
)
cProfile.run(
    'prof_parsing(float_string, "*** Basic Call, Float as String ***")', sort="time"
)
cProfile.run('prof_parsing(float_string, "*** Real Call ***", real_key)', sort="time")
cProfile.run('prof_parsing(number, "*** Basic Call, Number ***")', sort="time")
cProfile.run(
    'prof_parsing(fancy_string, "*** Basic Call, Mixed String ***")', sort="time"
)
cProfile.run('prof_parsing(some_bytes, "*** Basic Call, Byte String ***")', sort="time")
cProfile.run('prof_parsing(a_path, "*** Path Call ***", path_key)', sort="time")
cProfile.run('prof_parsing(a_list, "*** Basic Call, Recursive ***")', sort="time")
cProfile.run(
    'prof_parsing("434,930,000 dollars", "*** Locale Call ***", locale_key)',
    sort="time",
)
