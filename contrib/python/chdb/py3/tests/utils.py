import re
import os

import yatest.common as yc

data_file = yc.test_source_path("data/alltypes_dictionary.parquet")

# reset elapsed time to 0.0 from output, since it will be different each time
# eg: "elapsed": 0.001015,


def reset_elapsed(input):
    try:
        if not isinstance(input, str):
            input = input.decode()
        input = re.sub(r'("elapsed": )\d+\.\d+', r'\g<1>0.0', input)
        input = re.sub(r'(<elapsed>)\d+\.\d+(</elapsed>)', r'\g<1>0.0\g<2>', input)
        input = re.sub(r'(tz=).*]', r'\g<1>Etc/UTC]', input)
        input = input.replace('08:', '00:')
    except UnicodeDecodeError:
        pass
    return input
