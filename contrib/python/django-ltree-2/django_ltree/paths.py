import string
import math
from itertools import product

from .fields import PathValue


class PathGenerator:
    def __init__(self, prefix=None, skip=None):
        combinations = string.digits + string.ascii_letters

        self.skip_paths = [] if skip is None else skip[:]
        self.path_prefix = prefix if prefix else []
        self.product_iterator = product(
            combinations,
            repeat=self.guess_the_label_size(
                path_size=len(self.skip_paths), combination_size=len(combinations)
            ),
        )

    def __iter__(self):
        return self

    def __next__(self):
        for val in self.product_iterator:
            label = "".join(val)
            path = PathValue(self.path_prefix + [label])
            if path not in self.skip_paths:
                return path

    @staticmethod
    def guess_the_label_size(path_size: int, combination_size: int) -> int:
        calculated_path_size = -1  # -1 is here for 0th index items
        # The theoritical limit for this at the time of writing is 32 (python 3.12.2)
        label_size = 0

        last = 0

        while True:
            possible_cominations = math.perm(combination_size, label_size)
            if last > possible_cominations:
                raise ValueError("There is error in the input value")

            last = possible_cominations
            calculated_path_size += possible_cominations

            if calculated_path_size > path_size and label_size != 0:
                break

            label_size += 1

        return label_size
