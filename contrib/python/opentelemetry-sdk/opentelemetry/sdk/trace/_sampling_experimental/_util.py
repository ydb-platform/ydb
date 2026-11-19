# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

RANDOM_VALUE_BITS = 56
MAX_THRESHOLD = 1 << RANDOM_VALUE_BITS  # 0% sampling
MIN_THRESHOLD = 0  # 100% sampling
MAX_RANDOM_VALUE = MAX_THRESHOLD - 1
INVALID_THRESHOLD = -1
INVALID_RANDOM_VALUE = -1

_probability_threshold_scale = float.fromhex("0x1p56")


def calculate_threshold(sampling_probability: float) -> int:
    return MAX_THRESHOLD - round(
        sampling_probability * _probability_threshold_scale
    )


def is_valid_threshold(threshold: int) -> bool:
    return MIN_THRESHOLD <= threshold <= MAX_THRESHOLD


def is_valid_random_value(random_value: int) -> bool:
    return 0 <= random_value <= MAX_RANDOM_VALUE
