from typing import Tuple


def map_range(
    value: float, input_range: Tuple[float, float], output_range: Tuple[float, float]
) -> float:
    min_input, max_input = input_range
    min_output, max_output = output_range

    return ((value - min_input) / (max_input - min_input)) * (
        max_output - min_output
    ) + min_output
