"""Util functions which may be used outside of the class for convenience"""
from typing import List, Dict

def count_emojis(text: str) -> List[Dict]:
    """Count emojis in a given string and return a list of emojis with index, value, and spacing."""
    
    emoji_ranges = [
        (0x1F600, 0x1F64F),  # Emoticons
        (0x1F300, 0x1F5FF),  # Miscellaneous Symbols and Pictographs
        (0x1F680, 0x1F6FF),  # Transport and Map Symbols
        (0x2600, 0x26FF),    # Miscellaneous Symbols
        (0x2700, 0x27BF),    # Dingbats
        (0xFE00, 0xFE0F),    # Variation Selectors
        (0x1F900, 0x1F9FF),  # Supplemental Symbols and Pictographs
        (0x1F1E6, 0x1F1FF),  # Flags (iOS)
        (0x1F7E0, 0x1F7FF),  # Geometric Shapes Extended (🔴🟢)
        (0x2B00, 0x2BFF),    # Additional geometric symbols (⬛⬜)
        (0x2190, 0x21FF),    # Arrows (➡️ ⬆️ ⬇️ ⬅️)
        (0x1FA70, 0x1FAFF),  # Extended symbols (🛗 🛻 🪐 🪑)
    ]

    emojis = []

    for i, char in enumerate(text):
        code_point = ord(char)

        # Check if character is in an emoji range
        if any(start <= code_point <= end for start, end in emoji_ranges):
            emojis.append({"index": i, "value": char, "spacing": 2})

        # Handle surrogate pairs
        if (
            i + 1 < len(text)
            and 0xD800 <= code_point <= 0xDBFF
            and 0xDC00 <= ord(text[i + 1]) <= 0xDFFF
        ):
            full_code_point = 0x10000 + (code_point - 0xD800) * 0x400 + (ord(text[i + 1]) - 0xDC00)
            if any(start <= full_code_point <= end for start, end in emoji_ranges):
                emojis.append({"index": i, "value": char + text[i + 1], "spacing": 2})

        # Handle keycap sequences (1️⃣ 2️⃣ 3️⃣ #️⃣)
        if i + 1 < len(text) and ord(text[i + 1]) == 0x20E3:
            emojis.append({"index": i, "value": char + text[i + 1], "spacing": 2})

    return emojis


def find_longest_contiguous_strings(
    data: List[Dict], include_header: bool = False, delimiter: str = " "
) -> Dict:
    """Finds the longest contiguous strings in a list of dicts.

    Args:
        data (List[Dict]): List of dicts containing the data to be rendered in a markdown table.
            See markdown_table object description.
        include_header (bool, optional): True if also headers should be parsed. Defaults to False.
        delimiter (str, optional): Which delimiter character to use when splitting a cell's contents.
                                   Defaults to " ".

    Returns:
        Dict: Dictionary containing the minimal width of each column for the input data with keys
              corresponding to the table headers
    """
    longest_strings = {}
    if include_header:
        longest_strings = {key: len(key) for key in data[0].keys()}
    for dictionary in data:  # pylint: disable=R1702
        for key, value in dictionary.items():
            if isinstance(value, str):
                max_length = 0
                current_length = 0
                for char in value:
                    if char != delimiter:
                        current_length += 1
                        if current_length > max_length:
                            max_length = current_length
                    else:
                        current_length = 0
                if max_length > longest_strings.get(key, 0):
                    longest_strings[key] = max_length
    return longest_strings


def split_list_by_indices(lst: List, indices: List) -> List:
    """Used to split emojis into separate elements for the multirow rendering

    Args:
        lst (List): Input list which needs to be split
        indices (List): List of indices to split by

    Returns:
        List: List split by indices
    """

    split_list = []
    start = 0
    for index in indices:
        split_list.append(lst[start:index])
        start = index
    split_list.append(lst[start:])
    return split_list
