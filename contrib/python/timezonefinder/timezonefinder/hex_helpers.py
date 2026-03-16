import struct
import importlib.resources
from pathlib import Path
from typing import Dict, List

import numpy as np
from h3.api import numpy_int as h3

from timezonefinder.configs import (
    DTYPE_FORMAT_B,
    DTYPE_FORMAT_H,
    DTYPE_FORMAT_H_NUMPY,
    DTYPE_FORMAT_Q,
    NR_BYTES_B,
    NR_BYTES_I,
    NR_BYTES_Q,
    THRES_DTYPE_B,
    ShortcutMapping,
)


def export_shortcuts_binary(
    global_mapping: Dict[int, List[int]], path2shortcuts: Path
) -> int:
    """
    binary format:
        for every shortcut entry:
            - the hex id (uint64)
            - the amount of contained polygons n (uint8)
            - n polygon ids (uint16)

    """
    shortcut_space = 0
    with importlib.resources.as_file(path2shortcuts) as f:
        with open(f, "wb") as fp:
            for hex_id, poly_ids in global_mapping.items():
                fp.write(struct.pack(DTYPE_FORMAT_Q, hex_id))
                nr_polys = len(poly_ids)
                if nr_polys > THRES_DTYPE_B:
                    raise ValueError("value overflow: more polys than data type supports")
                fp.write(struct.pack(DTYPE_FORMAT_B, nr_polys))
                for poly_id in poly_ids:
                    fp.write(struct.pack(DTYPE_FORMAT_H, poly_id))

                shortcut_space += NR_BYTES_Q + NR_BYTES_B + (len(poly_ids) * NR_BYTES_I)

    return shortcut_space


def read_shortcuts_binary(path2shortcuts: Path) -> ShortcutMapping:
    mapping: ShortcutMapping = {}
    with importlib.resources.as_file(path2shortcuts) as f:
        with open(f, "rb") as fp:
            while 1:
                try:
                    hex_id: int = struct.unpack(DTYPE_FORMAT_Q, fp.read(NR_BYTES_Q))[0]
                except struct.error:
                    # EOF: buffer not long enough to unpack
                    break
                nr_polys: int = struct.unpack(DTYPE_FORMAT_B, fp.read(NR_BYTES_B))[0]
                poly_ids: np.ndarray = np.fromfile(
                    fp, dtype=DTYPE_FORMAT_H_NUMPY, count=nr_polys
                )
                mapping[hex_id] = poly_ids

    return mapping


def lies_in_h3_cell(h: int, lng: float, lat: float) -> bool:
    res = h3.h3_get_resolution(h)
    return h3.geo_to_h3(lat, lng, res) == h
