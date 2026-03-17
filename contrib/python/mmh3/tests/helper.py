# -*- coding: utf-8 -*-


# see also https://stackoverflow.com/a/1375939
def u32_to_s32(v: int) -> int:
    if v & 0x80000000:
        return -0x100000000 + v
    else:
        return v
