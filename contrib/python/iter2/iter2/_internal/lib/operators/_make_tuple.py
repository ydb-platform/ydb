import typing as tp


# ---

def make_tuple[*Values](*args: *Values) -> tp.Tuple[*Values]:
    return args
