import typing as tp


# ---

def unpack_and_call[*Values, Result](fn: tp.Callable[[*Values], Result]) -> tp.Callable[[tp.Tuple[*Values]], Result]:
    def _unpack_and_call(args: tp.Tuple[*Values]) -> Result:
        return fn(*args)
    return _unpack_and_call
