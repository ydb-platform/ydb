from ._pipe import pipe


# ---

def compose(*fns):
    return pipe(*reversed(fns))
