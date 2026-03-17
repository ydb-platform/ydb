from annet.annlib.types import Op


def change(key, diff, **kwargs):
    yield from [(True, add["row"], None) for add in diff[Op.ADDED]]
