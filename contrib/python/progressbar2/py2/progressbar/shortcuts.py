from . import bar


def progressbar(iterator, min_value=0, max_value=None,
                widgets=None, prefix=None, suffix=None, **kwargs):
    progressbar = bar.ProgressBar(
        min_value=min_value, max_value=max_value,
        widgets=widgets, prefix=prefix, suffix=suffix, **kwargs)

    for result in progressbar(iterator):
        yield result
