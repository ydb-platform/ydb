import os

kernel_name = 'python3'


def get_notebook_path(*args):
    import yatest.common as yc
    return os.path.join(os.path.dirname(yc.source_path(__file__)), 'notebooks', *args)


def get_notebook_dir(*args):
    return os.path.dirname(get_notebook_path(*args))
