""" Environment management utilities
"""
import os
import sys
import functools
from contextlib import contextmanager


# TODO: merge with cmdline
@contextmanager
def set_env(*args, **kwargs):
    """Context Mgr to set an environment variable

    """
    def update_environment(env):
        for k, v in env.items():
            if v is None:
                if k in os.environ:
                    del os.environ[k]
            else:
                os.environ[k] = str(v)

    # Backward compatibility with the old interface which only allowed to
    # update a single environment variable.
    new_values = dict([(args[0], args[1])]) if len(args) == 2 else {}
    new_values.update((k, v) for k, v in kwargs.items())

    # Save variables that are going to be updated.
    saved_values = dict((k, os.environ.get(k)) for k in new_values.keys())

    # Update variables to their temporary values
    try:
        update_environment(new_values)
        yield
    finally:
        # Restore original environment
        update_environment(saved_values)


set_home = functools.partial(set_env, 'HOME')


@contextmanager
def unset_env(env_var_skiplist):
    """Context Mgr to unset an environment variable temporarily."""
    def update_environment(env):
        os.environ.clear()
        os.environ.update(env)

    # Save variables that are going to be updated.
    saved_values = dict(os.environ)

    new_values = dict((k, v) for k, v in os.environ.items() if k not in env_var_skiplist)

    # Update variables to their temporary values
    update_environment(new_values)
    (yield)
    # Restore original environment
    update_environment(saved_values)


@contextmanager
def no_env(key):
    """
    Context Mgr to asserting no environment variable of the given name exists
    (sto enable the testing of the case where no env var of this name exists)
    """
    try:
        orig_value = os.environ[key]
        del os.environ[key]
        env_has_key = True
    except KeyError:
        env_has_key = False

    yield
    if env_has_key:
        os.environ[key] = orig_value
    else:
        # there shouldn't be a key in org state.. just check that there isn't
        try:
            del os.environ[key]
        except KeyError:
            pass


@contextmanager
def no_cov():
    """ Context manager to disable coverage in subprocesses. 
    """
    cov_keys = [i for i in os.environ.keys() if i.startswith('COV')]
    with unset_env(cov_keys):
        yield


def get_clean_python_env():
    """ Returns the shell environ stripped of its PYTHONPATH
    """
    env = dict(os.environ)
    if 'PYTHONPATH' in env:
        del(env['PYTHONPATH'])
    return env


def get_env_with_pythonpath():
    """ Returns the shell environ with PYTHONPATH set to the current sys.path.
        This is useful for scripts run under 'python setup.py test', which adds
        a bunch of test dependencies to sys.path at run-time.
    """
    env = get_clean_python_env()
    env['PYTHONPATH'] = ';'.join(sys.path)
    return env
