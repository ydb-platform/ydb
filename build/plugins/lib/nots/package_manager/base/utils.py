import os

from .constants import NODE_MODULES_DIRNAME, NODE_MODULES_WORKSPACE_BUNDLE_FILENAME, PACKAGE_JSON_FILENAME


def home_dir():
    """
    Stolen from ya (in the root of arcadia)
    """
    # Do not trust $HOME, as it is unreliable in certain environments
    # Temporarily delete os.environ["HOME"] to force reading current home directory from /etc/passwd
    home_from_env = os.environ.pop("HOME", None)
    try:
        home_from_passwd = os.path.expanduser("~")
        if os.path.isabs(home_from_passwd):
            # This home dir is valid, prefer it over $HOME
            return home_from_passwd
        else:
            # When python is built with musl (this is quire weird though),
            # only users from /etc/passwd will be properly resolved,
            # as musl does not have nss module for LDAP integration.
            return home_from_env

    finally:
        if home_from_env is not None:
            os.environ["HOME"] = home_from_env


def s_rooted(p):
    return os.path.join("$S", p)


def b_rooted(p):
    return os.path.join("$B", p)


def build_pj_path(p):
    return os.path.join(p, PACKAGE_JSON_FILENAME)


def build_nm_path(p):
    return os.path.join(p, NODE_MODULES_DIRNAME)


def build_nm_bundle_path(p):
    return os.path.join(p, NODE_MODULES_WORKSPACE_BUNDLE_FILENAME)


def extract_package_name_from_path(p):
    # if we have scope prefix then we are using the first two tokens, otherwise - only the first one
    parts = p.split("/", 2)
    return "/".join(parts[:2]) if p.startswith("@") else parts[0]
