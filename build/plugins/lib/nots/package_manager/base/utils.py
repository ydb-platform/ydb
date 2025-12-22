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


def build_tmp_pj_path(p):
    return os.path.join(p, "tmp." + PACKAGE_JSON_FILENAME)


def build_nm_path(p):
    return os.path.join(p, NODE_MODULES_DIRNAME)


def build_nm_bundle_path(p):
    return os.path.join(p, NODE_MODULES_WORKSPACE_BUNDLE_FILENAME)


def build_nots_path(build_root: str) -> str:
    home_nots = os.getenv("NOTS_STORE_PATH", os.path.join(home_dir(), ".nots"))
    build_nots = os.path.join(build_root, ".nots")

    if os.getenv("NOTS_BUILDER_ISOLATED_STORE", "no") == "yes":
        # this simulates the behavior of the distbuild
        os.makedirs(build_nots, exist_ok=True)
    else:
        try:
            if not os.path.exists(home_nots):
                os.makedirs(home_nots)
            if not os.path.exists(build_nots):
                os.symlink(home_nots, build_nots)
        except (OSError, RuntimeError):
            os.makedirs(build_nots, exist_ok=True)

    return build_nots


def build_nm_store_path(build_root: str, moddir: str) -> str:
    return os.path.join(build_nots_path(build_root), "nm_store", moddir)


def build_vs_store_path(build_root: str, moddir: str) -> str:
    return os.path.join(build_nots_path(build_root), "vm_store", moddir)


def build_traces_store_path(build_root: str, moddir: str) -> str:
    return os.path.join(build_nots_path(build_root), "traces", moddir)


def build_pnpm_store_path(build_root: str) -> str:
    return os.path.join(build_nots_path(build_root), "pnpm_store")


def extract_package_name_from_path(p):
    # if we have scope prefix then we are using the first two tokens, otherwise - only the first one
    parts = p.split("/", 2)
    return "/".join(parts[:2]) if p.startswith("@") else parts[0]
