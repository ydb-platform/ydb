import os
import string
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from urllib.parse import unquote

import escapism
from traitlets import Unicode


class KVStorePrefix(Unicode):
    def validate(self, obj, value):
        u = super().validate(obj, value)
        if u.endswith("/"):
            u = u.rstrip("/")

        proxy_class = type(obj).__name__
        if "Consul" in proxy_class and u.startswith("/"):
            u = u[1:]

        return u


def generate_rule(routespec):
    """Generate a traefik routing rule for a jupyterhub routespec


    - if a routespec doesn't start with a `/`, the first part is a hostname.
    - routespecs always end with `/`
    - routespecs are a path _prefix_, and should match anything under them
    - the root of the route without the trailing slash should match the rule,
      e.g. the routespec `/prefix/` should match `/prefix` and `/prefix/tree`

    Traefik rule documentation: https://doc.traefik.io/traefik/routing/routers/#rule
    """

    # validate assumption that routespecs always end with '/'
    if not routespec.endswith("/"):
        raise ValueError("routespec must end with /")
    routespec = unquote(routespec)

    # traefik won't match /proxy/path to /proxy/path/
    # so strip trailing slash for consistent behavior
    if routespec.startswith("/"):
        # Path-based route, e.g. /proxy/path/
        host = ""
        path = routespec
    else:
        # Host-based routing, e.g. host.tld/proxy/path/
        host, slash, path = routespec.partition("/")
        path = slash + path

    path_no_slash = path.rstrip("/")

    path_rule = f"PathPrefix(`{path}`)"
    if path_no_slash:
        # include exact Path('/prefix') so that both /prefix/ and /prefix
        # are served correctly
        path_rule = f"( {path_rule} || Path(`{path_no_slash}`) )"

    if host:
        rule = f"Host(`{host}`) && {path_rule}"
    else:
        rule = path_rule
    return rule


_safe = set(string.ascii_letters + string.digits + "-")


def generate_alias(routespec, kind=""):
    """Generate an alias for a routespec

    A safe string for use in key-value store keys, etc.
    """
    alias = escapism.escape(routespec, safe=_safe)
    if kind:
        alias = f"{kind}_{alias}"
    return alias


# atomic writing adapted from jupyter/notebook 5.7
# unlike atomic writing there, which writes the canonical path
# and only use the temp file for recovery,
# we write the temp file and then replace the canonical path
# to ensure that traefik never reads a partial file


@contextmanager
def atomic_writing(path):
    """Write temp file before copying it into place

    Avoids a partial file ever being present in `path`,
    which could cause traefik to load a partial routing table.
    """
    fileobj = NamedTemporaryFile(
        prefix=os.path.abspath(path) + "-tmp-", mode="w", delete=False
    )
    try:
        with fileobj as f:
            yield f
        os.replace(fileobj.name, path)
    finally:
        try:
            os.unlink(fileobj.name)
        except FileNotFoundError:
            # already deleted by os.replace above
            pass


class TraefikConfigFileHandler:
    """Handles reading and writing Traefik config files. Can operate
    on both toml and yaml files"""

    def __init__(self, file_path):
        file_ext = file_path.rsplit('.', 1)[-1]
        if file_ext == 'yaml':
            try:
                from ruamel.yaml import YAML
            except ImportError:
                raise ImportError(
                    "jupyterhub-traefik-proxy requires ruamel.yaml to use YAML config files"
                )
            config_handler = YAML(typ="rt")
        elif file_ext == 'toml':
            import toml as config_handler
        else:
            raise TypeError("type should be either 'toml' or 'yaml'")

        self.file_path = file_path
        # Redefined to either yaml.dump or toml.dump
        self._dump = config_handler.dump
        # self._dumps = config_handler.dumps
        # Redefined by __init__, to either yaml.load or toml.load
        self._load = config_handler.load

    def load(self):
        """Depending on self.file_path, call either yaml.load or toml.load"""
        with open(self.file_path) as fd:
            return self._load(fd)

    def dump(self, data):
        with open(self.file_path, "w") as f:
            self._dump(data, f)

    def atomic_dump(self, data):
        """Save data to self.file_path after opening self.file_path with
        :func:`atomic_writing`"""
        with atomic_writing(self.file_path) as f:
            self._dump(data, f)


def deep_merge(a, b):
    """Merges dict b into dict a, returning a

    Note: a is modified in the process!
    """
    for k, v in b.items():
        if k in a:
            if isinstance(a[k], dict):
                # recursive update
                a[k] = deep_merge(a[k], v)
            else:
                a[k] = v
        else:
            a[k] = v
    return a
