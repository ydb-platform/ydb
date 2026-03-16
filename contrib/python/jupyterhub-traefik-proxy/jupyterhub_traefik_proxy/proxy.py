"""Traefik implementation

Custom proxy implementations can subclass :class:`Proxy`
and register in JupyterHub config:

.. sourcecode:: python

    from mymodule import MyProxy
    c.JupyterHub.proxy_class = MyProxy

Route Specification:

- A routespec is a URL prefix ([host]/path/), e.g.
  'host.tld/path/' for host-based routing or '/path/' for default routing.
- Route paths should be normalized to always start and end with '/'
"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

import asyncio
import json
import os
import ssl
from os.path import abspath
from subprocess import Popen, TimeoutExpired
from urllib.parse import urlparse, urlunparse

import bcrypt
from jupyterhub.proxy import Proxy
from jupyterhub.utils import exponential_backoff, new_token, url_path_join
from tornado.httpclient import AsyncHTTPClient, HTTPClientError
from traitlets import Any, Bool, Dict, Integer, Unicode, default, observe, validate

from . import traefik_utils
from .traefik_utils import deep_merge


class TraefikProxy(Proxy):
    """JupyterHub Proxy implementation using traefik"""

    provider_name = ""  # must be set in subclasses

    traefik_process = Any()

    concurrency = Integer(
        10,
        config=True,
        help="""
        The number of requests allowed to be concurrently outstanding to the proxy

        Limiting this number avoids potential timeout errors
        by sending too many requests to update the proxy at once
        """,
    )
    semaphore = Any()

    @default('semaphore')
    def _default_semaphore(self):
        return asyncio.BoundedSemaphore(self.concurrency)

    @observe('concurrency')
    def _concurrency_changed(self, change):
        self.semaphore = asyncio.BoundedSemaphore(change.new)

    static_config_file = Unicode(
        "traefik.toml", config=True, help="""traefik's static configuration file"""
    )

    extra_static_config = Dict(
        config=True,
        help="""Extra static configuration for treafik.

        Merged with the default static config before writing to `.static_config_file`.

        Has no effect if `Proxy.should_start` is False.
        """,
    )

    extra_dynamic_config = Dict(
        config=True,
        help="""Extra dynamic configuration for treafik.

        Merged with the default dynamic config during startup.

        Always takes effect unless should_start and enable_setup_dynamic_config are both False.
        """,
    )

    enable_setup_dynamic_config = Bool(
        True,
        config=True,
        help="""
        Whether to initialize the traefik dynamic configuration
        from JupyterHub configuration, when should_start is False
        (dynamic configuration is always applied when should_start is True).

        Creates the traefik API router and TLS setup, if any.
        When True, only traefik static configuration must be managed by the external service
        (configuration of the endpoints and provider).
        The traefik api router should not already be configured via other dynamic configuration providers.

        When False, initial dynamic configuration must be handled externally
        and match TraefikProxy configuration, such as `traefik_api_url`,
        traefik_api_username` and `traefik_api_password`.
        Choose this if the traefik api router is already configured via dynamic configuration elsewhere.

        .. versionadded:: 1.1
        """,
    )

    toml_static_config_file = Unicode(
        config=True,
        help="Deprecated. Use static_config_file",
    ).tag(
        deprecated_in="1.0",
        deprecated_for="static_config_file",
    )

    def _deprecated_trait(self, change):
        """observer for deprecated traits"""
        trait = change.owner.traits()[change.name]
        old_attr = change.name
        new_attr = trait.metadata["deprecated_for"]
        version = trait.metadata["deprecated_in"]
        if "." in new_attr:
            new_cls_attr = new_attr
            new_attr = new_attr.rsplit(".", 1)[1]
        else:
            new_cls_attr = f"{self.__class__.__name__}.{new_attr}"

        new_value = getattr(self, new_attr)
        if new_value != change.new:
            # only warn if different
            # protects backward-compatible config from warnings
            # if they set the same value under both names
            message = "{cls}.{old} is deprecated in {cls} {version}, use {new} instead".format(
                cls=self.__class__.__name__,
                old=old_attr,
                new=new_cls_attr,
                version=version,
            )
            self.log.warning(message)

            setattr(self, new_attr, change.new)

    def __init__(self, **kwargs):
        # observe deprecated config names in oauthenticator
        for name, trait in self.class_traits().items():
            if trait.metadata.get("deprecated_in"):
                self.observe(self._deprecated_trait, name)
        super().__init__(**kwargs)
        # not calling .start, make sure we load our initial dynamic config
        # and that our static config matches what we expect
        # can't await here because we're not async,
        # so await in our Proxy methods
        if not self.should_start:
            self._start_future = asyncio.ensure_future(self._start_external())
        else:
            self._start_future = None

    static_config = Dict()
    dynamic_config = Dict()

    traefik_providers_throttle_duration = Unicode(
        "0s",
        config=True,
        help="""
            throttle traefik reloads of configuration.

            When traefik sees a change in configuration,
            it will wait this long before applying the next one.
            This affects how long adding a user to the proxy will take.

            See https://doc.traefik.io/traefik/providers/overview/#providersprovidersthrottleduration
            """,
    )

    traefik_api_url = Unicode(
        "http://localhost:8099",
        config=True,
        help="""traefik authenticated api endpoint url""",
    )

    traefik_api_validate_cert = Bool(
        True,
        config=True,
        help="""validate SSL certificate of traefik api endpoint""",
    )

    traefik_log_level = Unicode(config=True, help="""traefik's log level""")

    traefik_api_password = Unicode(
        config=True, help="""The password for traefik api login"""
    )

    traefik_env = Dict(
        config=True,
        help="""Environment variables to set for the traefik process.

        Only has an effect when traefik is a subprocess (should_start=True).
        """,
    )

    provider_name = Unicode(
        help="""The provider name that Traefik expects, e.g. file, consul, etcd"""
    )

    is_https = Bool(
        help="""Whether :attr:`.public_url` specifies an https entrypoint"""
    )

    @default("is_https")
    def get_is_https(self):
        # Check if we set https
        return urlparse(self.public_url).scheme == "https"

    @validate("public_url", "traefik_api_url")
    def _add_port(self, proposal):
        url = proposal.value
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(
                f"{self.__class__.__name__}.{proposal.trait.name} must be of the form http[s]://host:port/, got {url}"
            )
        if not parsed.port:
            # ensure port is defined
            if parsed.scheme == 'http':
                parsed = parsed._replace(netloc=f'{parsed.hostname}:80')
            elif parsed.scheme == 'https':
                parsed = parsed._replace(netloc=f'{parsed.hostname}:443')
            url = urlunparse(parsed)
        return url

    traefik_entrypoint = Unicode(
        help="""The traefik entrypoint name to use.

        By default, will be `http` if http or `https` if https.

        If running traefik externally with your own specified entrypoint name,
        set this value.
        """,
        config=True,
    )

    @default("traefik_entrypoint")
    def _default_traefik_entrypoint(self):
        """Find the traefik entrypoint that matches our :attrib:`self.public_url`"""
        if self.is_https:
            return "https"
        else:
            return "http"

    traefik_api_entrypoint = Unicode(
        "auth_api",
        help="""The traefik entrypoint name to use for API access.

        Separate from traefik_entrypoint,
        because this is usually only on localhost.
        """,
        config=True,
    )

    @default("traefik_api_password")
    def _warn_empty_password(self):
        self.log.warning("Traefik API password was not set.")

        if self.should_start:
            # Generating tokens is fine if the Hub is starting the proxy
            self.log.warning("Generating a random token for traefik_api_username...")
            return new_token()

        self.log.warning(
            "Please set c.TraefikProxy.traefik_api_password to authenticate with traefik"
            " if the proxy was not started by the Hub."
        )
        return ""

    traefik_api_username = Unicode(
        config=True, help="""The username for traefik api login"""
    )

    @default("traefik_api_username")
    def _warn_empty_username(self):
        self.log.warning("Traefik API username was not set.")

        if self.should_start:
            self.log.warning('Defaulting traefik_api_username to "jupyterhub"')
            return "jupyterhub"

        self.log.warning(
            "Please set c.TraefikProxy.traefik_api_username to authenticate with traefik"
            " if the proxy was not started by the Hub."
        )
        return ""

    traefik_api_hashed_password = Unicode(
        config=True,
        help="""
        Set the hashed password to use for the API

        If unspecified, `traefik_api_password` will be hashed with bcrypt.
        ref: https://doc.traefik.io/traefik/middlewares/http/basicauth/
        """,
    )

    @default("traefik_api_hashed_password")
    def _generate_htpassword(self):
        return bcrypt.hashpw(
            self.traefik_api_password.encode("utf8"), bcrypt.gensalt()
        ).decode("ascii")

    check_route_timeout = Integer(
        30,
        config=True,
        help="""Timeout (in seconds) when waiting for traefik to register an updated route.""",
    )

    async def _check_for_traefik_service(self, routespec, kind):
        """Check for an expected router or service in the Traefik API.

        This is used to wait for traefik to load configuration
        from a provider
        """
        # expected e.g. 'service' + '_' + routespec @ file
        routespec = self.validate_routespec(routespec)
        expected = (
            traefik_utils.generate_alias(routespec, kind) + "@" + self.provider_name
        )
        path = f"/api/http/{kind}s/{expected}"
        try:
            resp = await self._traefik_api_request(path)
            json.loads(resp.body)
        except HTTPClientError as e:
            if e.code == 404:
                self.log.debug(
                    "Traefik route for %s: %s not yet in %s", routespec, expected, kind
                )
                return False
            self.log.exception(f"Error checking traefik api for {kind} {routespec}")
            return False
        except Exception:
            self.log.exception(f"Error checking traefik api for {kind} {routespec}")
            return False

        # found the expected endpoint
        return True

    async def _wait_for_route(self, routespec):
        self.log.debug("Traefik route for %s: waiting to register", routespec)

        async def _check_traefik_dynamic_conf_ready():
            """Check if traefik loaded its dynamic configuration yet"""
            if not await self._check_for_traefik_service(routespec, "service"):
                return False
            if not await self._check_for_traefik_service(routespec, "router"):
                return False

            return True

        await exponential_backoff(
            _check_traefik_dynamic_conf_ready,
            f"Traefik route for {routespec}: not ready",
            scale_factor=1.2,
            timeout=self.check_route_timeout,
        )
        self.log.debug("Treafik route for %s: registered", routespec)

    async def _traefik_api_request(self, path):
        """Make an API request to traefik"""
        url = url_path_join(self.traefik_api_url, path)
        self.log.debug("Fetching traefik api %s", url)
        resp = await AsyncHTTPClient().fetch(
            url,
            auth_username=self.traefik_api_username,
            auth_password=self.traefik_api_password,
            validate_cert=self.traefik_api_validate_cert,
        )
        if resp.code >= 300:
            self.log.warning("%s GET %s", resp.code, url)
        else:
            self.log.debug("%s GET %s", resp.code, url)
        return resp

    async def _wait_for_static_config(self):
        async def _check_traefik_static_conf_ready():
            """Check if traefik loaded its static configuration yet"""
            try:
                await self._traefik_api_request("/api/overview")
                await self._traefik_api_request(
                    f"/api/entrypoints/{self.traefik_entrypoint}"
                )
            except ConnectionRefusedError:
                self.log.debug(
                    f"Connection Refused waiting for traefik at {self.traefik_api_url}. It's probably starting up..."
                )
                return False
            except HTTPClientError as e:
                if e.code == 599:
                    self.log.debug(
                        f"Connection error waiting for traefik at {self.traefik_api_url}. It's probably starting up..."
                    )
                    return False
                if e.code == 404:
                    if "/entrypoints/" in e.response.request.url:
                        self.log.warning(
                            f"c.{self.__class__.__name__}.traefik_entrypoint={self.traefik_entrypoint!r} not found in traefik. Is it correct?"
                        )
                    else:
                        self.log.debug(
                            f"traefik api at {e.response.request.url} overview not ready yet"
                        )
                    return False
                # unexpected
                self.log.error(f"Error checking for traefik static configuration {e}")
                return False
            except ssl.SSLError as e:
                # Can occur if SSL isn't set up yet
                self.log.warning(
                    f"SSL Error checking for traefik static configuration: {e}"
                )
                return False
            except OSError as e:
                # OSError can occur during SSL setup,
                # e.g. if socket is listening, but SSL isn't ready
                if self.traefik_api_url.startswith("https:"):
                    self.log.warning(
                        f"Error checking for traefik static configuration: {e}"
                    )
                    return False
                else:
                    # other OSErrors should be fatal
                    raise

            return True

        await exponential_backoff(
            _check_traefik_static_conf_ready,
            "Traefik static configuration not available",
            timeout=self.check_route_timeout,
        )

    def _stop_traefik(self):
        self.log.info("Cleaning up traefik proxy [pid=%i]...", self.traefik_process.pid)
        self.traefik_process.terminate()
        try:
            self.traefik_process.communicate(timeout=10)
        except TimeoutExpired:
            self.traefik_process.kill()
            self.traefik_process.communicate()
        finally:
            self.traefik_process.wait()

    def _start_traefik(self):
        env = os.environ.copy()
        env.update(self.traefik_env)
        try:
            self.traefik_process = Popen(
                ["traefik", "--configfile", abspath(self.static_config_file)],
                env=env,
            )
        except FileNotFoundError:
            self.log.error(
                "Failed to find traefik\n"
                "The proxy can be downloaded from https://github.com/traefik/traefik/releases/."
            )
            raise

    async def _setup_traefik_static_config(self):
        """When should_start=True, we are in control of traefik's static configuration
        file. This sets up the entrypoints and api handler in self.static_config, and
        then saves it to :attrib:`self.static_config_file`.

        Subclasses should specify any traefik providers themselves, in
        :attrib:`self.static_config["providers"]`
        """
        static_config = {
            "api": {},
            "providers": {
                "providersThrottleDuration": self.traefik_providers_throttle_duration,
            },
        }
        if self.traefik_log_level:
            static_config["log"] = {"level": self.traefik_log_level}

        entrypoints = static_config["entryPoints"] = {
            self.traefik_entrypoint: {
                "address": urlparse(self.public_url).netloc,
            },
            self.traefik_api_entrypoint: {
                "address": urlparse(self.traefik_api_url).netloc,
            },
        }

        if self.is_https:
            entrypoints[self.traefik_entrypoint]["http"] = {
                "tls": {
                    "options": "default",
                }
            }

        # load what we just defined at _lower_ priority
        # than anything added to self.static_config in a subclass before this
        self.static_config = deep_merge(static_config, self.static_config)
        if self.extra_static_config:
            self.static_config = deep_merge(
                self.static_config, self.extra_static_config
            )

        self.log.info(f"Writing traefik static config: {self.static_config}")

        try:
            handler = traefik_utils.TraefikConfigFileHandler(self.static_config_file)
            handler.atomic_dump(self.static_config)
        except Exception:
            self.log.error("Couldn't set up traefik's static config.")
            raise

    async def _setup_traefik_dynamic_config(self):
        self.log.debug("Setting up traefik's dynamic config...")
        api_url = urlparse(self.traefik_api_url)
        api_path = api_url.path if api_url.path.strip("/") else '/api'
        api_credentials = (
            f"{self.traefik_api_username}:{self.traefik_api_hashed_password}"
        )
        dynamic_config = {
            "http": {
                "routers": {},
                "middlewares": {},
            }
        }
        routers = dynamic_config["http"]["routers"]
        middlewares = dynamic_config["http"]["middlewares"]
        routers["route_api"] = {
            "rule": f"Host(`{api_url.hostname}`) && PathPrefix(`{api_path}`)",
            "entryPoints": [self.traefik_api_entrypoint],
            "service": "api@internal",
            "middlewares": ["auth_api"],
        }
        middlewares["auth_api"] = {"basicAuth": {"users": [api_credentials]}}

        # add default ssl cert/keys
        if self.ssl_cert and self.ssl_key:
            dynamic_config["tls"] = {
                "stores": {
                    "default": {
                        "defaultCertificate": {
                            "certFile": self.ssl_cert,
                            "keyFile": self.ssl_key,
                        }
                    }
                }
            }

        self.dynamic_config = deep_merge(dynamic_config, self.dynamic_config)
        if self.extra_dynamic_config:
            self.dynamic_config = deep_merge(
                self.dynamic_config, self.extra_dynamic_config
            )

        await self._apply_dynamic_config(self.dynamic_config, None)

    def validate_routespec(self, routespec):
        """Override jupyterhub's default Proxy.validate_routespec method, as traefik
        can set router rule's on both Host and PathPrefix rules combined.
        """
        if not routespec.endswith("/"):
            routespec = routespec + "/"
        return routespec

    async def start(self):
        """Start the proxy.

        Will be called during startup if should_start is True.

        **Subclasses must define this method**
        if the proxy is to be started by the Hub
        """
        await self._setup_traefik_static_config()
        await self._setup_traefik_dynamic_config()
        self._start_traefik()
        await self._wait_for_static_config()

    async def _start_external(self):
        """Startup function called when `not self.should_start`

        Ensures dynamic config is setup and static config is loaded
        """
        if self.enable_setup_dynamic_config:
            await self._setup_traefik_dynamic_config()
        else:
            self.log.info(
                "Assuming traefik dynamic configuration for API at %s is set up already.",
                self.traefik_api_url,
            )
        await self._wait_for_static_config()
        self._start_future = None

    async def stop(self):
        """Stop the proxy.

        Will be called during teardown if should_start is True.

        **Subclasses must define this method**
        if the proxy is to be started by the Hub
        """
        self._stop_traefik()
        _cleanup_result = self._cleanup()
        if _cleanup_result is not None:
            await _cleanup_result

    def _cleanup(self):
        """Cleanup after stop

        Extend if there's more to cleanup than the static config file
        """
        if self.should_start:
            try:
                os.remove(self.static_config_file)
            except Exception as e:
                self.log.error(
                    f"Failed to remove traefik config file {self.static_config_file}: {e}"
                )

    def _dynamic_config_for_route(self, routespec, target, data):
        """Returns two dicts, which will be used to update traefik configuration for a given route

        (traefik_config, jupyterhub_config) -
            where traefik_config is traefik dynamic_config to be merged,
            and jupyterhub_config is jupyterhub-specific data to be stored elsewhere
            (implementation-specific) and associated with the route
        """

        service_alias = traefik_utils.generate_alias(routespec, "service")
        router_alias = traefik_utils.generate_alias(routespec, "router")
        rule = traefik_utils.generate_rule(routespec)
        # dynamic config to deep merge
        traefik_config = {
            "http": {
                "routers": {},
                "services": {},
            },
        }
        jupyterhub_config = {
            "routes": {},
        }
        traefik_config["http"]["routers"][router_alias] = router = {
            "service": service_alias,
            "rule": rule,
            "entryPoints": [self.traefik_entrypoint],
        }
        traefik_config["http"]["services"][service_alias] = {
            "loadBalancer": {"servers": [{"url": target}], "passHostHeader": True}
        }

        # Add the data node to a separate top-level node, so traefik doesn't see it.
        # key needs to be key-value safe (no '/')
        # store original routespec, router, service aliases for easy lookup
        jupyterhub_config["routes"][router_alias] = {
            "data": data,
            "routespec": routespec,
            "target": target,
            "router": router_alias,
            "service": service_alias,
        }
        return traefik_config, jupyterhub_config

    async def add_route(self, routespec, target, data):
        """Add a route to the proxy.

        **Subclasses must define this method**

        Args:
            routespec (str): A URL prefix ([host]/path/) for which this route will be matched,
                e.g. host.name/path/
            target (str): A full URL that will be the target of this route.
            data (dict): A JSONable dict that will be associated with this route, and will
                be returned when retrieving information about this route.

        Will raise an appropriate Exception (FIXME: find what?) if the route could
        not be added.

        The proxy implementation should also have a way to associate the fact that a
        route came from JupyterHub.
        """
        if self._start_future and not self._start_future.done():
            await self._start_future
        routespec = self.validate_routespec(routespec)

        traefik_config, jupyterhub_config = self._dynamic_config_for_route(
            routespec, target, data
        )

        try:
            async with self.semaphore:
                await self._apply_dynamic_config(traefik_config, jupyterhub_config)
                await self._wait_for_route(routespec)
        except TimeoutError:
            self.log.error(f"Traefik route for {routespec} never appeared.")
            raise

    def _keys_for_route(self, routespec):
        """Return (traefik_keys, jupyterhub_keys)

        keys in dynamic_config and jupyterhub_dynamic_config that correspond to a route.

        These keys may be used in delete_route and get_route.

        each key is a list of strings, representing
        `config[key[0]][key[1]]` etc.

        i.e. ( (["http", "routers", "router_name"], ("routes", "route_name") )
        """
        service_alias = traefik_utils.generate_alias(routespec, "service")
        router_alias = traefik_utils.generate_alias(routespec, "router")
        traefik_keys = (
            ["http", "routers", router_alias],
            ["http", "services", service_alias],
        )
        jupyterhub_keys = (["routes", router_alias],)
        return traefik_keys, jupyterhub_keys

    async def _delete_dynamic_config(self, traefik_keys, jupyterhub_keys):
        """Delete keys from dynamic configuration

        Must be implemented in subclasses
        """
        raise NotImplementedError()

    async def delete_route(self, routespec):
        """Delete a route with a given routespec if it exists."""
        routespec = self.validate_routespec(routespec)
        traefik_keys, jupyterhub_keys = self._keys_for_route(routespec)
        await self._delete_dynamic_config(traefik_keys, jupyterhub_keys)
        self.log.debug("Route %s was deleted.", routespec)

    async def _get_jupyterhub_dynamic_config(self):
        """Get the jupyterhub part of our dynamic config

        This houses serialization of routespecs, etc.
        """
        raise NotImplementedError()

    async def check_routes(self, *args, **kwargs):
        if self._start_future and not self._start_future.done():
            await self._start_future
        return await super().check_routes(*args, **kwargs)

    async def get_all_routes(self):
        """Fetch and return all the routes associated by JupyterHub from the
        proxy.

        **Subclasses must define this method**

        Should return a dictionary of routes, where the keys are
        routespecs and each value is a dict of the form::

          {
            'routespec': the route specification ([host]/path/)
            'target': the target host URL (proto://host) for this route
            'data': the attached data dict for this route (as specified in add_route)
          }
        """
        if self._start_future and not self._start_future.done():
            await self._start_future

        jupyterhub_config = await self._get_jupyterhub_dynamic_config()

        all_routes = {}
        for _key, route in jupyterhub_config.get("routes", {}).items():
            all_routes[route["routespec"]] = {
                "routespec": route["routespec"],
                "data": route.get("data", {}),
                "target": route["target"],
            }
        return all_routes
