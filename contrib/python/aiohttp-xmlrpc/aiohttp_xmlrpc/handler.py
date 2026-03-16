import asyncio
import inspect
import logging
from abc import ABCMeta
from types import MappingProxyType

from aiohttp.web import HTTPBadRequest, HTTPError, Response, View
from lxml import etree

from . import exceptions
from .common import awaitable, py2xml, schema, xml2py


log = logging.getLogger(__name__)


# noinspection PyUnresolvedReferences
class XMLRPCViewMeta(ABCMeta):
    def __new__(cls, clsname, superclasses, attributedict):
        mapping_key = "__method_arg_mapping__"
        allowed_key = "__allowed_methods__"
        attributedict[mapping_key] = dict()
        attributedict[allowed_key] = dict()

        for superclass in superclasses:
            attributedict[mapping_key].update(
                getattr(superclass, mapping_key, {}),
            )

            attributedict[allowed_key].update(
                getattr(superclass, allowed_key, {}),
            )

        instance = super(XMLRPCViewMeta, cls).__new__(
            cls, clsname, superclasses, attributedict,
        )

        argmapping = getattr(instance, mapping_key)
        allowed_methods = getattr(instance, allowed_key)

        for key in attributedict.keys():
            if not key.startswith(instance.METHOD_PREFIX):
                continue

            # Get the value of the corresponding function
            value = getattr(instance, key)

            method_name = getattr(value, "__xmlrpc_name__", None)
            if method_name is None:
                method_name = key.replace(instance.METHOD_PREFIX, "", 1)

            allowed_methods[method_name] = key

            # Add the arg mapping in all cases
            argmapping[method_name] = inspect.getfullargspec(value)

        setattr(
            instance,
            mapping_key,
            MappingProxyType(argmapping),
        )

        setattr(
            instance,
            allowed_key,
            MappingProxyType(allowed_methods),
        )

        return instance


class XMLRPCView(View, metaclass=XMLRPCViewMeta):
    METHOD_PREFIX = "rpc_"
    DEBUG = False
    THREAD_POOL_EXECUTOR = None

    async def post(self, *args, **kwargs):
        try:
            xml_response = await self._handle()
        except HTTPError:
            raise
        except Exception as e:
            xml_response = self._format_error(e)
            log.exception(e)

        return self._make_response(xml_response)

    @classmethod
    def _make_response(cls, xml_response, status=200, reason=None):
        response = Response(status=status, reason=reason)
        response.headers["Content-Type"] = "text/xml; charset=utf-8"

        xml_data = cls._build_xml(xml_response)

        log.debug("Sending response:\n%s", xml_data)

        response.body = xml_data
        return response

    async def _parse_body(self, body):
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(
                self.THREAD_POOL_EXECUTOR,
                self._parse_xml,
                body,
            )
        except etree.DocumentInvalid:
            raise HTTPBadRequest

    # noinspection PyUnresolvedReferences
    def _lookup_method(self, method_name):
        if method_name not in self.__allowed_methods__:
            raise exceptions.ApplicationError(
                "Method %r not found" % method_name,
            )

        return awaitable(getattr(self, self.__allowed_methods__[method_name]))

    def _check_request(self):
        if "xml" not in self.request.headers.get("Content-Type", ""):
            raise HTTPBadRequest

    async def _handle(self):
        self._check_request()

        body = await self.request.read()
        xml_request = await self._parse_body(body)

        method_name = xml_request.xpath("//methodName[1]")[0].text
        method = self._lookup_method(method_name)

        log.info(
            "RPC Call: %s => %s.%s.%s",
            method_name,
            method.__module__,
            method.__class__.__name__,
            method.__name__,
        )

        args = list(
            map(
                xml2py,
                xml_request.xpath("//params/param/value"),
            ),
        )

        kwargs = {}
        argspec = self.__method_arg_mapping__[method_name]
        if argspec.varkw or argspec.kwonlyargs and isinstance(args[-1], dict):
            kwargs = args.pop(-1)

        result = await method(*args, **kwargs)
        return self._format_success(result)

    @staticmethod
    def _format_success(result):
        xml_response = etree.Element("methodResponse")
        xml_params = etree.Element("params")
        xml_param = etree.Element("param")
        xml_value = etree.Element("value")

        xml_value.append(py2xml(result))
        xml_param.append(xml_value)
        xml_params.append(xml_param)
        xml_response.append(xml_params)
        return xml_response

    @staticmethod
    def _format_error(exception: Exception):
        xml_response = etree.Element("methodResponse")
        xml_fault = etree.Element("fault")
        xml_value = etree.Element("value")

        xml_value.append(py2xml(exception))
        xml_fault.append(xml_value)
        xml_response.append(xml_fault)
        return xml_response

    @staticmethod
    def _parse_xml(xml_string):
        parser = etree.XMLParser(resolve_entities=False)
        root = etree.fromstring(xml_string, parser)
        schema.assertValid(root)
        return root

    @classmethod
    def _build_xml(cls, tree):
        return etree.tostring(
            tree,
            xml_declaration=True,
            encoding="utf-8",
            pretty_print=cls.DEBUG,
        )


def rename(new_name):
    def decorator(func):
        func.__xmlrpc_name__ = new_name
        return func
    return decorator
