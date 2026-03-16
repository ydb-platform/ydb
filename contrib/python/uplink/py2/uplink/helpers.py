# Standard library imports
import collections

# Local imports
from uplink import interfaces, utils
from uplink.clients import io


def get_api_definitions(service):
    """
    Returns all attributes with type
    `uplink.interfaces.RequestDefinitionBuilder` defined on the given
    class.

    Note:
        All attributes are considered, not only defined directly on the class.

    Args:
        service: A class object.
    """
    # In Python 3.3, `inspect.getmembers` doesn't respect the descriptor
    # protocol when the first argument is a class. In other words, the
    # function includes any descriptors bound to `service` as is rather
    # than calling the descriptor's __get__ method. This is seemingly
    # fixed in Python 2.7 and 3.4+ (TODO: locate corresponding bug
    # report in Python issue tracker). Directly invoking `getattr` to
    # force Python's attribute lookup protocol is a decent workaround to
    # ensure parity:
    class_attributes = ((k, getattr(service, k)) for k in dir(service))

    is_definition = interfaces.RequestDefinitionBuilder.__instancecheck__
    return [(k, v) for k, v in class_attributes if is_definition(v)]


def set_api_definition(service, name, definition):
    setattr(service, name, definition)


class RequestBuilder(object):
    def __init__(self, client, converter_registry, base_url):
        self._method = None
        self._relative_url_template = utils.URIBuilder("")
        self._return_type = None
        self._client = client
        self._base_url = base_url

        # TODO: Pass this in as constructor parameter
        # TODO: Delegate instantiations to uplink.HTTPClientAdapter
        self._info = collections.defaultdict(dict)
        self._context = {}

        self._converter_registry = converter_registry
        self._transaction_hooks = []
        self._request_templates = []

    @property
    def client(self):
        return self._client

    @property
    def method(self):
        return self._method

    @method.setter
    def method(self, method):
        self._method = method

    @property
    def base_url(self):
        return self._base_url

    def set_url_variable(self, variables):
        self._relative_url_template.set_variable(variables)

    @property
    def relative_url(self):
        return self._relative_url_template.build()

    @relative_url.setter
    def relative_url(self, url):
        self._relative_url_template = utils.URIBuilder(url)

    @property
    def info(self):
        return self._info

    @property
    def context(self):
        return self._context

    @property
    def transaction_hooks(self):
        return iter(self._transaction_hooks)

    def get_converter(self, converter_key, *args, **kwargs):
        return self._converter_registry[converter_key](*args, **kwargs)

    @property
    def return_type(self):
        return self._return_type

    @return_type.setter
    def return_type(self, return_type):
        self._return_type = return_type

    @property
    def request_template(self):
        return io.CompositeRequestTemplate(self._request_templates)

    @property
    def url(self):
        return utils.urlparse.urljoin(self.base_url, self.relative_url)

    def add_transaction_hook(self, hook):
        self._transaction_hooks.append(hook)

    def add_request_template(self, template):
        self._request_templates.append(template)
