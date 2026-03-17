import copy
import itertools
import logging

logger = logging.getLogger(__name__)


class OperationProxy(object):
    def __init__(self, service_proxy, operation_name):
        self._proxy = service_proxy
        self._op_name = operation_name

    @property
    def __doc__(self):
        return str(self._proxy._binding._operations[self._op_name])

    def __call__(self, *args, **kwargs):
        """Call the operation with the given args and kwargs.

        :rtype: zeep.xsd.CompoundValue

        """

        # Merge the default _soapheaders with the passed _soapheaders
        if self._proxy._client._default_soapheaders:
            op_soapheaders = kwargs.get("_soapheaders")
            if op_soapheaders:
                soapheaders = copy.deepcopy(self._proxy._client._default_soapheaders)
                if type(op_soapheaders) != type(soapheaders):
                    raise ValueError("Incompatible soapheaders definition")

                if isinstance(soapheaders, list):
                    soapheaders.extend(op_soapheaders)
                else:
                    soapheaders.update(op_soapheaders)
            else:
                soapheaders = self._proxy._client._default_soapheaders
            kwargs["_soapheaders"] = soapheaders

        return self._proxy._binding.send(
            self._proxy._client,
            self._proxy._binding_options,
            self._op_name,
            args,
            kwargs,
        )


class ServiceProxy(object):
    def __init__(self, client, binding, **binding_options):
        self._client = client
        self._binding_options = binding_options
        self._binding = binding
        self._operations = {
            name: OperationProxy(self, name) for name in self._binding.all()
        }

    def __getattr__(self, key):
        """Return the OperationProxy for the given key.

        :rtype: OperationProxy()

        """
        return self[key]

    def __getitem__(self, key):
        """Return the OperationProxy for the given key.

        :rtype: OperationProxy()

        """
        try:
            return self._operations[key]
        except KeyError:
            raise AttributeError("Service has no operation %r" % key)

    def __iter__(self):
        """ Return iterator over the services and their callables. """
        return iter(self._operations.items())

    def __dir__(self):
        """ Return the names of the operations. """
        return list(itertools.chain(dir(super(ServiceProxy, self)), self._operations))
