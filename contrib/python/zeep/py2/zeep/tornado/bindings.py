from tornado import gen

from zeep.wsdl import bindings

__all__ = ["AsyncSoap11Binding", "AsyncSoap12Binding"]


class AsyncSoapBinding(object):
    @gen.coroutine
    def send(self, client, options, operation, args, kwargs):
        envelope, http_headers = self._create(
            operation, args, kwargs, client=client, options=options
        )

        response = yield client.transport.post_xml(
            options["address"], envelope, http_headers
        )

        operation_obj = self.get(operation)
        raise gen.Return(self.process_reply(client, operation_obj, response))


class AsyncSoap11Binding(AsyncSoapBinding, bindings.Soap11Binding):
    pass


class AsyncSoap12Binding(AsyncSoapBinding, bindings.Soap12Binding):
    pass
