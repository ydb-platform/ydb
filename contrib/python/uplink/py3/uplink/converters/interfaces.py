class Converter:
    def convert(self, value):
        raise NotImplementedError

    def __call__(self, *args, **kwargs):
        return self.convert(*args, **kwargs)

    def set_chain(self, chain):
        pass


class Factory:
    """
    An adapter that handles serialization of HTTP request properties
    (e.g., headers, query parameters, request body) and deserialization
    of HTTP response bodies.

    Each concrete implementation of this abstract class typically
    encapsulates a specific encoding/decoding strategy
    (e.g., Protocol Buffers or JSON).

    !!! note
        Overriding all inherited methods is unnecessary; the default
        implementation is to return `None`, which tells the
        converter layer to move on to the next factory. Hence,
        you only should implement the methods you intend to support.
    """

    def create_response_body_converter(self, cls, request_definition):
        """
        Returns a callable that can convert a response body into the
        specified `cls`.

        The returned callable should expect a single positional
        argument: the response body.

        If this factory can't produce such a callable, it should return
        `None`, so another factory can have a chance to handle
        the type.

        Args:
            cls (type): The target class for conversion.
            request_definition: Metadata for the outgoing request.
                This object exposes two properties: the
                `method_annotations` (e.g., `~uplink.headers`) and
                `argument_annotations` (e.g., `~uplink.Body`) bound
                to the underlying consumer method
        """

    def create_request_body_converter(self, cls, request_definition):
        """
        Returns a callable that can convert `cls` into an acceptable
        request body.

        The returned callable should expect a single positional
        argument: an instance of given type, `cls`.

        If this factory can't produce such a callable, it should return
        `None`, so another factory can have a chance to handle
        the type.

        Args:
            cls (type): The target class for conversion.
            request_definition: Metadata for the outgoing request.
                This object exposes two properties: the
                `method_annotations` (e.g., `~uplink.headers`) and
                `argument_annotations` (e.g., `~uplink.Body`) bound
                to the underlying consumer method
        """

    def create_string_converter(self, cls, request_definition):
        """
        Returns a callable that can convert `cls` into a
        `str`.

        The returned callable should expect a single positional
        argument: an instance of given type, `cls`.

        If this factory can't produce such a callable, it should return
        `None`, so another factory can have a chance to handle
        the type.

        Args:
            cls (type): The target class for conversion.
            request_definition: Metadata for the outgoing request.
                This object exposes two properties: the
                `method_annotations` (e.g., `~uplink.headers`) and
                `argument_annotations` (e.g., `~uplink.Body`) bound
                to the underlying consumer method
        """


class ConverterFactory(Factory):
    # TODO: Remove this in v1.0.0 -- use Factory instead.

    def create_response_body_converter(self, cls, request_definition):
        return self.make_response_body_converter(
            cls,
            request_definition.argument_annotations,
            request_definition.method_annotations,
        )

    def create_request_body_converter(self, cls, request_definition):
        return self.make_request_body_converter(
            cls,
            request_definition.argument_annotations,
            request_definition.method_annotations,
        )

    def create_string_converter(self, cls, request_definition):
        return self.make_string_converter(
            cls,
            request_definition.argument_annotations,
            request_definition.method_annotations,
        )

    def make_response_body_converter(
        self, type, argument_annotations, method_annotations
    ):
        pass

    def make_request_body_converter(
        self, type, argument_annotations, method_annotations
    ):
        pass

    def make_string_converter(self, type, argument_annotations, method_annotations):
        pass
