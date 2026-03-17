# -*- coding: utf-8 -*-
NOT_IMPLEMENTED_ERROR_MSG = ('This method must be implemented by classes'
                             ' inheriting from BaseSerializer.')


class BaseSerializer(object):
    """
    Base Serializer class that provides an interface for other serializers.

    Usage:

    .. code-block:: python

        from betamax import Betamax, BaseSerializer


        class MySerializer(BaseSerializer):
            name = 'my'

            @staticmethod
            def generate_cassette_name(cassette_library_dir, cassette_name):
                # Generate a string that will give the relative path of a
                # cassette

            def serialize(self, cassette_data):
                # Take a dictionary and convert it to whatever

            def deserialize(self, cassette_data):
                # Uses a cassette file to return a dictionary with the
                # cassette information

        Betamax.register_serializer(MySerializer)

    The last line is absolutely necessary.

    """

    name = None
    stored_as_binary = False

    @staticmethod
    def generate_cassette_name(cassette_library_dir, cassette_name):
        raise NotImplementedError(NOT_IMPLEMENTED_ERROR_MSG)

    def __init__(self):
        if not self.name:
            raise ValueError("Serializer's name attribute must be a string"
                             " value, not None.")

        self.on_init()

    def on_init(self):
        """Method to implement if you wish something to happen in ``__init__``.

        The return value is not checked and this is called at the end of
        ``__init__``. It is meant to provide the matcher author a way to
        perform things during initialization of the instance that would
        otherwise require them to override ``BaseSerializer.__init__``.
        """
        return None

    def serialize(self, cassette_data):
        """A method that must be implemented by the Serializer author.

        :param dict cassette_data: A dictionary with two keys:
            ``http_interactions``, ``recorded_with``.
        :returns: Serialized data as a string.
        """
        raise NotImplementedError(NOT_IMPLEMENTED_ERROR_MSG)

    def deserialize(self, cassette_data):
        """A method that must be implemented by the Serializer author.

        The return value is extremely important. If it is not empty, the
        dictionary returned must have the following structure::

            {
                'http_interactions': [{
                    # Interaction
                },
                {
                    # Interaction
                }],
                'recorded_with': 'name of recorder'
            }

        :params str cassette_data: The data serialized as a string which needs
            to be deserialized.
        :returns: dictionary
        """
        raise NotImplementedError(NOT_IMPLEMENTED_ERROR_MSG)
