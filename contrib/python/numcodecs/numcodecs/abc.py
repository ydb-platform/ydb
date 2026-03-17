"""This module defines the :class:`Codec` base class, a common interface for
all codec classes.

Codec classes must implement :func:`Codec.encode` and :func:`Codec.decode`
methods. Inputs to and outputs from these methods may be any Python object
exporting a contiguous buffer via the new-style Python protocol.

Codec classes must implement a :func:`Codec.get_config` method,
which must return a dictionary holding all configuration parameters
required to enable encoding and decoding of data. The expectation is that
these configuration parameters will be stored or communicated separately
from encoded data, and thus the codecs do not need to store all encoding
parameters within the encoded data. For broad compatibility,
the configuration object must contain only JSON-serializable values. The
configuration object must also contain an 'id' field storing the codec
identifier (see below).

Codec classes must implement a :func:`Codec.from_config` class method,
which will return an instance of the class initialized from a configuration
object.

Finally, codec classes must set a `codec_id` class-level attribute. This
must be a string. Two different codec classes may set the same value for the
`codec_id` attribute if and only if they are fully compatible, meaning that
(1) configuration parameters are the same, and (2) given the same
configuration, one class could correctly decode data encoded by the
other and vice versa.

"""

from abc import ABC, abstractmethod


class Codec(ABC):
    """Codec abstract base class."""

    # override in sub-class
    codec_id: str | None = None
    """Codec identifier."""

    @abstractmethod
    def encode(self, buf):  # pragma: no cover
        """Encode data in `buf`.

        Parameters
        ----------
        buf : buffer-like
            Data to be encoded. May be any object supporting the new-style
            buffer protocol.

        Returns
        -------
        enc : buffer-like
            Encoded data. May be any object supporting the new-style buffer
            protocol.
        """

    @abstractmethod
    def decode(self, buf, out=None):  # pragma: no cover
        """Decode data in `buf`.

        Parameters
        ----------
        buf : buffer-like
            Encoded data. May be any object supporting the new-style buffer
            protocol.
        out : buffer-like, optional
            Writeable buffer to store decoded data. N.B. if provided, this buffer must
            be exactly the right size to store the decoded data.

        Returns
        -------
        dec : buffer-like
            Decoded data. May be any object supporting the new-style
            buffer protocol.
        """

    def get_config(self):
        """Return a dictionary holding configuration parameters for this
        codec. Must include an 'id' field with the codec identifier. All
        values must be compatible with JSON encoding."""

        # override in sub-class if need special encoding of config values

        # setup config object
        config = {'id': self.codec_id}

        # by default, assume all non-private members are configuration
        # parameters - override this in sub-class if not the case
        for k in self.__dict__:
            if not k.startswith('_'):
                config[k] = getattr(self, k)

        return config

    @classmethod
    def from_config(cls, config):
        """Instantiate codec from a configuration object."""
        # N.B., assume at this point the 'id' field has been removed from
        # the config object

        # override in sub-class if need special decoding of config values

        # by default, assume constructor accepts configuration parameters as
        # keyword arguments without any special decoding
        return cls(**config)

    def __eq__(self, other):
        # override in sub-class if need special equality comparison
        try:
            return self.get_config() == other.get_config()
        except AttributeError:
            return False

    def __repr__(self):
        # override in sub-class if need special representation

        # by default, assume all non-private members are configuration
        # parameters and valid keyword arguments to constructor function

        r = f'{type(self).__name__}('
        params = [
            f'{k}={getattr(self, k)!r}' for k in sorted(self.__dict__) if not k.startswith('_')
        ]
        r += ', '.join(params) + ')'
        return r
