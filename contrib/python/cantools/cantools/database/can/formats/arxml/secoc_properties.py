

class AutosarSecOCProperties:
    """This class collects all attributes that are required to implement the
    AUTOSAR secure on-board communication (SecOC) specification.

    Be aware that the AUTOSAR SecOC specification does not cover the
    required cryptographic primitives themselves, just the
    "scaffolding" around them...
    """
    def __init__(self,
                 auth_algorithm_name: str | None,
                 freshness_algorithm_name: str | None,
                 payload_length: int | None,
                 data_id: int | None,
                 auth_tx_bit_length: int | None,
                 freshness_bit_length: int | None,
                 freshness_tx_bit_length: int | None,
                 ):

        self._auth_algorithm_name = auth_algorithm_name
        self._freshness_algorithm_name = freshness_algorithm_name

        self._payload_length = payload_length
        self._data_id = data_id

        self._freshness_bit_length = freshness_bit_length
        self._freshness_tx_bit_length = freshness_tx_bit_length
        self._auth_tx_bit_length = auth_tx_bit_length

    @property
    def freshness_algorithm_name(self) -> str | None:
        """The name of the algorithm used for verifying the freshness of a
        message.

        This can be used to prevent replay attacks. Note that the
        algorithms themselves are manufacturer-specific, i.e., AUTOSAR
        does not define *any* freshness schemes.
        """
        return self._freshness_algorithm_name

    @property
    def auth_algorithm_name(self) -> str | None:
        """The name of the algorithm used for authentication

        Note that the algorithms themselves are manufacturer-specific,
        i.e., AUTOSAR does not define *any* cryptographic schemes.
        """
        return self._auth_algorithm_name

    @property
    def payload_length(self) -> int | None:
        """Returns the number of bytes covered by the payload of the secured
        message

        (The full message length is the length of the payload plus the
        size of the security trailer.)
        """
        return self._payload_length

    @property
    def data_id(self) -> int | None:
        """The data ID required for authentication.

        Be aware that this is a different data ID than that required
        for End-To-End protection.
        """
        return self._data_id

    @property
    def freshness_bit_length(self) -> int | None:
        """The number of bits of the full freshness counter.
        """
        return self._freshness_bit_length

    @property
    def freshness_tx_bit_length(self) -> int | None:
        """The number of least-significant bits of the freshness counter that
        is send as part of the secured frame.

        This number is at most as large as the number of bits of
        freshness counter objects.

        """
        return self._freshness_tx_bit_length

    @property
    def auth_tx_bit_length(self) -> int | None:
        """The number of most significant bits of the authenticator object
        send as part of the secured frame

        This is at most the length of the authenicator.
        """
        return self._auth_tx_bit_length
