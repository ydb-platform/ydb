# An example cipher suite for secure on-board communication. This is
# in no way cryptographically secure. DO NOT USE IN THE REAL WORLD!


from ..database import Message


class SnakeOilAuthenticator:
    """A snake oil authenticator for secure on-board communication

    The sole purpose of this class is to demonstrate how SecOC can be
    implemented using cantools. These algorithms are in no way
    cryptographically secure! DO NOT USE THEM IN THE REAL WORLD!
    """
    def __init__(self,
                 secret: bytes | str) -> None:
        if isinstance(secret, str):
            self._secret = secret.encode()
        else:
            self._secret = bytes(secret)

    def __call__(self,
                 dbmsg: Message,
                 auth_data: bytearray,
                 freshness_value: int) \
                -> bytearray:

        v0 = freshness_value%253

        # XOR the secret and the data which we ought to authenticate
        result = bytearray([v0]*5)
        for i in range(len(auth_data)):
            result[i % len(result)] ^= auth_data[i]
            result[i % len(result)] ^= self._secret[i%len(self._secret)]

        return result
