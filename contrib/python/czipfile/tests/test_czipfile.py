import base64
import io

import czipfile


ENCRYPTED = """
UEsDBAoACQAAAJaVS09Dz/WrFgAAAAoAAAAEABwAdGVzdFVUCQADbKOgXWyjoF11eAsAAQQPWwAA
BA1PAAApI1h5IkRfk5A9S9h9O5OanyWrp2hhUEsHCEPP9asWAAAACgAAAFBLAQIeAwoACQAAAJaV
S09Dz/WrFgAAAAoAAAAEABgAAAAAAAEAAAC0gQAAAAB0ZXN0VVQFAANso6BddXgLAAEED1sAAAQN
TwAAUEsFBgAAAAABAAEASgAAAGQAAAAAAA==
""".strip()


def test_czipfile():
    z = czipfile.ZipFile(io.BytesIO(base64.b64decode(ENCRYPTED)))
    with z.open('test', 'r', b'password') as f:
        assert f.read() == b'test-data\n'
