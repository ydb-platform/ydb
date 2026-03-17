import io
import logging
from hashlib import md5
from struct import pack

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _makekey(password, salt, block):
    r"""
    Return a intermediate key.

        >>> password = 'password1'
        >>> salt = b'\xe8w,\x1d\x91\xc5j7\x96Ga\xb2\x80\x182\x17'
        >>> block = 0
        >>> expected = b' \xbf2\xdd\xf5@\x85\x8cQ7D\xaf\x0f$\xe0<'
        >>> _makekey(password, salt, block) == expected
        True
    """
    # https://msdn.microsoft.com/en-us/library/dd920360(v=office.12).aspx
    password = password.encode("UTF-16LE")
    h0 = md5(password).digest()
    truncatedHash = h0[:5]
    intermediateBuffer = (truncatedHash + salt) * 16
    h1 = md5(intermediateBuffer).digest()
    truncatedHash = h1[:5]
    blockbytes = pack("<I", block)
    hfinal = md5(truncatedHash + blockbytes).digest()
    key = hfinal[: 128 // 8]
    return key


class DocumentXOR:
    def __init__(self):
        pass

    pad_array = [
        0xBB,
        0xFF,
        0xFF,
        0xBA,
        0xFF,
        0xFF,
        0xB9,
        0x80,
        0x00,
        0xBE,
        0x0F,
        0x00,
        0xBF,
        0x0F,
        0x00,
    ]
    initial_code = [
        0xE1F0,
        0x1D0F,
        0xCC9C,
        0x84C0,
        0x110C,
        0x0E10,
        0xF1CE,
        0x313E,
        0x1872,
        0xE139,
        0xD40F,
        0x84F9,
        0x280C,
        0xA96A,
        0x4EC3,
    ]

    xor_matrix = [
        0xAEFC,
        0x4DD9,
        0x9BB2,
        0x2745,
        0x4E8A,
        0x9D14,
        0x2A09,
        0x7B61,
        0xF6C2,
        0xFDA5,
        0xEB6B,
        0xC6F7,
        0x9DCF,
        0x2BBF,
        0x4563,
        0x8AC6,
        0x05AD,
        0x0B5A,
        0x16B4,
        0x2D68,
        0x5AD0,
        0x0375,
        0x06EA,
        0x0DD4,
        0x1BA8,
        0x3750,
        0x6EA0,
        0xDD40,
        0xD849,
        0xA0B3,
        0x5147,
        0xA28E,
        0x553D,
        0xAA7A,
        0x44D5,
        0x6F45,
        0xDE8A,
        0xAD35,
        0x4A4B,
        0x9496,
        0x390D,
        0x721A,
        0xEB23,
        0xC667,
        0x9CEF,
        0x29FF,
        0x53FE,
        0xA7FC,
        0x5FD9,
        0x47D3,
        0x8FA6,
        0x0F6D,
        0x1EDA,
        0x3DB4,
        0x7B68,
        0xF6D0,
        0xB861,
        0x60E3,
        0xC1C6,
        0x93AD,
        0x377B,
        0x6EF6,
        0xDDEC,
        0x45A0,
        0x8B40,
        0x06A1,
        0x0D42,
        0x1A84,
        0x3508,
        0x6A10,
        0xAA51,
        0x4483,
        0x8906,
        0x022D,
        0x045A,
        0x08B4,
        0x1168,
        0x76B4,
        0xED68,
        0xCAF1,
        0x85C3,
        0x1BA7,
        0x374E,
        0x6E9C,
        0x3730,
        0x6E60,
        0xDCC0,
        0xA9A1,
        0x4363,
        0x86C6,
        0x1DAD,
        0x3331,
        0x6662,
        0xCCC4,
        0x89A9,
        0x0373,
        0x06E6,
        0x0DCC,
        0x1021,
        0x2042,
        0x4084,
        0x8108,
        0x1231,
        0x2462,
        0x48C4,
    ]

    @staticmethod
    def verifypw(password, verificationBytes):
        r"""
        Return True if the given password is valid.

            >>> from struct import unpack
            >>> password = 'VelvetSweatshop'
            >>> (key,) = unpack('<H', b'\x0A\x9A')  # 0x9a0a
            >>> DocumentXOR.verifypw(password, key)
            True
        """
        # https://interoperability.blob.core.windows.net/files/MS-OFFCRYPTO/%5bMS-OFFCRYPTO%5d.pdf
        verifier = 0
        password_array = []
        password_array.append(len(password))
        password_array.extend([ord(ch) for ch in password])
        password_array.reverse()
        for password_byte in password_array:
            if verifier & 0x4000 == 0x0000:
                intermidiate_1 = 0
            else:
                intermidiate_1 = 1

            intermidiate_2 = verifier * 2
            intermidiate_2 = (
                intermidiate_2 & 0x7FFF
            )  # SET most significant bit of Intermediate2 TO 0

            intermidiate_3 = intermidiate_1 ^ intermidiate_2

            verifier = intermidiate_3 ^ password_byte

        return True if (verifier ^ 0xCE4B) == verificationBytes else False

    @staticmethod
    def xor_ror(byte1, byte2):
        return DocumentXOR.ror(byte1 ^ byte2, 1, 8)

    @staticmethod
    def create_xor_key_method1(password):
        xor_key = DocumentXOR.initial_code[len(password) - 1]
        current_element = 0x00000068

        data = [ord(ch) for ch in reversed(password)]
        for ch in data:
            for i in range(7):
                if ch & 0x40 != 0:
                    xor_key = (
                        xor_key ^ DocumentXOR.xor_matrix[current_element]
                    ) % 65536
                ch = (ch << 1) % 256
                current_element -= 1

        return xor_key

    @staticmethod
    def create_xor_array_method1(password):
        xor_key = DocumentXOR.create_xor_key_method1(password)

        index = len(password)

        obfuscation_array = [
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
        ]

        if index % 2 == 1:
            temp = (
                xor_key & 0xFF00
            ) >> 8  # SET Temp TO most significant byte of XorKey
            obfuscation_array[index] = DocumentXOR.xor_ror(
                DocumentXOR.pad_array[0], temp
            )

            index -= 1
            temp = xor_key & 0x00FF
            password_last_char = ord(password[-1])
            obfuscation_array[index] = DocumentXOR.xor_ror(password_last_char, temp)

        while index > 0:
            index -= 1
            temp = (xor_key & 0xFF00) >> 8
            obfuscation_array[index] = DocumentXOR.xor_ror(ord(password[index]), temp)

            index -= 1
            temp = xor_key & 0x00FF
            obfuscation_array[index] = DocumentXOR.xor_ror(ord(password[index]), temp)

        index = 15
        pad_index = 15 - len(password)

        while pad_index > 0:
            temp = (xor_key & 0xFF00) >> 8
            obfuscation_array[index] = DocumentXOR.xor_ror(
                DocumentXOR.pad_array[pad_index], temp
            )

            index -= 1
            pad_index -= 1

            temp = xor_key & 0x00FF
            obfuscation_array[index] = DocumentXOR.xor_ror(
                DocumentXOR.pad_array[pad_index], temp
            )

            index -= 1
            pad_index -= 1

        return obfuscation_array

    @staticmethod
    def ror(n, rotations, width):
        return (2**width - 1) & (n >> rotations | n << (width - rotations))

    @staticmethod
    def rol(n, rotations, width):
        return (2**width - 1) & (n << rotations | n >> (width - rotations))

    @staticmethod
    def decrypt(password, ibuf, plaintext, records, base):
        r"""
        Return decrypted data (DecryptData_Method1)
        """
        obuf = io.BytesIO()

        xor_array = DocumentXOR.create_xor_array_method1(password)

        data_index = 0
        record_index = 0
        while data_index < len(plaintext):
            count = 1
            if plaintext[data_index] == -1 or plaintext[data_index] == -2:
                for j in range(data_index + 1, len(plaintext)):
                    if plaintext[j] >= 0:
                        break
                    count += 1

                if plaintext[data_index] == -2:
                    xor_array_index = (data_index + count + 4) % 16
                else:
                    xor_array_index = (data_index + count) % 16
                temp_res = 0
                for item in range(count):
                    data_byte = ibuf.read(1)
                    temp_res = data_byte[0] ^ xor_array[xor_array_index]
                    temp_res = DocumentXOR.ror(temp_res, 5, 8)
                    obuf.write(temp_res.to_bytes(1, "little"))

                    xor_array_index += 1

                    xor_array_index = xor_array_index % 16
                record_index += 1

            else:
                obuf.write(ibuf.read(1))

            data_index += count

        obuf.seek(0)
        return obuf
