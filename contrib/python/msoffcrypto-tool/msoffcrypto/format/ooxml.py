import base64
import io
import logging
import zipfile
from struct import unpack
from xml.dom.minidom import parseString

import olefile

from msoffcrypto import exceptions
from msoffcrypto.format import base
from msoffcrypto.format.common import _parse_encryptionheader, _parse_encryptionverifier
from msoffcrypto.method.ecma376_agile import ECMA376Agile
from msoffcrypto.method.ecma376_standard import ECMA376Standard

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _is_ooxml(file):
    if not zipfile.is_zipfile(file):
        return False
    try:
        zfile = zipfile.ZipFile(file)
        with zfile.open("[Content_Types].xml") as stream:
            xml = parseString(stream.read())
            # Heuristic
            if (
                xml.documentElement.tagName == "Types"
                and xml.documentElement.namespaceURI
                == "http://schemas.openxmlformats.org/package/2006/content-types"
            ):
                return True
            else:
                return False
    except Exception:
        return False


def _parseinfo_standard(ole):
    (headerFlags,) = unpack("<I", ole.read(4))
    (encryptionHeaderSize,) = unpack("<I", ole.read(4))
    block = ole.read(encryptionHeaderSize)
    blob = io.BytesIO(block)
    header = _parse_encryptionheader(blob)
    block = ole.read()
    blob = io.BytesIO(block)
    algIdMap = {
        0x0000660E: "AES-128",
        0x0000660F: "AES-192",
        0x00006610: "AES-256",
    }
    verifier = _parse_encryptionverifier(
        blob, "AES" if header["algId"] & 0xFF00 == 0x6600 else "RC4"
    )  # TODO: Fix
    info = {
        "header": header,
        "verifier": verifier,
    }
    return info


def _parseinfo_agile(ole):
    ole.seek(8)
    xml = parseString(ole.read())
    keyDataSalt = base64.b64decode(
        xml.getElementsByTagName("keyData")[0].getAttribute("saltValue")
    )
    keyDataHashAlgorithm = xml.getElementsByTagName("keyData")[0].getAttribute(
        "hashAlgorithm"
    )
    keyDataBlockSize = int(
        xml.getElementsByTagName("keyData")[0].getAttribute("blockSize")
    )
    encryptedHmacKey = base64.b64decode(
        xml.getElementsByTagName("dataIntegrity")[0].getAttribute("encryptedHmacKey")
    )
    encryptedHmacValue = base64.b64decode(
        xml.getElementsByTagName("dataIntegrity")[0].getAttribute("encryptedHmacValue")
    )
    password_node = xml.getElementsByTagNameNS(
        "http://schemas.microsoft.com/office/2006/keyEncryptor/password", "encryptedKey"
    )[0]
    spinValue = int(password_node.getAttribute("spinCount"))
    encryptedKeyValue = base64.b64decode(
        password_node.getAttribute("encryptedKeyValue")
    )
    encryptedVerifierHashInput = base64.b64decode(
        password_node.getAttribute("encryptedVerifierHashInput")
    )
    encryptedVerifierHashValue = base64.b64decode(
        password_node.getAttribute("encryptedVerifierHashValue")
    )
    passwordSalt = base64.b64decode(password_node.getAttribute("saltValue"))
    passwordHashAlgorithm = password_node.getAttribute("hashAlgorithm")
    passwordKeyBits = int(password_node.getAttribute("keyBits"))
    info = {
        "keyDataSalt": keyDataSalt,
        "keyDataHashAlgorithm": keyDataHashAlgorithm,
        "keyDataBlockSize": keyDataBlockSize,
        "encryptedHmacKey": encryptedHmacKey,
        "encryptedHmacValue": encryptedHmacValue,
        "encryptedVerifierHashInput": encryptedVerifierHashInput,
        "encryptedVerifierHashValue": encryptedVerifierHashValue,
        "encryptedKeyValue": encryptedKeyValue,
        "spinValue": spinValue,
        "passwordSalt": passwordSalt,
        "passwordHashAlgorithm": passwordHashAlgorithm,
        "passwordKeyBits": passwordKeyBits,
    }
    return info


def _parseinfo(ole):
    versionMajor, versionMinor = unpack("<HH", ole.read(4))
    if versionMajor == 4 and versionMinor == 4:  # Agile
        return "agile", _parseinfo_agile(ole)
    elif versionMajor in [2, 3, 4] and versionMinor == 2:  # Standard
        return "standard", _parseinfo_standard(ole)
    elif versionMajor in [3, 4] and versionMinor == 3:  # Extensible
        raise exceptions.DecryptionError(
            "Unsupported EncryptionInfo version (Extensible Encryption)"
        )
    raise exceptions.DecryptionError(
        "Unsupported EncryptionInfo version ({}:{})".format(versionMajor, versionMinor)
    )


class OOXMLFile(base.BaseOfficeFile):
    """Return an OOXML file object.

    Examples:
        >>> with open("tests/inputs/example_password.docx", "rb") as f:
        ...     officefile = OOXMLFile(f)
        ...     officefile.load_key(password="Password1234_", verify_password=True)

        >>> with open("tests/inputs/example_password.docx", "rb") as f:
        ...     officefile = OOXMLFile(f)
        ...     officefile.load_key(password="0000", verify_password=True)
        Traceback (most recent call last):
            ...
        msoffcrypto.exceptions.InvalidKeyError: ...
    """

    def __init__(self, file):
        self.format = "ooxml"

        file.seek(0)  # TODO: Investigate the effect (required for olefile.isOleFile)

        # olefile cannot process non password protected ooxml files.
        # TODO: this code is duplicate of OfficeFile(). Merge?
        if olefile.isOleFile(file):
            ole = olefile.OleFileIO(file)
            self.file = ole

            try:
                with self.file.openstream("EncryptionInfo") as stream:
                    self.type, self.info = _parseinfo(stream)
            except IOError:
                raise exceptions.FileFormatError(
                    "Supposed to be an encrypted OOXML file, but no EncryptionInfo stream found"
                )

            logger.debug("OOXMLFile.type: {}".format(self.type))

            self.secret_key = None

            if self.type == "agile":
                # TODO: Support aliases?
                self.keyTypes = ("password", "private_key", "secret_key")
            elif self.type == "standard":
                self.keyTypes = ("password", "secret_key")
            elif self.type == "extensible":
                pass
        elif _is_ooxml(file):
            self.type = "plain"
            self.file = file
        else:
            raise exceptions.FileFormatError("Unsupported file format")

    def load_key(
        self, password=None, private_key=None, secret_key=None, verify_password=False
    ):
        """
        >>> with open("tests/outputs/ecma376standard_password_plain.docx", "rb") as f:
        ...     officefile = OOXMLFile(f)
        ...     officefile.load_key("1234")
        """
        if password:
            if self.type == "agile":
                self.secret_key = ECMA376Agile.makekey_from_password(
                    password,
                    self.info["passwordSalt"],
                    self.info["passwordHashAlgorithm"],
                    self.info["encryptedKeyValue"],
                    self.info["spinValue"],
                    self.info["passwordKeyBits"],
                )
                if verify_password:
                    verified = ECMA376Agile.verify_password(
                        password,
                        self.info["passwordSalt"],
                        self.info["passwordHashAlgorithm"],
                        self.info["encryptedVerifierHashInput"],
                        self.info["encryptedVerifierHashValue"],
                        self.info["spinValue"],
                        self.info["passwordKeyBits"],
                    )
                    if not verified:
                        raise exceptions.InvalidKeyError("Key verification failed")
            elif self.type == "standard":
                self.secret_key = ECMA376Standard.makekey_from_password(
                    password,
                    self.info["header"]["algId"],
                    self.info["header"]["algIdHash"],
                    self.info["header"]["providerType"],
                    self.info["header"]["keySize"],
                    self.info["verifier"]["saltSize"],
                    self.info["verifier"]["salt"],
                )
                if verify_password:
                    verified = ECMA376Standard.verifykey(
                        self.secret_key,
                        self.info["verifier"]["encryptedVerifier"],
                        self.info["verifier"]["encryptedVerifierHash"],
                    )
                    if not verified:
                        raise exceptions.InvalidKeyError("Key verification failed")
            elif self.type == "extensible":
                pass
            elif self.type == "plain":
                pass
        elif private_key:
            if self.type == "agile":
                self.secret_key = ECMA376Agile.makekey_from_privkey(
                    private_key, self.info["encryptedKeyValue"]
                )
            else:
                raise exceptions.DecryptionError(
                    "Unsupported key type for the encryption method"
                )
        elif secret_key:
            self.secret_key = secret_key
        else:
            raise exceptions.DecryptionError("No key specified")

    def decrypt(self, outfile, verify_integrity=False):
        """
        >>> from msoffcrypto import exceptions
        >>> from io import BytesIO; outfile = BytesIO()
        >>> with open("tests/outputs/ecma376standard_password_plain.docx", "rb") as f:
        ...     officefile = OOXMLFile(f)
        ...     officefile.load_key("1234")
        ...     officefile.decrypt(outfile)
        Traceback (most recent call last):
        msoffcrypto.exceptions.DecryptionError: Document is not encrypted
        """
        if self.type == "agile":
            with self.file.openstream("EncryptedPackage") as stream:
                if verify_integrity:
                    verified = ECMA376Agile.verify_integrity(
                        self.secret_key,
                        self.info["keyDataSalt"],
                        self.info["keyDataHashAlgorithm"],
                        self.info["keyDataBlockSize"],
                        self.info["encryptedHmacKey"],
                        self.info["encryptedHmacValue"],
                        stream,
                    )
                    if not verified:
                        raise exceptions.InvalidKeyError(
                            "Payload integrity verification failed"
                        )

                obuf = ECMA376Agile.decrypt(
                    self.secret_key,
                    self.info["keyDataSalt"],
                    self.info["keyDataHashAlgorithm"],
                    stream,
                )
            outfile.write(obuf)
        elif self.type == "standard":
            with self.file.openstream("EncryptedPackage") as stream:
                obuf = ECMA376Standard.decrypt(self.secret_key, stream)
            outfile.write(obuf)
        elif self.type == "plain":
            raise exceptions.DecryptionError("Document is not encrypted")
        else:
            raise exceptions.DecryptionError("Unsupported encryption method")

        # If the file is successfully decrypted, there must be a valid OOXML file, i.e. a valid zip file
        if not zipfile.is_zipfile(io.BytesIO(obuf)):
            raise exceptions.InvalidKeyError(
                "The file could not be decrypted with this password"
            )

    def encrypt(self, password, outfile):
        """
        >>> from msoffcrypto.format.ooxml import OOXMLFile
        >>> from io import BytesIO; outfile = BytesIO()
        >>> with open("tests/outputs/example.docx", "rb") as f:
        ...     officefile = OOXMLFile(f)
        ...     officefile.encrypt("1234", outfile)
        """
        if self.is_encrypted():
            raise exceptions.EncryptionError("File is already encrypted")

        self.file.seek(0)

        buf = ECMA376Agile.encrypt(password, self.file)

        if not olefile.isOleFile(buf):
            raise exceptions.EncryptionError("Unable to encrypt this file")

        outfile.write(buf)

    def is_encrypted(self):
        """
        >>> with open("tests/inputs/example_password.docx", "rb") as f:
        ...     officefile = OOXMLFile(f)
        ...     officefile.is_encrypted()
        True
        >>> with open("tests/outputs/ecma376standard_password_plain.docx", "rb") as f:
        ...     officefile = OOXMLFile(f)
        ...     officefile.is_encrypted()
        False
        """
        # Heuristic
        if self.type == "plain":
            return False
        elif isinstance(self.file, olefile.OleFileIO):
            return True
        else:
            return False
