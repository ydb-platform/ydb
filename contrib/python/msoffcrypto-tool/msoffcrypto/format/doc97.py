import io
import logging
import shutil
import tempfile
from collections import namedtuple
from struct import pack, unpack, unpack_from

import olefile

from msoffcrypto import exceptions
from msoffcrypto.format import base
from msoffcrypto.format.common import _parse_header_RC4CryptoAPI
from msoffcrypto.method.rc4 import DocumentRC4
from msoffcrypto.method.rc4_cryptoapi import DocumentRC4CryptoAPI

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

FibBase = namedtuple(
    "FibBase",
    [
        "wIdent",
        "nFib",
        "unused",
        "lid",
        "pnNext",
        "fDot",
        "fGlsy",
        "fComplex",
        "fHasPic",
        "cQuickSaves",
        "fEncrypted",
        "fWhichTblStm",
        "fReadOnlyRecommended",
        "fWriteReservation",
        "fExtChar",
        "fLoadOverride",
        "fFarEast",
        "nFibBack",
        "fObfuscation",
        "IKey",
        "envr",
        "fMac",
        "fEmptySpecial",
        "fLoadOverridePage",
        "reserved1",
        "reserved2",
        "fSpare0",
        "reserved3",
        "reserved4",
        "reserved5",
        "reserved6",
    ],
)


def _parseFibBase(blob):
    r"""
    Pasrse FibBase binary blob.

        >>> blob = io.BytesIO(b'\xec\xa5\xc1\x00G\x00\t\x04\x00\x00\x00\x13\xbf\x004\x00\
        ... \x00\x00\x00\x10\x00\x00\x00\x00\x00\x04\x00\x00\x16\x04\x00\x00')
        >>> fibbase = _parseFibBase(blob)
        >>> hex(fibbase.wIdent)
        '0xa5ec'
        >>> hex(fibbase.nFib)
        '0xc1'
        >>> hex(fibbase.fExtChar)
        '0x1'
    """
    getBit = lambda bits, i: (bits & (1 << i)) >> i
    getBitSlice = lambda bits, i, w: (bits & (2**w - 1 << i)) >> i

    # https://msdn.microsoft.com/en-us/library/dd944620(v=office.12).aspx
    (buf,) = unpack_from("<H", blob.read(2))
    wIdent = buf

    (buf,) = unpack_from("<H", blob.read(2))
    nFib = buf

    (buf,) = unpack_from("<H", blob.read(2))
    unused = buf

    (buf,) = unpack_from("<H", blob.read(2))
    lid = buf

    (buf,) = unpack_from("<H", blob.read(2))
    pnNext = buf

    (buf,) = unpack_from("<H", blob.read(2))
    fDot = getBit(buf, 0)
    fGlsy = getBit(buf, 1)
    fComplex = getBit(buf, 2)
    fHasPic = getBit(buf, 3)
    cQuickSaves = getBitSlice(buf, 4, 4)
    fEncrypted = getBit(buf, 8)
    fWhichTblStm = getBit(buf, 9)
    fReadOnlyRecommended = getBit(buf, 10)
    fWriteReservation = getBit(buf, 11)
    fExtChar = getBit(buf, 12)
    fLoadOverride = getBit(buf, 13)
    fFarEast = getBit(buf, 14)
    fObfuscation = getBit(buf, 15)

    (buf,) = unpack_from("<H", blob.read(2))
    nFibBack = buf

    (buf,) = unpack_from("<I", blob.read(4))
    IKey = buf

    (buf,) = unpack_from("<B", blob.read(1))
    envr = buf

    (buf,) = unpack_from("<B", blob.read(1))
    fMac = getBit(buf, 0)
    fEmptySpecial = getBit(buf, 1)
    fLoadOverridePage = getBit(buf, 2)
    reserved1 = getBit(buf, 3)
    reserved2 = getBit(buf, 4)
    fSpare0 = getBitSlice(buf, 5, 3)

    (buf,) = unpack_from("<H", blob.read(2))
    reserved3 = buf

    (buf,) = unpack_from("<H", blob.read(2))
    reserved4 = buf

    (buf,) = unpack_from("<I", blob.read(4))
    reserved5 = buf

    (buf,) = unpack_from("<I", blob.read(4))
    reserved6 = buf

    fibbase = FibBase(
        wIdent=wIdent,
        nFib=nFib,
        unused=unused,
        lid=lid,
        pnNext=pnNext,
        fDot=fDot,
        fGlsy=fGlsy,
        fComplex=fComplex,
        fHasPic=fHasPic,
        cQuickSaves=cQuickSaves,
        fEncrypted=fEncrypted,
        fWhichTblStm=fWhichTblStm,
        fReadOnlyRecommended=fReadOnlyRecommended,
        fWriteReservation=fWriteReservation,
        fExtChar=fExtChar,
        fLoadOverride=fLoadOverride,
        fFarEast=fFarEast,
        nFibBack=nFibBack,
        fObfuscation=fObfuscation,
        IKey=IKey,
        envr=envr,
        fMac=fMac,
        fEmptySpecial=fEmptySpecial,
        fLoadOverridePage=fLoadOverridePage,
        reserved1=reserved1,
        reserved2=reserved2,
        fSpare0=fSpare0,
        reserved3=reserved3,
        reserved4=reserved4,
        reserved5=reserved5,
        reserved6=reserved6,
    )
    return fibbase


def _packFibBase(fibbase):
    setBit = lambda bits, i, v: (bits & ~(1 << i)) | (v << i)
    setBitSlice = lambda bits, i, w, v: (bits & ~((2**w - 1) << i)) | (
        (v & (2**w - 1)) << i
    )

    blob = io.BytesIO()
    buf = pack("<H", fibbase.wIdent)
    blob.write(buf)

    buf = pack("<H", fibbase.nFib)
    blob.write(buf)

    buf = pack("<H", fibbase.unused)
    blob.write(buf)

    buf = pack("<H", fibbase.lid)
    blob.write(buf)

    buf = pack("<H", fibbase.pnNext)
    blob.write(buf)

    _buf = 0xFFFF
    _buf = setBit(_buf, 0, fibbase.fDot)
    _buf = setBit(_buf, 1, fibbase.fGlsy)
    _buf = setBit(_buf, 2, fibbase.fComplex)
    _buf = setBit(_buf, 3, fibbase.fHasPic)
    _buf = setBitSlice(_buf, 4, 4, fibbase.cQuickSaves)
    _buf = setBit(_buf, 8, fibbase.fEncrypted)
    _buf = setBit(_buf, 9, fibbase.fWhichTblStm)
    _buf = setBit(_buf, 10, fibbase.fReadOnlyRecommended)
    _buf = setBit(_buf, 11, fibbase.fWriteReservation)
    _buf = setBit(_buf, 12, fibbase.fExtChar)
    _buf = setBit(_buf, 13, fibbase.fLoadOverride)
    _buf = setBit(_buf, 14, fibbase.fFarEast)
    _buf = setBit(_buf, 15, fibbase.fObfuscation)
    buf = pack("<H", _buf)
    blob.write(buf)

    buf = pack("<H", fibbase.nFibBack)
    blob.write(buf)

    buf = pack("<I", fibbase.IKey)
    blob.write(buf)

    buf = pack("<B", fibbase.envr)
    blob.write(buf)

    _buf = 0xFF
    _buf = setBit(_buf, 0, fibbase.fMac)
    _buf = setBit(_buf, 1, fibbase.fEmptySpecial)
    _buf = setBit(_buf, 2, fibbase.fLoadOverridePage)
    _buf = setBit(_buf, 3, fibbase.reserved1)
    _buf = setBit(_buf, 4, fibbase.reserved2)
    _buf = setBitSlice(_buf, 5, 3, fibbase.fSpare0)
    buf = pack("<B", _buf)
    blob.write(buf)

    buf = pack("<H", fibbase.reserved3)
    blob.write(buf)

    buf = pack("<H", fibbase.reserved4)
    blob.write(buf)

    buf = pack("<I", fibbase.reserved5)
    blob.write(buf)

    buf = pack("<I", fibbase.reserved6)
    blob.write(buf)

    blob.seek(0)
    return blob


def _parseFib(blob):
    Fib = namedtuple("Fib", ["base"])
    fib = Fib(base=_parseFibBase(blob))
    return fib


def _parse_header_RC4(encryptionHeader):
    # RC4: https://msdn.microsoft.com/en-us/library/dd908560(v=office.12).aspx
    salt = encryptionHeader.read(16)
    encryptedVerifier = encryptionHeader.read(16)
    encryptedVerifierHash = encryptionHeader.read(16)
    info = {
        "salt": salt,
        "encryptedVerifier": encryptedVerifier,
        "encryptedVerifierHash": encryptedVerifierHash,
    }
    return info


class Doc97File(base.BaseOfficeFile):
    """Return a MS-DOC file object.

    Examples:
        >>> with open("tests/inputs/rc4cryptoapi_password.doc", "rb") as f:
        ...     officefile = Doc97File(f)
        ...     officefile.load_key(password="Password1234_")

        >>> with open("tests/inputs/rc4cryptoapi_password.doc", "rb") as f:
        ...     officefile = Doc97File(f)
        ...     officefile.load_key(password="0000")
        Traceback (most recent call last):
            ...
        msoffcrypto.exceptions.InvalidKeyError: ...
    """

    def __init__(self, file):
        self.file = file
        ole = olefile.OleFileIO(file)  # do not close this, would close file
        self.ole = ole
        self.format = "doc97"
        self.keyTypes = ["password"]
        self.key = None
        self.salt = None

        # https://msdn.microsoft.com/en-us/library/dd944620(v=office.12).aspx
        with ole.openstream("wordDocument") as stream:
            fib = _parseFib(stream)

        # https://msdn.microsoft.com/en-us/library/dd923367(v=office.12).aspx
        tablename = "1Table" if fib.base.fWhichTblStm == 1 else "0Table"

        Info = namedtuple("Info", ["fib", "tablename"])
        self.info = Info(
            fib=fib,
            tablename=tablename,
        )

    def load_key(self, password=None):
        fib = self.info.fib
        logger.debug(
            "fEncrypted: {}, fObfuscation: {}".format(
                fib.base.fEncrypted, fib.base.fObfuscation
            )
        )

        if fib.base.fEncrypted == 1:
            if fib.base.fObfuscation == 1:  # Using XOR obfuscation
                xor_obf_password_verifier = fib.base.IKey
                logger.debug(hex(xor_obf_password_verifier))
            else:  # elif fib.base.fObfuscation == 0:
                encryptionHeader_size = fib.base.IKey
                logger.debug(
                    "encryptionHeader_size: {}".format(hex(encryptionHeader_size))
                )
                with self.ole.openstream(self.info.tablename) as table:
                    encryptionHeader = (
                        table  # TODO why create a 2nd reference to same stream?
                    )
                    encryptionVersionInfo = table.read(4)
                    vMajor, vMinor = unpack("<HH", encryptionVersionInfo)
                    logger.debug("Version: {} {}".format(vMajor, vMinor))
                    if vMajor == 0x0001 and vMinor == 0x0001:  # RC4
                        info = _parse_header_RC4(encryptionHeader)
                        if DocumentRC4.verifypw(
                            password,
                            info["salt"],
                            info["encryptedVerifier"],
                            info["encryptedVerifierHash"],
                        ):
                            self.type = "rc4"
                            self.key = password
                            self.salt = info["salt"]
                        else:
                            raise exceptions.InvalidKeyError(
                                "Failed to verify password"
                            )
                    elif (
                        vMajor in [0x0002, 0x0003, 0x0004] and vMinor == 0x0002
                    ):  # RC4 CryptoAPI
                        info = _parse_header_RC4CryptoAPI(encryptionHeader)
                        if DocumentRC4CryptoAPI.verifypw(
                            password,
                            info["salt"],
                            info["keySize"],
                            info["encryptedVerifier"],
                            info["encryptedVerifierHash"],
                        ):
                            self.type = "rc4_cryptoapi"
                            self.key = password
                            self.salt = info["salt"]
                            self.keySize = info["keySize"]
                        else:
                            raise exceptions.InvalidKeyError(
                                "Failed to verify password"
                            )
                    else:
                        raise exceptions.DecryptionError(
                            "Unsupported encryption method"
                        )
        else:
            raise exceptions.DecryptionError("File is not encrypted")

    def decrypt(self, outfile):
        # fd, _outfile_path = tempfile.mkstemp()

        # shutil.copyfile(os.path.realpath(self.file.name), _outfile_path)
        # outole = olefile.OleFileIO(_outfile_path, write_mode=True)

        obuf1 = io.BytesIO()
        fibbase = FibBase(
            wIdent=self.info.fib.base.wIdent,
            nFib=self.info.fib.base.nFib,
            unused=self.info.fib.base.unused,
            lid=self.info.fib.base.lid,
            pnNext=self.info.fib.base.pnNext,
            fDot=self.info.fib.base.fDot,
            fGlsy=self.info.fib.base.fGlsy,
            fComplex=self.info.fib.base.fComplex,
            fHasPic=self.info.fib.base.fHasPic,
            cQuickSaves=self.info.fib.base.cQuickSaves,
            fEncrypted=0,
            fWhichTblStm=self.info.fib.base.fWhichTblStm,
            fReadOnlyRecommended=self.info.fib.base.fReadOnlyRecommended,
            fWriteReservation=self.info.fib.base.fWriteReservation,
            fExtChar=self.info.fib.base.fExtChar,
            fLoadOverride=self.info.fib.base.fLoadOverride,
            fFarEast=self.info.fib.base.fFarEast,
            nFibBack=self.info.fib.base.nFibBack,
            fObfuscation=0,
            IKey=0,
            envr=self.info.fib.base.envr,
            fMac=self.info.fib.base.fMac,
            fEmptySpecial=self.info.fib.base.fEmptySpecial,
            fLoadOverridePage=self.info.fib.base.fLoadOverridePage,
            reserved1=self.info.fib.base.reserved1,
            reserved2=self.info.fib.base.reserved2,
            fSpare0=self.info.fib.base.fSpare0,
            reserved3=self.info.fib.base.reserved3,
            reserved4=self.info.fib.base.reserved4,
            reserved5=self.info.fib.base.reserved5,
            reserved6=self.info.fib.base.reserved6,
        )
        FIB_LENGTH = 0x44

        header = _packFibBase(fibbase).read()
        logger.debug(len(header))
        obuf1.seek(0)
        obuf1.write(header)

        with self.ole.openstream("wordDocument") as worddocument:
            worddocument.seek(len(header))
            header = worddocument.read(FIB_LENGTH - len(header))
            worddocument.seek(0)
            logger.debug(len(header))
            obuf1.write(header)

            if self.type == "rc4":
                dec1 = DocumentRC4.decrypt(self.key, self.salt, worddocument)
            elif self.type == "rc4_cryptoapi":
                dec1 = DocumentRC4CryptoAPI.decrypt(
                    self.key, self.salt, self.keySize, worddocument
                )
            else:
                raise exceptions.DecryptionError(
                    "Unsupported encryption method: {}".format(self.type)
                )

            dec1.seek(FIB_LENGTH)
            obuf1.write(dec1.read())
            obuf1.seek(0)

        # TODO: Preserve header
        obuf2 = io.BytesIO()

        if self.type == "rc4":
            with self.ole.openstream(self.info.tablename) as stream:
                dec2 = DocumentRC4.decrypt(self.key, self.salt, stream)
        elif self.type == "rc4_cryptoapi":
            with self.ole.openstream(self.info.tablename) as stream:
                dec2 = DocumentRC4CryptoAPI.decrypt(
                    self.key, self.salt, self.keySize, stream
                )
        else:
            raise exceptions.DecryptionError(
                "Unsupported encryption method: {}".format(self.type)
            )

        obuf2.write(dec2.read())
        obuf2.seek(0)

        obuf3 = None
        if self.ole.exists("Data"):
            obuf3 = io.BytesIO()
            if self.type == "rc4":
                with self.ole.openstream("Data") as data_stream:
                    dec3 = DocumentRC4.decrypt(self.key, self.salt, data_stream)
            elif self.type == "rc4_cryptoapi":
                with self.ole.openstream("Data") as data_stream:
                    dec3 = DocumentRC4CryptoAPI.decrypt(
                        self.key, self.salt, self.keySize, data_stream
                    )
            else:
                raise exceptions.DecryptionError(
                    "Unsupported encryption method: {}".format(self.type)
                )
            obuf3.write(dec3.read())
            obuf3.seek(0)

        with tempfile.TemporaryFile() as _outfile:
            self.file.seek(0)
            shutil.copyfileobj(self.file, _outfile)
            outole = olefile.OleFileIO(_outfile, write_mode=True)

            outole.write_stream("wordDocument", obuf1.read())
            outole.write_stream(self.info.tablename, obuf2.read())
            if obuf3:
                outole.write_stream("Data", obuf3.read())

            # _outfile = open(_outfile_path, 'rb')

            _outfile.seek(0)

            shutil.copyfileobj(_outfile, outfile)

    def is_encrypted(self):
        r"""
        Test if the file is encrypted.

            >>> f = open("tests/inputs/plain.doc", "rb")
            >>> file = Doc97File(f)
            >>> file.is_encrypted()
            False
            >>> f = open("tests/inputs/rc4cryptoapi_password.doc", "rb")
            >>> file = Doc97File(f)
            >>> file.is_encrypted()
            True
        """
        return True if self.info.fib.base.fEncrypted == 1 else False
