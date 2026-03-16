import io
import logging
import shutil
import tempfile
from collections import namedtuple
from struct import pack, unpack

import olefile

from msoffcrypto import exceptions
from msoffcrypto.format import base
from msoffcrypto.format.common import _parse_header_RC4CryptoAPI
from msoffcrypto.method.rc4_cryptoapi import DocumentRC4CryptoAPI

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


RecordHeader = namedtuple(
    "RecordHeader",
    [
        "recVer",
        "recInstance",
        "recType",
        "recLen",
    ],
)


def _parseRecordHeader(blob):
    # RecordHeader: https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/df201194-0cd0-4dfb-bf10-eea353d8eabc
    getBitSlice = lambda bits, i, w: (bits & (2**w - 1 << i)) >> i

    blob.seek(0)

    (buf,) = unpack("<H", blob.read(2))
    recVer = getBitSlice(buf, 0, 4)
    recInstance = getBitSlice(buf, 4, 12)

    (recType,) = unpack("<H", blob.read(2))
    (recLen,) = unpack("<I", blob.read(4))

    rh = RecordHeader(
        recVer=recVer,
        recInstance=recInstance,
        recType=recType,
        recLen=recLen,
    )

    return rh


def _packRecordHeader(rh):
    setBitSlice = lambda bits, i, w, v: (bits & ~((2**w - 1) << i)) | (
        (v & (2**w - 1)) << i
    )

    blob = io.BytesIO()

    _buf = 0xFFFF
    _buf = setBitSlice(_buf, 0, 4, rh.recVer)
    _buf = setBitSlice(_buf, 4, 12, rh.recInstance)
    buf = pack("<H", _buf)
    blob.write(buf)

    buf = pack("<H", rh.recType)
    blob.write(buf)

    buf = pack("<I", rh.recLen)
    blob.write(buf)

    blob.seek(0)

    return blob


CurrentUserAtom = namedtuple(
    "CurrentUserAtom",
    [
        "rh",
        "size",
        "headerToken",
        "offsetToCurrentEdit",
        "lenUserName",
        "docFileVersion",
        "majorVersion",
        "minorVersion",
        "unused",
        "ansiUserName",
        "relVersion",
        "unicodeUserName",
    ],
)


def _parseCurrentUserAtom(blob):
    # CurrentUserAtom: https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/940d5700-e4d7-4fc0-ab48-fed5dbc48bc1

    # rh (8 bytes): A RecordHeader structure...
    buf = io.BytesIO(blob.read(8))
    rh = _parseRecordHeader(buf)
    # logger.debug(rh)

    # ...Sub-fields are further specified in the following table.
    assert rh.recVer == 0x0
    assert rh.recInstance == 0x000
    assert rh.recType == 0x0FF6

    (size,) = unpack("<I", blob.read(4))
    # logger.debug(hex(size))

    # size (4 bytes): ...It MUST be 0x00000014.
    assert size == 0x00000014

    # headerToken (4 bytes): An unsigned integer that specifies
    # a token used to identify whether the file is encrypted.
    (headerToken,) = unpack("<I", blob.read(4))

    # TODO: Check headerToken value

    (offsetToCurrentEdit,) = unpack("<I", blob.read(4))

    (lenUserName,) = unpack("<H", blob.read(2))
    (docFileVersion,) = unpack("<H", blob.read(2))
    (
        majorVersion,
        minorVersion,
    ) = unpack("<BB", blob.read(2))
    unused = blob.read(2)
    ansiUserName = blob.read(lenUserName)
    (relVersion,) = unpack("<I", blob.read(4))
    unicodeUserName = blob.read(2 * lenUserName)

    return CurrentUserAtom(
        rh=rh,
        size=size,
        headerToken=headerToken,
        offsetToCurrentEdit=offsetToCurrentEdit,
        lenUserName=lenUserName,
        docFileVersion=docFileVersion,
        majorVersion=majorVersion,
        minorVersion=minorVersion,
        unused=unused,
        ansiUserName=ansiUserName,
        relVersion=relVersion,
        unicodeUserName=unicodeUserName,
    )


def _packCurrentUserAtom(currentuseratom):
    blob = io.BytesIO()

    buf = _packRecordHeader(currentuseratom.rh).read()
    blob.write(buf)
    buf = pack("<I", currentuseratom.size)
    blob.write(buf)
    buf = pack("<I", currentuseratom.headerToken)
    blob.write(buf)
    buf = pack("<I", currentuseratom.offsetToCurrentEdit)
    blob.write(buf)
    buf = pack("<H", currentuseratom.lenUserName)
    blob.write(buf)
    buf = pack("<H", currentuseratom.docFileVersion)
    blob.write(buf)
    buf = pack("<BB", currentuseratom.majorVersion, currentuseratom.minorVersion)
    blob.write(buf)
    buf = currentuseratom.unused
    blob.write(buf)
    buf = currentuseratom.ansiUserName
    blob.write(buf)
    buf = pack("<I", currentuseratom.relVersion)
    blob.write(buf)
    buf = currentuseratom.unicodeUserName
    blob.write(buf)

    blob.seek(0)

    return blob


CurrentUser = namedtuple("CurrentUser", ["currentuseratom"])


def _parseCurrentUser(blob):
    # Current User Stream: https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/76cfa657-07a6-464b-81ab-4c017c611f64
    currentuser = CurrentUser(currentuseratom=_parseCurrentUserAtom(blob))
    return currentuser


def _packCurrentUser(currentuser):
    blob = io.BytesIO()

    buf = _packCurrentUserAtom(currentuser.currentuseratom).read()
    blob.write(buf)

    blob.seek(0)

    return blob


UserEditAtom = namedtuple(
    "UserEditAtom",
    [
        "rh",
        "lastSlideIdRef",
        "version",
        "minorVersion",
        "majorVersion",
        "offsetLastEdit",
        "offsetPersistDirectory",
        "docPersistIdRef",
        "persistIdSeed",
        "lastView",
        "unused",
        "encryptSessionPersistIdRef",
    ],
)


def _parseUserEditAtom(blob):
    # UserEditAtom: https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/3ffb3fab-95de-4873-98aa-d508fbbac981

    # rh (8 bytes): A RecordHeader structure...
    buf = io.BytesIO(blob.read(8))
    rh = _parseRecordHeader(buf)
    # logger.debug(rh)

    # ...Sub-fields are further specified in the following table.
    assert rh.recVer == 0x0
    assert rh.recInstance == 0x000
    assert rh.recType == 0x0FF5
    assert (
        rh.recLen == 0x0000001C or rh.recLen == 0x00000020
    )  # 0x0000001c + len(encryptSessionPersistIdRef)

    (lastSlideIdRef,) = unpack("<I", blob.read(4))
    (version,) = unpack("<H", blob.read(2))
    (
        minorVersion,
        majorVersion,
    ) = unpack("<BB", blob.read(2))
    # majorVersion, minorVersion, = unpack("<BB", blob.read(2))

    (offsetLastEdit,) = unpack("<I", blob.read(4))
    (offsetPersistDirectory,) = unpack("<I", blob.read(4))
    (docPersistIdRef,) = unpack("<I", blob.read(4))

    (persistIdSeed,) = unpack("<I", blob.read(4))
    (lastView,) = unpack("<H", blob.read(2))
    unused = blob.read(2)

    # encryptSessionPersistIdRef (4 bytes): An optional PersistIdRef
    # that specifies the value to look up in the persist object directory
    # to find the offset of the CryptSession10Container record (section 2.3.7).
    buf = blob.read(4)
    if len(buf) == 4:
        (encryptSessionPersistIdRef,) = unpack("<I", buf)
    else:
        encryptSessionPersistIdRef = None

    return UserEditAtom(
        rh=rh,
        lastSlideIdRef=lastSlideIdRef,
        version=version,
        minorVersion=minorVersion,
        majorVersion=majorVersion,
        offsetLastEdit=offsetLastEdit,
        offsetPersistDirectory=offsetPersistDirectory,
        docPersistIdRef=docPersistIdRef,
        persistIdSeed=persistIdSeed,
        lastView=lastView,
        unused=unused,
        encryptSessionPersistIdRef=encryptSessionPersistIdRef,
    )


def _packUserEditAtom(usereditatom):
    blob = io.BytesIO()

    buf = _packRecordHeader(usereditatom.rh).read()
    blob.write(buf)
    buf = pack("<I", usereditatom.lastSlideIdRef)
    blob.write(buf)
    buf = pack("<H", usereditatom.version)
    blob.write(buf)
    buf = pack("<BB", usereditatom.minorVersion, usereditatom.majorVersion)
    blob.write(buf)
    buf = pack("<I", usereditatom.offsetLastEdit)
    blob.write(buf)
    buf = pack("<I", usereditatom.offsetPersistDirectory)
    blob.write(buf)
    buf = pack("<I", usereditatom.docPersistIdRef)
    blob.write(buf)
    buf = pack("<I", usereditatom.persistIdSeed)
    blob.write(buf)
    buf = pack("<H", usereditatom.lastView)
    blob.write(buf)
    buf = usereditatom.unused
    blob.write(buf)
    # Optional value
    if usereditatom.encryptSessionPersistIdRef is not None:
        buf = pack("<I", usereditatom.encryptSessionPersistIdRef)
        blob.write(buf)

    blob.seek(0)

    return blob


PersistDirectoryEntry = namedtuple(
    "PersistDirectoryEntry",
    [
        "persistId",
        "cPersist",
        "rgPersistOffset",
    ],
)


def _parsePersistDirectoryEntry(blob):
    # PersistDirectoryEntry: https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/6214b5a6-7ca2-4a86-8a0e-5fd3d3eff1c9
    getBitSlice = lambda bits, i, w: (bits & (2**w - 1 << i)) >> i

    (buf,) = unpack("<I", blob.read(4))
    persistId = getBitSlice(buf, 0, 20)
    cPersist = getBitSlice(buf, 20, 12)

    # cf. PersistOffsetEntry: https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/a056484a-2132-4e1e-aa54-6e387f9695cf
    size_rgPersistOffset = 4 * cPersist
    _rgPersistOffset = blob.read(size_rgPersistOffset)
    _rgPersistOffset = io.BytesIO(_rgPersistOffset)
    rgPersistOffset = []
    pos = 0
    while pos < size_rgPersistOffset:
        (persistoffsetentry,) = unpack("<I", _rgPersistOffset.read(4))
        rgPersistOffset.append(persistoffsetentry)
        pos += 4

    return PersistDirectoryEntry(
        persistId=persistId,
        cPersist=cPersist,
        rgPersistOffset=rgPersistOffset,
    )


def _packPersistDirectoryEntry(directoryentry):
    setBitSlice = lambda bits, i, w, v: (bits & ~((2**w - 1) << i)) | (
        (v & (2**w - 1)) << i
    )

    blob = io.BytesIO()

    _buf = 0xFFFFFFFF
    _buf = setBitSlice(_buf, 0, 20, directoryentry.persistId)
    _buf = setBitSlice(_buf, 20, 12, directoryentry.cPersist)
    buf = pack("<I", _buf)
    blob.write(buf)

    for v in directoryentry.rgPersistOffset:
        buf = pack("<I", v)
        blob.write(buf)

    blob.seek(0)

    return blob


PersistDirectoryAtom = namedtuple(
    "PersistDirectoryAtom",
    [
        "rh",
        "rgPersistDirEntry",
    ],
)


def _parsePersistDirectoryAtom(blob):
    # PersistDirectoryAtom: https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/d10a093d-860f-409c-b065-aeb24b830505

    # rh (8 bytes): A RecordHeader structure...
    buf = io.BytesIO(blob.read(8))
    rh = _parseRecordHeader(buf)
    # logger.debug(rh)

    # ...Sub-fields are further specified in the following table.
    assert rh.recVer == 0x0
    assert rh.recInstance == 0x000
    assert rh.recType == 0x1772

    _rgPersistDirEntry = blob.read(rh.recLen)
    _rgPersistDirEntry = io.BytesIO(_rgPersistDirEntry)
    rgPersistDirEntry = []
    pos = 0
    while pos < rh.recLen:
        persistdirectoryentry = _parsePersistDirectoryEntry(_rgPersistDirEntry)
        size_persistdirectoryentry = 4 + 4 * len(persistdirectoryentry.rgPersistOffset)
        # logger.debug((persistdirectoryentry, size_persistdirectoryentry))
        rgPersistDirEntry.append(persistdirectoryentry)
        pos += size_persistdirectoryentry

    return PersistDirectoryAtom(
        rh=rh,
        rgPersistDirEntry=rgPersistDirEntry,
    )


def _packPersistDirectoryAtom(directoryatom):
    blob = io.BytesIO()

    buf = _packRecordHeader(directoryatom.rh).read()
    blob.write(buf)

    for v in directoryatom.rgPersistDirEntry:
        buf = _packPersistDirectoryEntry(v)
        blob.write(buf.read())

    blob.seek(0)

    return blob


def _parseCryptSession10Container(blob):
    # CryptSession10Container: https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/b0963334-4408-4621-879a-ef9c54551fd8

    CryptSession10Container = namedtuple(
        "CryptSession10Container",
        [
            "rh",
            "data",
        ],
    )

    # rh (8 bytes): A RecordHeader structure...
    buf = io.BytesIO(blob.read(8))
    rh = _parseRecordHeader(buf)
    # logger.debug(rh)

    # ...Sub-fields are further specified in the following table.
    assert rh.recVer == 0xF
    # The specified value fails
    # assert rh.recInstance == 0x000
    assert rh.recType == 0x2F14

    data = blob.read(rh.recLen)

    return CryptSession10Container(
        rh=rh,
        data=data,
    )


def construct_persistobjectdirectory(data):
    # PowerPoint Document Stream: https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/1fc22d56-28f9-4818-bd45-67c2bf721ccf

    # 1. Read the CurrentUserAtom record (section 2.3.2) from the Current User Stream (section 2.1.1). ...
    data.currentuser.seek(0)
    currentuser = _parseCurrentUser(data.currentuser)
    # logger.debug(currentuser)

    # 2. Seek, in the PowerPoint Document Stream, to the offset specified by the offsetToCurrentEdit field of
    # the CurrentUserAtom record identified in step 1.
    data.powerpointdocument.seek(currentuser.currentuseratom.offsetToCurrentEdit)

    persistdirectoryatom_stack = []

    # The stream MUST contain exactly one UserEditAtom record.
    # https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/b0963334-4408-4621-879a-ef9c54551fd8
    for i in range(1):
        # 3. Read the UserEditAtom record at the current offset. ...
        usereditatom = _parseUserEditAtom(data.powerpointdocument)
        # logger.debug(usereditatom)

        # 4. Seek to the offset specified by the offsetPersistDirectory field of the UserEditAtom record identified in step 3.
        data.powerpointdocument.seek(usereditatom.offsetPersistDirectory)

        # 5. Read the PersistDirectoryAtom record at the current offset. ...
        persistdirectoryatom = _parsePersistDirectoryAtom(data.powerpointdocument)
        # logger.debug(persistdirectoryatom)
        persistdirectoryatom_stack.append(persistdirectoryatom)

        # 6. Seek to the offset specified by the offsetLastEdit field in the UserEditAtom record identified in step 3.
        # 7. Repeat steps 3 through 6 until offsetLastEdit is 0x00000000.
        if usereditatom.offsetLastEdit == 0x00000000:
            break
        else:
            data.powerpointdocument.seek(usereditatom.offsetLastEdit)

    # 8. Construct the complete persist object directory for this file as follows:
    persistobjectdirectory = {}

    # 8a. For each PersistDirectoryAtom record previously identified in step 5,
    # add the persist object identifier and persist object stream offset pairs to
    # the persist object directory starting with the PersistDirectoryAtom record
    # last identified, that is, the one closest to the beginning of the stream.
    # 8b. Continue adding these pairs to the persist object directory for each PersistDirectoryAtom record
    # in the reverse order that they were identified in step 5; that is, the pairs from the PersistDirectoryAtom record
    # closest to the end of the stream are added last.
    # 8c. When adding a new pair to the persist object directory, if the persist object identifier
    # already exists in the persist object directory, the persist object stream offset from
    # the new pair replaces the existing persist object stream offset for that persist object identifier.
    while len(persistdirectoryatom_stack) > 0:
        persistdirectoryatom = persistdirectoryatom_stack.pop()
        for entry in persistdirectoryatom.rgPersistDirEntry:
            # logger.debug("persistId: %d" % entry.persistId)
            for i, offset in enumerate(entry.rgPersistOffset):
                persistobjectdirectory[entry.persistId + i] = offset

    return persistobjectdirectory


class Ppt97File(base.BaseOfficeFile):
    """Return a MS-PPT file object.

    Examples:
        >>> with open("tests/inputs/rc4cryptoapi_password.ppt", "rb") as f:
        ...     officefile = Ppt97File(f)
        ...     officefile.load_key(password="Password1234_")

        >>> with open("tests/inputs/rc4cryptoapi_password.ppt", "rb") as f:
        ...     officefile = Ppt97File(f)
        ...     officefile.load_key(password="0000")
        Traceback (most recent call last):
            ...
        msoffcrypto.exceptions.InvalidKeyError: ...
    """

    def __init__(self, file):
        self.file = file
        ole = olefile.OleFileIO(file)  # do not close this, would close file
        self.ole = ole
        self.format = "ppt97"
        self.keyTypes = ["password"]
        self.key = None
        self.salt = None

        # streams closed in destructor:
        currentuser = ole.openstream("Current User")
        powerpointdocument = ole.openstream("PowerPoint Document")

        Data = namedtuple("Data", ["currentuser", "powerpointdocument"])
        self.data = Data(
            currentuser=currentuser,
            powerpointdocument=powerpointdocument,
        )

    def __del__(self):
        """Destructor, closes opened streams."""
        if hasattr(self, "data") and self.data:
            if self.data.currentuser:
                self.data.currentuser.close()
            if self.data.powerpointdocument:
                self.data.powerpointdocument.close()

    def load_key(self, password=None):
        persistobjectdirectory = construct_persistobjectdirectory(self.data)
        logger.debug("[*] persistobjectdirectory: {}".format(persistobjectdirectory))

        self.data.currentuser.seek(0)
        currentuser = _parseCurrentUser(self.data.currentuser)
        logger.debug("[*] currentuser: {}".format(currentuser))

        self.data.powerpointdocument.seek(
            currentuser.currentuseratom.offsetToCurrentEdit
        )
        usereditatom = _parseUserEditAtom(self.data.powerpointdocument)
        logger.debug("[*] usereditatom: {}".format(usereditatom))

        # cf. Part 2 in https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/1fc22d56-28f9-4818-bd45-67c2bf721ccf
        cryptsession10container_offset = persistobjectdirectory[
            usereditatom.encryptSessionPersistIdRef
        ]
        logger.debug(
            "[*] cryptsession10container_offset: {}".format(
                cryptsession10container_offset
            )
        )

        self.data.powerpointdocument.seek(cryptsession10container_offset)
        cryptsession10container = _parseCryptSession10Container(
            self.data.powerpointdocument
        )
        logger.debug("[*] cryptsession10container: {}".format(cryptsession10container))

        encryptionInfo = io.BytesIO(cryptsession10container.data)

        encryptionVersionInfo = encryptionInfo.read(4)
        vMajor, vMinor = unpack("<HH", encryptionVersionInfo)
        logger.debug("[*] encryption version: {} {}".format(vMajor, vMinor))

        assert vMajor in [0x0002, 0x0003, 0x0004] and vMinor == 0x0002  # RC4 CryptoAPI

        info = _parse_header_RC4CryptoAPI(encryptionInfo)
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
            raise exceptions.InvalidKeyError("Failed to verify password")

    def decrypt(self, outfile):
        # Current User Stream
        self.data.currentuser.seek(0)
        currentuser = _parseCurrentUser(self.data.currentuser)
        # logger.debug(currentuser)

        cuatom = currentuser.currentuseratom

        currentuser_new = CurrentUser(
            currentuseratom=CurrentUserAtom(
                rh=cuatom.rh,
                size=cuatom.size,
                # https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/940d5700-e4d7-4fc0-ab48-fed5dbc48bc1
                # 0xE391C05F: The file SHOULD NOT<6> be an encrypted document.
                headerToken=0xE391C05F,
                offsetToCurrentEdit=cuatom.offsetToCurrentEdit,
                lenUserName=cuatom.lenUserName,
                docFileVersion=cuatom.docFileVersion,
                majorVersion=cuatom.majorVersion,
                minorVersion=cuatom.minorVersion,
                unused=cuatom.unused,
                ansiUserName=cuatom.ansiUserName,
                relVersion=cuatom.relVersion,
                unicodeUserName=cuatom.unicodeUserName,
            )
        )

        buf = _packCurrentUser(currentuser_new)
        buf.seek(0)
        currentuser_buf = buf

        # List of encrypted parts: https://docs.microsoft.com/en-us/openspecs/office_file_formats/ms-ppt/b0963334-4408-4621-879a-ef9c54551fd8

        # PowerPoint Document Stream

        self.data.powerpointdocument.seek(0)
        powerpointdocument_size = len(self.data.powerpointdocument.read())
        logger.debug("[*] powerpointdocument_size: {}".format(powerpointdocument_size))

        self.data.powerpointdocument.seek(0)
        dec_bytearray = bytearray(self.data.powerpointdocument.read())

        # UserEditAtom
        self.data.powerpointdocument.seek(
            currentuser.currentuseratom.offsetToCurrentEdit
        )
        # currentuseratom_raw = self.data.powerpointdocument.read(40)

        self.data.powerpointdocument.seek(
            currentuser.currentuseratom.offsetToCurrentEdit
        )
        usereditatom = _parseUserEditAtom(self.data.powerpointdocument)
        # logger.debug(usereditatom)
        # logger.debug(["offsetToCurrentEdit", currentuser.currentuseratom.offsetToCurrentEdit])

        rh_new = RecordHeader(
            recVer=usereditatom.rh.recVer,
            recInstance=usereditatom.rh.recInstance,
            recType=usereditatom.rh.recType,
            recLen=usereditatom.rh.recLen - 4,  # Omit encryptSessionPersistIdRef field
        )

        # logger.debug([_packRecordHeader(usereditatom.rh).read(), _packRecordHeader(rh_new).read()])

        usereditatom_new = UserEditAtom(
            rh=rh_new,
            lastSlideIdRef=usereditatom.lastSlideIdRef,
            version=usereditatom.version,
            minorVersion=usereditatom.minorVersion,
            majorVersion=usereditatom.majorVersion,
            offsetLastEdit=usereditatom.offsetLastEdit,
            offsetPersistDirectory=usereditatom.offsetPersistDirectory,
            docPersistIdRef=usereditatom.docPersistIdRef,
            persistIdSeed=usereditatom.persistIdSeed,
            lastView=usereditatom.lastView,
            unused=usereditatom.unused,
            encryptSessionPersistIdRef=0x00000000,  # Clear
        )

        # logger.debug(currentuseratom_raw)
        # logger.debug(_packUserEditAtom(usereditatom).read())
        # logger.debug(_packUserEditAtom(usereditatom_new).read())

        buf = _packUserEditAtom(usereditatom_new)
        buf.seek(0)
        buf_bytes = bytearray(buf.read())
        offset = currentuser.currentuseratom.offsetToCurrentEdit
        dec_bytearray[offset : offset + len(buf_bytes)] = buf_bytes

        # PersistDirectoryAtom
        self.data.powerpointdocument.seek(
            currentuser.currentuseratom.offsetToCurrentEdit
        )
        usereditatom = _parseUserEditAtom(self.data.powerpointdocument)
        # logger.debug(usereditatom)

        self.data.powerpointdocument.seek(usereditatom.offsetPersistDirectory)
        persistdirectoryatom = _parsePersistDirectoryAtom(self.data.powerpointdocument)
        # logger.debug(persistdirectoryatom)

        persistdirectoryatom_new = PersistDirectoryAtom(
            rh=persistdirectoryatom.rh,
            rgPersistDirEntry=[
                PersistDirectoryEntry(
                    persistId=persistdirectoryatom.rgPersistDirEntry[0].persistId,
                    # Omit CryptSession10Container
                    cPersist=persistdirectoryatom.rgPersistDirEntry[0].cPersist - 1,
                    rgPersistOffset=persistdirectoryatom.rgPersistDirEntry[
                        0
                    ].rgPersistOffset,
                ),
            ],
        )

        self.data.powerpointdocument.seek(usereditatom.offsetPersistDirectory)
        buf = _packPersistDirectoryAtom(persistdirectoryatom_new)
        buf_bytes = bytearray(buf.read())
        offset = usereditatom.offsetPersistDirectory
        dec_bytearray[offset : offset + len(buf_bytes)] = buf_bytes

        # Persist Objects
        self.data.powerpointdocument.seek(0)
        persistobjectdirectory = construct_persistobjectdirectory(self.data)

        directory_items = list(persistobjectdirectory.items())

        for i, (persistId, offset) in enumerate(directory_items):
            self.data.powerpointdocument.seek(offset)
            buf = self.data.powerpointdocument.read(8)
            rh = _parseRecordHeader(io.BytesIO(buf))
            logger.debug("[*] rh: {}".format(rh))

            # CryptSession10Container
            if rh.recType == 0x2F14:
                logger.debug("[*] CryptSession10Container found")
                # Remove encryption, pad by zero to preserve stream size
                dec_bytearray[offset : offset + (8 + rh.recLen)] = b"\x00" * (
                    8 + rh.recLen
                )
                continue

            # The UserEditAtom record (section 2.3.3) and the PersistDirectoryAtom record (section 2.3.4) MUST NOT be encrypted.
            if rh.recType in [0x0FF5, 0x1772]:
                logger.debug("[*] UserEditAtom/PersistDirectoryAtom found")
                continue

            # TODO: Fix here
            recLen = directory_items[i + 1][1] - offset - 8
            logger.debug("[*] recLen: {}".format(recLen))

            self.data.powerpointdocument.seek(offset)
            enc_buf = io.BytesIO(self.data.powerpointdocument.read(8 + recLen))
            blocksize = self.keySize * (
                (8 + recLen) // self.keySize + 1
            )  # Undocumented
            dec = DocumentRC4CryptoAPI.decrypt(
                self.key,
                self.salt,
                self.keySize,
                enc_buf,
                blocksize=blocksize,
                block=persistId,
            )
            dec_bytes = bytearray(dec.read())
            dec_bytearray[offset : offset + len(dec_bytes)] = dec_bytes

        # To BytesIO
        dec_buf = io.BytesIO(dec_bytearray)

        dec_buf.seek(0)
        for i, (persistId, offset) in enumerate(directory_items):
            dec_buf.seek(offset)
            buf = dec_buf.read(8)
            rh = _parseRecordHeader(io.BytesIO(buf))
            logger.debug("[*] rh: {}".format(rh))

        dec_buf.seek(0)
        logger.debug(
            "[*] powerpointdocument_size={}, len(dec_buf.read())={}".format(
                powerpointdocument_size, len(dec_buf.read())
            )
        )

        dec_buf.seek(0)
        powerpointdocument_dec_buf = dec_buf

        # TODO: Pictures Stream
        # TODO: Encrypted Summary Info Stream

        with tempfile.TemporaryFile() as _outfile:
            self.file.seek(0)
            shutil.copyfileobj(self.file, _outfile)
            outole = olefile.OleFileIO(_outfile, write_mode=True)

            outole.write_stream("Current User", currentuser_buf.read())
            outole.write_stream(
                "PowerPoint Document", powerpointdocument_dec_buf.read()
            )

            # Finalize
            _outfile.seek(0)
            shutil.copyfileobj(_outfile, outfile)

        return

    def is_encrypted(self):
        r"""
        Test if the file is encrypted.

            >>> f = open("tests/inputs/plain.ppt", "rb")
            >>> file = Ppt97File(f)
            >>> file.is_encrypted()
            False
            >>> f = open("tests/inputs/rc4cryptoapi_password.ppt", "rb")
            >>> file = Ppt97File(f)
            >>> file.is_encrypted()
            True
        """
        self.data.currentuser.seek(0)
        currentuser = _parseCurrentUser(self.data.currentuser)
        logger.debug("[*] currentuser: {}".format(currentuser))

        self.data.powerpointdocument.seek(
            currentuser.currentuseratom.offsetToCurrentEdit
        )
        usereditatom = _parseUserEditAtom(self.data.powerpointdocument)
        logger.debug("[*] usereditatom: {}".format(usereditatom))

        if usereditatom.rh.recLen == 0x00000020:  # Cf. _parseUserEditAtom
            return True
        else:
            return False
