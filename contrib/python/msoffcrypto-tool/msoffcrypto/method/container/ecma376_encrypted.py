import io
from datetime import datetime
from struct import pack

import olefile

# An encrypted ECMA376 file is stored as an OLE container.
#
# At this point, creating an Ole file is somewhat of a chore, since
# the latest OleFile (v0.47) does not really do it.
#
# See https://github.com/decalage2/olefile/issues/6
#
# This file is not meant to support all manners of OLE files; it creates
# what we need (an OLE file with an encrypted stream + supporting streams).
# Nothing more, nothing less. So, unlike OleFile, we can take _a lot_ of
# shortcuts.
#
# Probably very brittle.
#
# File format:
#
# https://github.com/libyal/libolecf/blob/main/documentation/OLE%20Compound%20File%20format.asciidoc
#
# Initial C++ code from https://github.com/herumi/msoffice (BSD-3)


def datetime2filetime(dt):
    """
    Convert Python datetime.datetime to FILETIME (64 bits unsigned int)

    A file time is a 64-bit value that represents the number of 100-nanosecond intervals that have elapsed
    since 12:00 A.M. January 1, 1601 Coordinated Universal Time (UTC).

    https://learn.microsoft.com/en-us/windows/win32/sysinfo/file-times
    """
    _FILETIME_NULL_DATE = datetime(1601, 1, 1, 0, 0, 0)
    return int((dt - _FILETIME_NULL_DATE).total_seconds() * 10000000)


class RedBlack:
    RED = 0  # Note that this is per-spec; olefile.py shows the opposite
    BLACK = 1


class DirectoryEntryType:
    EMPTY = 0
    STORAGE = 1
    STREAM = 2
    LOCK_BYTES = 3
    PROPERTY = 4
    ROOT_STORAGE = 5


class SectorTypes:
    MAXREGSECT = 0xFFFFFFFA
    DIFSECT = 0xFFFFFFFC
    FATSECT = 0xFFFFFFFD
    ENDOFCHAIN = 0xFFFFFFFE
    FREESECT = 0xFFFFFFFF
    NOSTREAM = 0xFFFFFFFF


class DSPos:
    # Order in the directories array; must be in sync with getDirectoryEntries()

    iRoot = 0
    iEncryptionPackage = 1
    iDataSpaces = 2
    iVersion = 3
    iDataSpaceMap = 4
    iDataSpaceInfo = 5
    iStongEncryptionDataSpace = 6
    iTransformInfo = 7
    iStrongEncryptionTransform = 8
    iPrimary = 9
    iEncryptionInfo = 10
    dirNum = 11


class DefaultContent:
    # Lifted off of Herumi/msoffice (C++ package)
    # https://github.com/herumi/msoffice/blob/master/include/resource.hpp

    Version = b"\x3c\x00\x00\x00\x4d\x00\x69\x00\x63\x00\x72\x00\x6f\x00\x73\x00\x6f\x00\x66\x00\x74\x00\x2e\x00\x43\x00\x6f\x00\x6e\x00\x74\x00\x61\x00\x69\x00\x6e\x00\x65\x00\x72\x00\x2e\x00\x44\x00\x61\x00\x74\x00\x61\x00\x53\x00\x70\x00\x61\x00\x63\x00\x65\x00\x73\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00"
    Primary = b"\x58\x00\x00\x00\x01\x00\x00\x00\x4c\x00\x00\x00\x7b\x00\x46\x00\x46\x00\x39\x00\x41\x00\x33\x00\x46\x00\x30\x00\x33\x00\x2d\x00\x35\x00\x36\x00\x45\x00\x46\x00\x2d\x00\x34\x00\x36\x00\x31\x00\x33\x00\x2d\x00\x42\x00\x44\x00\x44\x00\x35\x00\x2d\x00\x35\x00\x41\x00\x34\x00\x31\x00\x43\x00\x31\x00\x44\x00\x30\x00\x37\x00\x32\x00\x34\x00\x36\x00\x7d\x00\x4e\x00\x00\x00\x4d\x00\x69\x00\x63\x00\x72\x00\x6f\x00\x73\x00\x6f\x00\x66\x00\x74\x00\x2e\x00\x43\x00\x6f\x00\x6e\x00\x74\x00\x61\x00\x69\x00\x6e\x00\x65\x00\x72\x00\x2e\x00\x45\x00\x6e\x00\x63\x00\x72\x00\x79\x00\x70\x00\x74\x00\x69\x00\x6f\x00\x6e\x00\x54\x00\x72\x00\x61\x00\x6e\x00\x73\x00\x66\x00\x6f\x00\x72\x00\x6d\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00"
    DataSpaceMap = b"\x08\x00\x00\x00\x01\x00\x00\x00\x68\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x45\x00\x6e\x00\x63\x00\x72\x00\x79\x00\x70\x00\x74\x00\x65\x00\x64\x00\x50\x00\x61\x00\x63\x00\x6b\x00\x61\x00\x67\x00\x65\x00\x32\x00\x00\x00\x53\x00\x74\x00\x72\x00\x6f\x00\x6e\x00\x67\x00\x45\x00\x6e\x00\x63\x00\x72\x00\x79\x00\x70\x00\x74\x00\x69\x00\x6f\x00\x6e\x00\x44\x00\x61\x00\x74\x00\x61\x00\x53\x00\x70\x00\x61\x00\x63\x00\x65\x00\x00\x00"
    StrongEncryptionDataSpace = b"\x08\x00\x00\x00\x01\x00\x00\x00\x32\x00\x00\x00\x53\x00\x74\x00\x72\x00\x6f\x00\x6e\x00\x67\x00\x45\x00\x6e\x00\x63\x00\x72\x00\x79\x00\x70\x00\x74\x00\x69\x00\x6f\x00\x6e\x00\x54\x00\x72\x00\x61\x00\x6e\x00\x73\x00\x66\x00\x6f\x00\x72\x00\x6d\x00\x00\x00"


class Header:
    FIRSTNUMDIFAT = 109
    BUFFER_SIZE = 512  # Size taken when writing out to disk/buffer

    def __init__(self):
        self.minorVersion = 0x003E
        self.majorVersion = 3
        self.sectorShift = 9
        self.numDirectorySectors = 0
        self.numFatSectors = 0
        self.firstDirectorySectorLocation = SectorTypes.ENDOFCHAIN
        self.transactionSignatureNumber = 0
        self.firstMiniFatSectorLocation = SectorTypes.ENDOFCHAIN
        self.numMiniFatSectors = 0
        self.firstDifatSectorLocation = SectorTypes.ENDOFCHAIN
        self.numDifatSectors = 0
        self.sectorSize = 1 << self.sectorShift
        self.difat = []

    def write_to(self, obuf):
        obuf.write(olefile.MAGIC)
        obuf.write(b"\0" * 16)  # CLSID

        byteOrder = 0xFFFE  # Little-Endian
        miniSectorShift = 6
        miniStreamCutoffSize = 0x1000
        reserved = 0

        obuf.write(
            pack(
                "<HHHHHHHHIIIIIIIII",
                self.minorVersion,
                self.majorVersion,
                byteOrder,
                self.sectorShift,
                miniSectorShift,
                reserved,
                reserved,
                reserved,
                self.numDirectorySectors,
                self.numFatSectors,
                self.firstDirectorySectorLocation,
                self.transactionSignatureNumber,
                miniStreamCutoffSize,
                self.firstMiniFatSectorLocation,
                self.numMiniFatSectors,
                self.firstDifatSectorLocation,
                self.numDifatSectors,
            )
        )

        difatSize = len(self.difat)

        for i in range(min(difatSize, Header.FIRSTNUMDIFAT)):
            obuf.write(pack("<I", self.difat[i]))

        for i in range(difatSize, Header.FIRSTNUMDIFAT):
            obuf.write(pack("<I", SectorTypes.NOSTREAM))


class DirectoryEntry:
    def __init__(
        self,
        name="",
        _type=DirectoryEntryType.EMPTY,
        color=RedBlack.RED,
        leftId=SectorTypes.NOSTREAM,
        rightId=SectorTypes.NOSTREAM,
        childId=SectorTypes.NOSTREAM,
        clsid="",
        bits=0,
        ct=0,
        mt=0,
        loc=0,
        content=b"",
    ):
        self.Name = name
        self.Type = _type
        self.Color = color
        self.LeftSiblingId = leftId
        self.RightSiblingId = rightId
        self.ChildId = childId
        self.CLSID = clsid
        self.StateBits = bits
        self.CreationTime = ct
        self.ModificationTime = mt
        self.StartingSectorLocation = loc
        self.Content = content

    def write_header_to(self, obuf):
        """
        Write 128 bytes header in the output buffer. The Name property needs to be converted to UTF-16; Content is _not_
        written out by this method.
        """
        name16 = self.Name.encode(
            "UTF-16-LE"
        )  # Write in Little Endian; omit the BOM at the start of the output
        directoryNameSize = len(name16) + 2  # Count the null terminator in the size

        obuf.write(
            name16 + b"\0" * 2
        )  # Specs calls for us to store the null-terminator
        obuf.write(
            b"\0" * (64 - directoryNameSize)
        )  # Pad name to 64 bytes (thus max 31 chars + \x00\x00)
        obuf.write(pack("<H", directoryNameSize if directoryNameSize > 2 else 0))
        obuf.write(
            pack(
                "<BBIII",
                self.Type,
                self.Color,
                self.LeftSiblingId,
                self.RightSiblingId,
                self.ChildId,
            )
        )

        if self.CLSID:
            obuf.write(self.CLSID)
        else:
            obuf.write(b"\0" * 16)

        obuf.write(pack("<I", self.StateBits))

        self.write_filetime(obuf, self.CreationTime)
        self.write_filetime(obuf, self.ModificationTime)

        obuf.write(pack("<IQ", self.StartingSectorLocation, len(self.Content)))

    def write_filetime(self, obuf, ft):
        # Write the lower 32 bits and upper 32 bits, in this order.
        obuf.write(pack("<II", ft & 0xFFFFFFFF, ft >> 32))

    @property
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, n):
        if len(n) > 31:
            raise ValueError("Name cannot be longer than 31 characters")

        if set("!:/").intersection(n):
            raise ValueError("Name contains invalid characters (!:/)")

        self._Name = n

    @property
    def CLSID(self):
        return self._CLSID

    @CLSID.setter
    def CLSID(self, c):
        if c and len(c) != 16:
            raise ValueError("CLSID must be blank, or 16 characters long")

        self._CLSID = c

    @property
    def LeftSiblingId(self):
        return self._LeftSiblingId

    @LeftSiblingId.setter
    def LeftSiblingId(self, id):
        self._valid_id(id)
        self._LeftSiblingId = id

    @property
    def RightSiblingId(self):
        return self._RightSiblingId

    @RightSiblingId.setter
    def RightSiblingId(self, id):
        self._valid_id(id)
        self._RightSiblingId = id

    @property
    def ChildId(self):
        return self._ChildId

    @ChildId.setter
    def ChildId(self, id):
        self._valid_id(id)
        self._ChildId = id

    def _valid_id(self, id):
        if not ((id <= SectorTypes.MAXREGSECT) or (id == SectorTypes.NOSTREAM)):
            raise ValueError("Invalid id received")


class ECMA376EncryptedLayout:
    def __init__(self, sectorSize):
        self.sectorSize = sectorSize
        self.miniFatNum = 0
        self.miniFatDataSectorNum = 0
        self.miniFatSectors = 0
        self.numMiniFatSectors = 1
        self.difatSectorNum = 0
        self.fatSectorNum = 0
        self.difatPos = 0
        self.directoryEntrySectorNum = 0
        self.encryptionPackageSectorNum = 0

    @property
    def fatPos(self):
        return self.difatPos + self.difatSectorNum

    @property
    def miniFatPos(self):
        return self.fatPos + self.fatSectorNum

    @property
    def directoryEntryPos(self):
        return self.miniFatPos + self.numMiniFatSectors

    @property
    def miniFatDataPos(self):
        return self.directoryEntryPos + self.directoryEntrySectorNum

    @property
    def contentSectorNum(self):
        return (
            self.numMiniFatSectors
            + self.directoryEntrySectorNum
            + self.miniFatDataSectorNum
            + self.encryptionPackageSectorNum
        )

    @property
    def encryptionPackagePos(self):
        return self.miniFatDataPos + self.miniFatDataSectorNum

    @property
    def totalSectors(self):
        return self.difatSectorNum + self.fatSectorNum + self.contentSectorNum

    @property
    def totalSize(self):
        return Header.BUFFER_SIZE + self.totalSectors * self.sectorSize

    @property
    def offsetDirectoryEntries(self):
        return Header.BUFFER_SIZE + self.directoryEntryPos * self.sectorSize

    @property
    def offsetMiniFatData(self):
        return Header.BUFFER_SIZE + self.miniFatDataPos * self.sectorSize

    @property
    def offsetFat(self):
        return Header.BUFFER_SIZE + self.fatPos * self.sectorSize

    @property
    def offsetMiniFat(self):
        return Header.BUFFER_SIZE + self.miniFatPos * self.sectorSize

    def offsetDifat(self, n):
        return Header.BUFFER_SIZE + (self.difatPos + n) * self.sectorSize

    def offsetData(self, startingSectorLocation):
        return Header.BUFFER_SIZE + startingSectorLocation * self.sectorSize

    def offsetMiniData(self, startingSectorLocation):
        return self.offsetMiniFatData + startingSectorLocation * 64


class ECMA376Encrypted:
    def __init__(self, encryptedPackage=b"", encryptionInfo=b""):
        self._header = self._get_default_header()
        self._dirs = self._get_directory_entries()

        self.set_payload(encryptedPackage, encryptionInfo)

    def write_to(self, obuf):
        """
        Writes the encrypted data to obuf
        """

        # Create a temporary buffer with seek/tell capabilities, we do not want to assume the passed-in buffer has such
        # capabilities (ie: piping to stdout).
        _obuf = io.BytesIO()

        self._write_to(_obuf)

        # Finalize and write to client buffer.
        obuf.write(_obuf.getvalue())

    def set_payload(self, encryptedPackage, encryptionInfo):
        self._dirs[DSPos.iEncryptionPackage].Content = encryptedPackage
        self._dirs[DSPos.iEncryptionInfo].Content = encryptionInfo

    def _get_default_header(self):
        return Header()

    def _get_directory_entries(self):
        ft = datetime2filetime(datetime.now())

        directories = [  # Must follow DSPos ordering
            DirectoryEntry(
                "Root Entry",
                DirectoryEntryType.ROOT_STORAGE,
                RedBlack.RED,
                ct=ft,
                mt=ft,
                childId=DSPos.iEncryptionInfo,
            ),
            DirectoryEntry(
                "EncryptedPackage",
                DirectoryEntryType.STREAM,
                RedBlack.RED,
                ct=ft,
                mt=ft,
            ),
            DirectoryEntry(
                "\x06DataSpaces",
                DirectoryEntryType.STORAGE,
                RedBlack.RED,
                ct=ft,
                mt=ft,
                childId=DSPos.iDataSpaceMap,
            ),
            DirectoryEntry(
                "Version",
                DirectoryEntryType.STREAM,
                RedBlack.BLACK,
                ct=ft,
                mt=ft,
                content=DefaultContent.Version,
            ),
            DirectoryEntry(
                "DataSpaceMap",
                DirectoryEntryType.STREAM,
                RedBlack.BLACK,
                ct=ft,
                mt=ft,
                leftId=DSPos.iVersion,
                rightId=DSPos.iDataSpaceInfo,
                content=DefaultContent.DataSpaceMap,
            ),
            DirectoryEntry(
                "DataSpaceInfo",
                DirectoryEntryType.STORAGE,
                RedBlack.BLACK,
                ct=ft,
                mt=ft,
                rightId=DSPos.iTransformInfo,
                childId=DSPos.iStongEncryptionDataSpace,
            ),
            DirectoryEntry(
                "StrongEncryptionDataSpace",
                DirectoryEntryType.STREAM,
                RedBlack.BLACK,
                ct=ft,
                mt=ft,
                content=DefaultContent.StrongEncryptionDataSpace,
            ),
            DirectoryEntry(
                "TransformInfo",
                DirectoryEntryType.STORAGE,
                RedBlack.RED,
                ct=ft,
                mt=ft,
                childId=DSPos.iStrongEncryptionTransform,
            ),
            DirectoryEntry(
                "StrongEncryptionTransform",
                DirectoryEntryType.STORAGE,
                RedBlack.BLACK,
                ct=ft,
                mt=ft,
                childId=DSPos.iPrimary,
            ),
            DirectoryEntry(
                "\x06Primary",
                DirectoryEntryType.STREAM,
                RedBlack.BLACK,
                ct=ft,
                mt=ft,
                content=DefaultContent.Primary,
            ),
            DirectoryEntry(
                "EncryptionInfo",
                DirectoryEntryType.STREAM,
                RedBlack.BLACK,
                ct=ft,
                mt=ft,
                leftId=DSPos.iDataSpaces,
                rightId=DSPos.iEncryptionPackage,
            ),
        ]

        return directories

    def _write_to(self, obuf):
        layout = ECMA376EncryptedLayout(self._header.sectorSize)

        self._set_sector_locations_of_streams(layout)
        self._detect_sector_num(layout)

        self._header.firstDirectorySectorLocation = layout.directoryEntryPos
        self._header.firstMiniFatSectorLocation = layout.miniFatPos
        self._header.numMiniFatSectors = layout.numMiniFatSectors

        self._dirs[DSPos.iRoot].StartingSectorLocation = layout.miniFatDataPos
        self._dirs[DSPos.iRoot].Content = b"\0" * (64 * layout.miniFatNum)
        self._dirs[
            DSPos.iEncryptionPackage
        ].StartingSectorLocation = layout.encryptionPackagePos

        for i in range(min(layout.fatSectorNum, Header.FIRSTNUMDIFAT)):
            self._header.difat.append(layout.fatPos + i)

        self._header.numFatSectors = layout.fatSectorNum
        self._header.numDifatSectors = layout.difatSectorNum

        if layout.difatSectorNum > 0:
            self._header.firstDifatSectorLocation = layout.difatPos

        # Zero out the output buffer; some sections pad, some sections don't ... but we need the buffer to have the proper size
        # so we can jump around
        obuf.write(b"\0" * layout.totalSize)
        obuf.seek(0)

        self._header.write_to(obuf)

        self._write_DIFAT(obuf, layout)
        self._write_FAT_start(obuf, layout)
        self._write_MiniFAT(obuf, layout)

        self._write_directory_entries(obuf, layout)
        self._write_Content(obuf, layout)

    def _write_directory_entries(self, obuf, layout: ECMA376EncryptedLayout):
        obuf.seek(layout.offsetDirectoryEntries)

        for d in self._dirs:
            d.write_header_to(obuf)  # This must write 128 bytes, no more, no less.

        if obuf.tell() != (layout.offsetDirectoryEntries + len(self._dirs) * 128):
            # TODO: Use appropriate custom exception
            raise Exception(
                "Buffer did not advance as expected when writing out directory entries"
            )

    def _write_Content(self, obuf, layout: ECMA376EncryptedLayout):
        for d in self._dirs:
            size = len(d.Content)

            if size:
                if size <= 4096:  # Small content goes in the minifat section
                    obuf.seek(layout.offsetMiniData(d.StartingSectorLocation))
                    obuf.write(d.Content)
                else:
                    obuf.seek(layout.offsetData(d.StartingSectorLocation))
                    obuf.write(d.Content)

    def _write_FAT_start(self, obuf, layout: ECMA376EncryptedLayout):
        v = ([SectorTypes.DIFSECT] * layout.difatSectorNum) + (
            [SectorTypes.FATSECT] * layout.fatSectorNum
        )
        v += [
            layout.numMiniFatSectors,
            layout.directoryEntrySectorNum,
            layout.miniFatDataSectorNum,
            layout.encryptionPackageSectorNum,
        ]

        obuf.seek(layout.offsetFat)
        self._write_FAT(obuf, v, layout.fatSectorNum * layout.sectorSize)

    def _write_MiniFAT(self, obuf, layout: ECMA376EncryptedLayout):
        obuf.seek(layout.offsetMiniFat)
        self._write_FAT(
            obuf, layout.miniFatSectors, layout.numMiniFatSectors * layout.sectorSize
        )

    def _write_FAT(self, obuf, entries, blockSize):
        v = 0

        startPos = obuf.tell()
        max_n = blockSize // 4  # 4 bytes per entry with <I

        # TODO: Use appropriate custom exception
        for e in entries:
            if e <= SectorTypes.MAXREGSECT:
                for j in range(1, e):
                    v += 1

                    if v > max_n:
                        raise Exception("Attempting to write beyond block size")

                    obuf.write(pack("<I", v))

                if v == max_n:
                    raise Exception("Attempting to write beyond block size")

                obuf.write(pack("<I", SectorTypes.ENDOFCHAIN))
            else:
                if v == max_n:
                    raise Exception("Attempting to write beyond block size")

                obuf.write(pack("<I", e))

            v += 1

        obuf.write(pack("<I", SectorTypes.FREESECT) * (max_n - v))

        if obuf.tell() - startPos != blockSize:
            # TODO: Use appropriate custom exception
            raise Exception("_write_FAT() did not completely fill the block space.")

    def _write_DIFAT(self, obuf, layout: ECMA376EncryptedLayout):
        if layout.difatSectorNum < 1:
            return

        v = Header.FIRSTNUMDIFAT + layout.difatSectorNum

        for i in range(layout.difatSectorNum):
            obuf.seek(layout.offsetDifat(i))

            for j in range(layout.sectorSize // 4 - 1):  # 4 == sizeof(32 bit int)
                obuf.write(pack("<I", v))

                v += 1

                if v > layout.difatSectorNum + layout.fatSectorNum:
                    for k in range(j, layout.sectorSize // 4 - 1):
                        obuf.write(pack("<I", SectorTypes.FREESECT))

                    obuf.write(pack("<I", SectorTypes.ENDOFCHAIN))
                    return

            # The next seek is _probably_ not needed...
            obuf.seek(layout.offsetDifat(i) + layout.sectorSize - 4)
            obuf.write(pack("<I", layout.difatPos + i + 1))

    def _detect_sector_num(self, layout: ECMA376EncryptedLayout):
        numInFat = layout.sectorSize // 4  # Number of 4-bytes integers

        difatSectorNum = 0
        fatSectorNum = 0

        for i in range(10):
            a = self._get_block_num(
                difatSectorNum + fatSectorNum + layout.contentSectorNum, numInFat
            )
            b = (
                0
                if a <= Header.FIRSTNUMDIFAT
                else self._get_block_num(a - Header.FIRSTNUMDIFAT, numInFat - 1)
            )

            if (b == difatSectorNum) and (a == fatSectorNum):
                layout.fatSectorNum = fatSectorNum
                layout.difatSectorNum = difatSectorNum

                return

            difatSectorNum = b
            fatSectorNum = a

        raise IndexError(
            "Unable to detect sector number within a reasonsable amount of loops"
        )

    def _set_sector_locations_of_streams(self, layout: ECMA376EncryptedLayout):
        # Use all streams, except the encrypted package which is special (and the main reason why we're doing all this!)
        streamsOfInterest = list(
            filter(
                lambda d: d.Type == DirectoryEntryType.STREAM
                and d.Name != "EncryptedPackage",
                self._dirs,
            )
        )

        miniFatSectors = []
        miniFatNum = 0
        miniFatDataSectorNum = 0

        pos = 0
        for s in streamsOfInterest:
            n = self._get_MiniFAT_sector_number(len(s.Content))

            miniFatSectors.append(n)

            s.StartingSectorLocation = pos
            pos += n

        miniFatNum = pos
        miniFatDataSectorNum = self._get_block_num(
            miniFatNum, (self._header.sectorSize // 64)
        )

        if self._get_block_num(miniFatDataSectorNum, 128) > 1:
            raise ValueError("Unexpected layout size; too large")

        layout.miniFatNum = miniFatNum
        layout.miniFatDataSectorNum = miniFatDataSectorNum
        layout.miniFatSectors = miniFatSectors

        layout.directoryEntrySectorNum = self._get_block_num(len(self._dirs), 4)
        layout.encryptionPackageSectorNum = self._get_block_num(
            len(self._dirs[DSPos.iEncryptionPackage].Content), layout.sectorSize
        )

    def _get_MiniFAT_sector_number(self, size):
        return self._get_block_num(size, 64)

    def _get_block_num(self, x, block):
        return (x + block - 1) // block
