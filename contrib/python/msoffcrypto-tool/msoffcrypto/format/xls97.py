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
from msoffcrypto.method.rc4 import DocumentRC4
from msoffcrypto.method.rc4_cryptoapi import DocumentRC4CryptoAPI
from msoffcrypto.method.xor_obfuscation import DocumentXOR

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


recordNameNum = {
    "Formula": 6,
    "EOF": 10,
    "CalcCount": 12,
    "CalcMode": 13,
    "CalcPrecision": 14,
    "CalcRefMode": 15,
    "CalcDelta": 16,
    "CalcIter": 17,
    "Protect": 18,
    "Password": 19,
    "Header": 20,
    "Footer": 21,
    "ExternSheet": 23,
    "Lbl": 24,
    "WinProtect": 25,
    "VerticalPageBreaks": 26,
    "HorizontalPageBreaks": 27,
    "Note": 28,
    "Selection": 29,
    "Date1904": 34,
    "ExternName": 35,
    "LeftMargin": 38,
    "RightMargin": 39,
    "TopMargin": 40,
    "BottomMargin": 41,
    "PrintRowCol": 42,
    "PrintGrid": 43,
    "FilePass": 47,
    "Font": 49,
    "PrintSize": 51,
    "Continue": 60,
    "Window1": 61,
    "Backup": 64,
    "Pane": 65,
    "CodePage": 66,
    "Pls": 77,
    "DCon": 80,
    "DConRef": 81,
    "DConName": 82,
    "DefColWidth": 85,
    "XCT": 89,
    "CRN": 90,
    "FileSharing": 91,
    "WriteAccess": 92,
    "Obj": 93,
    "Uncalced": 94,
    "CalcSaveRecalc": 95,
    "Template": 96,
    "Intl": 97,
    "ObjProtect": 99,
    "ColInfo": 125,
    "Guts": 128,
    "WsBool": 129,
    "GridSet": 130,
    "HCenter": 131,
    "VCenter": 132,
    "BoundSheet8": 133,
    "WriteProtect": 134,
    "Country": 140,
    "HideObj": 141,
    "Sort": 144,
    "Palette": 146,
    "Sync": 151,
    "LPr": 152,
    "DxGCol": 153,
    "FnGroupName": 154,
    "FilterMode": 155,
    "BuiltInFnGroupCount": 156,
    "AutoFilterInfo": 157,
    "AutoFilter": 158,
    "Scl": 160,
    "Setup": 161,
    "ScenMan": 174,
    "SCENARIO": 175,
    "SxView": 176,
    "Sxvd": 177,
    "SXVI": 178,
    "SxIvd": 180,
    "SXLI": 181,
    "SXPI": 182,
    "DocRoute": 184,
    "RecipName": 185,
    "MulRk": 189,
    "MulBlank": 190,
    "Mms": 193,
    "SXDI": 197,
    "SXDB": 198,
    "SXFDB": 199,
    "SXDBB": 200,
    "SXNum": 201,
    "SxBool": 202,
    "SxErr": 203,
    "SXInt": 204,
    "SXString": 205,
    "SXDtr": 206,
    "SxNil": 207,
    "SXTbl": 208,
    "SXTBRGIITM": 209,
    "SxTbpg": 210,
    "ObProj": 211,
    "SXStreamID": 213,
    "DBCell": 215,
    "SXRng": 216,
    "SxIsxoper": 217,
    "BookBool": 218,
    "DbOrParamQry": 220,
    "ScenarioProtect": 221,
    "OleObjectSize": 222,
    "XF": 224,
    "InterfaceHdr": 225,
    "InterfaceEnd": 226,
    "SXVS": 227,
    "MergeCells": 229,
    "BkHim": 233,
    "MsoDrawingGroup": 235,
    "MsoDrawing": 236,
    "MsoDrawingSelection": 237,
    "PhoneticInfo": 239,
    "SxRule": 240,
    "SXEx": 241,
    "SxFilt": 242,
    "SxDXF": 244,
    "SxItm": 245,
    "SxName": 246,
    "SxSelect": 247,
    "SXPair": 248,
    "SxFmla": 249,
    "SxFormat": 251,
    "SST": 252,
    "LabelSst": 253,
    "ExtSST": 255,
    "SXVDEx": 256,
    "SXFormula": 259,
    "SXDBEx": 290,
    "RRDInsDel": 311,
    "RRDHead": 312,
    "RRDChgCell": 315,
    "RRTabId": 317,
    "RRDRenSheet": 318,
    "RRSort": 319,
    "RRDMove": 320,
    "RRFormat": 330,
    "RRAutoFmt": 331,
    "RRInsertSh": 333,
    "RRDMoveBegin": 334,
    "RRDMoveEnd": 335,
    "RRDInsDelBegin": 336,
    "RRDInsDelEnd": 337,
    "RRDConflict": 338,
    "RRDDefName": 339,
    "RRDRstEtxp": 340,
    "LRng": 351,
    "UsesELFs": 352,
    "DSF": 353,
    "CUsr": 401,
    "CbUsr": 402,
    "UsrInfo": 403,
    "UsrExcl": 404,
    "FileLock": 405,
    "RRDInfo": 406,
    "BCUsrs": 407,
    "UsrChk": 408,
    "UserBView": 425,
    "UserSViewBegin": 426,
    "UserSViewBegin_Chart": 426,
    "UserSViewEnd": 427,
    "RRDUserView": 428,
    "Qsi": 429,
    "SupBook": 430,
    "Prot4Rev": 431,
    "CondFmt": 432,
    "CF": 433,
    "DVal": 434,
    "DConBin": 437,
    "TxO": 438,
    "RefreshAll": 439,
    "HLink": 440,
    "Lel": 441,
    "CodeName": 442,
    "SXFDBType": 443,
    "Prot4RevPass": 444,
    "ObNoMacros": 445,
    "Dv": 446,
    "Excel9File": 448,
    "RecalcId": 449,
    "EntExU2": 450,
    "Dimensions": 512,
    "Blank": 513,
    "Number": 515,
    "Label": 516,
    "BoolErr": 517,
    "String": 519,
    "Row": 520,
    "Index": 523,
    "Array": 545,
    "DefaultRowHeight": 549,
    "Table": 566,
    "Window2": 574,
    "RK": 638,
    "Style": 659,
    "BigName": 1048,
    "Format": 1054,
    "ContinueBigName": 1084,
    "ShrFmla": 1212,
    "HLinkTooltip": 2048,
    "WebPub": 2049,
    "QsiSXTag": 2050,
    "DBQueryExt": 2051,
    "ExtString": 2052,
    "TxtQry": 2053,
    "Qsir": 2054,
    "Qsif": 2055,
    "RRDTQSIF": 2056,
    "BOF": 2057,
    "OleDbConn": 2058,
    "WOpt": 2059,
    "SXViewEx": 2060,
    "SXTH": 2061,
    "SXPIEx": 2062,
    "SXVDTEx": 2063,
    "SXViewEx9": 2064,
    "ContinueFrt": 2066,
    "RealTimeData": 2067,
    "ChartFrtInfo": 2128,
    "FrtWrapper": 2129,
    "StartBlock": 2130,
    "EndBlock": 2131,
    "StartObject": 2132,
    "EndObject": 2133,
    "CatLab": 2134,
    "YMult": 2135,
    "SXViewLink": 2136,
    "PivotChartBits": 2137,
    "FrtFontList": 2138,
    "SheetExt": 2146,
    "BookExt": 2147,
    "SXAddl": 2148,
    "CrErr": 2149,
    "HFPicture": 2150,
    "FeatHdr": 2151,
    "Feat": 2152,
    "DataLabExt": 2154,
    "DataLabExtContents": 2155,
    "CellWatch": 2156,
    "FeatHdr11": 2161,
    "Feature11": 2162,
    "DropDownObjIds": 2164,
    "ContinueFrt11": 2165,
    "DConn": 2166,
    "List12": 2167,
    "Feature12": 2168,
    "CondFmt12": 2169,
    "CF12": 2170,
    "CFEx": 2171,
    "XFCRC": 2172,
    "XFExt": 2173,
    "AutoFilter12": 2174,
    "ContinueFrt12": 2175,
    "MDTInfo": 2180,
    "MDXStr": 2181,
    "MDXTuple": 2182,
    "MDXSet": 2183,
    "MDXProp": 2184,
    "MDXKPI": 2185,
    "MDB": 2186,
    "PLV": 2187,
    "Compat12": 2188,
    "DXF": 2189,
    "TableStyles": 2190,
    "TableStyle": 2191,
    "TableStyleElement": 2192,
    "StyleExt": 2194,
    "NamePublish": 2195,
    "NameCmt": 2196,
    "SortData": 2197,
    "Theme": 2198,
    "GUIDTypeLib": 2199,
    "FnGrp12": 2200,
    "NameFnGrp12": 2201,
    "MTRSettings": 2202,
    "CompressPictures": 2203,
    "HeaderFooter": 2204,
    "CrtLayout12": 2205,
    "CrtMlFrt": 2206,
    "CrtMlFrtContinue": 2207,
    "ForceFullCalculation": 2211,
    "ShapePropsStream": 2212,
    "TextPropsStream": 2213,
    "RichTextStream": 2214,
    "CrtLayout12A": 2215,
    "Units": 4097,
    "Chart": 4098,
    "Series": 4099,
    "DataFormat": 4102,
    "LineFormat": 4103,
    "MarkerFormat": 4105,
    "AreaFormat": 4106,
    "PieFormat": 4107,
    "AttachedLabel": 4108,
    "SeriesText": 4109,
    "ChartFormat": 4116,
    "Legend": 4117,
    "SeriesList": 4118,
    "Bar": 4119,
    "Line": 4120,
    "Pie": 4121,
    "Area": 4122,
    "Scatter": 4123,
    "CrtLine": 4124,
    "Axis": 4125,
    "Tick": 4126,
    "ValueRange": 4127,
    "CatSerRange": 4128,
    "AxisLine": 4129,
    "CrtLink": 4130,
    "DefaultText": 4132,
    "Text": 4133,
    "FontX": 4134,
    "ObjectLink": 4135,
    "Frame": 4146,
    "Begin": 4147,
    "End": 4148,
    "PlotArea": 4149,
    "Chart3d": 4154,
    "PicF": 4156,
    "DropBar": 4157,
    "Radar": 4158,
    "Surf": 4159,
    "RadarArea": 4160,
    "AxisParent": 4161,
    "LegendException": 4163,
    "ShtProps": 4164,
    "SerToCrt": 4165,
    "AxesUsed": 4166,
    "SBaseRef": 4168,
    "SerParent": 4170,
    "SerAuxTrend": 4171,
    "IFmtRecord": 4174,
    "Pos": 4175,
    "AlRuns": 4176,
    "BRAI": 4177,
    "SerAuxErrBar": 4187,
    "ClrtClient": 4188,
    "SerFmt": 4189,
    "Chart3DBarShape": 4191,
    "Fbi": 4192,
    "BopPop": 4193,
    "AxcExt": 4194,
    "Dat": 4195,
    "PlotGrowth": 4196,
    "SIIndex": 4197,
    "GelFrame": 4198,
    "BopPopCustom": 4199,
    "Fbi2": 4200,
}


def _parse_header_RC4(encryptionInfo):
    # RC4: https://msdn.microsoft.com/en-us/library/dd908560(v=office.12).aspx
    salt = encryptionInfo.read(16)
    encryptedVerifier = encryptionInfo.read(16)
    encryptedVerifierHash = encryptionInfo.read(16)
    info = {
        "salt": salt,
        "encryptedVerifier": encryptedVerifier,
        "encryptedVerifierHash": encryptedVerifierHash,
    }
    return info


class _BIFFStream:
    def __init__(self, data):
        self.data = data

    def has_record(self, target):
        pos = self.data.tell()
        while True:
            h = self.data.read(4)
            if not h:
                self.data.seek(pos)
                return False
            num, size = unpack("<HH", h)
            if num == target:
                self.data.seek(pos)
                return True
            else:
                self.data.read(size)

    def skip_to(self, target):
        while True:
            h = self.data.read(4)
            if not h:
                raise exceptions.ParseError("Record not found")
            num, size = unpack("<HH", h)
            if num == target:
                return num, size
            else:
                self.data.read(size)

    def iter_record(self):
        while True:
            h = self.data.read(4)
            if not h:
                break
            num, size = unpack("<HH", h)
            record = io.BytesIO(self.data.read(size))
            yield num, size, record


class Xls97File(base.BaseOfficeFile):
    """Return a MS-XLS file object.

    Examples:
        >>> with open("tests/inputs/rc4cryptoapi_password.xls", "rb") as f:
        ...     officefile = Xls97File(f)
        ...     officefile.load_key(password="Password1234_")

        >>> with open("tests/inputs/xor_password_123456789012345.xls", "rb") as f:
        ...     officefile = Xls97File(f)
        ...     officefile.load_key(password="123456789012345")

        >>> with open("tests/inputs/rc4cryptoapi_password.xls", "rb") as f:
        ...     officefile = Xls97File(f)
        ...     officefile.load_key(password="0000")
        Traceback (most recent call last):
            ...
        msoffcrypto.exceptions.InvalidKeyError: ...
    """

    def __init__(self, file):
        self.file = file
        ole = olefile.OleFileIO(file)  # do not close this, would close file
        self.ole = ole
        self.format = "xls97"
        self.keyTypes = ["password"]
        self.key = None
        self.salt = None

        workbook = ole.openstream("Workbook")  # closed in destructor

        Data = namedtuple("Data", ["workbook"])
        self.data = Data(
            workbook=workbook,
        )

    def __del__(self):
        """Destructor, closes opened stream."""
        if hasattr(self, "data") and self.data and self.data.workbook:
            self.data.workbook.close()

    def load_key(self, password=None):
        self.data.workbook.seek(0)
        workbook = _BIFFStream(self.data.workbook)

        # workbook stream consists of records, each of which begins with its ID number.
        # Record IDs (in decimal) are listed here: https://msdn.microsoft.com/en-us/library/dd945945(v=office.12).aspx
        # workbook stream's structure is WORKBOOK = BOF WORKBOOKCONTENT and so forth
        # as in https://msdn.microsoft.com/en-us/library/dd952177(v=office.12).aspx
        # A record begins with its length (in bytes).

        (num,) = unpack("<H", workbook.data.read(2))

        assert num == 2057  # BOF

        (size,) = unpack("<H", workbook.data.read(2))
        workbook.data.read(size)  # Skip BOF

        num, size = workbook.skip_to(
            recordNameNum["FilePass"]
        )  # Skip to FilePass; TODO: Raise exception if not encrypted

        # FilePass: https://msdn.microsoft.com/en-us/library/dd952596(v=office.12).aspx
        # If this record exists, the workbook MUST be encrypted.
        (wEncryptionType,) = unpack("<H", workbook.data.read(2))

        encryptionInfo = io.BytesIO(workbook.data.read(size - 2))

        if wEncryptionType == 0x0000:  # XOR obfuscation
            key, verificationBytes = unpack("<HH", encryptionInfo.read(4))

            if DocumentXOR.verifypw(password, verificationBytes):
                self.type = "xor"
                self.key = password
                self.loc_index = 0
            else:
                raise exceptions.InvalidKeyError("Failed to verify password")

        elif wEncryptionType == 0x0001:  # RC4
            encryptionVersionInfo = encryptionInfo.read(4)
            vMajor, vMinor = unpack("<HH", encryptionVersionInfo)

            logger.debug("Version: {} {}".format(vMajor, vMinor))

            if vMajor == 0x0001 and vMinor == 0x0001:  # RC4
                info = _parse_header_RC4(encryptionInfo)

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
                    raise exceptions.InvalidKeyError("Failed to verify password")

            elif (
                vMajor in [0x0002, 0x0003, 0x0004] and vMinor == 0x0002
            ):  # RC4 CryptoAPI
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

            else:
                raise exceptions.DecryptionError("Unsupported encryption method")

    def decrypt(self, outfile):
        # fd, _outfile_path = tempfile.mkstemp()

        # shutil.copyfile(os.path.realpath(self.file.name), _outfile_path)
        # outole = olefile.OleFileIO(_outfile_path, write_mode=True)

        # List of encrypted parts: https://msdn.microsoft.com/en-us/library/dd905723(v=office.12).aspx

        # Workbook stream
        self.data.workbook.seek(0)
        workbook = _BIFFStream(self.data.workbook)

        plain_buf = []
        encrypted_buf = io.BytesIO()
        record_info = []

        for i, (num, size, record) in enumerate(workbook.iter_record()):
            # Remove encryption, pad by zero to preserve stream size
            if num == recordNameNum["FilePass"]:
                plain_buf += [0, 0] + list(pack("<H", size)) + [0] * size
                encrypted_buf.write(b"\x00" * (4 + size))
            # The following records MUST NOT be obfuscated or encrypted: BOF (section 2.4.21),
            # FilePass (section 2.4.117), UsrExcl (section 2.4.339), FileLock (section 2.4.116),
            # InterfaceHdr (section 2.4.146), RRDInfo (section 2.4.227), and RRDHead (section 2.4.226).
            elif num in [
                recordNameNum["BOF"],
                recordNameNum["FilePass"],
                recordNameNum["UsrExcl"],
                recordNameNum["FileLock"],
                recordNameNum["InterfaceHdr"],
                recordNameNum["RRDInfo"],
                recordNameNum["RRDHead"],
            ]:
                header = pack("<HH", num, size)
                plain_buf += list(header) + list(record.read())
                encrypted_buf.write(b"\x00" * (4 + size))
            # The lbPlyPos field of the BoundSheet8 record (section 2.4.28) MUST NOT be encrypted.
            elif num == recordNameNum["BoundSheet8"]:
                header = pack("<HH", num, size)
                plain_buf += (
                    list(header) + list(record.read(4)) + [-2] * (size - 4)
                )  # Preserve lbPlyPos
                encrypted_buf.write(b"\x00" * 4 + b"\x00" * 4 + record.read())
            else:
                header = pack("<HH", num, size)
                plain_buf += list(header) + [-1] * size
                encrypted_buf.write(b"\x00" * 4 + record.read())

        self.data_size = encrypted_buf.tell()
        encrypted_buf.seek(0)

        if self.type == "rc4":
            dec = DocumentRC4.decrypt(
                self.key, self.salt, encrypted_buf, blocksize=1024
            )
        elif self.type == "rc4_cryptoapi":
            dec = DocumentRC4CryptoAPI.decrypt(
                self.key, self.salt, self.keySize, encrypted_buf, blocksize=1024
            )
        elif self.type == "xor":
            dec = DocumentXOR.decrypt(
                self.key, encrypted_buf, plain_buf, record_info, 10
            )
        else:
            raise exceptions.DecryptionError(
                "Unsupported encryption method: {}".format(self.type)
            )

        for c in plain_buf:
            if c == -1 or c == -2:
                dec.seek(1, 1)
            else:
                dec.write(bytearray([c]))

        dec.seek(0)

        # f = open('Workbook', 'wb')
        # f.write(dec.read())
        # dec.seek(0)

        workbook_dec = dec

        with tempfile.TemporaryFile() as _outfile:
            self.file.seek(0)
            shutil.copyfileobj(self.file, _outfile)
            outole = olefile.OleFileIO(_outfile, write_mode=True)

            outole.write_stream("Workbook", workbook_dec.read())

            # _outfile = open(_outfile_path, 'rb')

            _outfile.seek(0)

            shutil.copyfileobj(_outfile, outfile)

        return

    def is_encrypted(self):
        r"""
        Test if the file is encrypted.

            >>> f = open("tests/inputs/plain.xls", "rb")
            >>> file = Xls97File(f)
            >>> file.is_encrypted()
            False
            >>> f = open("tests/inputs/rc4cryptoapi_password.xls", "rb")
            >>> file = Xls97File(f)
            >>> file.is_encrypted()
            True
        """
        # Utilising the method above, check for encryption type.
        self.data.workbook.seek(0)
        workbook = _BIFFStream(self.data.workbook)

        (num,) = unpack("<H", workbook.data.read(2))
        assert num == 2057

        (size,) = unpack("<H", workbook.data.read(2))
        workbook.data.read(size)

        if not workbook.has_record(recordNameNum["FilePass"]):
            return False

        num, size = workbook.skip_to(recordNameNum["FilePass"])
        (wEncryptionType,) = unpack("<H", workbook.data.read(2))

        if wEncryptionType == 0x0001:  # RC4
            return True
        elif wEncryptionType == 0x0000:  # XOR obfuscation
            return True
        else:
            return False
