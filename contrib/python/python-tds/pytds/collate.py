import codecs
import struct

TDS_CHARSET_ISO_8859_1 = 1
TDS_CHARSET_CP1251 = 2
TDS_CHARSET_CP1252 = 3
TDS_CHARSET_UCS_2LE = 4

TDS_CHARSET_UNICODE = 5

ucs2_codec = codecs.lookup("utf_16_le")


def sortid2charset(sort_id):
    sql_collate = sort_id
    #
    # The table from the MSQLServer reference "Windows Collation Designators"
    # and from " NLS Information for Microsoft Windows XP"
    #
    if sql_collate in (
        30,  # SQL_Latin1_General_CP437_BIN
        31,  # SQL_Latin1_General_CP437_CS_AS
        32,  # SQL_Latin1_General_CP437_CI_AS
        33,  # SQL_Latin1_General_Pref_CP437_CI_AS
        34,
    ):  # SQL_Latin1_General_CP437_CI_AI
        return "CP437"
    elif sql_collate in (
        40,  # SQL_Latin1_General_CP850_BIN
        41,  # SQL_Latin1_General_CP850_CS_AS
        42,  # SQL_Latin1_General_CP850_CI_AS
        43,  # SQL_Latin1_General_Pref_CP850_CI_AS
        44,  # SQL_Latin1_General_CP850_CI_AI
        49,  # SQL_1xCompat_CP850_CI_AS
        55,  # SQL_AltDiction_CP850_CS_AS
        56,  # SQL_AltDiction_Pref_CP850_CI_AS
        57,  # SQL_AltDiction_CP850_CI_AI
        58,  # SQL_Scandinavian_Pref_CP850_CI_AS
        59,  # SQL_Scandinavian_CP850_CS_AS
        60,  # SQL_Scandinavian_CP850_CI_AS
        61,
    ):  # SQL_AltDiction_CP850_CI_AS
        return "CP850"
    elif sql_collate in (
        80,  # SQL_Latin1_General_1250_BIN
        81,  # SQL_Latin1_General_CP1250_CS_AS
        82,  # SQL_Latin1_General_Cp1250_CI_AS_KI_WI
        83,  # SQL_Czech_Cp1250_CS_AS_KI_WI
        84,  # SQL_Czech_Cp1250_CI_AS_KI_WI
        85,  # SQL_Hungarian_Cp1250_CS_AS_KI_WI
        86,  # SQL_Hungarian_Cp1250_CI_AS_KI_WI
        87,  # SQL_Polish_Cp1250_CS_AS_KI_WI
        88,  # SQL_Polish_Cp1250_CI_AS_KI_WI
        89,  # SQL_Romanian_Cp1250_CS_AS_KI_WI
        90,  # SQL_Romanian_Cp1250_CI_AS_KI_WI
        91,  # SQL_Croatian_Cp1250_CS_AS_KI_WI
        92,  # SQL_Croatian_Cp1250_CI_AS_KI_WI
        93,  # SQL_Slovak_Cp1250_CS_AS_KI_WI
        94,  # SQL_Slovak_Cp1250_CI_AS_KI_WI
        95,  # SQL_Slovenian_Cp1250_CS_AS_KI_WI
        96,  # SQL_Slovenian_Cp1250_CI_AS_KI_WI
    ):
        return "CP1250"
    elif sql_collate in (
        104,  # SQL_Latin1_General_1251_BIN
        105,  # SQL_Latin1_General_CP1251_CS_AS
        106,  # SQL_Latin1_General_CP1251_CI_AS
        107,  # SQL_Ukrainian_Cp1251_CS_AS_KI_WI
        108,  # SQL_Ukrainian_Cp1251_CI_AS_KI_WI
    ):
        return "CP1251"
    elif sql_collate in (
        51,  # SQL_Latin1_General_Cp1_CS_AS_KI_WI
        52,  # SQL_Latin1_General_Cp1_CI_AS_KI_WI
        53,  # SQL_Latin1_General_Pref_Cp1_CI_AS_KI_WI
        54,  # SQL_Latin1_General_Cp1_CI_AI_KI_WI
        183,  # SQL_Danish_Pref_Cp1_CI_AS_KI_WI
        184,  # SQL_SwedishPhone_Pref_Cp1_CI_AS_KI_WI
        185,  # SQL_SwedishStd_Pref_Cp1_CI_AS_KI_WI
        186,  # SQL_Icelandic_Pref_Cp1_CI_AS_KI_WI
    ):
        return "CP1252"
    elif sql_collate in (
        112,  # SQL_Latin1_General_1253_BIN
        113,  # SQL_Latin1_General_CP1253_CS_AS
        114,  # SQL_Latin1_General_CP1253_CI_AS
        120,  # SQL_MixDiction_CP1253_CS_AS
        121,  # SQL_AltDiction_CP1253_CS_AS
        122,  # SQL_AltDiction2_CP1253_CS_AS
        124,  # SQL_Latin1_General_CP1253_CI_AI
    ):
        return "CP1253"
    elif sql_collate in (
        128,  # SQL_Latin1_General_1254_BIN
        129,  # SQL_Latin1_General_Cp1254_CS_AS_KI_WI
        130,  # SQL_Latin1_General_Cp1254_CI_AS_KI_WI
    ):
        return "CP1254"
    elif sql_collate in (
        136,  # SQL_Latin1_General_1255_BIN
        137,  # SQL_Latin1_General_CP1255_CS_AS
        138,  # SQL_Latin1_General_CP1255_CI_AS
    ):
        return "CP1255"
    elif sql_collate in (
        144,  # SQL_Latin1_General_1256_BIN
        145,  # SQL_Latin1_General_CP1256_CS_AS
        146,  # SQL_Latin1_General_CP1256_CI_AS
    ):
        return "CP1256"
    elif sql_collate in (
        152,  # SQL_Latin1_General_1257_BIN
        153,  # SQL_Latin1_General_CP1257_CS_AS
        154,  # SQL_Latin1_General_CP1257_CI_AS
        155,  # SQL_Estonian_Cp1257_CS_AS_KI_WI
        156,  # SQL_Estonian_Cp1257_CI_AS_KI_WI
        157,  # SQL_Latvian_Cp1257_CS_AS_KI_WI
        158,  # SQL_Latvian_Cp1257_CI_AS_KI_WI
        159,  # SQL_Lithuanian_Cp1257_CS_AS_KI_WI
        160,  # SQL_Lithuanian_Cp1257_CI_AS_KI_WI
    ):
        return "CP1257"
    else:
        raise Exception("Invalid collation: 0x%X" % (sql_collate,))


def lcid2charset(lcid):
    if lcid in (
        0x405,
        0x40E,  # 0x1040e
        0x415,
        0x418,
        0x41A,
        0x41B,
        0x41C,
        0x424,
        # 0x81a, seem wrong in XP table TODO check
        0x104E,
    ):
        return "CP1250"
    elif lcid in (
        0x402,
        0x419,
        0x422,
        0x423,
        0x42F,
        0x43F,
        0x440,
        0x444,
        0x450,
        0x81A,  # ??
        0x82C,
        0x843,
        0xC1A,
    ):
        return "CP1251"
    elif lcid in (
        0x1007,
        0x1009,
        0x100A,
        0x100C,
        0x1407,
        0x1409,
        0x140A,
        0x140C,
        0x1809,
        0x180A,
        0x180C,
        0x1C09,
        0x1C0A,
        0x2009,
        0x200A,
        0x2409,
        0x240A,
        0x2809,
        0x280A,
        0x2C09,
        0x2C0A,
        0x3009,
        0x300A,
        0x3409,
        0x340A,
        0x380A,
        0x3C0A,
        0x400A,
        0x403,
        0x406,
        0x407,  # 0x10407
        0x409,
        0x40A,
        0x40B,
        0x40C,
        0x40F,
        0x410,
        0x413,
        0x414,
        0x416,
        0x41D,
        0x421,
        0x42D,
        0x436,
        0x437,  # 0x10437
        0x438,
        # 0x439,  ??? Unicode only
        0x43E,
        0x440A,
        0x441,
        0x456,
        0x480A,
        0x4C0A,
        0x500A,
        0x807,
        0x809,
        0x80A,
        0x80C,
        0x810,
        0x813,
        0x814,
        0x816,
        0x81D,
        0x83E,
        0xC07,
        0xC09,
        0xC0A,
        0xC0C,
    ):
        return "CP1252"
    elif lcid == 0x408:
        return "CP1253"
    elif lcid in (0x41F, 0x42C, 0x443):
        return "CP1254"
    elif lcid == 0x40D:
        return "CP1255"
    elif lcid in (
        0x1001,
        0x1401,
        0x1801,
        0x1C01,
        0x2001,
        0x2401,
        0x2801,
        0x2C01,
        0x3001,
        0x3401,
        0x3801,
        0x3C01,
        0x4001,
        0x401,
        0x420,
        0x429,
        0x801,
        0xC01,
    ):
        return "CP1256"
    elif lcid in (0x425, 0x426, 0x427, 0x827):  # ??
        return "CP1257"
    elif lcid == 0x42A:
        return "CP1258"
    elif lcid == 0x41E:
        return "CP874"
    elif lcid == 0x411:  # 0x10411
        return "CP932"
    elif lcid in (0x1004, 0x804):  # 0x20804
        return "CP936"
    elif lcid == 0x412:  # 0x10412
        return "CP949"
    elif lcid in (
        0x1404,
        0x404,  # 0x30404
        0xC04,
    ):
        return "CP950"
    else:
        return "CP1252"


class Collation(object):
    _coll_struct = struct.Struct("<LB")
    wire_size = _coll_struct.size
    f_ignore_case = 0x100000
    f_ignore_accent = 0x200000
    f_ignore_width = 0x400000
    f_ignore_kana = 0x800000
    f_binary = 0x1000000
    f_binary2 = 0x2000000

    def __init__(
        self,
        lcid,
        sort_id,
        ignore_case,
        ignore_accent,
        ignore_width,
        ignore_kana,
        binary,
        binary2,
        version,
    ):
        self.lcid = lcid
        self.sort_id = sort_id
        self.ignore_case = ignore_case
        self.ignore_accent = ignore_accent
        self.ignore_width = ignore_width
        self.ignore_kana = ignore_kana
        self.binary = binary
        self.binary2 = binary2
        self.version = version

    def __repr__(self):
        fmt = (
            "Collation(lcid={0}, sort_id={1}, ignore_case={2}, ignore_accent={3}, ignore_width={4},"
            " ignore_kana={5}, binary={6}, binary2={7}, version={8})"
        )
        return fmt.format(
            self.lcid,
            self.sort_id,
            self.ignore_case,
            self.ignore_accent,
            self.ignore_width,
            self.ignore_kana,
            self.binary,
            self.binary2,
            self.version,
        )

    @classmethod
    def unpack(cls, b):
        lump, sort_id = cls._coll_struct.unpack_from(b)
        lcid = lump & 0xFFFFF
        ignore_case = bool(lump & cls.f_ignore_case)
        ignore_accent = bool(lump & cls.f_ignore_accent)
        ignore_width = bool(lump & cls.f_ignore_width)
        ignore_kana = bool(lump & cls.f_ignore_kana)
        binary = bool(lump & cls.f_binary)
        binary2 = bool(lump & cls.f_binary2)
        version = (lump & 0xF0000000) >> 26
        return cls(
            lcid=lcid,
            ignore_case=ignore_case,
            ignore_accent=ignore_accent,
            ignore_width=ignore_width,
            ignore_kana=ignore_kana,
            binary=binary,
            binary2=binary2,
            version=version,
            sort_id=sort_id,
        )

    def pack(self):
        lump = 0
        lump |= self.lcid & 0xFFFFF
        lump |= (self.version << 26) & 0xF0000000
        if self.ignore_case:
            lump |= self.f_ignore_case
        if self.ignore_accent:
            lump |= self.f_ignore_accent
        if self.ignore_width:
            lump |= self.f_ignore_width
        if self.ignore_kana:
            lump |= self.f_ignore_kana
        if self.binary:
            lump |= self.f_binary
        if self.binary2:
            lump |= self.f_binary2
        return self._coll_struct.pack(lump, self.sort_id)

    def get_charset(self):
        if self.sort_id:
            return sortid2charset(self.sort_id)
        else:
            return lcid2charset(self.lcid)

    def get_codec(self):
        return codecs.lookup(self.get_charset())

    # TODO: define __repr__ and __unicode__


raw_collation = Collation(0, 0, 0, 0, 0, 0, 0, 0, 0)
