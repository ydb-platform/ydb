"""Module EEPROM parsing and decoding"""

from enum import Enum, IntEnum
from struct import unpack
from typing import Union

# Generic I2C address for low page of all modules,
# where the module identifier can be found,
# from ethtool project, netlink/module-eeprom.c file
ETH_I2C_ADDRESS_LOW = 0x50

# SFF 8079 specific values,
# from ethtool project, sfpid.c file
SFF8079_PAGE_SIZE = 0x80
SFF8079_I2C_ADDRESS_LOW = ETH_I2C_ADDRESS_LOW
SFF8079_I2C_ADDRESS_HIGH = 0x51

# Length of SFF fields containing array,
# from SFF-8472 specification
SFF_FIELD_VENDOR_NAME_LEN = 16
SFF_FIELD_VENDOR_OUI_LEN = 3
SFF_FIELD_VENDOR_PN_LEN = 16
SFF_FIELD_VENDOR_REV_LEN = 4
SFF_FIELD_VENDOR_SN_LEN = 16
SFF_FIELD_DATE_CODE_LEN = 8

# Offset at which the A2 page is in the EEPROM blob returned by the kernel,
# from ethtool project, sfpdiag.c file
SFF_PAGE_A2_BASE = 0x100


class ModuleInfoField(Enum):
    """List of parsed fields describing the module information"""

    IDENTIFIER = "Identifier"
    EXTENDED_IDENTIFIER = "Extended identifier"
    CONNECTOR = "Connector"
    TRANSCEIVER_TYPE = "Transceiver type"
    ENCODING = "Encoding"
    BR_NOMINAL = "BR, Nominal, in MBd"
    BR_MIN = "BR margin, max, in %"
    BR_MAX = "BR margin, min, in %"
    RATE_IDENTIFIER = "Rate identifier"
    LENGTH_SFM_KM = "Length (SMF), in km"
    LENGTH_SFM_100M = "Length (SMF), in m"
    LENGTH_50UM_10M = "Length (50um), in m"
    LENGTH_62_5UM_10M = "Length (62.5um), in m"
    LENGTH_COPPER = "Length (Copper), in m"
    LENGTH_OM3_10M = "Length (OM3), in m"
    VENDOR_NAME = "Vendor name"
    VENDOR_OUI = "Vendor OUI"
    VENDOR_PN = "Vendor PN"
    VENDOR_REV = "Vendor rev"
    PASSIVE_COPPER_COMP = "Passive Cu compliance"
    ACTIVE_COPPER_COMP = "Active Cu compliance"
    LASER_WAVELENGTH = "Laser wavelength, in nm"
    OPTION = "Option"
    VENDOR_SN = "Vendor SN"
    DATE_CODE = "Date code"
    DIAG_MON_SUPPORT = "Optical diagnostics support"
    ALARM_SUPPORT = "Alarm/warning flags implemented"
    TEMP = "Module temperature, in °C"
    TEMP_HALRM = "Module temperature high alarm"
    TEMP_LALRM = "Module temperature low alarm"
    TEMP_HWARN = "Module temperature high warning"
    TEMP_LWARN = "Module temperature low warning"
    BIAS = "Laser bias current, in mA"
    BIAS_HALRM = "Laser bias current high alarm"
    BIAS_LALRM = "Laser bias current low alarm"
    BIAS_HWARN = "Laser bias current high warning"
    BIAS_LWARN = "Laser bias current low warning"
    VCC = "Module voltage, in V"
    VCC_HALRM = "Module voltage high alarm"
    VCC_LALRM = "Module voltage low alarm"
    VCC_HWARN = "Module voltage high warning"
    VCC_LWARN = "Module voltage low warning"
    TX_PWR = "Laser output power, in mW"
    TX_PWR_HALRM = "Laser output power high alarm"
    TX_PWR_LALRM = "Laser output power low alarm"
    TX_PWR_HWARN = "Laser output power high warning"
    TX_PWR_LWARN = "Laser output power low warning"
    RX_PWR = "Receiver signal, in mW"
    RX_PWR_HALRM = "Laser rx power high alarm"
    RX_PWR_LALRM = "Laser rx power low alarm"
    RX_PWR_HWARN = "Laser rx power high warning"
    RX_PWR_LWARN = "Laser rx power low warning"


class SffModuleType(IntEnum):
    """EEPROM Standards for plug in modules,
    from Linux kernel, include/uapi/linux/ethtool.h file"""

    SFF_8079 = 1
    SFF_8472 = 2
    SFF_8636 = 3
    SFF_8436 = 4


class SffModulePageLength(IntEnum):
    """Module EEPROM page length,
    from Linux kernel, include/uapi/linux/ethtool.h file"""

    SFF_8079_LEN = 256
    SFF_8472_LEN = 512
    SFF_8636_LEN = 256
    SFF_8436_LEN = 256


class SffOffset(IntEnum):
    """Offset of each data field, defined in
    SFF-8472 specification, section 4.4"""

    ID = 0
    ID_EXT = 1
    CONNECTOR = 2
    TRANSCEIVER_3 = 3
    TRANSCEIVER_4 = 4
    TRANSCEIVER_5 = 5
    TRANSCEIVER_6 = 6
    TRANSCEIVER_7 = 7
    TRANSCEIVER_8 = 8
    TRANSCEIVER_9 = 9
    TRANSCEIVER_10 = 10
    ENCODING = 11
    BR_NOMINAL = 12
    RATE_IDENTIFIER = 13
    LENGTH_SFM_KM = 14
    LENGTH_SFM_100M = 15
    LENGTH_50UM_10M = 16
    LENGTH_62_5UM_10M = 17
    LENGTH_COPPER = 18
    LENGTH_OM3_10M = 19
    VENDOR_NAME = 20  # 16 bytes
    TRANSCEIVER_36 = 36
    VENDOR_OUI = 37  # 3 bytes
    VENDOR_PN = 40  # 16 bytes
    VENDOR_REV = 56  # 4 bytes
    LINK_CHAR_60 = 60
    LINK_CHAR_61 = 61
    # 62 is reserved
    CC_BASE = 63
    OPTIONS_64 = 64
    OPTIONS_65 = 65
    BR_MAX = 66
    BR_MIN = 67
    VENDOR_SN = 68  # 16 bytes
    DATE_CODE = 84  # 8 bytes
    DIAG_MON = 92
    ENHANCED_OPTIONS = 93
    SFF_8472_COMPLIANCE = 94
    CC_EXT = 95
    TEMP_HALRM = SFF_PAGE_A2_BASE + 0
    TEMP_LALRM = SFF_PAGE_A2_BASE + 2
    TEMP_HWARN = SFF_PAGE_A2_BASE + 4
    TEMP_LWARN = SFF_PAGE_A2_BASE + 6
    VCC_HALRM = SFF_PAGE_A2_BASE + 8
    VCC_LALRM = SFF_PAGE_A2_BASE + 10
    VCC_HWARN = SFF_PAGE_A2_BASE + 12
    VCC_LWARN = SFF_PAGE_A2_BASE + 14
    BIAS_HALRM = SFF_PAGE_A2_BASE + 16
    BIAS_LALRM = SFF_PAGE_A2_BASE + 18
    BIAS_HWARN = SFF_PAGE_A2_BASE + 20
    BIAS_LWARN = SFF_PAGE_A2_BASE + 22
    TX_PWR_HALRM = SFF_PAGE_A2_BASE + 24
    TX_PWR_LALRM = SFF_PAGE_A2_BASE + 26
    TX_PWR_HWARN = SFF_PAGE_A2_BASE + 28
    TX_PWR_LWARN = SFF_PAGE_A2_BASE + 30
    RX_PWR_HALRM = SFF_PAGE_A2_BASE + 32
    RX_PWR_LALRM = SFF_PAGE_A2_BASE + 34
    RX_PWR_HWARN = SFF_PAGE_A2_BASE + 36
    RX_PWR_LWARN = SFF_PAGE_A2_BASE + 38
    TEMP = SFF_PAGE_A2_BASE + 96
    VCC = SFF_PAGE_A2_BASE + 98
    BIAS = SFF_PAGE_A2_BASE + 100
    TX_PWR = SFF_PAGE_A2_BASE + 102
    RX_PWR = SFF_PAGE_A2_BASE + 104
    ALRM_FLG_1 = SFF_PAGE_A2_BASE + 112
    ALRM_FLG_2 = SFF_PAGE_A2_BASE + 113
    WARN_FLG_1 = SFF_PAGE_A2_BASE + 116
    WARN_FLG_2 = SFF_PAGE_A2_BASE + 117


class Sff8024Id(IntEnum):
    """Module identifiers, defined in SFF-8024 specification, section 4.2"""

    UNKNOWN = 0x00
    GBIC = 0x01
    SOLDERED_MODULE = 0x02
    SFP = 0x03
    XBI_300_PIN = 0x04
    XENPAK = 0x05
    XFP = 0x06
    XFF = 0x07
    XFP_E = 0x08
    XPAK = 0x09
    X2 = 0x0A
    DWDM_SFP = 0x0B
    QSFP = 0x0C
    QSFP_PLUS = 0x0D
    CXP = 0x0E
    HD4X = 0x0F
    HD8X = 0x10
    QSFP28 = 0x11
    CXP2 = 0x12
    CDFP = 0x13
    HD4X_FANOUT = 0x14
    HD8X_FANOUT = 0x15
    CDFP_S3 = 0x16
    MICRO_QSFP = 0x17
    QSFP_DD = 0x18
    OSFP = 0x19
    DSFP = 0x1B
    QSFP_PLUS_CMIS = 0x1E
    SFP_DD_CMIS = 0x1F
    SFP_PLUS_CMIS = 0x20
    UNALLOCATED_LAST = 0x7F
    VENDOR_START = 0x80
    VENDOR_LAST = 0xFF

    @property
    def last(self):
        """Last allocated id"""
        return self.SFP_PLUS_CMIS


class Sff8024Connector(IntEnum):
    """Connector references, defined in SFF-8024 specification, section 4.4"""

    UNKNOWN = 0x00
    SC = 0x01
    FC_STYLE_1 = 0x02
    FC_STYLE_2 = 0x03
    BNC_TNC = 0x04
    FC_COAX = 0x05
    FIBER_JACK = 0x06
    LC = 0x07
    MT_RJ = 0x08
    MU = 0x09
    SG = 0x0A
    OPT_PT = 0x0B
    MPO = 0x0C
    MPO_2 = 0x0D
    # 0E-1F are reserved
    HSDC_II = 0x20
    COPPER_PT = 0x21
    RJ45 = 0x22
    NO_SEPARABLE = 0x23
    MXC_2X16 = 0x24
    CS_OPTICAL = 0x25
    CS_OPTICAL_MINI = 0x26
    MPO_2X12 = 0x27
    MPO_1X16 = 0x28
    NO_SEP_QSFP_DD = 0x6F
    UNALLOCATED_LAST = 0x7F
    VENDOR_START = 0x80
    VENDOR_LAST = 0xFF

    @property
    def last(self):
        """Last allocated id"""
        return self.MPO_1X16


class Sff8024Encoding(IntEnum):
    """Encoding references, defined in SFF-8024 specification, section 4.3"""

    CODE_UNSPEC = 0x00
    CODE_8B10B = 0x01
    CODE_4B5B = 0x02
    CODE_NRZ = 0x03
    # SFF-8472      - Manchester
    # SFF-8436/8636 - SONET Scrambled
    CODE_04 = 0x04
    # SFF-8472      - SONET Scrambled
    # SFF-8436/8636 - 64B/66B
    CODE_05 = 0x05
    # SFF-8472      - 64B/66B
    # SFF-8436/8636 - Manchester
    CODE_06 = 0x06
    CODE_256B = 0x07
    CODE_PAM4 = 0x08


class Sff8024PowerMeas(IntEnum):
    """Power measurement type, defined in
    SFF-8472 specification, section 8.8
    """

    OMA = 0
    AVERAGE_POWER = 1


class Sff8024AlarmWarningSrc(Enum):
    """Alarm and warning source, defined in
    SFF-8472 specification, section 9.4
    """

    TEMP = "TEMP"
    VOLTAGE = "VCC"
    BIAS = "BIAS"
    TX_POWER = "TX_PWR"
    RX_POWER = "RX_PWR"


class Sff8024AlarmWarningType(Enum):
    """Alarm and warning type, defined in
    SFF-8472 specification, section 9.4
    """

    HIGH_ALARM = "HALRM"
    LOW_ALARM = "LALRM"
    HIGH_WARNING = "HWARN"
    LOW_WARNING = "LWARN"


def _is_bit_set(eeprom: tuple[int], offset: int, bit: int) -> bool:
    """Generic function to check if a bit is set in the EEPROM data

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :param offset: byte offset
    :type offset: int
    :param bit: bit to check in the byte
    :type bit: int
    :return: bit set or not
    :rtype: bool
    """
    return eeprom[offset] & (1 << bit) != 0


def _sff8079_get_array(
    eeprom: tuple[int], start: int, length: int, decode: bool = True
) -> str:
    """Generic function to extract an array from the EEPROM data

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :param start: array start
    :type start: int
    :param length: array length
    :type length: int
    :param decode: decode the content as an ASCII string, defaults to True
    :type decode: bool, optional
    :return: extracted array
    :rtype: str
    """
    array = eeprom[start : start + length]

    if decode:
        array_desc = bytearray(array).decode().strip()
    else:
        array_desc = ":".join([f"{x:02x}" for x in array])

    return array_desc


def _sff8472_get_temp(eeprom: tuple[int], offset: int) -> float:
    """Extract temperature from EEPROM array

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :param offset: offset to read
    :type offset: int
    :return: temperature in °C
    :rtype: float
    """
    return round(
        unpack("!h", bytearray([eeprom[offset], eeprom[offset + 1]]))[0] / 256,
        2,
    )


def _sff8472_get_bias(eeprom: tuple[int], offset: int) -> float:
    """Extract bias current from EEPROM array

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :param offset: offset to read
    :type offset: int
    :return: bias current in mA
    :rtype: float
    """
    return (eeprom[offset] << 8 | eeprom[offset + 1]) / 500


def _sff8472_get_voltage_or_power(eeprom: tuple[int], offset: int) -> float:
    """Extract voltage or power from EEPROM array

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :param offset: offset to read
    :type offset: int
    :return: voltage in V or power in mW
    :rtype: float
    """
    return (eeprom[offset] << 8 | eeprom[offset + 1]) / 10000


def sff8024_get_identifier(
    eeprom: tuple[int], id_offset: int = SffOffset.ID
) -> tuple[str, str]:
    """Parsing of module identifier, defined
    in SFF-8024 specification, section 4.2

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :param id_offset: offset of identifier byte
    :type id_offset: int
    :return: parsed description and value
    :rtype: tuple[str, str]
    """
    module_id = eeprom[id_offset]
    if module_id == Sff8024Id.UNKNOWN:
        module_desc = "no module present, unknown, or unspecified"
    elif module_id == Sff8024Id.GBIC:
        module_desc = "GBIC"
    elif module_id == Sff8024Id.SOLDERED_MODULE:
        module_desc = "module soldered to motherboard"
    elif module_id == Sff8024Id.SFP:
        module_desc = "SFP"
    elif module_id == Sff8024Id.XBI_300_PIN:
        module_desc = "300 pin XBI"
    elif module_id == Sff8024Id.XENPAK:
        module_desc = "XENPAK"
    elif module_id == Sff8024Id.XFP:
        module_desc = "XFP"
    elif module_id == Sff8024Id.XFF:
        module_desc = "XFF"
    elif module_id == Sff8024Id.XFP_E:
        module_desc = "XFP-E"
    elif module_id == Sff8024Id.XPAK:
        module_desc = "XPAK"
    elif module_id == Sff8024Id.X2:
        module_desc = "X2"
    elif module_id == Sff8024Id.DWDM_SFP:
        module_desc = "DWDM-SFP"
    elif module_id == Sff8024Id.QSFP:
        module_desc = "QSFP"
    elif module_id == Sff8024Id.QSFP_PLUS:
        module_desc = "QSFP+"
    elif module_id == Sff8024Id.CXP:
        module_desc = "CXP"
    elif module_id == Sff8024Id.HD4X:
        module_desc = "Shielded Mini Multilane HD 4X"
    elif module_id == Sff8024Id.HD8X:
        module_desc = "Shielded Mini Multilane HD 8X"
    elif module_id == Sff8024Id.QSFP28:
        module_desc = "QSFP28"
    elif module_id == Sff8024Id.CXP2:
        module_desc = "CXP2/CXP28"
    elif module_id == Sff8024Id.CDFP:
        module_desc = "CDFP Style 1/Style 2"
    elif module_id == Sff8024Id.HD4X_FANOUT:
        module_desc = "Shielded Mini Multilane HD 4X Fanout Cable"
    elif module_id == Sff8024Id.HD8X_FANOUT:
        module_desc = "Shielded Mini Multilane HD 8X Fanout Cable"
    elif module_id == Sff8024Id.CDFP_S3:
        module_desc = "CDFP Style 3"
    elif module_id == Sff8024Id.MICRO_QSFP:
        module_desc = "microQSFP"
    elif module_id == Sff8024Id.QSFP_DD:
        module_desc = (
            "QSFP-DD Double Density 8X Pluggable Transceiver (INF-8628)"
        )
    elif module_id == Sff8024Id.OSFP:
        module_desc = "OSFP 8X Pluggable Transceiver"
    elif module_id == Sff8024Id.DSFP:
        module_desc = "DSFP Dual Small Form Factor Pluggable Transceiver"
    elif module_id == Sff8024Id.QSFP_PLUS_CMIS:
        module_desc = (
            "QSFP+ or later with Common Management"
            " Interface Specification (CMIS)"
        )
    elif module_id == Sff8024Id.SFP_DD_CMIS:
        module_desc = (
            "SFP-DD Double Density 2X Pluggable Transceiver with"
            " Common Management Interface Specification (CMIS)"
        )
    elif module_id == Sff8024Id.SFP_PLUS_CMIS:
        module_desc = (
            "SFP+ and later with Common Management"
            " Interface Specification (CMIS)"
        )
    else:
        module_desc = "reserved or unknown"

    return (ModuleInfoField.IDENTIFIER.value, module_desc)


def sff8079_get_ext_identifier(eeprom: tuple[int]) -> tuple[str, str]:
    """Parsing of extended module identifier, defined
    in SFF-8472 specification, section 5.2

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, str]
    """
    ext_id = eeprom[SffOffset.ID_EXT]
    if ext_id == 0x00:
        ext_id_desc = "GBIC not specified / not MOD_DEF compliant"
    elif ext_id == 0x04:
        ext_id_desc = "GBIC/SFP defined by 2-wire interface ID"
    elif ext_id <= 0x07:
        ext_id_desc = f"GBIC compliant with MOD_DEF {ext_id}"
    else:
        ext_id_desc = "unknown"

    return (ModuleInfoField.EXTENDED_IDENTIFIER.value, ext_id_desc)


def sff8024_get_connector(
    eeprom: tuple[int], offset: int = SffOffset.CONNECTOR
) -> tuple[str, str]:
    """Parsing of connector reference, defined
    in SFF-8024 specification, section 4.4

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :param offset: offset of connector byte
    :type offset: int
    :return: parsed description and value
    :rtype: tuple[str, str]
    """
    connector_id = eeprom[offset]

    if connector_id == Sff8024Connector.UNKNOWN:
        connector_desc = "unknown or unspecified"
    elif connector_id == Sff8024Connector.SC:
        connector_desc = "SC"
    elif connector_id == Sff8024Connector.FC_STYLE_1:
        connector_desc = "Fibre Channel Style 1 copper"
    elif connector_id == Sff8024Connector.FC_STYLE_2:
        connector_desc = "Fibre Channel Style 2 copper"
    elif connector_id == Sff8024Connector.BNC_TNC:
        connector_desc = "BNC/TNC"
    elif connector_id == Sff8024Connector.FC_COAX:
        connector_desc = "Fibre Channel coaxial headers"
    elif connector_id == Sff8024Connector.FIBER_JACK:
        connector_desc = "FibreJack"
    elif connector_id == Sff8024Connector.LC:
        connector_desc = "LC"
    elif connector_id == Sff8024Connector.MT_RJ:
        connector_desc = "MT-RJ"
    elif connector_id == Sff8024Connector.MU:
        connector_desc = "MU"
    elif connector_id == Sff8024Connector.SG:
        connector_desc = "SG"
    elif connector_id == Sff8024Connector.OPT_PT:
        connector_desc = "Optical pigtail"
    elif connector_id == Sff8024Connector.MPO:
        connector_desc = "MPO Parallel Optic"
    elif connector_id == Sff8024Connector.MPO_2:
        connector_desc = "MPO Parallel Optic - 2x16"
    elif connector_id == Sff8024Connector.HSDC_II:
        connector_desc = "HSSDC II"
    elif connector_id == Sff8024Connector.COPPER_PT:
        connector_desc = "Copper pigtail"
    elif connector_id == Sff8024Connector.RJ45:
        connector_desc = "RJ45"
    elif connector_id == Sff8024Connector.NO_SEPARABLE:
        connector_desc = "No separable connector"
    elif connector_id == Sff8024Connector.MXC_2X16:
        connector_desc = "MXC 2x16"
    elif connector_id == Sff8024Connector.CS_OPTICAL:
        connector_desc = "CS optical connector"
    elif connector_id == Sff8024Connector.CS_OPTICAL_MINI:
        connector_desc = "Mini CS optical connector"
    elif connector_id == Sff8024Connector.MPO_2X12:
        connector_desc = "MPO 2x12"
    elif connector_id == Sff8024Connector.MPO_1X16:
        connector_desc = "MPO 1x16"
    else:
        connector_desc = "reserved or unknown"

    return (ModuleInfoField.CONNECTOR.value, connector_desc)


def sff8079_get_transceiver(eeprom: tuple[int]) -> tuple[str, list[str]]:
    """Parsing of transceiver reference, defined
    in SFF-8472 specification, section 5.4

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, list[str]]
    """
    transceiver_type = []
    for offset, bit, type_str in (
        (
            SffOffset.TRANSCEIVER_3,
            7,
            "10G Ethernet: 10G Base-ER [SFF-8472 rev10.4 onwards]",
        ),
        (SffOffset.TRANSCEIVER_3, 6, "10G Ethernet: 10G Base-LRM"),
        (SffOffset.TRANSCEIVER_3, 5, "10G Ethernet: 10G Base-LR"),
        (SffOffset.TRANSCEIVER_3, 4, "10G Ethernet: 10G Base-SR"),
        (SffOffset.TRANSCEIVER_3, 3, "Infiniband: 1X SX"),
        (SffOffset.TRANSCEIVER_3, 2, "Infiniband: 1X LX"),
        (SffOffset.TRANSCEIVER_3, 1, "Infiniband: 1X Copper Active"),
        (SffOffset.TRANSCEIVER_3, 0, "Infiniband: 1X Copper Passive"),
        # ESCON Compliance Codes
        (SffOffset.TRANSCEIVER_4, 7, "ESCON: ESCON MMF, 1310nm LED"),
        (SffOffset.TRANSCEIVER_4, 6, "ESCON: ESCON SMF, 1310nm Laser"),
        # SONET Compliance Codes
        (SffOffset.TRANSCEIVER_4, 5, "SONET: OC-192, short reach"),
        (SffOffset.TRANSCEIVER_4, 4, "SONET: SONET reach specifier bit 1"),
        (SffOffset.TRANSCEIVER_4, 3, "SONET: SONET reach specifier bit 2"),
        (SffOffset.TRANSCEIVER_4, 2, "SONET: OC-48, long reach"),
        (SffOffset.TRANSCEIVER_4, 1, "SONET: OC-48, intermediate reach"),
        (SffOffset.TRANSCEIVER_4, 0, "SONET: OC-48, short reach"),
        (SffOffset.TRANSCEIVER_5, 6, "SONET: OC-12, single mode, long reach"),
        (
            SffOffset.TRANSCEIVER_5,
            5,
            "SONET: OC-12, single mode, inter. reach",
        ),
        (SffOffset.TRANSCEIVER_5, 4, "SONET: OC-12, short reach"),
        (SffOffset.TRANSCEIVER_5, 2, "SONET: OC-3, single mode, long reach"),
        (SffOffset.TRANSCEIVER_5, 1, "SONET: OC-3, single mode, inter. reach"),
        (SffOffset.TRANSCEIVER_5, 0, "SONET: OC-3, short reach"),
        # Ethernet Compliance Codes
        (SffOffset.TRANSCEIVER_6, 7, "Ethernet: BASE-PX"),
        (SffOffset.TRANSCEIVER_6, 6, "Ethernet: BASE-BX10"),
        (SffOffset.TRANSCEIVER_6, 5, "Ethernet: 100BASE-FX"),
        (SffOffset.TRANSCEIVER_6, 4, "Ethernet: 100BASE-LX/LX10"),
        (SffOffset.TRANSCEIVER_6, 3, "Ethernet: 1000BASE-T"),
        (SffOffset.TRANSCEIVER_6, 2, "Ethernet: 1000BASE-CX"),
        (SffOffset.TRANSCEIVER_6, 1, "Ethernet: 1000BASE-LX"),
        (SffOffset.TRANSCEIVER_6, 0, "Ethernet: 1000BASE-SX"),
        # Fibre Channel link length
        (SffOffset.TRANSCEIVER_7, 7, "FC: very long distance (V)"),
        (SffOffset.TRANSCEIVER_7, 6, "FC: short distance (S)"),
        (SffOffset.TRANSCEIVER_7, 5, "FC: intermediate distance (I)"),
        (SffOffset.TRANSCEIVER_7, 4, "FC: long distance (L)"),
        (SffOffset.TRANSCEIVER_7, 3, "FC: medium distance (M)"),
        # Fibre Channel transmitter technology
        (SffOffset.TRANSCEIVER_7, 2, "FC: Shortwave laser, linear Rx (SA)"),
        (SffOffset.TRANSCEIVER_7, 1, "FC: Longwave laser (LC)"),
        (SffOffset.TRANSCEIVER_7, 0, "FC: Electrical inter-enclosure (EL)"),
        (SffOffset.TRANSCEIVER_8, 7, "FC: Electrical intra-enclosure (EL)"),
        (SffOffset.TRANSCEIVER_8, 6, "FC: Shortwave laser w/o OFC (SN)"),
        (SffOffset.TRANSCEIVER_8, 5, "FC: Shortwave laser with OFC (SL)"),
        (SffOffset.TRANSCEIVER_8, 4, "FC: Longwave laser (LL)"),
        (SffOffset.TRANSCEIVER_8, 3, "Active Cable"),
        (SffOffset.TRANSCEIVER_8, 2, "Passive Cable"),
        (SffOffset.TRANSCEIVER_8, 1, "FC: Copper FC-BaseT"),
        # Fibre Channel transmission media
        (SffOffset.TRANSCEIVER_9, 7, "FC: Twin Axial Pair (TW)"),
        (SffOffset.TRANSCEIVER_9, 6, "FC: Twisted Pair (TP)"),
        (SffOffset.TRANSCEIVER_9, 5, "FC: Miniature Coax (MI)"),
        (SffOffset.TRANSCEIVER_9, 4, "FC: Video Coax (TV)"),
        (SffOffset.TRANSCEIVER_9, 3, "FC: Multimode, 62.5um (M6)"),
        (SffOffset.TRANSCEIVER_9, 2, "FC: Multimode, 50um (M5)"),
        (SffOffset.TRANSCEIVER_9, 0, "FC: Single Mode (SM)"),
        # Fibre Channel speed
        (SffOffset.TRANSCEIVER_10, 7, "FC: 1200 MBytes/sec"),
        (SffOffset.TRANSCEIVER_10, 6, "FC: 800 MBytes/sec"),
        (SffOffset.TRANSCEIVER_10, 4, "FC: 400 MBytes/sec"),
        (SffOffset.TRANSCEIVER_10, 2, "FC: 200 MBytes/sec"),
        (SffOffset.TRANSCEIVER_10, 0, "FC: 100 MBytes/sec"),
    ):
        if _is_bit_set(eeprom, offset, bit):
            transceiver_type.append(type_str)

    for offset, value, type_str in (
        # Extended Specification Compliance Codes from SFF-8024
        (
            SffOffset.TRANSCEIVER_36,
            0x1,
            "Extended: 100G AOC or 25GAUI C2M AOC with worst BER of 5x10^(-5)",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x2,
            "Extended: 100G Base-SR4 or 25GBase-SR",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x3,
            "Extended: 100G Base-LR4 or 25GBase-LR",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x4,
            "Extended: 100G Base-ER4 or 25GBase-ER",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x8,
            "Extended: 100G ACC or 25GAUI C2M ACC with worst BER of 5x10^(-5)",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0xB,
            "Extended: 100G Base-CR4 or 25G Base-CR CA-L",
        ),
        (SffOffset.TRANSCEIVER_36, 0xC, "Extended: 25G Base-CR CA-S"),
        (SffOffset.TRANSCEIVER_36, 0xD, "Extended: 25G Base-CR CA-N"),
        (
            SffOffset.TRANSCEIVER_36,
            0x16,
            "Extended: 10Gbase-T with SFI electrical interface",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x18,
            "Extended: 100G AOC or 25GAUI C2M AOC with worst BER of 10^(-12)",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x19,
            "Extended: 100G ACC or 25GAUI C2M ACC with worst BER of 10^(-12)",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x1A,
            "Extended: 100GE-DWDM2 (DWDM transceiver using 2 wavelengths"
            " on a 1550 nm DWDM grid with a reach up to 80 km",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x1B,
            "Extended: 100G 1550nm WDM (4 wavelengths)",
        ),
        (SffOffset.TRANSCEIVER_36, 0x1C, "Extended: 10Gbase-T Short Reach"),
        (SffOffset.TRANSCEIVER_36, 0x1D, "Extended: 5GBASE-T"),
        (SffOffset.TRANSCEIVER_36, 0x1E, "Extended: 2.5GBASE-T"),
        (SffOffset.TRANSCEIVER_36, 0x1F, "Extended: 40G SWDM4"),
        (SffOffset.TRANSCEIVER_36, 0x20, "Extended: 100G SWDM4"),
        (SffOffset.TRANSCEIVER_36, 0x21, "Extended: 100G PAM4 BiDi"),
        (
            SffOffset.TRANSCEIVER_36,
            0x22,
            "Extended: 4WDM-10 MSA (10km version of 100G CWDM4"
            " with same RS(528,514) FEC in host system)",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x23,
            "Extended: 4WDM-20 MSA (20km version of 100GBASE-LR4"
            " with RS(528,514) FEC in host system)",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x24,
            "Extended: 4WDM-40 MSA (40km reach with APD receiver"
            " and RS(528,514) FEC in host system)",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x25,
            "Extended: 100GBASE-DR (clause 140), CAUI-4 (no FEC)",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x26,
            "Extended: 100G-FR or 100GBASE-FR1 (clause 140), CAUI-4 (no FEC)",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x27,
            "Extended: 100G-LR or 100GBASE-LR1 (clause 140), CAUI-4 (no FEC)",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x30,
            "Extended: Active Copper Cable with 50GAUI, 100GAUI-2 or"
            " 200GAUI-4 C2M. Providing a worst BER of 10-6 or below",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x31,
            "Extended: Active Optical Cable with 50GAUI, 100GAUI-2 or"
            " 200GAUI-4 C2M. Providing a worst BER of 10-6 or below",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x32,
            "Extended: Active Copper Cable with 50GAUI, 100GAUI-2 or"
            " 200GAUI-4 C2M. Providing a worst BER of 2.6x10-4 for ACC,"
            " 10-5 for AUI, or below",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x33,
            "Extended: Active Optical Cable with 50GAUI, 100GAUI-2 or"
            " 200GAUI-4 C2M. Providing a worst BER of 2.6x10-4 for ACC,"
            " 10-5 for AUI, or below",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x40,
            "Extended: 50GBASE-CR, 100GBASE-CR2, or 200GBASE-CR4",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x41,
            "Extended: 50GBASE-SR, 100GBASE-SR2, or 200GBASE-SR4",
        ),
        (
            SffOffset.TRANSCEIVER_36,
            0x42,
            "Extended: 50GBASE-FR or 200GBASE-DR4",
        ),
        (SffOffset.TRANSCEIVER_36, 0x43, "Extended: 200GBASE-FR4"),
        (SffOffset.TRANSCEIVER_36, 0x44, "Extended: 200G 1550 nm PSM4"),
        (SffOffset.TRANSCEIVER_36, 0x45, "Extended: 50GBASE-LR"),
        (SffOffset.TRANSCEIVER_36, 0x46, "Extended: 200GBASE-LR4"),
        (SffOffset.TRANSCEIVER_36, 0x50, "Extended: 64GFC EA"),
        (SffOffset.TRANSCEIVER_36, 0x51, "Extended: 64GFC SW"),
        (SffOffset.TRANSCEIVER_36, 0x52, "Extended: 64GFC LW"),
        (SffOffset.TRANSCEIVER_36, 0x53, "Extended: 128GFC EA"),
        (SffOffset.TRANSCEIVER_36, 0x54, "Extended: 128GFC SW"),
        (SffOffset.TRANSCEIVER_36, 0x55, "Extended: 128GFC LW"),
    ):
        if eeprom[offset] == value:
            transceiver_type.append(type_str)

    return (ModuleInfoField.TRANSCEIVER_TYPE.value, transceiver_type)


def sff8024_get_encoding(
    eeprom: tuple[int], sff_type: int, offset: int = SffOffset.ENCODING
) -> tuple[str, str]:
    """Parsing of encoding reference, defined
    in SFF-8024 specification, section 4.3

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :param sff_type: module SFF type
    :type sff_type: int
    :param offset: offset of encoding byte
    :type offset: int
    :return: parsed description and value
    :rtype: tuple[str, str]
    """
    encoding_id = eeprom[offset]
    encoding_desc = "reserved or unknown"

    if encoding_id == Sff8024Encoding.CODE_UNSPEC:
        encoding_desc = "unspecified"
    elif encoding_id == Sff8024Encoding.CODE_8B10B:
        encoding_desc = "8B/10B"
    elif encoding_id == Sff8024Encoding.CODE_4B5B:
        encoding_desc = "4B/5B"
    elif encoding_id == Sff8024Encoding.CODE_NRZ:
        encoding_desc = "NRZ"
    elif encoding_id == Sff8024Encoding.CODE_04:
        if sff_type == SffModuleType.SFF_8472:
            encoding_desc = "Manchester"
        elif sff_type == SffModuleType.SFF_8636:
            encoding_desc = "SONET Scrambled"
    elif encoding_id == Sff8024Encoding.CODE_05:
        if sff_type == SffModuleType.SFF_8472:
            encoding_desc = "SONET Scrambled"
        elif sff_type == SffModuleType.SFF_8636:
            encoding_desc = "64B/66B"
    elif encoding_id == Sff8024Encoding.CODE_06:
        if sff_type == SffModuleType.SFF_8472:
            encoding_desc = "64B/66B"
        elif sff_type == SffModuleType.SFF_8636:
            encoding_desc = "Manchester"
    elif encoding_id == Sff8024Encoding.CODE_256B:
        encoding_desc = "256B/257B (transcoded FEC-enabled data)"
    elif encoding_id == Sff8024Encoding.CODE_PAM4:
        encoding_desc = "PAM4"

    return (ModuleInfoField.ENCODING.value, encoding_desc)


def sff8079_get_baud_rates(eeprom: tuple[int]) -> list[tuple[str, int]]:
    """Parsing of signal rate, defined
    in SFF-8472 specification, section 5.6

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: list[tuple[str, int]]
    """
    if eeprom[SffOffset.BR_NOMINAL] == 0:
        br_nom = 0
        br_min = 0
        br_max = 0
    elif eeprom[SffOffset.BR_NOMINAL] == 255:
        br_nom = eeprom[SffOffset.BR_MAX] * 250
        br_max = eeprom[SffOffset.BR_MIN]
        br_min = eeprom[SffOffset.BR_MIN]
    else:
        br_nom = eeprom[SffOffset.BR_NOMINAL] * 100
        br_max = eeprom[SffOffset.BR_MAX]
        br_min = eeprom[SffOffset.BR_MIN]

    return [
        (ModuleInfoField.BR_NOMINAL.value, br_nom),
        (ModuleInfoField.BR_MIN.value, br_min),
        (ModuleInfoField.BR_MAX.value, br_max),
    ]


def sff8079_get_rate_identifier(eeprom: tuple[int]) -> tuple[str, str]:
    """Parsing of rate identifier, defined
    in SFF-8472 specification, section 5.7

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, str]
    """
    rate_id = eeprom[SffOffset.RATE_IDENTIFIER]

    if rate_id == 0x00:
        rate_desc = "unspecified"
    elif rate_id == 0x01:
        rate_desc = "4/2/1G Rate_Select & AS0/AS1"
    elif rate_id == 0x02:
        rate_desc = "8/4/2G Rx Rate_Select only"
    elif rate_id == 0x03:
        rate_desc = "8/4/2G Independent Rx & Tx Rate_Select"
    elif rate_id == 0x04:
        rate_desc = "8/4/2G Tx Rate_Select only"
    else:
        rate_desc = "reserved or unknown"

    return (ModuleInfoField.RATE_IDENTIFIER.value, rate_desc)


def sff8079_get_lengths(eeprom: tuple[int]) -> list[tuple[str, int]]:
    """Parsing of link lengths, defined
    in SFF-8472 specification, section 6

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: list[tuple[str, int]]
    """
    sfm_km = eeprom[SffOffset.LENGTH_SFM_KM]
    sfm_m = eeprom[SffOffset.LENGTH_SFM_100M] * 100
    f50um_m = eeprom[SffOffset.LENGTH_50UM_10M] * 10
    f62_5um_m = eeprom[SffOffset.LENGTH_62_5UM_10M] * 10
    copper_m = eeprom[SffOffset.LENGTH_COPPER]
    o3m_m = eeprom[SffOffset.LENGTH_OM3_10M] * 10

    return [
        (ModuleInfoField.LENGTH_SFM_KM.value, sfm_km),
        (ModuleInfoField.LENGTH_SFM_100M.value, sfm_m),
        (ModuleInfoField.LENGTH_50UM_10M.value, f50um_m),
        (ModuleInfoField.LENGTH_62_5UM_10M.value, f62_5um_m),
        (ModuleInfoField.LENGTH_COPPER.value, copper_m),
        (ModuleInfoField.LENGTH_OM3_10M.value, o3m_m),
    ]


def sff8079_get_vendor_name(eeprom: tuple[int]) -> tuple[str, str]:
    """Parsing of vendor name, defined
    in SFF-8472 specification, section 7.1

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, str]
    """
    return (
        ModuleInfoField.VENDOR_NAME.value,
        _sff8079_get_array(
            eeprom, SffOffset.VENDOR_NAME, SFF_FIELD_VENDOR_NAME_LEN
        ),
    )


def sff8079_get_vendor_oui(eeprom: tuple[int]) -> tuple[str, str]:
    """Parsing of vendor Organizationally Unique Identifier, defined
    in SFF-8472 specification, section 7.2

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, str]
    """
    return (
        ModuleInfoField.VENDOR_OUI.value,
        _sff8079_get_array(
            eeprom,
            SffOffset.VENDOR_OUI,
            SFF_FIELD_VENDOR_OUI_LEN,
            decode=False,
        ),
    )


def sff8079_get_vendor_pn(eeprom: tuple[int]) -> tuple[str, str]:
    """Parsing of vendor Part Number, defined
    in SFF-8472 specification, section 7.3

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, str]
    """
    return (
        ModuleInfoField.VENDOR_PN.value,
        _sff8079_get_array(
            eeprom, SffOffset.VENDOR_PN, SFF_FIELD_VENDOR_PN_LEN
        ),
    )


def sff8079_get_vendor_rev(eeprom: tuple[int]) -> tuple[str, str]:
    """Parsing of vendor revision number, defined
    in SFF-8472 specification, section 7.4

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, str]
    """
    return (
        ModuleInfoField.VENDOR_REV.value,
        _sff8079_get_array(
            eeprom, SffOffset.VENDOR_REV, SFF_FIELD_VENDOR_REV_LEN
        ),
    )


def sff8079_get_wavelength_or_copper_compliance(
    eeprom: tuple[int],
) -> tuple[str, Union[str, int]]:
    """Parsing of link lengths, defined
    in SFF-8472 specification, section 8.1

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, Union[str, int]]
    """
    resp_desc: Union[str, int] = ""

    if _is_bit_set(eeprom, SffOffset.TRANSCEIVER_8, 2):
        resp_id = eeprom[SffOffset.LINK_CHAR_60]
        resp_field = ModuleInfoField.PASSIVE_COPPER_COMP.value
        if resp_id == 0x00:
            resp_desc = "unspecified"
        elif resp_id == 0x01:
            resp_desc = "SFF-8431 appendix E"
        else:
            resp_desc = "unknown"
        resp_desc += " [SFF-8472 rev10.4 only]"

    elif _is_bit_set(eeprom, SffOffset.TRANSCEIVER_8, 3):
        resp_id = eeprom[SffOffset.LINK_CHAR_60]
        resp_field = ModuleInfoField.ACTIVE_COPPER_COMP.value
        if resp_id == 0x00:
            resp_desc = "unspecified"
        elif resp_id == 0x01:
            resp_desc = "SFF-8431 appendix E"
        elif resp_id == 0x04:
            resp_desc = "SFF-8431 limiting"
        else:
            resp_desc = "unknown"
        resp_desc += " [SFF-8472 rev10.4 only]"

    else:
        resp_field = ModuleInfoField.LASER_WAVELENGTH.value
        resp_desc = (eeprom[SffOffset.LINK_CHAR_60] << 8) | eeprom[
            SffOffset.LINK_CHAR_61
        ]

    return (resp_field, resp_desc)


def sff8079_get_options(eeprom: tuple[int]) -> tuple[str, list[str]]:
    """Parsing of options values, defined
    in SFF-8472 specification, section 8.3

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, list[str]]
    """
    options_desc = []
    if _is_bit_set(eeprom, SffOffset.OPTIONS_65, 1):
        options_desc.append("RX_LOS implemented")
    if _is_bit_set(eeprom, SffOffset.OPTIONS_65, 2):
        options_desc.append("RX_LOS implemented, inverted")
    if _is_bit_set(eeprom, SffOffset.OPTIONS_65, 3):
        options_desc.append("TX_FAULT implemented")
    if _is_bit_set(eeprom, SffOffset.OPTIONS_65, 4):
        options_desc.append("TX_DISABLE implemented")
    if _is_bit_set(eeprom, SffOffset.OPTIONS_65, 5):
        options_desc.append("RATE_SELECT implemented")
    if _is_bit_set(eeprom, SffOffset.OPTIONS_65, 6):
        options_desc.append("Tunable transmitter technology")
    if _is_bit_set(eeprom, SffOffset.OPTIONS_65, 7):
        options_desc.append("Receiver decision threshold implemented")
    if _is_bit_set(eeprom, SffOffset.OPTIONS_64, 0):
        options_desc.append("Linear receiver output implemented")
    if _is_bit_set(eeprom, SffOffset.OPTIONS_64, 1):
        options_desc.append("Power level 2 requirement")
    if _is_bit_set(eeprom, SffOffset.OPTIONS_64, 2):
        options_desc.append("Cooled transceiver implemented")
    if _is_bit_set(eeprom, SffOffset.OPTIONS_64, 3):
        options_desc.append("Retimer or CDR implemented")
    if _is_bit_set(eeprom, SffOffset.OPTIONS_64, 4):
        options_desc.append("Paging implemented")
    if _is_bit_set(eeprom, SffOffset.OPTIONS_64, 5):
        options_desc.append("Power level 3 requirement")

    return (ModuleInfoField.OPTION.value, options_desc)


def sff8079_get_vendor_sn(eeprom: tuple[int]) -> tuple[str, str]:
    """Parsing of vendor Serial Number, defined
    in SFF-8472 specification, section 8.6

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, str]
    """
    return (
        ModuleInfoField.VENDOR_SN.value,
        _sff8079_get_array(
            eeprom, SffOffset.VENDOR_SN, SFF_FIELD_VENDOR_SN_LEN
        ),
    )


def sff8079_get_date_code(eeprom: tuple[int]) -> tuple[str, str]:
    """Parsing of vendor date code, defined
    in SFF-8472 specification, section 8.7

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, str]
    """
    return (
        ModuleInfoField.DATE_CODE.value,
        _sff8079_get_array(
            eeprom, SffOffset.DATE_CODE, SFF_FIELD_DATE_CODE_LEN
        ),
    )


def sff8472_get_diag_mon_support(eeprom: tuple[int]) -> tuple[str, bool]:
    """Parsing of diagnostic monitoring support, defined
    in SFF-8472 specification, section 8.8

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, bool]
    """
    return (
        ModuleInfoField.DIAG_MON_SUPPORT.value,
        _is_bit_set(eeprom, SffOffset.DIAG_MON, 6),
    )


def sff8472_get_alarm_support(eeprom: tuple[int]) -> tuple[str, bool]:
    """Parsing of alarm support, defined
    in SFF-8472 specification, section 8.8

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: tuple[str, bool]
    """
    return (
        ModuleInfoField.ALARM_SUPPORT.value,
        _is_bit_set(eeprom, SffOffset.ENHANCED_OPTIONS, 7),
    )


def sff8472_get_power_measurement_type(eeprom: tuple[int]) -> Sff8024PowerMeas:
    """Parsing of power measurement type, defined
    in SFF-8472 specification, section 8.8

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed value
    :rtype: Sff8024PowerMeas
    """
    return Sff8024PowerMeas(_is_bit_set(eeprom, SffOffset.DIAG_MON, 3))


def sff8472_get_diag_mon(eeprom: tuple[int]) -> list[tuple[str, float]]:
    """Parsing of diagnostic monitoring, defined
    in SFF-8472 specification, section 9.8

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: list[tuple[str, float]]
    """
    rx_pwr_str: str = ModuleInfoField.RX_PWR.value
    power_meas_type = sff8472_get_power_measurement_type(eeprom)
    if power_meas_type == Sff8024PowerMeas.OMA:
        rx_pwr_str += " (OMA)"
    elif power_meas_type == Sff8024PowerMeas.AVERAGE_POWER:
        rx_pwr_str += " (average optical power)"

    return [
        (
            ModuleInfoField.TEMP.value,
            _sff8472_get_temp(eeprom, SffOffset.TEMP),
        ),
        (
            ModuleInfoField.VCC.value,
            _sff8472_get_voltage_or_power(eeprom, SffOffset.VCC),
        ),
        (
            ModuleInfoField.BIAS.value,
            _sff8472_get_bias(eeprom, SffOffset.BIAS),
        ),
        (
            ModuleInfoField.TX_PWR.value,
            _sff8472_get_voltage_or_power(eeprom, SffOffset.TX_PWR),
        ),
        (rx_pwr_str, _sff8472_get_voltage_or_power(eeprom, SffOffset.RX_PWR)),
    ]


def sff8472_get_alarm_warning_thresholds(
    eeprom: tuple[int],
) -> list[tuple[str, float]]:
    """Parsing of alarm and warning thresholds, defined
    in SFF-8472 specification, section 9.4

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: list[tuple[str, float]]
    """
    aw_thresholds: list[tuple[str, float]] = []
    for aw_type, getter, suffix in (
        (
            Sff8024AlarmWarningSrc.TEMP.value,
            _sff8472_get_temp,
            "threshold, in °C",
        ),
        (
            Sff8024AlarmWarningSrc.VOLTAGE.value,
            _sff8472_get_voltage_or_power,
            "threshold, in V",
        ),
        (
            Sff8024AlarmWarningSrc.BIAS.value,
            _sff8472_get_bias,
            "threshold, in mA",
        ),
        (
            Sff8024AlarmWarningSrc.TX_POWER.value,
            _sff8472_get_voltage_or_power,
            "threshold, in mW",
        ),
        (
            Sff8024AlarmWarningSrc.RX_POWER.value,
            _sff8472_get_voltage_or_power,
            "threshold, in mW",
        ),
    ):
        for aw in [aw.value for aw in Sff8024AlarmWarningType]:
            aw_name = f"{aw_type}_{aw}"
            aw_thresholds.append(
                (
                    f"{getattr(ModuleInfoField, aw_name).value} {suffix}",
                    getter(eeprom, getattr(SffOffset, aw_name)),
                )
            )
    return aw_thresholds


def sff8472_get_alarm_warning(eeprom: tuple[int]) -> list[tuple[str, bool]]:
    """Parsing of alarm and warning, defined
    in SFF-8472 specification, section 9.9

    :param eeprom: EEPROM data
    :type eeprom: tuple[int]
    :return: parsed description and value
    :rtype: list[tuple[str, bool]]
    """
    return [
        (aw_type, _is_bit_set(eeprom, offset, bit))
        for aw_type, offset, bit in (
            (ModuleInfoField.TEMP_HALRM.value, SffOffset.ALRM_FLG_1, 7),
            (ModuleInfoField.TEMP_LALRM.value, SffOffset.ALRM_FLG_1, 6),
            (ModuleInfoField.TEMP_HWARN.value, SffOffset.WARN_FLG_1, 7),
            (ModuleInfoField.TEMP_LWARN.value, SffOffset.WARN_FLG_1, 6),
            (ModuleInfoField.VCC_HALRM.value, SffOffset.ALRM_FLG_1, 5),
            (ModuleInfoField.VCC_LALRM.value, SffOffset.ALRM_FLG_1, 4),
            (ModuleInfoField.VCC_HWARN.value, SffOffset.WARN_FLG_1, 5),
            (ModuleInfoField.VCC_LWARN.value, SffOffset.WARN_FLG_1, 4),
            (ModuleInfoField.BIAS_HALRM.value, SffOffset.ALRM_FLG_1, 3),
            (ModuleInfoField.BIAS_LALRM.value, SffOffset.ALRM_FLG_1, 2),
            (ModuleInfoField.BIAS_HWARN.value, SffOffset.WARN_FLG_1, 3),
            (ModuleInfoField.BIAS_LWARN.value, SffOffset.WARN_FLG_1, 2),
            (ModuleInfoField.TX_PWR_HALRM.value, SffOffset.ALRM_FLG_1, 1),
            (ModuleInfoField.TX_PWR_LALRM.value, SffOffset.ALRM_FLG_1, 0),
            (ModuleInfoField.TX_PWR_HWARN.value, SffOffset.WARN_FLG_1, 1),
            (ModuleInfoField.TX_PWR_LWARN.value, SffOffset.WARN_FLG_1, 0),
            (ModuleInfoField.RX_PWR_HALRM.value, SffOffset.ALRM_FLG_2, 7),
            (ModuleInfoField.RX_PWR_LALRM.value, SffOffset.ALRM_FLG_2, 6),
            (ModuleInfoField.RX_PWR_HWARN.value, SffOffset.WARN_FLG_2, 7),
            (ModuleInfoField.RX_PWR_LWARN.value, SffOffset.WARN_FLG_2, 6),
        )
    ]


class ModuleInfoParser:
    """Class used to retrieve EEPROM data from a module
    and to parse it to extract the module information
    """

    def __init__(self, ethtool, ifname):
        self.ethtool = ethtool
        self.ifname = ifname

    @staticmethod
    def parse_sff8079_page0(
        eeprom: tuple[int],
    ) -> list[tuple[str, Union[str, list[str], int]]]:
        """Parsing of EEPROM page 0 for SFF 8079 modules

        :param eeprom: EEPROM data
        :type eeprom: tuple[int]
        :return: parsed description and value
        :rtype: list[tuple[str, Union[str, list[str], int]]
        """
        return [
            sff8024_get_identifier(eeprom),
            sff8079_get_ext_identifier(eeprom),
            sff8024_get_connector(eeprom),
            sff8079_get_transceiver(eeprom),
            sff8024_get_encoding(eeprom, SffModuleType.SFF_8472),
            *sff8079_get_baud_rates(eeprom),
            sff8079_get_rate_identifier(eeprom),
            *sff8079_get_lengths(eeprom),
            sff8079_get_wavelength_or_copper_compliance(eeprom),
            sff8079_get_vendor_name(eeprom),
            sff8079_get_vendor_oui(eeprom),
            sff8079_get_vendor_pn(eeprom),
            sff8079_get_vendor_rev(eeprom),
            sff8079_get_options(eeprom),
            sff8079_get_vendor_sn(eeprom),
            sff8079_get_date_code(eeprom),
        ]

    @staticmethod
    def parse_sff8079_page2(
        eeprom: tuple[int],
    ) -> list[tuple[str, Union[bool, float]]]:
        """Parsing of EEPROM page 2 for SFF 8079 modules

        :param eeprom: EEPROM data
        :type eeprom: tuple[int]
        :return: parsed description and value
        :rtype: list[tuple[str, Union[bool, float]]]
        """
        return [
            sff8472_get_diag_mon_support(eeprom),
            *sff8472_get_diag_mon(eeprom),
            sff8472_get_alarm_support(eeprom),
            *sff8472_get_alarm_warning(eeprom),
            *sff8472_get_alarm_warning_thresholds(eeprom),
        ]

    def get_and_parse_sff8079(self):
        """EEPROM parsing for SFF 8079 modules"""
        # Retrieve the first EEPROM page
        resp = self.ethtool.get_module_eeprom(
            self.ifname,
            length=SFF8079_PAGE_SIZE,
            i2c_address=SFF8079_I2C_ADDRESS_LOW,
        )
        raw_data = resp.data
        parsed = self.parse_sff8079_page0(raw_data)

        # Check if the second page should be retrieved
        if not sff8472_get_diag_mon_support(raw_data)[0][1]:
            return parsed

        # Retrieve the second page
        resp = self.ethtool.get_module_eeprom(
            self.ifname,
            length=SFF8079_PAGE_SIZE,
            i2c_address=SFF8079_I2C_ADDRESS_HIGH,
        )

        # Add zero padding to align the EEPROM page
        raw_data = raw_data + (0,) * (
            SffModulePageLength.SFF_8079_LEN - len(raw_data)
        )
        raw_data += resp.data
        parsed += self.parse_sff8079_page2(raw_data)

        return parsed

    def get_and_parse_sff8636(self):
        """EEPROM parsing for SFF 8636 modules"""
        raise NotImplementedError(
            "Parsing for SFF 8636 modules not implemented"
        )

    def get_and_parse_cmis(self):
        """EEPROM parsing for CMIS modules"""
        raise NotImplementedError("Parsing for CMIS modules not implemented")

    def parse_module_info(self):
        """Retrieve module identifier, and parse its EEPROM accordingly"""
        # Fetch the SFF-8024 Identifier Value. For all supported standards, it
        # is located at I2C address 0x50, byte 0. See section 4.1 in SFF-8024,
        # revision 4.9.
        resp = self.ethtool.get_module_eeprom(
            self.ifname, length=1, i2c_address=ETH_I2C_ADDRESS_LOW
        )
        mod_type = resp.data[SffOffset.ID]

        if mod_type in (
            Sff8024Id.GBIC,
            Sff8024Id.SOLDERED_MODULE,
            Sff8024Id.SFP,
        ):
            return self.get_and_parse_sff8079()

        if mod_type in (Sff8024Id.QSFP, Sff8024Id.QSFP28, Sff8024Id.QSFP_PLUS):
            return self.get_and_parse_sff8636()

        if mod_type in (
            Sff8024Id.QSFP_DD,
            Sff8024Id.OSFP,
            Sff8024Id.DSFP,
            Sff8024Id.QSFP_PLUS_CMIS,
            Sff8024Id.SFP_DD_CMIS,
            Sff8024Id.SFP_PLUS_CMIS,
        ):
            return self.get_and_parse_cmis()

        # By default retrieve the first 128 bytes
        return self.ethtool.get_module_eeprom(
            self.ifname, length=128, i2c_address=ETH_I2C_ADDRESS_LOW
        )
