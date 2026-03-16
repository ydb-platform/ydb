# -*- coding: utf-8 -*-
"""VISA VPP-4.3 constants (VPP-4.3.2 spec, section 3).

Makes all "completion and error codes", "attribute values", "event type
values", and "values and ranges" defined in the VISA specification VPP-4.3.2,
section 3, available as variable values.

The module exports the values under the original, all-uppercase names.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import enum
import sys

from typing_extensions import Literal

is_64bits = sys.maxsize > 2**32


def _to_int(x: int) -> int:
    """Convert a signed completion and error code to the proper value.

    This function is necessary because the VISA specification is flawed: It defines
    the VISA codes, which have a value less than zero, in their internal 32-bit
    signed integer representation. However, this is positive.  ctypes doesn't
    care about that and (correctly) returns the negative value, which is left as
    such by Python.

    Parameters
    ----------
    x : int
        Value in 32-bit notation as listed in the VPP-4.3.2 specification

    Returns
    -------
    int
        Properly signed value

    """
    if x > 0x7FFFFFFF:
        return int(x - 0x100000000)
    else:
        return int(x)


# fmt: off

# ======================================================================================
# --- VISA constants  ------------------------------------------------------------------
# ======================================================================================

# Status codes : success
VI_SUCCESS                   = _to_int(0x00000000)
VI_SUCCESS_EVENT_EN          = _to_int(0x3FFF0002)
VI_SUCCESS_EVENT_DIS         = _to_int(0x3FFF0003)
VI_SUCCESS_QUEUE_EMPTY       = _to_int(0x3FFF0004)
VI_SUCCESS_TERM_CHAR         = _to_int(0x3FFF0005)
VI_SUCCESS_MAX_CNT           = _to_int(0x3FFF0006)
VI_SUCCESS_DEV_NPRESENT      = _to_int(0x3FFF007D)
VI_SUCCESS_TRIG_MAPPED       = _to_int(0x3FFF007E)
VI_SUCCESS_QUEUE_NEMPTY      = _to_int(0x3FFF0080)
VI_SUCCESS_NCHAIN            = _to_int(0x3FFF0098)
VI_SUCCESS_NESTED_SHARED     = _to_int(0x3FFF0099)
VI_SUCCESS_NESTED_EXCLUSIVE  = _to_int(0x3FFF009A)
VI_SUCCESS_SYNC              = _to_int(0x3FFF009B)

# Status codes : warning
VI_WARN_QUEUE_OVERFLOW       = _to_int(0x3FFF000C)
VI_WARN_CONFIG_NLOADED       = _to_int(0x3FFF0077)
VI_WARN_NULL_OBJECT          = _to_int(0x3FFF0082)
VI_WARN_NSUP_ATTR_STATE      = _to_int(0x3FFF0084)
VI_WARN_UNKNOWN_STATUS       = _to_int(0x3FFF0085)
VI_WARN_NSUP_BUF             = _to_int(0x3FFF0088)

# The following one is a non-standard NI extension
VI_WARN_EXT_FUNC_NIMPL       = _to_int(0x3FFF00A9)

# Status codes : errors
VI_ERROR_SYSTEM_ERROR        = _to_int(0xBFFF0000)
VI_ERROR_INV_OBJECT          = _to_int(0xBFFF000E)
VI_ERROR_RSRC_LOCKED         = _to_int(0xBFFF000F)
VI_ERROR_INV_EXPR            = _to_int(0xBFFF0010)
VI_ERROR_RSRC_NFOUND         = _to_int(0xBFFF0011)
VI_ERROR_INV_RSRC_NAME       = _to_int(0xBFFF0012)
VI_ERROR_INV_ACC_MODE        = _to_int(0xBFFF0013)
VI_ERROR_TMO                 = _to_int(0xBFFF0015)
VI_ERROR_CLOSING_FAILED      = _to_int(0xBFFF0016)
VI_ERROR_INV_DEGREE          = _to_int(0xBFFF001B)
VI_ERROR_INV_JOB_ID          = _to_int(0xBFFF001C)
VI_ERROR_NSUP_ATTR           = _to_int(0xBFFF001D)
VI_ERROR_NSUP_ATTR_STATE     = _to_int(0xBFFF001E)
VI_ERROR_ATTR_READONLY       = _to_int(0xBFFF001F)
VI_ERROR_INV_LOCK_TYPE       = _to_int(0xBFFF0020)
VI_ERROR_INV_ACCESS_KEY      = _to_int(0xBFFF0021)
VI_ERROR_INV_EVENT           = _to_int(0xBFFF0026)
VI_ERROR_INV_MECH            = _to_int(0xBFFF0027)
VI_ERROR_HNDLR_NINSTALLED    = _to_int(0xBFFF0028)
VI_ERROR_INV_HNDLR_REF       = _to_int(0xBFFF0029)
VI_ERROR_INV_CONTEXT         = _to_int(0xBFFF002A)
VI_ERROR_QUEUE_OVERFLOW      = _to_int(0xBFFF002D)
VI_ERROR_NENABLED            = _to_int(0xBFFF002F)
VI_ERROR_ABORT               = _to_int(0xBFFF0030)
VI_ERROR_RAW_WR_PROT_VIOL    = _to_int(0xBFFF0034)
VI_ERROR_RAW_RD_PROT_VIOL    = _to_int(0xBFFF0035)
VI_ERROR_OUTP_PROT_VIOL      = _to_int(0xBFFF0036)
VI_ERROR_INP_PROT_VIOL       = _to_int(0xBFFF0037)
VI_ERROR_BERR                = _to_int(0xBFFF0038)
VI_ERROR_IN_PROGRESS         = _to_int(0xBFFF0039)
VI_ERROR_INV_SETUP           = _to_int(0xBFFF003A)
VI_ERROR_QUEUE_ERROR         = _to_int(0xBFFF003B)
VI_ERROR_ALLOC               = _to_int(0xBFFF003C)
VI_ERROR_INV_MASK            = _to_int(0xBFFF003D)
VI_ERROR_IO                  = _to_int(0xBFFF003E)
VI_ERROR_INV_FMT             = _to_int(0xBFFF003F)
VI_ERROR_NSUP_FMT            = _to_int(0xBFFF0041)
VI_ERROR_LINE_IN_USE         = _to_int(0xBFFF0042)
VI_ERROR_NSUP_MODE           = _to_int(0xBFFF0046)
VI_ERROR_SRQ_NOCCURRED       = _to_int(0xBFFF004A)
VI_ERROR_INV_SPACE           = _to_int(0xBFFF004E)
VI_ERROR_INV_OFFSET          = _to_int(0xBFFF0051)
VI_ERROR_INV_WIDTH           = _to_int(0xBFFF0052)
VI_ERROR_NSUP_OFFSET         = _to_int(0xBFFF0054)
VI_ERROR_NSUP_VAR_WIDTH      = _to_int(0xBFFF0055)
VI_ERROR_WINDOW_NMAPPED      = _to_int(0xBFFF0057)
VI_ERROR_RESP_PENDING        = _to_int(0xBFFF0059)
VI_ERROR_NLISTENERS          = _to_int(0xBFFF005F)
VI_ERROR_NCIC                = _to_int(0xBFFF0060)
VI_ERROR_NSYS_CNTLR          = _to_int(0xBFFF0061)
VI_ERROR_NSUP_OPER           = _to_int(0xBFFF0067)
VI_ERROR_INTR_PENDING        = _to_int(0xBFFF0068)
VI_ERROR_ASRL_PARITY         = _to_int(0xBFFF006A)
VI_ERROR_ASRL_FRAMING        = _to_int(0xBFFF006B)
VI_ERROR_ASRL_OVERRUN        = _to_int(0xBFFF006C)
VI_ERROR_TRIG_NMAPPED        = _to_int(0xBFFF006E)
VI_ERROR_NSUP_ALIGN_OFFSET   = _to_int(0xBFFF0070)
VI_ERROR_USER_BUF            = _to_int(0xBFFF0071)
VI_ERROR_RSRC_BUSY           = _to_int(0xBFFF0072)
VI_ERROR_NSUP_WIDTH          = _to_int(0xBFFF0076)
VI_ERROR_INV_PARAMETER       = _to_int(0xBFFF0078)
VI_ERROR_INV_PROT            = _to_int(0xBFFF0079)
VI_ERROR_INV_SIZE            = _to_int(0xBFFF007B)
VI_ERROR_WINDOW_MAPPED       = _to_int(0xBFFF0080)
VI_ERROR_NIMPL_OPER          = _to_int(0xBFFF0081)
VI_ERROR_INV_LENGTH          = _to_int(0xBFFF0083)
VI_ERROR_INV_MODE            = _to_int(0xBFFF0091)
VI_ERROR_SESN_NLOCKED        = _to_int(0xBFFF009C)
VI_ERROR_MEM_NSHARED         = _to_int(0xBFFF009D)
VI_ERROR_LIBRARY_NFOUND      = _to_int(0xBFFF009E)
VI_ERROR_NSUP_INTR           = _to_int(0xBFFF009F)
VI_ERROR_INV_LINE            = _to_int(0xBFFF00A0)
VI_ERROR_FILE_ACCESS         = _to_int(0xBFFF00A1)
VI_ERROR_FILE_IO             = _to_int(0xBFFF00A2)
VI_ERROR_NSUP_LINE           = _to_int(0xBFFF00A3)
VI_ERROR_NSUP_MECH           = _to_int(0xBFFF00A4)
VI_ERROR_INTF_NUM_NCONFIG    = _to_int(0xBFFF00A5)
VI_ERROR_CONN_LOST           = _to_int(0xBFFF00A6)

# The following two are a non-standard NI extensions
VI_ERROR_MACHINE_NAVAIL      = _to_int(0xBFFF00A7)
VI_ERROR_NPERMISSION         = _to_int(0xBFFF00A8)


#
# Attribute constants
#
# All attribute codes are unsigned long, so no _to_int() is necessary.
#

VI_ATTR_RSRC_CLASS           = 0xBFFF0001
VI_ATTR_RSRC_NAME            = 0xBFFF0002
VI_ATTR_RSRC_IMPL_VERSION    = 0x3FFF0003
VI_ATTR_RSRC_LOCK_STATE      = 0x3FFF0004
VI_ATTR_MAX_QUEUE_LENGTH     = 0x3FFF0005
VI_ATTR_USER_DATA_32         = 0x3FFF0007
VI_ATTR_USER_DATA_64         = 0x3FFF000A
VI_ATTR_USER_DATA            = (
    VI_ATTR_USER_DATA_64 if is_64bits else VI_ATTR_USER_DATA_32
)
VI_ATTR_FDC_CHNL             = 0x3FFF000D
VI_ATTR_FDC_MODE             = 0x3FFF000F
VI_ATTR_FDC_GEN_SIGNAL_EN    = 0x3FFF0011
VI_ATTR_FDC_USE_PAIR         = 0x3FFF0013
VI_ATTR_SEND_END_EN          = 0x3FFF0016
VI_ATTR_TERMCHAR             = 0x3FFF0018
VI_ATTR_TMO_VALUE            = 0x3FFF001A
VI_ATTR_GPIB_READDR_EN       = 0x3FFF001B
VI_ATTR_IO_PROT              = 0x3FFF001C
VI_ATTR_DMA_ALLOW_EN         = 0x3FFF001E

VI_ATTR_ASRL_BAUD            = 0x3FFF0021
VI_ATTR_ASRL_DATA_BITS       = 0x3FFF0022
VI_ATTR_ASRL_PARITY          = 0x3FFF0023
VI_ATTR_ASRL_STOP_BITS       = 0x3FFF0024
VI_ATTR_ASRL_FLOW_CNTRL      = 0x3FFF0025
VI_ATTR_ASRL_DISCARD_NULL    = 0x3FFF00B0
VI_ATTR_ASRL_CONNECTED       = 0x3FFF01BB
VI_ATTR_ASRL_BREAK_STATE     = 0x3FFF01BC
VI_ATTR_ASRL_BREAK_LEN       = 0x3FFF01BD
VI_ATTR_ASRL_ALLOW_TRANSMIT  = 0x3FFF01BE
VI_ATTR_ASRL_WIRE_MODE       = 0x3FFF01BF  # National instrument only

VI_ATTR_RD_BUF_OPER_MODE     = 0x3FFF002A
VI_ATTR_RD_BUF_SIZE          = 0x3FFF002B
VI_ATTR_WR_BUF_OPER_MODE     = 0x3FFF002D
VI_ATTR_WR_BUF_SIZE          = 0x3FFF002E
VI_ATTR_SUPPRESS_END_EN      = 0x3FFF0036
VI_ATTR_TERMCHAR_EN          = 0x3FFF0038
VI_ATTR_DEST_ACCESS_PRIV     = 0x3FFF0039
VI_ATTR_DEST_BYTE_ORDER      = 0x3FFF003A
VI_ATTR_SRC_ACCESS_PRIV      = 0x3FFF003C
VI_ATTR_SRC_BYTE_ORDER       = 0x3FFF003D
VI_ATTR_SRC_INCREMENT        = 0x3FFF0040
VI_ATTR_DEST_INCREMENT       = 0x3FFF0041
VI_ATTR_WIN_ACCESS_PRIV      = 0x3FFF0045
VI_ATTR_WIN_BYTE_ORDER       = 0x3FFF0047

VI_ATTR_GPIB_ATN_STATE       = 0x3FFF0057
VI_ATTR_GPIB_ADDR_STATE      = 0x3FFF005C
VI_ATTR_GPIB_CIC_STATE       = 0x3FFF005E
VI_ATTR_GPIB_NDAC_STATE      = 0x3FFF0062
VI_ATTR_GPIB_SRQ_STATE       = 0x3FFF0067
VI_ATTR_GPIB_SYS_CNTRL_STATE = 0x3FFF0068
VI_ATTR_GPIB_HS488_CBL_LEN   = 0x3FFF0069
VI_ATTR_CMDR_LA              = 0x3FFF006B
VI_ATTR_VXI_DEV_CLASS        = 0x3FFF006C
VI_ATTR_MAINFRAME_LA         = 0x3FFF0070
VI_ATTR_MANF_NAME            = 0xBFFF0072
VI_ATTR_MODEL_NAME           = 0xBFFF0077
VI_ATTR_VXI_VME_INTR_STATUS  = 0x3FFF008B
VI_ATTR_VXI_TRIG_STATUS      = 0x3FFF008D
VI_ATTR_VXI_VME_SYSFAIL_STATE = 0x3FFF0094

VI_ATTR_WIN_BASE_ADDR_32     = 0x3FFF0098
VI_ATTR_WIN_BASE_ADDR_64     = 0x3FFF009B
VI_ATTR_WIN_BASE_ADDR        = (
    VI_ATTR_WIN_BASE_ADDR_64 if is_64bits else VI_ATTR_WIN_BASE_ADDR_32
)
VI_ATTR_WIN_SIZE             = 0x3FFF009A
VI_ATTR_ASRL_AVAIL_NUM       = 0x3FFF00AC
VI_ATTR_MEM_BASE_32          = 0x3FFF00AD
VI_ATTR_MEM_BASE_64          = 0x3FFF00D0
VI_ATTR_MEM_BASE             = (
    VI_ATTR_MEM_BASE_64 if is_64bits else VI_ATTR_MEM_BASE_32
)
VI_ATTR_ASRL_CTS_STATE       = 0x3FFF00AE
VI_ATTR_ASRL_DCD_STATE       = 0x3FFF00AF
VI_ATTR_ASRL_DSR_STATE       = 0x3FFF00B1
VI_ATTR_ASRL_DTR_STATE       = 0x3FFF00B2
VI_ATTR_ASRL_END_IN          = 0x3FFF00B3
VI_ATTR_ASRL_END_OUT         = 0x3FFF00B4
VI_ATTR_ASRL_REPLACE_CHAR    = 0x3FFF00BE
VI_ATTR_ASRL_RI_STATE        = 0x3FFF00BF
VI_ATTR_ASRL_RTS_STATE       = 0x3FFF00C0
VI_ATTR_ASRL_XON_CHAR        = 0x3FFF00C1
VI_ATTR_ASRL_XOFF_CHAR       = 0x3FFF00C2
VI_ATTR_WIN_ACCESS           = 0x3FFF00C3
VI_ATTR_RM_SESSION           = 0x3FFF00C4
VI_ATTR_VXI_LA               = 0x3FFF00D5
VI_ATTR_MANF_ID              = 0x3FFF00D9
VI_ATTR_MEM_SIZE_32          = 0x3FFF00DD
VI_ATTR_MEM_SIZE_64          = 0x3FFF00D1
VI_ATTR_MEM_SIZE             = (
    VI_ATTR_MEM_SIZE_64 if is_64bits else VI_ATTR_MEM_SIZE_32
)
VI_ATTR_MEM_SPACE            = 0x3FFF00DE
VI_ATTR_MODEL_CODE           = 0x3FFF00DF
VI_ATTR_SLOT                 = 0x3FFF00E8
VI_ATTR_INTF_INST_NAME       = 0xBFFF00E9
VI_ATTR_IMMEDIATE_SERV       = 0x3FFF0100
VI_ATTR_INTF_PARENT_NUM      = 0x3FFF0101
VI_ATTR_RSRC_SPEC_VERSION    = 0x3FFF0170
VI_ATTR_INTF_TYPE            = 0x3FFF0171
VI_ATTR_GPIB_PRIMARY_ADDR    = 0x3FFF0172
VI_ATTR_GPIB_SECONDARY_ADDR  = 0x3FFF0173
VI_ATTR_RSRC_MANF_NAME       = 0xBFFF0174
VI_ATTR_RSRC_MANF_ID         = 0x3FFF0175
VI_ATTR_INTF_NUM             = 0x3FFF0176
VI_ATTR_TRIG_ID              = 0x3FFF0177
VI_ATTR_GPIB_REN_STATE       = 0x3FFF0181
VI_ATTR_GPIB_UNADDR_EN       = 0x3FFF0184
VI_ATTR_DEV_STATUS_BYTE      = 0x3FFF0189
VI_ATTR_FILE_APPEND_EN       = 0x3FFF0192
VI_ATTR_VXI_TRIG_SUPPORT     = 0x3FFF0194
VI_ATTR_TCPIP_ADDR           = 0xBFFF0195
VI_ATTR_TCPIP_HOSTNAME       = 0xBFFF0196
VI_ATTR_TCPIP_PORT           = 0x3FFF0197
VI_ATTR_TCPIP_DEVICE_NAME    = 0xBFFF0199
VI_ATTR_TCPIP_NODELAY        = 0x3FFF019A
VI_ATTR_TCPIP_KEEPALIVE      = 0x3FFF019B
VI_ATTR_TCPIP_HISLIP_OVERLAP_EN = 0x3FFF0300
VI_ATTR_TCPIP_HISLIP_VERSION = 0x3FFF0301
VI_ATTR_TCPIP_HISLIP_MAX_MESSAGE_KB = 0x3FFF0302
VI_ATTR_TCPIP_IS_HISLIP      = 0x3FFF0303
VI_ATTR_4882_COMPLIANT       = 0x3FFF019F
VI_ATTR_USB_SERIAL_NUM       = 0xBFFF01A0
VI_ATTR_USB_INTFC_NUM        = 0x3FFF01A1
VI_ATTR_USB_PROTOCOL         = 0x3FFF01A7
VI_ATTR_USB_MAX_INTR_SIZE    = 0x3FFF01AF
VI_ATTR_USB_BULK_OUT_PIPE    = _to_int(0x3FFF01A2)
VI_ATTR_USB_BULK_IN_PIPE     = _to_int(0x3FFF01A3)
VI_ATTR_USB_INTR_IN_PIPE     = _to_int(0x3FFF01A4)
VI_ATTR_USB_CLASS            = _to_int(0x3FFF01A5)
VI_ATTR_USB_SUBCLASS         = _to_int(0x3FFF01A6)
VI_ATTR_USB_ALT_SETTING      = _to_int(0x3FFF01A8)
VI_ATTR_USB_END_IN           = _to_int(0x3FFF01A9)
VI_ATTR_USB_NUM_INTFCS       = _to_int(0x3FFF01AA)
VI_ATTR_USB_NUM_PIPES        = _to_int(0x3FFF01AB)
VI_ATTR_USB_BULK_OUT_STATUS  = _to_int(0x3FFF01AC)
VI_ATTR_USB_BULK_IN_STATUS   = _to_int(0x3FFF01AD)
VI_ATTR_USB_INTR_IN_STATUS   = _to_int(0x3FFF01AE)
VI_ATTR_USB_CTRL_PIPE        = _to_int(0x3FFF01B0)
VI_ATTR_USB_RECV_INTR_SIZE   = 0x3FFF41B0
VI_ATTR_USB_RECV_INTR_DATA   = 0xBFFF41B1

VI_ATTR_JOB_ID               = 0x3FFF4006
VI_ATTR_EVENT_TYPE           = 0x3FFF4010
VI_ATTR_SIGP_STATUS_ID       = 0x3FFF4011
VI_ATTR_RECV_TRIG_ID         = 0x3FFF4012
VI_ATTR_INTR_STATUS_ID       = 0x3FFF4023
VI_ATTR_STATUS               = 0x3FFF4025
VI_ATTR_RET_COUNT_32         = 0x3FFF4026
VI_ATTR_RET_COUNT_64         = 0x3FFF4028
VI_ATTR_RET_COUNT            = VI_ATTR_RET_COUNT_64 if is_64bits else VI_ATTR_RET_COUNT_32
VI_ATTR_BUFFER               = 0x3FFF4027
VI_ATTR_RECV_INTR_LEVEL      = 0x3FFF4041
VI_ATTR_OPER_NAME            = 0xBFFF4042
VI_ATTR_GPIB_RECV_CIC_STATE  = 0x3FFF4193
VI_ATTR_RECV_TCPIP_ADDR      = 0xBFFF4198

VI_ATTR_PXI_DEV_NUM          = _to_int(0x3FFF0201)
VI_ATTR_PXI_FUNC_NUM         = _to_int(0x3FFF0202)
VI_ATTR_PXI_BUS_NUM          = _to_int(0x3FFF0205)
VI_ATTR_PXI_CHASSIS          = _to_int(0x3FFF0206)
VI_ATTR_PXI_SLOTPATH         = _to_int(0xBFFF0207)
VI_ATTR_PXI_SLOT_LBUS_LEFT   = _to_int(0x3FFF0208)
VI_ATTR_PXI_SLOT_LBUS_RIGHT  = _to_int(0x3FFF0209)
VI_ATTR_PXI_TRIG_BUS         = _to_int(0x3FFF020A)
VI_ATTR_PXI_STAR_TRIG_BUS    = _to_int(0x3FFF020B)
VI_ATTR_PXI_STAR_TRIG_LINE   = _to_int(0x3FFF020C)

VI_ATTR_PXI_IS_EXPRESS       = _to_int(0x3FFF0240)
VI_ATTR_PXI_SLOT_LWIDTH      = _to_int(0x3FFF0241)
VI_ATTR_PXI_MAX_LWIDTH       = _to_int(0x3FFF0242)
VI_ATTR_PXI_ACTUAL_LWIDTH    = _to_int(0x3FFF0243)
VI_ATTR_PXI_DSTAR_BUS        = _to_int(0x3FFF0244)
VI_ATTR_PXI_DSTAR_SET        = _to_int(0x3FFF0245)

VI_ATTR_PXI_SRC_TRIG_BUS     = _to_int(0x3FFF020D)
VI_ATTR_PXI_DEST_TRIG_BUS    = _to_int(0x3FFF020E)

VI_ATTR_PXI_RECV_INTR_SEQ    = _to_int(0x3FFF4240)
VI_ATTR_PXI_RECV_INTR_DATA   = _to_int(0x3FFF4241)

VI_ATTR_PXI_MEM_TYPE_BAR0    = _to_int(0x3FFF0211)
VI_ATTR_PXI_MEM_TYPE_BAR1    = _to_int(0x3FFF0212)
VI_ATTR_PXI_MEM_TYPE_BAR2    = _to_int(0x3FFF0213)
VI_ATTR_PXI_MEM_TYPE_BAR3    = _to_int(0x3FFF0214)
VI_ATTR_PXI_MEM_TYPE_BAR4    = _to_int(0x3FFF0215)
VI_ATTR_PXI_MEM_TYPE_BAR5    = _to_int(0x3FFF0216)

VI_ATTR_PXI_MEM_BASE_BAR0_32 = _to_int(0x3FFF0221)
VI_ATTR_PXI_MEM_BASE_BAR1_32 = _to_int(0x3FFF0222)
VI_ATTR_PXI_MEM_BASE_BAR2_32 = _to_int(0x3FFF0223)
VI_ATTR_PXI_MEM_BASE_BAR3_32 = _to_int(0x3FFF0224)
VI_ATTR_PXI_MEM_BASE_BAR4_32 = _to_int(0x3FFF0225)
VI_ATTR_PXI_MEM_BASE_BAR5_32 = _to_int(0x3FFF0226)
VI_ATTR_PXI_MEM_SIZE_BAR0_32 = _to_int(0x3FFF0231)
VI_ATTR_PXI_MEM_SIZE_BAR1_32 = _to_int(0x3FFF0232)
VI_ATTR_PXI_MEM_SIZE_BAR2_32 = _to_int(0x3FFF0233)
VI_ATTR_PXI_MEM_SIZE_BAR3_32 = _to_int(0x3FFF0234)
VI_ATTR_PXI_MEM_SIZE_BAR4_32 = _to_int(0x3FFF0235)
VI_ATTR_PXI_MEM_SIZE_BAR5_32 = _to_int(0x3FFF0236)

VI_ATTR_PXI_MEM_BASE_BAR0_64 = _to_int(0x3FFF0228)
VI_ATTR_PXI_MEM_BASE_BAR1_64 = _to_int(0x3FFF0229)
VI_ATTR_PXI_MEM_BASE_BAR2_64 = _to_int(0x3FFF022A)
VI_ATTR_PXI_MEM_BASE_BAR3_64 = _to_int(0x3FFF022B)
VI_ATTR_PXI_MEM_BASE_BAR4_64 = _to_int(0x3FFF022C)
VI_ATTR_PXI_MEM_BASE_BAR5_64 = _to_int(0x3FFF022D)
VI_ATTR_PXI_MEM_SIZE_BAR0_64 = _to_int(0x3FFF0238)
VI_ATTR_PXI_MEM_SIZE_BAR1_64 = _to_int(0x3FFF0239)
VI_ATTR_PXI_MEM_SIZE_BAR2_64 = _to_int(0x3FFF023A)
VI_ATTR_PXI_MEM_SIZE_BAR3_64 = _to_int(0x3FFF023B)
VI_ATTR_PXI_MEM_SIZE_BAR4_64 = _to_int(0x3FFF023C)
VI_ATTR_PXI_MEM_SIZE_BAR5_64 = _to_int(0x3FFF023D)

VI_ATTR_PXI_MEM_BASE_BAR0   = (
    VI_ATTR_PXI_MEM_BASE_BAR0_64 if is_64bits else VI_ATTR_PXI_MEM_BASE_BAR0_32
)
VI_ATTR_PXI_MEM_BASE_BAR1   = (
    VI_ATTR_PXI_MEM_BASE_BAR1_64 if is_64bits else VI_ATTR_PXI_MEM_BASE_BAR1_32
)
VI_ATTR_PXI_MEM_BASE_BAR2   = (
    VI_ATTR_PXI_MEM_BASE_BAR2_64 if is_64bits else VI_ATTR_PXI_MEM_BASE_BAR2_32
)
VI_ATTR_PXI_MEM_BASE_BAR3   = (
    VI_ATTR_PXI_MEM_BASE_BAR3_64 if is_64bits else VI_ATTR_PXI_MEM_BASE_BAR3_32
)
VI_ATTR_PXI_MEM_BASE_BAR4   = (
    VI_ATTR_PXI_MEM_BASE_BAR4_64 if is_64bits else VI_ATTR_PXI_MEM_BASE_BAR4_32
)
VI_ATTR_PXI_MEM_BASE_BAR5   = (
    VI_ATTR_PXI_MEM_BASE_BAR5_64 if is_64bits else VI_ATTR_PXI_MEM_BASE_BAR5_32
)
VI_ATTR_PXI_MEM_SIZE_BAR0   = (
    VI_ATTR_PXI_MEM_SIZE_BAR0_64 if is_64bits else VI_ATTR_PXI_MEM_SIZE_BAR0_32
)
VI_ATTR_PXI_MEM_SIZE_BAR1   = (
    VI_ATTR_PXI_MEM_SIZE_BAR1_64 if is_64bits else VI_ATTR_PXI_MEM_SIZE_BAR1_32
)
VI_ATTR_PXI_MEM_SIZE_BAR2   = (
    VI_ATTR_PXI_MEM_SIZE_BAR2_64 if is_64bits else VI_ATTR_PXI_MEM_SIZE_BAR2_32
)
VI_ATTR_PXI_MEM_SIZE_BAR3   = (
    VI_ATTR_PXI_MEM_SIZE_BAR3_64 if is_64bits else VI_ATTR_PXI_MEM_SIZE_BAR3_32
)
VI_ATTR_PXI_MEM_SIZE_BAR4   = (
    VI_ATTR_PXI_MEM_SIZE_BAR4_64 if is_64bits else VI_ATTR_PXI_MEM_SIZE_BAR4_32
)
VI_ATTR_PXI_MEM_SIZE_BAR5   = (
    VI_ATTR_PXI_MEM_SIZE_BAR5_64 if is_64bits else VI_ATTR_PXI_MEM_SIZE_BAR5_32
)

#
# Event Types
#
# All event codes are unsigned long, so no _to_int() is necessary.
#

VI_EVENT_IO_COMPLETION       = 0x3FFF2009
VI_EVENT_TRIG                = 0xBFFF200A
VI_EVENT_SERVICE_REQ         = 0x3FFF200B
VI_EVENT_CLEAR               = 0x3FFF200D
VI_EVENT_EXCEPTION           = 0xBFFF200E
VI_EVENT_GPIB_CIC            = 0x3FFF2012
VI_EVENT_GPIB_TALK           = 0x3FFF2013
VI_EVENT_GPIB_LISTEN         = 0x3FFF2014
VI_EVENT_VXI_VME_SYSFAIL     = 0x3FFF201D
VI_EVENT_VXI_VME_SYSRESET    = 0x3FFF201E
VI_EVENT_VXI_SIGP            = 0x3FFF2020
VI_EVENT_VXI_VME_INTR        = 0xBFFF2021
VI_EVENT_PXI_INTR            = 0x3FFF2022
VI_EVENT_TCPIP_CONNECT       = 0x3FFF2036
VI_EVENT_USB_INTR            = 0x3FFF2037
VI_ALL_ENABLED_EVENTS        = 0x3FFF7FFF

VI_ATTR_VXI_TRIG_DIR        = _to_int(0x3FFF4044)
VI_ATTR_VXI_TRIG_LINES_EN   = _to_int(0x3FFF4043)

#
# Values and Ranges
#

VI_FIND_BUFLEN               = 256
VI_NULL                      = 0

VI_TRUE                      = 1
VI_FALSE                     = 0

VI_INTF_GPIB                 = 1
VI_INTF_VXI                  = 2
VI_INTF_GPIB_VXI             = 3
VI_INTF_ASRL                 = 4
VI_INTF_PXI                  = 5
VI_INTF_TCPIP                = 6
VI_INTF_USB                  = 7
VI_INTF_RIO                  = 8
VI_INTF_FIREWIRE             = 9

VI_PROT_NORMAL               = 1
VI_PROT_FDC                  = 2
VI_PROT_HS488                = 3
VI_PROT_4882_STRS            = 4
VI_PROT_USBTMC_VENDOR        = 5

VI_FDC_NORMAL                = 1
VI_FDC_STREAM                = 2

VI_LOCAL_SPACE               = 0
VI_A16_SPACE                 = 1
VI_A24_SPACE                 = 2
VI_A32_SPACE                 = 3
VI_A64_SPACE                 = 4
VI_OPAQUE_SPACE              = 0xFFFF

VI_UNKNOWN_LA                = -1
VI_UNKNOWN_SLOT              = -1
VI_UNKNOWN_LEVEL             = -1

VI_QUEUE                     = 1
VI_HNDLR                     = 2
VI_SUSPEND_HNDLR             = 4
VI_ALL_MECH                  = 0xFFFF

VI_ANY_HNDLR                 = 0

VI_TRIG_ALL                  = -2
VI_TRIG_SW                   = -1
VI_TRIG_TTL0                 = 0
VI_TRIG_TTL1                 = 1
VI_TRIG_TTL2                 = 2
VI_TRIG_TTL3                 = 3
VI_TRIG_TTL4                 = 4
VI_TRIG_TTL5                 = 5
VI_TRIG_TTL6                 = 6
VI_TRIG_TTL7                 = 7
VI_TRIG_TTL8                 = 32
VI_TRIG_TTL9                 = 33
VI_TRIG_TTL10                = 34
VI_TRIG_TTL11                = 35
VI_TRIG_ECL0                 = 8
VI_TRIG_ECL1                 = 9
VI_TRIG_ECL2                 = 10
VI_TRIG_ECL3                 = 11
VI_TRIG_ECL4                 = 12
VI_TRIG_ECL5                 = 13
VI_TRIG_STAR_SLOT1           = 14
VI_TRIG_STAR_SLOT2           = 15
VI_TRIG_STAR_SLOT3           = 16
VI_TRIG_STAR_SLOT4           = 17
VI_TRIG_STAR_SLOT5           = 18
VI_TRIG_STAR_SLOT6           = 19
VI_TRIG_STAR_SLOT7           = 20
VI_TRIG_STAR_SLOT8           = 21
VI_TRIG_STAR_SLOT9           = 22
VI_TRIG_STAR_SLOT10          = 23
VI_TRIG_STAR_SLOT11          = 24
VI_TRIG_STAR_SLOT12          = 25
VI_TRIG_STAR_INSTR           = 26
VI_TRIG_PANEL_IN             = 27
VI_TRIG_PANEL_OUT            = 28
VI_TRIG_STAR_VXI0            = 29
VI_TRIG_STAR_VXI1            = 30
VI_TRIG_STAR_VXI2            = 31

VI_TRIG_PROT_DEFAULT         = 0
VI_TRIG_PROT_ON              = 1
VI_TRIG_PROT_OFF             = 2
VI_TRIG_PROT_SYNC            = 5
VI_TRIG_PROT_RESERVE         = 6
VI_TRIG_PROT_UNRESERVE       = 7

VI_READ_BUF                  = 1
VI_WRITE_BUF                 = 2
VI_READ_BUF_DISCARD          = 4
VI_WRITE_BUF_DISCARD         = 8
VI_IO_IN_BUF                 = 16
VI_IO_OUT_BUF                = 32
VI_IO_IN_BUF_DISCARD         = 64
VI_IO_OUT_BUF_DISCARD        = 128

VI_FLUSH_ON_ACCESS           = 1
VI_FLUSH_WHEN_FULL           = 2
VI_FLUSH_DISABLE             = 3

VI_NMAPPED                   = 1
VI_USE_OPERS                 = 2
VI_DEREF_ADDR                = 3

VI_TMO_IMMEDIATE             = 0
# Attention! The following is *really* positive!  (unsigned long)
VI_TMO_INFINITE              = 0xFFFFFFFF

VI_NO_LOCK                   = 0
VI_EXCLUSIVE_LOCK            = 1
VI_SHARED_LOCK               = 2
VI_LOAD_CONFIG               = 4

VI_NO_SEC_ADDR               = 0xFFFF

VI_ASRL_PAR_NONE             = 0
VI_ASRL_PAR_ODD              = 1
VI_ASRL_PAR_EVEN             = 2
VI_ASRL_PAR_MARK             = 3
VI_ASRL_PAR_SPACE            = 4

VI_ASRL_STOP_ONE             = 10
VI_ASRL_STOP_ONE5            = 15
VI_ASRL_STOP_TWO             = 20

VI_ASRL_FLOW_NONE            = 0
VI_ASRL_FLOW_XON_XOFF        = 1
VI_ASRL_FLOW_RTS_CTS         = 2
VI_ASRL_FLOW_DTR_DSR         = 4

VI_ASRL_END_NONE             = 0
VI_ASRL_END_LAST_BIT         = 1
VI_ASRL_END_TERMCHAR         = 2
VI_ASRL_END_BREAK            = 3

# The following are National Instrument only
VI_ASRL_WIRE_485_4           = 0
VI_ASRL_WIRE_485_2_DTR_ECHO  = 1
VI_ASRL_WIRE_485_2_DTR_CTRL  = 2
VI_ASRL_WIRE_485_2_AUTO      = 3
VI_ASRL_WIRE_232_DTE         = 128
VI_ASRL_WIRE_232_DCE         = 129
VI_ASRL_WIRE_232_AUTO        = 130

VI_STATE_ASSERTED            = 1
VI_STATE_UNASSERTED          = 0
VI_STATE_UNKNOWN             = -1

VI_BIG_ENDIAN                = 0
VI_LITTLE_ENDIAN             = 1

VI_DATA_PRIV                 = 0
VI_DATA_NPRIV                = 1
VI_PROG_PRIV                 = 2
VI_PROG_NPRIV                = 3
VI_BLCK_PRIV                 = 4
VI_BLCK_NPRIV                = 5
VI_D64_PRIV                  = 6
VI_D64_NPRIV                 = 7
VI_D64_2EVME                 = 8
VI_D64_SST160                = 9
VI_D64_SST267                = 10
VI_D64_SST320                = 11


VI_WIDTH_8                   = 1
VI_WIDTH_16                  = 2
VI_WIDTH_32                  = 4
VI_WIDTH_64                  = 8

VI_GPIB_REN_DEASSERT         = 0
VI_GPIB_REN_ASSERT           = 1
VI_GPIB_REN_DEASSERT_GTL     = 2
VI_GPIB_REN_ASSERT_ADDRESS   = 3
VI_GPIB_REN_ASSERT_LLO       = 4
VI_GPIB_REN_ASSERT_ADDRESS_LLO = 5
VI_GPIB_REN_ADDRESS_GTL      = 6

VI_GPIB_ATN_DEASSERT         = 0
VI_GPIB_ATN_ASSERT           = 1
VI_GPIB_ATN_DEASSERT_HANDSHAKE = 2
VI_GPIB_ATN_ASSERT_IMMEDIATE = 3

VI_GPIB_HS488_DISABLED       = 0
VI_GPIB_HS488_NIMPL          = -1

VI_GPIB_UNADDRESSED          = 0
VI_GPIB_TALKER               = 1
VI_GPIB_LISTENER             = 2

VI_VXI_CMD16                 = 0x0200
VI_VXI_CMD16_RESP16          = 0x0202
VI_VXI_RESP16                = 0x0002
VI_VXI_CMD32                 = 0x0400
VI_VXI_CMD32_RESP16          = 0x0402
VI_VXI_CMD32_RESP32          = 0x0404
VI_VXI_RESP32                = 0x0004

VI_ASSERT_SIGNAL             = -1
VI_ASSERT_USE_ASSIGNED       = 0
VI_ASSERT_IRQ1               = 1
VI_ASSERT_IRQ2               = 2
VI_ASSERT_IRQ3               = 3
VI_ASSERT_IRQ4               = 4
VI_ASSERT_IRQ5               = 5
VI_ASSERT_IRQ6               = 6
VI_ASSERT_IRQ7               = 7

VI_UTIL_ASSERT_SYSRESET      = 1
VI_UTIL_ASSERT_SYSFAIL       = 2
VI_UTIL_DEASSERT_SYSFAIL     = 3

VI_VXI_CLASS_MEMORY          = 0
VI_VXI_CLASS_EXTENDED        = 1
VI_VXI_CLASS_MESSAGE         = 2
VI_VXI_CLASS_REGISTER        = 3
VI_VXI_CLASS_OTHER           = 4

VI_PXI_LBUS_UNKNOWN = -1
VI_PXI_LBUS_NONE    = 0
VI_PXI_LBUS_STAR_TRIG_BUS_0 = 1000
VI_PXI_LBUS_STAR_TRIG_BUS_1 = 1001
VI_PXI_LBUS_STAR_TRIG_BUS_2 = 1002
VI_PXI_LBUS_STAR_TRIG_BUS_3 = 1003
VI_PXI_LBUS_STAR_TRIG_BUS_4 = 1004
VI_PXI_LBUS_STAR_TRIG_BUS_5 = 1005
VI_PXI_LBUS_STAR_TRIG_BUS_6 = 1006
VI_PXI_LBUS_STAR_TRIG_BUS_7 = 1007
VI_PXI_LBUS_STAR_TRIG_BUS_8 = 1008
VI_PXI_LBUS_STAR_TRIG_BUS_9 = 1009
VI_PXI_STAR_TRIG_CONTROLLER = 1413
VI_PXI_LBUS_SCXI = 2000
VI_PXI_ALLOC_SPACE = 9
VI_PXI_CFG_SPACE = 10
VI_PXI_BAR0_SPACE = 11
VI_PXI_BAR1_SPACE = 12
VI_PXI_BAR2_SPACE = 13
VI_PXI_BAR3_SPACE = 14
VI_PXI_BAR4_SPACE = 15
VI_PXI_BAR5_SPACE = 16

VI_PXI_ADDR_NONE = 0
VI_PXI_ADDR_MEM  = 1
VI_PXI_ADDR_IO   = 2
VI_PXI_ADDR_CFG  = 3

VI_USB_PIPE_STATE_UNKNOWN = -1
VI_USB_PIPE_READY = 0
VI_USB_PIPE_STALLED = 1

# From VI_ATTR_USB_END_IN
VI_USB_END_NONE             = 0
VI_USB_END_SHORT            = 4
VI_USB_END_SHORT_OR_COUNT   = 5

# "Backwards compatibility" according to NI

VI_NORMAL                    = VI_PROT_NORMAL
VI_FDC                       = VI_PROT_FDC
VI_HS488                     = VI_PROT_HS488
VI_ASRL488                   = VI_PROT_4882_STRS
VI_ASRL_IN_BUF               = VI_IO_IN_BUF
VI_ASRL_OUT_BUF              = VI_IO_OUT_BUF
VI_ASRL_IN_BUF_DISCARD       = VI_IO_IN_BUF_DISCARD
VI_ASRL_OUT_BUF_DISCARD      = VI_IO_OUT_BUF_DISCARD

# fmt: on

# ======================================================================================
# --- Enumeration for easier handling of the constants ---------------------------------
# ======================================================================================


@enum.unique
class VisaBoolean(enum.IntEnum):
    """Visa boolean values."""

    true = VI_TRUE
    false = VI_FALSE


# Constants useful for all kind of resources.


@enum.unique
class Timeouts(enum.IntEnum):
    """Special timeout values."""

    #: Minimal timeout value
    immediate = VI_TMO_IMMEDIATE

    #: Infinite timeout
    infinite = VI_TMO_INFINITE


@enum.unique
class Lock(enum.IntEnum):
    """Kind of lock to use when locking a resource."""

    #: Obtains a exclusive lock on the VISA resource.
    exclusive = VI_EXCLUSIVE_LOCK

    #: Obtains a lock on the VISA resouce which may be shared
    #: between multiple VISA sessions.
    shared = VI_SHARED_LOCK


@enum.unique
class AccessModes(enum.IntEnum):
    """Whether and how to lock a resource when opening a connection."""

    #: Does not obtain any lock on the VISA resource.
    no_lock = VI_NO_LOCK

    #: Obtains a exclusive lock on the VISA resource.
    exclusive_lock = VI_EXCLUSIVE_LOCK

    #: Obtains a lock on the VISA resouce which may be shared
    #: between multiple VISA sessions.
    shared_lock = VI_SHARED_LOCK


@enum.unique
class InterfaceType(enum.IntEnum):
    """The hardware interface."""

    # Used for unknown interface type strings.
    unknown = -1

    #: GPIB Interface.
    gpib = VI_INTF_GPIB

    #: VXI (VME eXtensions for Instrumentation), VME, MXI (Multisystem eXtension Interface).
    vxi = VI_INTF_VXI

    #: GPIB VXI (VME eXtensions for Instrumentation).
    gpib_vxi = VI_INTF_GPIB_VXI

    #: Serial devices connected to either an RS-232 or RS-485 controller.
    asrl = VI_INTF_ASRL

    #: PXI device.
    pxi = VI_INTF_PXI

    #: TCPIP device.
    tcpip = VI_INTF_TCPIP

    #: Universal Serial Bus (USB) hardware bus.
    usb = VI_INTF_USB

    #: Rio device.
    rio = VI_INTF_RIO

    #: Firewire device.
    firewire = VI_INTF_FIREWIRE

    #: Rohde and Schwarz Device via Passport
    rsnrp = 33024

    #: Lecroy VICP via passport
    vicp = 36000  # FIXME

    #: prologix usb
    prlgx_asrl = 34567  # an arbitrarily chosen value

    #: prologix tcpip
    prlgx_tcpip = 76543  # an arbitrarily chosen value


@enum.unique
class LineState(enum.IntEnum):
    """State of a hardware line or signal.

    The line for which the state can be queried are:
    - ASRC resource: BREAK, CTS, DCD, DSR, DTR, RI, RTS signals
    - GPIB resources: ATN, NDAC, REN, SRQ lines
    - VXI BACKPLANE:  VXI/VME SYSFAIL backplane line

    Search for LineState in attributes.py for more details.

    """

    #: The line/signal is currently asserted
    asserted = VI_STATE_ASSERTED

    #: The line/signal is currently deasserted
    unasserted = VI_STATE_UNASSERTED

    #: The state of the line/signal is unknown
    unknown = VI_STATE_UNKNOWN


@enum.unique
class IOProtocol(enum.IntEnum):
    """IO protocol used for communication.

    See attributes.AttrVI_ATTR_IO_PROT for more details.

    """

    normal = VI_PROT_NORMAL

    #: Fast data channel (FDC) protocol for VXI
    fdc = VI_PROT_FDC

    #: High speed 488 transfer for GPIB
    hs488 = VI_PROT_HS488

    #: 488 style transfer for serial
    protocol4882_strs = VI_PROT_4882_STRS

    #: Test measurement class vendor specific for USB
    usbtmc_vendor = VI_PROT_USBTMC_VENDOR


@enum.unique
class EventMechanism(enum.IntEnum):
    """The available event mechanisms for event handling."""

    #: Queue events that can then be queried using wait_on_event
    queue = VI_QUEUE

    #: Use a specified callback handler to deal with events
    handler = VI_HNDLR

    #: Queue events to be passed to the handler when the system is switch to the
    #: handler mechanism.
    suspend_handler = VI_SUSPEND_HNDLR

    #: Use to disable or discard events no matter the handling mechanism
    all = VI_ALL_MECH


# Message based resources relevant constants


@enum.unique
class EventType(enum.IntEnum):
    """The available event types for event handling."""

    #: Notification that an asynchronous operation has completed.
    io_completion = VI_EVENT_IO_COMPLETION

    #: Notification that a trigger interrupt was received from the device.
    #: For VISA, the only triggers that can be sensed are VXI hardware triggers
    #: on the assertion edge (SYNC and ON trigger protocols only).
    trig = VI_EVENT_TRIG

    #: Notification that a service request was received from the device.
    service_request = VI_EVENT_SERVICE_REQ

    #: Notification that the local controller has been sent a device clear message.
    clear = VI_EVENT_CLEAR

    #: Notification that an error condition has occurred during an operation
    #: invocation.
    exception = VI_EVENT_EXCEPTION

    #: Notification that the GPIB controller has gained or lost CIC (controller
    #: in charge) status.
    gpib_controller_in_charge = VI_EVENT_GPIB_CIC

    #: Notification that the GPIB controller has been addressed to talk.
    gpib_talk = VI_EVENT_GPIB_TALK

    #: Notification that the GPIB controller has been addressed to listen.
    gpib_listen = VI_EVENT_GPIB_LISTEN

    #: Notification that the VXI/VME SYSFAIL* line has been asserted.
    vxi_vme_sysfail = VI_EVENT_VXI_VME_SYSFAIL

    #: Notification that the VXI/VME SYSRESET* line has been asserted.
    vxi_vme_sysreset = VI_EVENT_VXI_VME_SYSRESET

    #: Notification that a VXIbus signal or VXIbus interrupt was received from
    #: the device.
    vxi_signal_interrupt = VI_EVENT_VXI_SIGP

    #: Notification that a VXIbus interrupt was received from the device.
    vxi_vme_interrupt = VI_EVENT_VXI_VME_INTR

    #: Notification that a PCI Interrupt was received from the device.
    pxi_interrupt = VI_EVENT_PXI_INTR

    #: Notification that a TCP/IP connection has been made.
    tcpip_connect = VI_EVENT_TCPIP_CONNECT

    #: Notification that a vendor-specific USB interrupt was received from the device.
    usb_interrupt = VI_EVENT_USB_INTR

    #: Value equivalent to all events. Use to switch handling mechanism for all
    #: events in one call or disabling all events.
    all_enabled = VI_ALL_ENABLED_EVENTS


@enum.unique
class BufferType(enum.IntFlag):
    """Buffer potentially available on a message based resource.

    Used with the set_buffer function to alter the size of a buffer.

    """

    #: Formatted read buffer
    read = VI_READ_BUF

    #: Formatted write buffer
    write = VI_WRITE_BUF

    #: I/O communication receive buffer.
    io_in = VI_IO_IN_BUF

    #: I/O communication transmit buffer.
    io_out = VI_IO_OUT_BUF


@enum.unique
class BufferOperation(enum.IntFlag):
    """Possible action of the buffer when performing a flush."""

    #: Discard the read buffer contents and if data was present in the read buffer
    #: and no END-indicator was present, read from the device until encountering
    #: an END indicator (which causes the loss of data).
    discard_read_buffer = VI_READ_BUF

    #: Discard the read buffer contents (does not perform any I/O to the device).
    discard_read_buffer_no_io = VI_READ_BUF_DISCARD

    #: Flush the write buffer by writing all buffered data to the device.
    flush_write_buffer = VI_WRITE_BUF

    #: Discard the write buffer contents (does not perform any I/O to the device).
    discard_write_buffer = VI_WRITE_BUF_DISCARD

    #: Discard the receive buffer contents (does not perform any I/O to the device).
    discard_receive_buffer = VI_IO_IN_BUF_DISCARD

    #: Discards the receive buffer contents (same as VI_IO_IN_BUF_DISCARD)
    discard_receive_buffer2 = VI_IO_IN_BUF

    #: Flush the transmit buffer by writing all buffered data to the device.
    flush_transmit_buffer = VI_IO_OUT_BUF

    #: Discard the transmit buffer contents (does not perform any I/O to the device).
    discard_transmit_buffer = VI_IO_OUT_BUF_DISCARD


# Constants related to serial resources


@enum.unique
class StopBits(enum.IntEnum):
    """The number of stop bits that indicate the end of a frame on a serial resource.

    Used only for ASRL resources.

    """

    one = VI_ASRL_STOP_ONE
    one_and_a_half = VI_ASRL_STOP_ONE5
    two = VI_ASRL_STOP_TWO


@enum.unique
class Parity(enum.IntEnum):
    """Parity type to use with every frame transmitted and received on a serial session.

    Used only for ASRL resources.

    """

    none = VI_ASRL_PAR_NONE
    odd = VI_ASRL_PAR_ODD
    even = VI_ASRL_PAR_EVEN
    mark = VI_ASRL_PAR_MARK
    space = VI_ASRL_PAR_SPACE


@enum.unique
class SerialTermination(enum.IntEnum):
    """The available methods for terminating a serial transfer."""

    #: The transfer terminates when all requested data is transferred
    #: or when an error occurs.
    none = VI_ASRL_END_NONE

    #: The transfer occurs with the last bit not set until the last
    #: character is sent.
    last_bit = VI_ASRL_END_LAST_BIT

    #: The transfer terminate by searching for "/"
    #: appending the termination character.
    termination_char = VI_ASRL_END_TERMCHAR

    #: The write transmits a break after all the characters for the
    #: write are sent.
    termination_break = VI_ASRL_END_BREAK


@enum.unique
class WireMode(enum.IntEnum):
    """Valid modes for National Instruments hardware supporting it."""

    #: 4-wire mode.
    rs485_4 = VI_ASRL_WIRE_485_4

    #: 2-wire DTR mode controlled with echo.
    rs485_2_dtr_echo = VI_ASRL_WIRE_485_2_DTR_ECHO

    #: 2-wire DTR mode controlled without echo
    rs485_2_dtr_ctrl = VI_ASRL_WIRE_485_2_DTR_CTRL

    #: 2-wire auto mode controlled with TXRDY
    rs485_2_auto = VI_ASRL_WIRE_485_2_AUTO

    #: Use DTE mode
    rs232_dte = VI_ASRL_WIRE_232_DTE

    #: Use DCE mode
    rs232_dce = VI_ASRL_WIRE_232_DCE

    #: Auto detect the mode to use
    rs232_auto = VI_ASRL_WIRE_232_AUTO

    #: Unknown mode
    unknown = VI_STATE_UNKNOWN


@enum.unique
class ControlFlow(enum.IntFlag):
    """Control flow for a serial resource."""

    none = VI_ASRL_FLOW_NONE
    xon_xoff = VI_ASRL_FLOW_XON_XOFF
    rts_cts = VI_ASRL_FLOW_RTS_CTS
    dtr_dsr = VI_ASRL_FLOW_DTR_DSR


# USB specific constants


@enum.unique
class USBEndInput(enum.IntEnum):
    """Method used to terminate input on USB RAW."""

    none = VI_USB_END_NONE
    short = VI_USB_END_SHORT
    short_or_count = VI_USB_END_SHORT_OR_COUNT


# GPIB specific value


@enum.unique
class AddressState(enum.IntEnum):
    """State of a GPIB resource.

    Corresponds to the Attribute.GPIB_address_state attribute

    """

    #: The resource is unadressed
    unaddressed = VI_GPIB_UNADDRESSED

    #: The resource is addressed to talk
    talker = VI_GPIB_TALKER

    #: The resource is addressed to listen
    listenr = VI_GPIB_LISTENER


@enum.unique
class ATNLineOperation(enum.IntEnum):
    """Operation that can be performed on the GPIB ATN line.

    These operations are available only to GPIB INTFC resources

    """

    #: Assert ATN line synchronously (in 488 terminology). If a data handshake
    #: is in progress, ATN will not be asserted until the handshake is complete.
    asrt = VI_GPIB_ATN_ASSERT

    #: Assert ATN line asynchronously (in 488 terminology). This should generally
    #: be used only under error conditions.
    asrt_immediate = VI_GPIB_ATN_ASSERT_IMMEDIATE

    #: Deassert the ATN line
    deassert = VI_GPIB_ATN_DEASSERT

    #: Deassert ATN line, and enter shadow handshake mode. The local board will
    #: participate in data handshakes as an Acceptor without actually reading the data.
    deassert_handshake = VI_GPIB_ATN_DEASSERT_HANDSHAKE


@enum.unique
class RENLineOperation(enum.IntEnum):
    """Operation that can be performed on the REN line.

    Some of these operation are available to GPIB INSTR, GPIB INTFC, USB INSTR,
    TCPIP INSTR, please see the VISA reference for more details.

    """

    #: Send the Go To Local command (GTL) to this device.
    address_gtl = VI_GPIB_REN_ADDRESS_GTL

    #: Assert REN line.
    asrt = VI_GPIB_REN_ASSERT

    #: Assert REN line and address this device.
    asrt_address = VI_GPIB_REN_ASSERT_ADDRESS

    #: Address this device and send it LLO, putting it in RWLS
    asrt_address_llo = VI_GPIB_REN_ASSERT_ADDRESS_LLO

    #: Send LLO to any devices that are addressed to listen.
    asrt_llo = VI_GPIB_REN_ASSERT_LLO

    #: Deassert REN line.
    deassert = VI_GPIB_REN_DEASSERT

    #: Send the Go To Local command (GTL) to this device and deassert REN line.
    deassert_gtl = VI_GPIB_REN_DEASSERT_GTL


@enum.unique
class AddressSpace(enum.IntEnum):
    """Address space for register based resources."""

    #: A16 address space of VXI/MXI bus.
    a16 = VI_A16_SPACE

    #: A24 address space of VXI/MXI bus.
    a24 = VI_A24_SPACE

    #: A32 address space of VXI/MXI bus.
    a32 = VI_A32_SPACE

    #: A64 address space of VXI/MXI bus.
    a64 = VI_A64_SPACE

    #: PCI configuration space.
    pxi_config = VI_PXI_CFG_SPACE

    #: Specified PCI memory or I/O space
    pxi_bar0 = VI_PXI_BAR0_SPACE
    pxi_bar1 = VI_PXI_BAR1_SPACE
    pxi_bar2 = VI_PXI_BAR2_SPACE
    pxi_bar3 = VI_PXI_BAR3_SPACE
    pxi_bar4 = VI_PXI_BAR4_SPACE
    pxi_bar5 = VI_PXI_BAR5_SPACE

    #: Physical locally allocated memory.
    pxi_allocated = VI_PXI_ALLOC_SPACE


@enum.unique
class AddressModifiers(enum.IntEnum):
    """Address modifier to be used in high-level register operations."""

    data_private = VI_DATA_PRIV
    data_non_private = VI_DATA_NPRIV
    program_private = VI_PROG_PRIV
    program_non_private = VI_PROG_NPRIV
    block_private = VI_BLCK_PRIV
    block_non_private = VI_BLCK_NPRIV
    d64_private = VI_D64_PRIV
    d64_non_private = VI_D64_NPRIV
    d64_2vme = VI_D64_2EVME
    d64_sst160 = VI_D64_SST160
    d64_sst267 = VI_D64_SST267
    d64_sst320 = VI_D64_SST320


@enum.unique
class AssertSignalInterrupt(enum.IntEnum):
    """Line on which to perform an assertion or interrupt.

    Used only for VXI backplane and servant resources.

    """

    #: Use a VXI signal
    signal = VI_ASSERT_SIGNAL

    #: Use the mechanism specified in the response of Asynchronous Mode Control
    #: command. (VXI SERVANT only)
    use_assigned = VI_ASSERT_USE_ASSIGNED

    #: Send the interrupt via the specified VXI/VME IRQ line
    irq1 = VI_ASSERT_IRQ1
    irq2 = VI_ASSERT_IRQ2
    irq3 = VI_ASSERT_IRQ3
    irq4 = VI_ASSERT_IRQ4
    irq5 = VI_ASSERT_IRQ5
    irq6 = VI_ASSERT_IRQ6
    irq7 = VI_ASSERT_IRQ7


@enum.unique
class UtilityBusSignal(enum.IntEnum):
    """Operation on the utility line of a VXI backplane or servant."""

    #: Assert the SYSRESET ie perform a HARD RESET on the whole VXI bus.
    sysrest = VI_UTIL_ASSERT_SYSRESET

    #: Assert the SYSFAIL line.
    sysfail_assert = VI_UTIL_ASSERT_SYSFAIL

    #: Deassert the SYSFAIL line.
    sysfail_deassert = VI_UTIL_DEASSERT_SYSFAIL


@enum.unique
class VXICommands(enum.IntEnum):
    """VXI commands that can be sent using the vxi_command_query."""

    #: Send a command fitting in a 16-bit integer
    command_16 = VI_VXI_CMD16

    #: Read a response fitting in a 16-bit integer
    response16 = VI_VXI_RESP16

    #: Send a command and read a response both fitting in a 16-bit integer
    command_response_16 = VI_VXI_CMD16_RESP16

    #: Send a command fitting in a 32-bit integer
    command_32 = VI_VXI_CMD32

    #: Read a response fitting in a 32-bit integer
    response32 = VI_VXI_RESP32

    #: Send a command and read a response both fitting in a 32-bit integer
    command_response_32 = VI_VXI_CMD32_RESP32

    #: Send a command (32-bit integer) and read a response (16-bit integer)
    command_32_response_16 = VI_VXI_CMD32_RESP16


@enum.unique
class PXIMemory(enum.IntEnum):
    """Memory type used in a PXI BAR."""

    none = VI_PXI_ADDR_NONE
    memory = VI_PXI_ADDR_MEM
    io = VI_PXI_ADDR_IO
    cfg = VI_PXI_ADDR_CFG


@enum.unique
class VXIClass(enum.IntEnum):
    """VXI-defined device class."""

    memory = VI_VXI_CLASS_MEMORY
    extended = VI_VXI_CLASS_EXTENDED
    message = VI_VXI_CLASS_MESSAGE
    register = VI_VXI_CLASS_REGISTER
    other = VI_VXI_CLASS_OTHER


@enum.unique
class TriggerProtocol(enum.IntEnum):
    """Trigger protocol used when assering a resource trigger."""

    # FIXME The VISA standard is not very detailed on those
    #: Default protocol.
    #: This is the only valid protocol for software trigger on ASRL, GPIB, USB
    #: and VXI resources
    default = VI_TRIG_PROT_DEFAULT

    #:
    on = VI_TRIG_PROT_ON

    #:
    off = VI_TRIG_PROT_OFF

    #: For VXI devices equivalent to default
    sync = VI_TRIG_PROT_SYNC

    #: On PXI resources used to reserve a line for triggering
    reserve = VI_TRIG_PROT_RESERVE

    #: On PXI resources used to unreserve a line for triggering
    unreserve = VI_TRIG_PROT_UNRESERVE


@enum.unique
class InputTriggerLine(enum.IntEnum):
    """Trigger lines which can be mapped to another line.

    VXI, PXI devices.

    """

    #: TTL trigger lines
    ttl0 = VI_TRIG_TTL0
    ttl1 = VI_TRIG_TTL1
    ttl2 = VI_TRIG_TTL2
    ttl3 = VI_TRIG_TTL3
    ttl4 = VI_TRIG_TTL4
    ttl5 = VI_TRIG_TTL5
    ttl6 = VI_TRIG_TTL6
    ttl7 = VI_TRIG_TTL7

    # PXI specific TTL trigger lines
    ttl8 = VI_TRIG_TTL8
    ttl9 = VI_TRIG_TTL9
    ttl10 = VI_TRIG_TTL10
    ttl11 = VI_TRIG_TTL11

    #: ECL trigger lines
    ecl0 = VI_TRIG_ECL0
    ecl1 = VI_TRIG_ECL1
    ecl2 = VI_TRIG_ECL2
    ecl3 = VI_TRIG_ECL3
    ecl4 = VI_TRIG_ECL4
    ecl5 = VI_TRIG_ECL5

    #: Panel IN trigger line
    panel = VI_TRIG_PANEL_IN

    #: VXI STAR trigger input lines
    slot1 = VI_TRIG_STAR_SLOT1
    slot2 = VI_TRIG_STAR_SLOT2
    slot3 = VI_TRIG_STAR_SLOT3
    slot4 = VI_TRIG_STAR_SLOT4
    slot5 = VI_TRIG_STAR_SLOT5
    slot6 = VI_TRIG_STAR_SLOT6
    slot7 = VI_TRIG_STAR_SLOT7
    slot8 = VI_TRIG_STAR_SLOT8
    slot9 = VI_TRIG_STAR_SLOT9
    slot10 = VI_TRIG_STAR_SLOT10
    slot11 = VI_TRIG_STAR_SLOT11
    slot12 = VI_TRIG_STAR_SLOT12


@enum.unique
class OutputTriggerLine(enum.IntEnum):
    """Trigger lines to which another line can be mapped to.

    VXI, PXI devices.

    """

    #: TTL trigger lines
    ttl0 = VI_TRIG_TTL0
    ttl1 = VI_TRIG_TTL1
    ttl2 = VI_TRIG_TTL2
    ttl3 = VI_TRIG_TTL3
    ttl4 = VI_TRIG_TTL4
    ttl5 = VI_TRIG_TTL5
    ttl6 = VI_TRIG_TTL6
    ttl7 = VI_TRIG_TTL7

    # PXI specific TTL trigger lines
    ttl8 = VI_TRIG_TTL8
    ttl9 = VI_TRIG_TTL9
    ttl10 = VI_TRIG_TTL10
    ttl11 = VI_TRIG_TTL11

    #: VXI ECL trigger lines
    ecl0 = VI_TRIG_ECL0
    ecl1 = VI_TRIG_ECL1
    ecl2 = VI_TRIG_ECL2
    ecl3 = VI_TRIG_ECL3
    ecl4 = VI_TRIG_ECL4
    ecl5 = VI_TRIG_ECL5

    #: VXI STAR trigger out lines
    vxi0 = VI_TRIG_STAR_VXI0
    vxi1 = VI_TRIG_STAR_VXI1
    vxi2 = VI_TRIG_STAR_VXI2

    #: Panel OUT trigger line
    panel = VI_TRIG_PANEL_OUT

    #: All trigger lines (used only when unmapping lines)
    all = VI_TRIG_ALL


@enum.unique
class TriggerID(enum.IntEnum):
    """Identifier of the currently active trigerring mechanism on a resource."""

    #: Trigger using a serial word
    serial_word = VI_TRIG_SW

    #: TTL trigger lines
    ttl0 = VI_TRIG_TTL0
    ttl1 = VI_TRIG_TTL1
    ttl2 = VI_TRIG_TTL2
    ttl3 = VI_TRIG_TTL3
    ttl4 = VI_TRIG_TTL4
    ttl5 = VI_TRIG_TTL5
    ttl6 = VI_TRIG_TTL6
    ttl7 = VI_TRIG_TTL7

    # PXI specific TTL trigger lines
    ttl8 = VI_TRIG_TTL8
    ttl9 = VI_TRIG_TTL9
    ttl10 = VI_TRIG_TTL10
    ttl11 = VI_TRIG_TTL11

    #: VXI ECL trigger lines
    ecl0 = VI_TRIG_ECL0
    ecl1 = VI_TRIG_ECL1
    ecl2 = VI_TRIG_ECL2
    ecl3 = VI_TRIG_ECL3
    ecl4 = VI_TRIG_ECL4
    ecl5 = VI_TRIG_ECL5

    #: VXI STAR trigger out lines
    vxi0 = VI_TRIG_STAR_VXI0
    vxi1 = VI_TRIG_STAR_VXI1
    vxi2 = VI_TRIG_STAR_VXI2

    #: FIXME No definition in the VISA standards
    instr = VI_TRIG_STAR_INSTR


@enum.unique
class TriggerEventID(enum.IntEnum):
    """Identifier of the triggering mechanism on which a trigger event was received."""

    #: TTL trigger lines
    ttl0 = VI_TRIG_TTL0
    ttl1 = VI_TRIG_TTL1
    ttl2 = VI_TRIG_TTL2
    ttl3 = VI_TRIG_TTL3
    ttl4 = VI_TRIG_TTL4
    ttl5 = VI_TRIG_TTL5
    ttl6 = VI_TRIG_TTL6
    ttl7 = VI_TRIG_TTL7

    # PXI specific TTL trigger lines
    ttl8 = VI_TRIG_TTL8
    ttl9 = VI_TRIG_TTL9
    ttl10 = VI_TRIG_TTL10
    ttl11 = VI_TRIG_TTL11

    #: VXI ECL trigger lines
    ecl0 = VI_TRIG_ECL0
    ecl1 = VI_TRIG_ECL1
    ecl2 = VI_TRIG_ECL2
    ecl3 = VI_TRIG_ECL3
    ecl4 = VI_TRIG_ECL4
    ecl5 = VI_TRIG_ECL5

    #: FIXME No definition in the VISA standards
    instr = VI_TRIG_STAR_INSTR


@enum.unique
class ByteOrder(enum.IntEnum):
    """Byte order in register data transfer."""

    big_endian = VI_BIG_ENDIAN
    little_endian = VI_LITTLE_ENDIAN


@enum.unique
class DataWidth(enum.IntEnum):
    """Word width used when transferring data to/from register based resources."""

    #: Transfer data using 1 byte word
    bit_8 = VI_WIDTH_8

    #: Transfer data using 2 byte word
    bit_16 = VI_WIDTH_16

    #: Transfer data using 4 byte word
    bit_32 = VI_WIDTH_32

    #: Transfer data using 8 byte word
    bit_64 = VI_WIDTH_64

    @classmethod
    def from_literal(cls, value: Literal[8, 16, 32, 64]) -> "DataWidth":
        """Convert a literal width in the proper enum value."""
        if value not in (8, 16, 32, 64):
            raise ValueError(
                f"Invalid datawidth {value} specified. Valid values are (8, 16, 32, 64"
            )
        return cls(value // 8)


# Status code


@enum.unique
class StatusCode(enum.IntEnum):
    """Status codes that VISA driver-level operations can return."""

    #: The operation was aborted.
    error_abort = VI_ERROR_ABORT

    #: Insufficient system resources to perform necessary memory allocation.
    error_allocation = VI_ERROR_ALLOC

    #: The specified attribute is read-only.
    error_attribute_read_only = VI_ERROR_ATTR_READONLY

    #: Bus error occurred during transfer.
    error_bus_error = VI_ERROR_BERR

    #: Unable to deallocate the previously allocated data structures corresponding
    #: to this session or object reference.
    error_closing_failed = VI_ERROR_CLOSING_FAILED

    #: The connection for the specified session has been lost.
    error_connection_lost = VI_ERROR_CONN_LOST

    #: An error occurred while trying to open the specified file.
    #: Possible causes include an invalid path or lack of access rights.
    error_file_access = VI_ERROR_FILE_ACCESS

    #: An error occurred while performing I/O on the specified file.
    error_file_i_o = VI_ERROR_FILE_IO

    #: A handler is not currently installed for the specified event.
    error_handler_not_installed = VI_ERROR_HNDLR_NINSTALLED

    #: Unable to queue the asynchronous operation because there is already
    #: an operation in progress.
    error_in_progress = VI_ERROR_IN_PROGRESS

    #: Device reported an input protocol error during transfer.
    error_input_protocol_violation = VI_ERROR_INP_PROT_VIOL

    #: The interface type is valid but the specified interface number is not configured.
    error_interface_number_not_configured = VI_ERROR_INTF_NUM_NCONFIG

    #: An interrupt is still pending from a previous call.
    error_interrupt_pending = VI_ERROR_INTR_PENDING

    #: The access key to the resource associated with this session is invalid.
    error_invalid_access_key = VI_ERROR_INV_ACCESS_KEY

    #: Invalid access mode.
    error_invalid_access_mode = VI_ERROR_INV_ACC_MODE

    #: Invalid address space specified.
    error_invalid_address_space = VI_ERROR_INV_SPACE

    #: Specified event context is invalid.
    error_invalid_context = VI_ERROR_INV_CONTEXT

    #: Specified degree is invalid.
    error_invalid_degree = VI_ERROR_INV_DEGREE

    #: Specified event type is not supported by the resource.
    error_invalid_event = VI_ERROR_INV_EVENT

    #: Invalid expression specified for search.
    error_invalid_expression = VI_ERROR_INV_EXPR

    #: A format specifier in the format string is invalid.
    error_invalid_format = VI_ERROR_INV_FMT

    #: The specified handler reference is invalid.
    error_invalid_handler_reference = VI_ERROR_INV_HNDLR_REF

    #: Specified job identifier is invalid.
    error_invalid_job_i_d = VI_ERROR_INV_JOB_ID

    #: Invalid length specified.
    error_invalid_length = VI_ERROR_INV_LENGTH

    #: The value specified by the line parameter is invalid.
    error_invalid_line = VI_ERROR_INV_LINE

    #: The specified type of lock is not supported by this resource.
    error_invalid_lock_type = VI_ERROR_INV_LOCK_TYPE

    #: Invalid buffer mask specified.
    error_invalid_mask = VI_ERROR_INV_MASK

    #: Invalid mechanism specified.
    error_invalid_mechanism = VI_ERROR_INV_MECH

    #: The specified mode is invalid.
    error_invalid_mode = VI_ERROR_INV_MODE

    #: The specified session or object reference is invalid.
    error_invalid_object = VI_ERROR_INV_OBJECT

    #: Invalid offset specified.
    error_invalid_offset = VI_ERROR_INV_OFFSET

    #: The value of an unknown parameter is invalid.
    error_invalid_parameter = VI_ERROR_INV_PARAMETER

    #: The protocol specified is invalid.
    error_invalid_protocol = VI_ERROR_INV_PROT

    #: Invalid resource reference specified. Parsing error.
    error_invalid_resource_name = VI_ERROR_INV_RSRC_NAME

    #: Unable to start operation because setup is invalid due to inconsistent
    #: state of properties.
    error_invalid_setup = VI_ERROR_INV_SETUP

    #: Invalid size of window specified.
    error_invalid_size = VI_ERROR_INV_SIZE

    #: Invalid source or destination width specified.
    error_invalid_width = VI_ERROR_INV_WIDTH

    #: Could not perform operation because of I/O error.
    error_io = VI_ERROR_IO

    #: A code library required by VISA could not be located or loaded.
    error_library_not_found = VI_ERROR_LIBRARY_NFOUND

    #: The specified trigger line is currently in use.
    error_line_in_use = VI_ERROR_LINE_IN_USE

    #: The remote machine does not exist or is not accepting any connections.
    error_machine_not_available = VI_ERROR_MACHINE_NAVAIL

    #: The device does not export any memory.
    error_memory_not_shared = VI_ERROR_MEM_NSHARED

    #: No listeners condition is detected (both NRFD and NDAC are deasserted).
    error_no_listeners = VI_ERROR_NLISTENERS

    #: The specified operation is unimplemented.
    error_nonimplemented_operation = VI_ERROR_NIMPL_OPER

    #: The specified attribute is not defined or supported by the referenced
    #: session, event, or find list.
    error_nonsupported_attribute = VI_ERROR_NSUP_ATTR

    #: The specified state of the attribute is not valid or is not supported as
    #: defined by the session, event, or find list.
    error_nonsupported_attribute_state = VI_ERROR_NSUP_ATTR_STATE

    #: A format specifier in the format string is not supported.
    error_nonsupported_format = VI_ERROR_NSUP_FMT

    #: The interface cannot generate an interrupt on the requested level or with
    #: the requested statusID value.
    error_nonsupported_interrupt = VI_ERROR_NSUP_INTR

    #: The specified trigger source line (trigSrc) or destination line (trigDest)
    #: is not supported by this VISA implementation, or the combination of lines
    #: is not a valid mapping.
    error_nonsupported_line = VI_ERROR_NSUP_LINE

    #: The specified mechanism is not supported for the specified event type.
    error_nonsupported_mechanism = VI_ERROR_NSUP_MECH

    #: The specified mode is not supported by this VISA implementation.
    error_nonsupported_mode = VI_ERROR_NSUP_MODE

    #: Specified offset is not accessible from this hardware.
    error_nonsupported_offset = VI_ERROR_NSUP_OFFSET

    #: The specified offset is not properly aligned for the access width of
    #: the operation.
    error_nonsupported_offset_alignment = VI_ERROR_NSUP_ALIGN_OFFSET

    #: The session or object reference does not support this operation.
    error_nonsupported_operation = VI_ERROR_NSUP_OPER

    #: Cannot support source and destination widths that are different.
    error_nonsupported_varying_widths = VI_ERROR_NSUP_VAR_WIDTH

    #: Specified width is not supported by this hardware.
    error_nonsupported_width = VI_ERROR_NSUP_WIDTH

    #: Access to the remote machine is denied.
    error_no_permission = VI_ERROR_NPERMISSION

    #: The interface associated with this session is not currently the
    #: Controller-in-Charge.
    error_not_cic = VI_ERROR_NCIC

    #: The session must be enabled for events of the specified type in order to
    #: receive them.
    error_not_enabled = VI_ERROR_NENABLED

    #: The interface associated with this session is not the system controller.
    error_not_system_controller = VI_ERROR_NSYS_CNTLR

    #: Device reported an output protocol error during transfer.
    error_output_protocol_violation = VI_ERROR_OUTP_PROT_VIOL

    #: Unable to queue asynchronous operation.
    error_queue_error = VI_ERROR_QUEUE_ERROR

    #: The event queue for the specified type has overflowed, usually due to
    #: not closing previous events.
    error_queue_overflow = VI_ERROR_QUEUE_OVERFLOW

    #: Violation of raw read protocol occurred during transfer.
    error_raw_read_protocol_violation = VI_ERROR_RAW_RD_PROT_VIOL

    #: Violation of raw write protocol occurred during transfer.
    error_raw_write_protocol_violation = VI_ERROR_RAW_WR_PROT_VIOL

    #: The resource is valid, but VISA cannot currently access it.
    error_resource_busy = VI_ERROR_RSRC_BUSY

    #: Specified type of lock cannot be obtained or specified operation cannot
    #: be performed because the resource is locked.
    error_resource_locked = VI_ERROR_RSRC_LOCKED

    #: Insufficient location information, or the device or resource is not
    #: present in the system.
    error_resource_not_found = VI_ERROR_RSRC_NFOUND

    #: A previous response is still pending, causing a multiple query error.
    error_response_pending = VI_ERROR_RESP_PENDING

    #: A framing error occurred during transfer.
    error_serial_framing = VI_ERROR_ASRL_FRAMING

    #: An overrun error occurred during transfer. A character was not read from
    #: the hardware before the next character arrived.
    error_serial_overrun = VI_ERROR_ASRL_OVERRUN

    #: A parity error occurred during transfer.
    error_serial_parity = VI_ERROR_ASRL_PARITY

    #: The current session did not have any lock on the resource.
    error_session_not_locked = VI_ERROR_SESN_NLOCKED

    #: Service request has not been received for the session.
    error_srq_not_occurred = VI_ERROR_SRQ_NOCCURRED

    #: Unknown system error.
    error_system_error = VI_ERROR_SYSTEM_ERROR

    #: Timeout expired before operation completed.
    error_timeout = VI_ERROR_TMO

    #: The path from the trigger source line (trigSrc) to the destination line
    #: (trigDest) is not currently mapped.
    error_trigger_not_mapped = VI_ERROR_TRIG_NMAPPED

    #: A specified user buffer is not valid or cannot be accessed for the
    #: required size.
    error_user_buffer = VI_ERROR_USER_BUF

    #: The specified session currently contains a mapped window.
    error_window_already_mapped = VI_ERROR_WINDOW_MAPPED

    #: The specified session is currently unmapped.
    error_window_not_mapped = VI_ERROR_WINDOW_NMAPPED

    #: Operation completed successfully.
    success = VI_SUCCESS

    #: Session opened successfully, but the device at the specified address is
    #: not responding.
    success_device_not_present = VI_SUCCESS_DEV_NPRESENT

    #: Specified event is already disabled for at least one of the specified mechanisms.
    success_event_already_disabled = VI_SUCCESS_EVENT_DIS

    #: Specified event is already enabled for at least one of the specified mechanisms.
    success_event_already_enabled = VI_SUCCESS_EVENT_EN

    #: The number of bytes read is equal to the input count.
    success_max_count_read = VI_SUCCESS_MAX_CNT

    #: Operation completed successfully, and this session has nested exclusive locks.
    success_nested_exclusive = VI_SUCCESS_NESTED_EXCLUSIVE

    #: Operation completed successfully, and this session has nested shared locks.
    success_nested_shared = VI_SUCCESS_NESTED_SHARED

    #: Event handled successfully. Do not invoke any other handlers on this session
    #: for this event.
    success_no_more_handler_calls_in_chain = VI_SUCCESS_NCHAIN

    #: Operation completed successfully, but the queue was already empty.
    success_queue_already_empty = VI_SUCCESS_QUEUE_EMPTY

    #: Wait terminated successfully on receipt of an event notification. There
    #: is still at least one more event occurrence of the requested type(s)
    #: available for this session.
    success_queue_not_empty = VI_SUCCESS_QUEUE_NEMPTY

    #: Asynchronous operation request was performed synchronously.
    success_synchronous = VI_SUCCESS_SYNC

    #: The specified termination character was read.
    success_termination_character_read = VI_SUCCESS_TERM_CHAR

    #: The path from the trigger source line (trigSrc) to the destination line
    #: (trigDest) is already mapped.
    success_trigger_already_mapped = VI_SUCCESS_TRIG_MAPPED

    #: The specified configuration either does not exist or could not be loaded.
    #: The VISA-specified defaults are used.
    warning_configuration_not_loaded = VI_WARN_CONFIG_NLOADED

    #: The operation succeeded, but a lower level driver did not implement the
    #: extended functionality.
    warning_ext_function_not_implemented = VI_WARN_EXT_FUNC_NIMPL

    #: Although the specified state of the attribute is valid, it is not supported
    #: by this resource implementation.
    warning_nonsupported_attribute_state = VI_WARN_NSUP_ATTR_STATE

    #: The specified buffer is not supported.
    warning_nonsupported_buffer = VI_WARN_NSUP_BUF

    #: The specified object reference is uninitialized.
    warning_null_object = VI_WARN_NULL_OBJECT

    #: VISA received more event information of the specified type than the
    #: configured queue size could hold.
    warning_queue_overflow = VI_WARN_QUEUE_OVERFLOW

    #: The status code passed to the operation could not be interpreted.
    warning_unknown_status = VI_WARN_UNKNOWN_STATUS


# --- Attributes -----------------------------------------------------------------------


@enum.unique
class EventAttribute(enum.IntEnum):
    """The possible attributes of VISA events."""

    event_type = VI_ATTR_EVENT_TYPE
    status = VI_ATTR_STATUS
    operation_name = VI_ATTR_OPER_NAME
    job_id = VI_ATTR_JOB_ID
    return_count = VI_ATTR_RET_COUNT
    buffer = VI_ATTR_BUFFER
    received_trigger_id = VI_ATTR_RECV_TRIG_ID
    gpib_received_cic_state = VI_ATTR_GPIB_RECV_CIC_STATE
    received_tcpip_connect = VI_ATTR_RECV_TCPIP_ADDR
    usb_received_interrupt_size = VI_ATTR_USB_RECV_INTR_SIZE
    usb_received_interrupt_data = VI_ATTR_USB_RECV_INTR_DATA
    signal_register_status_id = VI_ATTR_SIGP_STATUS_ID
    interrupt_status_id = VI_ATTR_INTR_STATUS_ID
    received_interrupt_level = VI_ATTR_RECV_INTR_LEVEL
    pxi_received_interrupt_sequence = VI_ATTR_PXI_RECV_INTR_SEQ
    pxi_received_interrupt_data = VI_ATTR_PXI_RECV_INTR_DATA


@enum.unique
class ResourceAttribute(enum.IntEnum):
    """The possible attributes of VISA resources."""

    # All sessions
    resource_manager_session = VI_ATTR_RM_SESSION
    interface_type = VI_ATTR_INTF_TYPE
    interface_number = VI_ATTR_INTF_NUM
    interface_instrument_name = VI_ATTR_INTF_INST_NAME
    resource_class = VI_ATTR_RSRC_CLASS
    resource_name = VI_ATTR_RSRC_NAME
    resource_impl_version = VI_ATTR_RSRC_IMPL_VERSION
    resource_lock_state = VI_ATTR_RSRC_LOCK_STATE
    resource_spec_version = VI_ATTR_RSRC_SPEC_VERSION
    resource_manufacturer_name = VI_ATTR_RSRC_MANF_NAME
    resource_manufacturer_id = VI_ATTR_RSRC_MANF_ID
    timeout_value = VI_ATTR_TMO_VALUE
    max_queue_length = VI_ATTR_MAX_QUEUE_LENGTH
    user_data = VI_ATTR_USER_DATA
    trigger_id = VI_ATTR_TRIG_ID  # most resources no USB, nor TCPIP::SOCKET

    # Message based resource attributes
    send_end_enabled = VI_ATTR_SEND_END_EN
    suppress_end_enabled = VI_ATTR_SUPPRESS_END_EN
    termchar_enabled = VI_ATTR_TERMCHAR_EN
    termchar = VI_ATTR_TERMCHAR
    io_prot = VI_ATTR_IO_PROT
    file_append_enabled = VI_ATTR_FILE_APPEND_EN
    read_buffer_operation_mode = VI_ATTR_RD_BUF_OPER_MODE
    read_buffer_size = VI_ATTR_RD_BUF_SIZE
    write_buffer_operation_mode = VI_ATTR_WR_BUF_OPER_MODE
    write_buffer_size = VI_ATTR_WR_BUF_SIZE

    dma_allow_enabled = VI_ATTR_DMA_ALLOW_EN

    # TCPIP specific attributes
    tcpip_address = VI_ATTR_TCPIP_ADDR
    tcpip_hostname = VI_ATTR_TCPIP_HOSTNAME
    tcpip_port = VI_ATTR_TCPIP_PORT
    tcpip_device_name = VI_ATTR_TCPIP_DEVICE_NAME
    tcpip_nodelay = VI_ATTR_TCPIP_NODELAY
    tcpip_keepalive = VI_ATTR_TCPIP_KEEPALIVE
    tcpip_is_hislip = VI_ATTR_TCPIP_IS_HISLIP
    tcpip_hislip_version = VI_ATTR_TCPIP_HISLIP_VERSION
    tcpip_hislip_overlap_enable = VI_ATTR_TCPIP_HISLIP_OVERLAP_EN
    tcpip_hislip_max_message_kb = VI_ATTR_TCPIP_HISLIP_MAX_MESSAGE_KB

    # GPIB specific attributes
    gpib_primary_address = VI_ATTR_GPIB_PRIMARY_ADDR
    gpib_secondary_address = VI_ATTR_GPIB_SECONDARY_ADDR
    gpib_system_controller = VI_ATTR_GPIB_SYS_CNTRL_STATE
    gpib_cic_state = VI_ATTR_GPIB_CIC_STATE
    gpib_ren_state = VI_ATTR_GPIB_REN_STATE
    gpib_atn_state = VI_ATTR_GPIB_ATN_STATE
    gpib_ndac_state = VI_ATTR_GPIB_NDAC_STATE
    gpib_srq_state = VI_ATTR_GPIB_SRQ_STATE
    gpib_address_state = VI_ATTR_GPIB_ADDR_STATE
    gpib_unadress_enable = VI_ATTR_GPIB_UNADDR_EN
    gpib_readdress_enabled = VI_ATTR_GPIB_READDR_EN
    gpib_hs488_cable_length = VI_ATTR_GPIB_HS488_CBL_LEN

    # Serial specific attributes
    asrl_avalaible_number = VI_ATTR_ASRL_AVAIL_NUM
    asrl_baud_rate = VI_ATTR_ASRL_BAUD
    asrl_data_bits = VI_ATTR_ASRL_DATA_BITS
    asrl_parity = VI_ATTR_ASRL_PARITY
    asrl_stop_bits = VI_ATTR_ASRL_STOP_BITS
    asrl_flow_control = VI_ATTR_ASRL_FLOW_CNTRL
    asrl_discard_null = VI_ATTR_ASRL_DISCARD_NULL
    asrl_connected = VI_ATTR_ASRL_CONNECTED
    asrl_allow_transmit = VI_ATTR_ASRL_ALLOW_TRANSMIT
    asrl_end_in = VI_ATTR_ASRL_END_IN
    asrl_end_out = VI_ATTR_ASRL_END_OUT
    asrl_break_length = VI_ATTR_ASRL_BREAK_LEN
    asrl_break_state = VI_ATTR_ASRL_BREAK_STATE
    asrl_replace_char = VI_ATTR_ASRL_REPLACE_CHAR
    asrl_xon_char = VI_ATTR_ASRL_XON_CHAR
    asrl_xoff_char = VI_ATTR_ASRL_XOFF_CHAR
    asrl_cts_state = VI_ATTR_ASRL_CTS_STATE
    asrl_dsr_state = VI_ATTR_ASRL_DSR_STATE
    asrl_dtr_state = VI_ATTR_ASRL_DTR_STATE
    asrl_rts_state = VI_ATTR_ASRL_RTS_STATE
    asrl_wire_mode = VI_ATTR_ASRL_WIRE_MODE
    asrl_dcd_state = VI_ATTR_ASRL_DCD_STATE
    asrl_ri_state = VI_ATTR_ASRL_RI_STATE

    # USB specific attributes
    usb_interface_number = VI_ATTR_USB_INTFC_NUM
    usb_serial_number = VI_ATTR_USB_SERIAL_NUM
    usb_protocol = VI_ATTR_USB_PROTOCOL
    usb_max_interrupt_size = VI_ATTR_USB_MAX_INTR_SIZE
    usb_class = VI_ATTR_USB_CLASS
    usb_subclass = VI_ATTR_USB_SUBCLASS
    usb_bulk_in_status = VI_ATTR_USB_BULK_IN_STATUS
    usb_bulk_in_pipe = VI_ATTR_USB_BULK_IN_PIPE
    usb_bulk_out_status = VI_ATTR_USB_BULK_OUT_STATUS
    usb_bulk_out_pipe = VI_ATTR_USB_BULK_OUT_PIPE
    usb_interrupt_in_pipe = VI_ATTR_USB_INTR_IN_PIPE
    usb_alt_setting = VI_ATTR_USB_ALT_SETTING
    usb_end_in = VI_ATTR_USB_END_IN
    usb_number_interfaces = VI_ATTR_USB_NUM_INTFCS
    usb_number_pipes = VI_ATTR_USB_NUM_PIPES
    usb_interrupt_in_status = VI_ATTR_USB_INTR_IN_STATUS
    usb_control_pipe = VI_ATTR_USB_CTRL_PIPE

    #  USB, VXI, GPIB-VXI, PXI specific attributes
    manufacturer_name = VI_ATTR_MANF_NAME
    manufacturer_id = VI_ATTR_MANF_ID
    model_name = VI_ATTR_MODEL_NAME
    model_code = VI_ATTR_MODEL_CODE

    # GPIB INTFC, VXI SERVANT
    device_status_byte = VI_ATTR_DEV_STATUS_BYTE

    #  (USB, VXI, GPIB-VXI)::INSTR specific attributes
    is_4882_compliant = VI_ATTR_4882_COMPLIANT

    # (VXI, GPIB-VXI and PXI)::INSTR specific attributes
    slot = VI_ATTR_SLOT
    window_access = VI_ATTR_WIN_ACCESS
    window_base_address = VI_ATTR_WIN_BASE_ADDR
    window_size = VI_ATTR_WIN_SIZE
    source_increment = VI_ATTR_SRC_INCREMENT
    destination_increment = VI_ATTR_DEST_INCREMENT

    # VXI and GPIB-VXI specific attributes
    fdc_channel = VI_ATTR_FDC_CHNL
    fdc_mode = VI_ATTR_FDC_MODE
    fdc_generate_signal_enabled = VI_ATTR_FDC_GEN_SIGNAL_EN
    fdc_use_pair = VI_ATTR_FDC_USE_PAIR
    mainframe_logical_address = VI_ATTR_MAINFRAME_LA
    vxi_logical_address = VI_ATTR_VXI_LA
    commander_logical_address = VI_ATTR_CMDR_LA
    memory_space = VI_ATTR_MEM_SPACE
    memory_size = VI_ATTR_MEM_SIZE
    memory_base = VI_ATTR_MEM_BASE
    immediate_servant = VI_ATTR_IMMEDIATE_SERV
    destination_access_private = VI_ATTR_DEST_ACCESS_PRIV
    destination_byte_order = VI_ATTR_DEST_BYTE_ORDER
    source_access_private = VI_ATTR_SRC_ACCESS_PRIV
    source_byte_order = VI_ATTR_SRC_BYTE_ORDER
    window_access_private = VI_ATTR_WIN_ACCESS_PRIV
    window_byte_order = VI_ATTR_WIN_BYTE_ORDER
    vxi_trigger_support = VI_ATTR_VXI_TRIG_SUPPORT

    # GPIB-VXI specific attributes
    interface_parent_number = VI_ATTR_INTF_PARENT_NUM

    # VXI specific attributes
    vxi_device_class = VI_ATTR_VXI_DEV_CLASS  # INSTR
    vxi_trig_dir = VI_ATTR_VXI_TRIG_DIR  # INSTR
    vxi_trig_lines_enabled = VI_ATTR_VXI_TRIG_LINES_EN  # INSTR
    vxi_vme_interrupt_status = VI_ATTR_VXI_VME_INTR_STATUS  # BACKPLANE
    vxi_trigger_status = VI_ATTR_VXI_TRIG_STATUS  # BACKPLANE
    vxi_vme_sysfail_state = VI_ATTR_VXI_VME_SYSFAIL_STATE  # BACKPLANE

    # PXI specific attributes
    pxi_device_number = VI_ATTR_PXI_DEV_NUM
    pxi_function_num = VI_ATTR_PXI_FUNC_NUM
    pxi_bus_number = VI_ATTR_PXI_BUS_NUM
    pxi_chassis = VI_ATTR_PXI_CHASSIS
    pxi_slotpath = VI_ATTR_PXI_SLOTPATH
    pxi_slot_lbus_left = VI_ATTR_PXI_SLOT_LBUS_LEFT
    pxi_slot_lbus_right = VI_ATTR_PXI_SLOT_LBUS_RIGHT
    pxi_is_express = VI_ATTR_PXI_IS_EXPRESS
    pxi_slot_lwidth = VI_ATTR_PXI_SLOT_LWIDTH
    pxi_max_ldwidth = VI_ATTR_PXI_MAX_LWIDTH
    pxi_actual_ldwidth = VI_ATTR_PXI_ACTUAL_LWIDTH
    pxi_dstar_bus = VI_ATTR_PXI_DSTAR_BUS
    pxi_dstar_set = VI_ATTR_PXI_DSTAR_SET
    pxi_trig_bus = VI_ATTR_PXI_TRIG_BUS
    pxi_star_trig_bus = VI_ATTR_PXI_STAR_TRIG_BUS
    pxi_star_trig_line = VI_ATTR_PXI_STAR_TRIG_LINE
    pxi_source_trigger_bus = VI_ATTR_PXI_SRC_TRIG_BUS
    pxi_destination_trigger_bus = VI_ATTR_PXI_DEST_TRIG_BUS

    # PXI BAR memory scpecific attributes
    pxi_memory_type_bar0 = VI_ATTR_PXI_MEM_TYPE_BAR0
    pxi_memory_type_bar1 = VI_ATTR_PXI_MEM_TYPE_BAR1
    pxi_memory_type_bar2 = VI_ATTR_PXI_MEM_TYPE_BAR2
    pxi_memory_type_bar3 = VI_ATTR_PXI_MEM_TYPE_BAR3
    pxi_memory_type_bar4 = VI_ATTR_PXI_MEM_TYPE_BAR4
    pxi_memory_type_bar5 = VI_ATTR_PXI_MEM_TYPE_BAR5

    pxi_memory_base_bar0 = VI_ATTR_PXI_MEM_BASE_BAR0
    pxi_memory_base_bar1 = VI_ATTR_PXI_MEM_BASE_BAR1
    pxi_memory_base_bar2 = VI_ATTR_PXI_MEM_BASE_BAR2
    pxi_memory_base_bar3 = VI_ATTR_PXI_MEM_BASE_BAR3
    pxi_memory_base_bar4 = VI_ATTR_PXI_MEM_BASE_BAR4
    pxi_memory_base_bar5 = VI_ATTR_PXI_MEM_BASE_BAR5
    pxi_memory_size_bar0 = VI_ATTR_PXI_MEM_SIZE_BAR0
    pxi_memory_size_bar1 = VI_ATTR_PXI_MEM_SIZE_BAR1
    pxi_memory_size_bar2 = VI_ATTR_PXI_MEM_SIZE_BAR2
    pxi_memory_size_bar3 = VI_ATTR_PXI_MEM_SIZE_BAR3
    pxi_memory_size_bar4 = VI_ATTR_PXI_MEM_SIZE_BAR4
    pxi_memory_size_bar5 = VI_ATTR_PXI_MEM_SIZE_BAR5
