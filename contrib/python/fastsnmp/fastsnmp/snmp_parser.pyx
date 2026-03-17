# cython: nonecheck=False, boundscheck=False, wraparound=False, language_level=3
# cython: c_string_type=str, c_string_encoding=ascii
# based on https://pypi.python.org/pypi/libsnmp/

# X.690
# http://www.itu.int/ITU-T/studygroups/com17/languages/X.690-0207.pdf

import cython
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.long cimport PyLong_FromLong
from cpython.ref cimport Py_INCREF
from cpython.exc cimport PyErr_SetString
from cpython.unicode cimport PyUnicode_DecodeASCII
from libc.stdio cimport sprintf
from libc.string cimport memcpy
from itertools import cycle
from libc.stdint cimport uint64_t, uint32_t, uint8_t, int64_t, INT64_MAX
import struct
DEF MAX_OID_LEN_STR=500
DEF MAX_INT_LEN=30


class SNMPException(Exception):
    pass


class VarBindUnpackException(SNMPException):
    pass


class DecodeException(SNMPException):
    def __init__(self, part):
        self.part = part


class VarBindContentException(SNMPException):
    pass

class NoSuchInstance:
    pass

class NoSuchObject:
    pass

class EndOfMibView:
    pass

end_of_mib_view = EndOfMibView()

DEF _UNIVERSAL = 0x00
DEF _APPLICATION = 0x40
DEF _CONTEXT_SPECIFIC = 0x80
DEF _PRIVATE = 0xC0

DEF INT_1 = b'\x01'
DEF INT_0 = b'\x00'

DEF ASN_TAG_FORMAT_PRIMITIVE = 0x00
DEF ASN_TAG_FORMAT_CONSTRUCTED = 0x20

DEF ASN_SNMP_GET = _CONTEXT_SPECIFIC | ASN_TAG_FORMAT_CONSTRUCTED | 0x00  # 160
DEF ASN_SNMP_GETNEXT = _CONTEXT_SPECIFIC | ASN_TAG_FORMAT_CONSTRUCTED | 0x01  # 161
DEF ASN_SNMP_RESPONSE = _CONTEXT_SPECIFIC | ASN_TAG_FORMAT_CONSTRUCTED | 0x02  # 162
DEF ASN_SNMP_SET = _CONTEXT_SPECIFIC | ASN_TAG_FORMAT_CONSTRUCTED | 0x03  # 163
DEF ASN_SNMP_TRAP = _CONTEXT_SPECIFIC | ASN_TAG_FORMAT_CONSTRUCTED | 0x04  # 164
DEF ASN_SNMP_GETBULK = _CONTEXT_SPECIFIC | ASN_TAG_FORMAT_CONSTRUCTED | 0x05  # 165
DEF ASN_SNMP_INFORM = _CONTEXT_SPECIFIC | ASN_TAG_FORMAT_CONSTRUCTED | 0x06  # 166
DEF ASN_SNMP_TRAP2 = _CONTEXT_SPECIFIC | ASN_TAG_FORMAT_CONSTRUCTED | 0x07  # 167

DEF ASN_SNMP_GET_BYTE = bytes([ASN_SNMP_GET])
DEF ASN_SNMP_GETNEXT_BYTE = bytes([ASN_SNMP_GETNEXT])
DEF ASN_SNMP_RESPONSE_BYTE = bytes([ASN_SNMP_RESPONSE])
DEF ASN_SNMP_SET_BYTE = bytes([ASN_SNMP_SET])
DEF ASN_SNMP_TRAP_BYTE = bytes([ASN_SNMP_TRAP])
DEF ASN_SNMP_GETBULK_BYTE = bytes([ASN_SNMP_GETBULK])
DEF ASN_SNMP_INFORM_BYTE = bytes([ASN_SNMP_INFORM])
DEF ASN_SNMP_TRAP2_BYTE = bytes([ASN_SNMP_TRAP2])

DEF ASN_TAG_CLASS_UNIVERSAL = 0x00
DEF ASN_TAG_CLASS_APPLICATION = 0x40
DEF ASN_TAG_CLASS_CONTEXT = 0x80
DEF ASN_TAG_CLASS_PRIVATE = 0xC0

# rfc2578
DEF ASN_A_IPADDRESS = _APPLICATION | 0x00
DEF ASN_A_COUNTER32 = _APPLICATION | 0x01
DEF ASN_A_UNSIGNED32 = _APPLICATION | 0x02
DEF ASN_A_GAUGE32 = ASN_A_UNSIGNED32
DEF ASN_A_TIMETICKS = _APPLICATION | 0X03
DEF ASN_A_OPAQUE = _APPLICATION | 0X04
DEF ASN_A_COUNTER64 = _APPLICATION | 0x06

DEF ASN_A_IPADDRESS_BYTE = bytes([ASN_A_IPADDRESS])
DEF ASN_A_COUNTER32_BYTE = bytes([ASN_A_COUNTER32])
DEF ASN_A_UNSIGNED32_BYTE = bytes([ASN_A_UNSIGNED32])
DEF ASN_A_GAUGE32_BYTE = bytes([ASN_A_GAUGE32])
DEF ASN_A_TIMETICKS_BYTE = bytes([ASN_A_TIMETICKS])
DEF ASN_A_OPAQUE_BYTE = bytes([ASN_A_OPAQUE])
DEF ASN_A_COUNTER64_BYTE = bytes([ASN_A_COUNTER64])

# rfc2741
DEF ASN_U_EOC = _UNIVERSAL | 0x00  # 0 End of Content
DEF ASN_U_INTEGER = _UNIVERSAL | 0x02  # 2
DEF ASN_U_OCTETSTRING = _UNIVERSAL | 0x04  # 4
DEF ASN_U_NULL = _UNIVERSAL | 0x05  # 5
DEF ASN_U_OBJECTID = _UNIVERSAL | 0x06  # 6
DEF ASN_U_ENUMERATED = _UNIVERSAL | 0x10  # 10
DEF ASN_U_SEQUENCE = _UNIVERSAL | ASN_TAG_FORMAT_CONSTRUCTED | 0x10  # 48

DEF ASN_U_NO_SUCH_OBJECT = _UNIVERSAL | _CONTEXT_SPECIFIC | ASN_TAG_FORMAT_PRIMITIVE | 0x0  # 128
DEF ASN_U_NO_SUCH_INSTANCE = _UNIVERSAL | _CONTEXT_SPECIFIC | ASN_TAG_FORMAT_PRIMITIVE | 0x1  # 129
DEF ASN_U_END_OF_MIB_VIEW = _UNIVERSAL | _CONTEXT_SPECIFIC | ASN_TAG_FORMAT_PRIMITIVE | 0x2  # 130

DEF ASN_OPAQUE_FLOAT = 40824 # 40824
DEF ASN_OPAQUE_BOOL = 7937 # 7937

DEF ASN_U_INTEGER_BYTE = bytes([ASN_U_INTEGER])
DEF ASN_U_OCTETSTRING_BYTE = bytes([ASN_U_OCTETSTRING])
DEF ASN_U_NULL_BYTE = bytes([ASN_U_NULL])
DEF ASN_U_OBJECTID_BYTE = bytes([ASN_U_OBJECTID])
DEF ASN_U_ENUMERATED_BYTE = bytes([ASN_U_ENUMERATED])
DEF ASN_U_SEQUENCE_BYTE = bytes([ASN_U_SEQUENCE])


TYPE_NAME_TO_TYPE = {
    'Integer': ASN_U_INTEGER_BYTE,
    'Counter32': ASN_A_COUNTER32_BYTE,
    'Counter64': ASN_A_COUNTER64_BYTE,
    'OctetString': ASN_U_OCTETSTRING_BYTE,
    'Null': ASN_U_NULL_BYTE,
    'ObjectID': ASN_U_OBJECTID_BYTE,
    'Sequence': ASN_U_SEQUENCE_BYTE,
}


ASN_SNMP_MSG_TYPES = {
    'Get': ASN_SNMP_GET_BYTE,
    'GetNext': ASN_SNMP_GETNEXT_BYTE,
    'Response': ASN_SNMP_RESPONSE_BYTE,
    'Set': ASN_SNMP_SET_BYTE,
    'Trap': ASN_SNMP_TRAP_BYTE,
    'GetBulk': ASN_SNMP_GETBULK_BYTE,
}

# caches
length_cache = {}
length_cache[0] = b'\x00'
length_cache[1] = b'\x01'

# sub id 1 and 2 bytes
# int
cdef struct SID12_ti:
    uint64_t SID1
    uint64_t SID2

# str
cdef struct SID12_t:
    size_t strlen
    char *str

cdef SID12_ti *sid12i = [{'SID1': 0, 'SID2': 0},{'SID1': 0, 'SID2': 1},{'SID1': 0, 'SID2': 2},{'SID1': 0, 'SID2': 3},{'SID1': 0, 'SID2': 4},{'SID1': 0, 'SID2': 5},{'SID1': 0, 'SID2': 6},{'SID1': 0, 'SID2': 7},{'SID1': 0, 'SID2': 8},{'SID1': 0, 'SID2': 9},{'SID1': 0, 'SID2': 10},{'SID1': 0, 'SID2': 11},{'SID1': 0, 'SID2': 12},{'SID1': 0, 'SID2': 13},{'SID1': 0, 'SID2': 14},{'SID1': 0, 'SID2': 15},{'SID1': 0, 'SID2': 16},{'SID1': 0, 'SID2': 17},{'SID1': 0, 'SID2': 18},{'SID1': 0, 'SID2': 19},{'SID1': 0, 'SID2': 20},{'SID1': 0, 'SID2': 21},{'SID1': 0, 'SID2': 22},{'SID1': 0, 'SID2': 23},{'SID1': 0, 'SID2': 24},{'SID1': 0, 'SID2': 25},{'SID1': 0, 'SID2': 26},{'SID1': 0, 'SID2': 27},{'SID1': 0, 'SID2': 28},{'SID1': 0, 'SID2': 29},{'SID1': 0, 'SID2': 30},{'SID1': 0, 'SID2': 31},{'SID1': 0, 'SID2': 32},{'SID1': 0, 'SID2': 33},{'SID1': 0, 'SID2': 34},{'SID1': 0, 'SID2': 35},{'SID1': 0, 'SID2': 36},{'SID1': 0, 'SID2': 37},{'SID1': 0, 'SID2': 38},{'SID1': 0, 'SID2': 39},{'SID1': 1, 'SID2': 0},{'SID1': 1, 'SID2': 1},{'SID1': 1, 'SID2': 2},{'SID1': 1, 'SID2': 3},{'SID1': 1, 'SID2': 4},{'SID1': 1, 'SID2': 5},{'SID1': 1, 'SID2': 6},{'SID1': 1, 'SID2': 7},{'SID1': 1, 'SID2': 8},{'SID1': 1, 'SID2': 9},{'SID1': 1, 'SID2': 10},{'SID1': 1, 'SID2': 11},{'SID1': 1, 'SID2': 12},{'SID1': 1, 'SID2': 13},{'SID1': 1, 'SID2': 14},{'SID1': 1, 'SID2': 15},{'SID1': 1, 'SID2': 16},{'SID1': 1, 'SID2': 17},{'SID1': 1, 'SID2': 18},{'SID1': 1, 'SID2': 19},{'SID1': 1, 'SID2': 20},{'SID1': 1, 'SID2': 21},{'SID1': 1, 'SID2': 22},{'SID1': 1, 'SID2': 23},{'SID1': 1, 'SID2': 24},{'SID1': 1, 'SID2': 25},{'SID1': 1, 'SID2': 26},{'SID1': 1, 'SID2': 27},{'SID1': 1, 'SID2': 28},{'SID1': 1, 'SID2': 29},{'SID1': 1, 'SID2': 30},{'SID1': 1, 'SID2': 31},{'SID1': 1, 'SID2': 32},{'SID1': 1, 'SID2': 33},{'SID1': 1, 'SID2': 34},{'SID1': 1, 'SID2': 35},{'SID1': 1, 'SID2': 36},{'SID1': 1, 'SID2': 37},{'SID1': 1, 'SID2': 38},{'SID1': 1, 'SID2': 39},{'SID1': 2, 'SID2': 0},{'SID1': 2, 'SID2': 1},{'SID1': 2, 'SID2': 2},{'SID1': 2, 'SID2': 3},{'SID1': 2, 'SID2': 4},{'SID1': 2, 'SID2': 5},{'SID1': 2, 'SID2': 6},{'SID1': 2, 'SID2': 7},{'SID1': 2, 'SID2': 8},{'SID1': 2, 'SID2': 9},{'SID1': 2, 'SID2': 10},{'SID1': 2, 'SID2': 11},{'SID1': 2, 'SID2': 12},{'SID1': 2, 'SID2': 13},{'SID1': 2, 'SID2': 14},{'SID1': 2, 'SID2': 15},{'SID1': 2, 'SID2': 16},{'SID1': 2, 'SID2': 17},{'SID1': 2, 'SID2': 18},{'SID1': 2, 'SID2': 19},{'SID1': 2, 'SID2': 20},{'SID1': 2, 'SID2': 21},{'SID1': 2, 'SID2': 22},{'SID1': 2, 'SID2': 23},{'SID1': 2, 'SID2': 24},{'SID1': 2, 'SID2': 25},{'SID1': 2, 'SID2': 26},{'SID1': 2, 'SID2': 27},{'SID1': 2, 'SID2': 28},{'SID1': 2, 'SID2': 29},{'SID1': 2, 'SID2': 30},{'SID1': 2, 'SID2': 31},{'SID1': 2, 'SID2': 32},{'SID1': 2, 'SID2': 33},{'SID1': 2, 'SID2': 34},{'SID1': 2, 'SID2': 35},{'SID1': 2, 'SID2': 36},{'SID1': 2, 'SID2': 37},{'SID1': 2, 'SID2': 38},{'SID1': 2, 'SID2': 39}]
cdef SID12_t *sid12s = [{'str': b'0.0\x00', 'strlen': 3},{'str': b'0.1\x00', 'strlen': 3},{'str': b'0.2\x00', 'strlen': 3},{'str': b'0.3\x00', 'strlen': 3},{'str': b'0.4\x00', 'strlen': 3},{'str': b'0.5\x00', 'strlen': 3},{'str': b'0.6\x00', 'strlen': 3},{'str': b'0.7\x00', 'strlen': 3},{'str': b'0.8\x00', 'strlen': 3},{'str': b'0.9\x00', 'strlen': 3},{'str': b'0.10', 'strlen': 4},{'str': b'0.11', 'strlen': 4},{'str': b'0.12', 'strlen': 4},{'str': b'0.13', 'strlen': 4},{'str': b'0.14', 'strlen': 4},{'str': b'0.15', 'strlen': 4},{'str': b'0.16', 'strlen': 4},{'str': b'0.17', 'strlen': 4},{'str': b'0.18', 'strlen': 4},{'str': b'0.19', 'strlen': 4},{'str': b'0.20', 'strlen': 4},{'str': b'0.21', 'strlen': 4},{'str': b'0.22', 'strlen': 4},{'str': b'0.23', 'strlen': 4},{'str': b'0.24', 'strlen': 4},{'str': b'0.25', 'strlen': 4},{'str': b'0.26', 'strlen': 4},{'str': b'0.27', 'strlen': 4},{'str': b'0.28', 'strlen': 4},{'str': b'0.29', 'strlen': 4},{'str': b'0.30', 'strlen': 4},{'str': b'0.31', 'strlen': 4},{'str': b'0.32', 'strlen': 4},{'str': b'0.33', 'strlen': 4},{'str': b'0.34', 'strlen': 4},{'str': b'0.35', 'strlen': 4},{'str': b'0.36', 'strlen': 4},{'str': b'0.37', 'strlen': 4},{'str': b'0.38', 'strlen': 4},{'str': b'0.39', 'strlen': 4},{'str': b'1.0\x00', 'strlen': 3},{'str': b'1.1\x00', 'strlen': 3},{'str': b'1.2\x00', 'strlen': 3},{'str': b'1.3\x00', 'strlen': 3},{'str': b'1.4\x00', 'strlen': 3},{'str': b'1.5\x00', 'strlen': 3},{'str': b'1.6\x00', 'strlen': 3},{'str': b'1.7\x00', 'strlen': 3},{'str': b'1.8\x00', 'strlen': 3},{'str': b'1.9\x00', 'strlen': 3},{'str': b'1.10', 'strlen': 4},{'str': b'1.11', 'strlen': 4},{'str': b'1.12', 'strlen': 4},{'str': b'1.13', 'strlen': 4},{'str': b'1.14', 'strlen': 4},{'str': b'1.15', 'strlen': 4},{'str': b'1.16', 'strlen': 4},{'str': b'1.17', 'strlen': 4},{'str': b'1.18', 'strlen': 4},{'str': b'1.19', 'strlen': 4},{'str': b'1.20', 'strlen': 4},{'str': b'1.21', 'strlen': 4},{'str': b'1.22', 'strlen': 4},{'str': b'1.23', 'strlen': 4},{'str': b'1.24', 'strlen': 4},{'str': b'1.25', 'strlen': 4},{'str': b'1.26', 'strlen': 4},{'str': b'1.27', 'strlen': 4},{'str': b'1.28', 'strlen': 4},{'str': b'1.29', 'strlen': 4},{'str': b'1.30', 'strlen': 4},{'str': b'1.31', 'strlen': 4},{'str': b'1.32', 'strlen': 4},{'str': b'1.33', 'strlen': 4},{'str': b'1.34', 'strlen': 4},{'str': b'1.35', 'strlen': 4},{'str': b'1.36', 'strlen': 4},{'str': b'1.37', 'strlen': 4},{'str': b'1.38', 'strlen': 4},{'str': b'1.39', 'strlen': 4},{'str': b'2.0\x00', 'strlen': 3},{'str': b'2.1\x00', 'strlen': 3},{'str': b'2.2\x00', 'strlen': 3},{'str': b'2.3\x00', 'strlen': 3},{'str': b'2.4\x00', 'strlen': 3},{'str': b'2.5\x00', 'strlen': 3},{'str': b'2.6\x00', 'strlen': 3},{'str': b'2.7\x00', 'strlen': 3},{'str': b'2.8\x00', 'strlen': 3},{'str': b'2.9\x00', 'strlen': 3},{'str': b'2.10', 'strlen': 4},{'str': b'2.11', 'strlen': 4},{'str': b'2.12', 'strlen': 4},{'str': b'2.13', 'strlen': 4},{'str': b'2.14', 'strlen': 4},{'str': b'2.15', 'strlen': 4},{'str': b'2.16', 'strlen': 4},{'str': b'2.17', 'strlen': 4},{'str': b'2.18', 'strlen': 4},{'str': b'2.19', 'strlen': 4},{'str': b'2.20', 'strlen': 4},{'str': b'2.21', 'strlen': 4},{'str': b'2.22', 'strlen': 4},{'str': b'2.23', 'strlen': 4},{'str': b'2.24', 'strlen': 4},{'str': b'2.25', 'strlen': 4},{'str': b'2.26', 'strlen': 4},{'str': b'2.27', 'strlen': 4},{'str': b'2.28', 'strlen': 4},{'str': b'2.29', 'strlen': 4},{'str': b'2.30', 'strlen': 4},{'str': b'2.31', 'strlen': 4},{'str': b'2.32', 'strlen': 4},{'str': b'2.33', 'strlen': 4},{'str': b'2.34', 'strlen': 4},{'str': b'2.35', 'strlen': 4},{'str': b'2.36', 'strlen': 4},{'str': b'2.37', 'strlen': 4},{'str': b'2.38', 'strlen': 4},{'str': b'2.39', 'strlen': 4},]

cdef char **INT_TO_STRING = [
    b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9',
    b'10', b'11', b'12', b'13', b'14', b'15', b'16', b'17', b'18', b'19',
    b'20', b'21', b'22', b'23', b'24', b'25', b'26', b'27', b'28', b'29',
    b'30', b'31', b'32', b'33', b'34', b'35', b'36', b'37', b'38', b'39',
    b'40', b'41', b'42', b'43', b'44', b'45', b'46', b'47', b'48', b'49',
    b'50', b'51', b'52', b'53', b'54', b'55', b'56', b'57', b'58', b'59',
    b'60', b'61', b'62', b'63', b'64', b'65', b'66', b'67', b'68', b'69',
    b'70', b'71', b'72', b'73', b'74', b'75', b'76', b'77', b'78', b'79',
    b'80', b'81', b'82', b'83', b'84', b'85', b'86', b'87', b'88', b'89',
    b'90', b'91', b'92', b'93', b'94', b'95', b'96', b'97', b'98', b'99',
]

cdef inline int primitive_decode(char *stream, size_t stream_len, uint64_t *result, size_t *result_len):
    cdef size_t i
    cdef uint8_t sid
    cdef int retval = 0
    result_len[0] = 0
    result[0] = 0

    for i in range(stream_len):
        result[result_len[0]] <<= 7
        sid = <uint8_t>stream[i]
        result[result_len[0]] |= sid & 0x7f
        if sid & 0x80 == 0:
            result_len[0] +=1
            result[result_len[0]] = 0

    return retval


cdef int objectid_decode_str(const unsigned char *stream, size_t stream_len, char *out, size_t *out_length):
    cdef uint64_t result[122]
    cdef uint64_t oid_part
    cdef size_t n, tmp_n, cpy_len, ret_len, sid12_enc_len, result_len=0
    cdef SID12_t tmp_sid
    cdef char *oid_part_char

    if stream_len <= 0:
        return -1
    if <size_t>stream[0] > 127:
        return -2

    tmp_sid = sid12s[<size_t>stream[0]]

    sid12_enc_len = tmp_sid.strlen

    memcpy(out, tmp_sid.str, sid12_enc_len)
    out += sid12_enc_len
    out_length[0] = sid12_enc_len

    primitive_decode((<char *>stream)+1, stream_len-1, result, &result_len)

    for i in range(result_len):
        oid_part = result[i]
        out[0] = b'.'
        n = 1
        if oid_part < 100:
            oid_part_char = INT_TO_STRING[oid_part]
            if oid_part < 10:
                cpy_len = 1
            else:
                cpy_len = 2
            n += cpy_len
            memcpy(out+1, oid_part_char, cpy_len)
        else:
            tmp_n = sprintf(out+1, "%llu", oid_part)
            n += tmp_n
        out += n
        out_length[0] += n
    return 0


def objectid_decode(stream):
    cdef const unsigned char *stream_char = stream
    cdef size_t stream_len = len(stream)
    if stream_len <= 0:
        raise SNMPException("empty stream")

    cdef char ret_str[MAX_OID_LEN_STR]
    cdef size_t ret_length
    cdef int ret

    ret = objectid_decode_str(stream_char, stream_len, ret_str, &ret_length)
    if ret != 0:
        raise SNMPException("invalid stream: objectid_decode_str err = (%s)" % (ret,))
    return <str>ret_str[:ret_length]


cdef inline tuple objectid_decode_tuple(char *stream, size_t stream_len):
    cdef size_t result_len=0
    cdef uint64_t result[120]

    objectid_decode_c(stream, stream_len, result, &result_len)
    ret = PyTuple_New(result_len)

    for i in range(result_len):
        val = PyLong_FromLong(result[i])
        Py_INCREF(val)
        PyTuple_SET_ITEM(ret, i, val)
    return ret

cdef inline int objectid_decode_c(char *stream, size_t stream_len, uint64_t *result, size_t *result_len):
    cdef object value
    cdef SID12_ti *sid12_ptr
    cdef size_t i, enc_len=0
    cdef tuple ret
    sid12_ptr = &sid12i[<size_t>stream[0]]
    result[0] = sid12_ptr.SID1
    result[1] = sid12_ptr.SID2
    result_len[0] = 2

    if stream_len > 1:
        primitive_decode(stream+1, stream_len-1, result+2, &enc_len)
        result_len[0] += enc_len

    return 0

cdef inline int primitive_encode7(uint64_t *value, char *result_ptr) except -1:
    """
    Primitive encoding
    """
    cdef unsigned int size = 0

    if value[0] < <uint64_t>0x80:  # 7 bit
        result_ptr[0] = value[0]
        size = 1
    elif value[0] < <uint64_t>0x4000:  # 14 bit
        result_ptr[0] = value[0] >> 7 | 0x80
        result_ptr[1] = value[0] & 0x7f
        size = 2
    elif value[0] < <uint64_t>0x200000:  # 21 bit
        result_ptr[0] = value[0] >> 14 & 0x7f | 0x80
        result_ptr[1] = value[0] >> 7 | 0x80
        result_ptr[2] = value[0] & 0x7f
        size = 3
    elif value[0] < <uint64_t>0x10000000:  # 28 bit
        result_ptr[0] = value[0] >> 21 & 0x7f | 0x80
        result_ptr[1] = value[0] >> 14 & 0x7f | 0x80
        result_ptr[2] = value[0] >> 7 | 0x80
        result_ptr[3] = value[0] & 0x7f
        size = 4
    elif value[0] < <uint64_t>0x800000000:  # 35 bit
        result_ptr[0] = value[0] >> 28 & 0x7f | 0x80
        result_ptr[1] = value[0] >> 21 & 0x7f | 0x80
        result_ptr[2] = value[0] >> 14 & 0x7f | 0x80
        result_ptr[3] = value[0] >> 7 | 0x80
        result_ptr[4] = value[0] & 0x7f
        size = 5
    elif value[0] < <uint64_t>0x40000000000:  # 42 bit
        result_ptr[0] = value[0] >> 35 & 0x7f | 0x80
        result_ptr[1] = value[0] >> 28 & 0x7f | 0x80
        result_ptr[2] = value[0] >> 21 & 0x7f | 0x80
        result_ptr[3] = value[0] >> 14 & 0x7f | 0x80
        result_ptr[4] = value[0] >> 7 | 0x80
        result_ptr[5] = value[0] & 0x7f
        size = 6
    elif value[0] < <uint64_t>0x2000000000000:  # 49 bit
        result_ptr[0] = value[0] >> 42 & 0x7f | 0x80
        result_ptr[1] = value[0] >> 35 & 0x7f | 0x80
        result_ptr[2] = value[0] >> 28 & 0x7f | 0x80
        result_ptr[3] = value[0] >> 21 & 0x7f | 0x80
        result_ptr[4] = value[0] >> 14 & 0x7f | 0x80
        result_ptr[5] = value[0] >> 7 | 0x80
        result_ptr[6] = value[0] & 0x7f
        size = 7
    elif value[0] < <uint64_t>0x8000000000000000:  # 63 bit
        result_ptr[0] = value[0] >> 56 & 0x7f | 0x80
        result_ptr[1] = value[0] >> 49 & 0x7f | 0x80
        result_ptr[2] = value[0] >> 42 & 0x7f | 0x80
        result_ptr[3] = value[0] >> 35 & 0x7f | 0x80
        result_ptr[4] = value[0] >> 28 & 0x7f | 0x80
        result_ptr[5] = value[0] >> 21 & 0x7f | 0x80
        result_ptr[6] = value[0] >> 14 & 0x7f | 0x80
        result_ptr[7] = value[0] >> 7 | 0x80
        result_ptr[8] = value[0] & 0x7f
        size = 8
    else:  # 64 bit
        PyErr_SetString(OverflowError, "value too long")
        return -1
    return size

cdef inline uint64_t primitive_size(uint64_t value):
    """
    Primitive size
    """
    if value < <uint64_t>0x80:  # 7 bit
        return <uint64_t>1
    elif value < <uint64_t>0x8000:  # 15 bit
        return <uint64_t>2
    elif value < <uint64_t>0x800000:  # 23 bit
        return <uint64_t>3
    elif value < <uint64_t>0x80000000:  # 31 bit
        return <uint64_t>4
    elif value < <uint64_t>0x8000000000:  # 39 bit
        return <uint64_t>5
    elif value < <uint64_t>0x800000000000:  # 47 bit
        return <uint64_t>6
    elif value < <uint64_t>0x80000000000000:  # 55 bit
        return <uint64_t>7
    elif value < <uint64_t>0x8000000000000000:  # 63 bit
        return <uint64_t>8
    else:  # 64 bit
        return <uint64_t>9

cdef inline void primitive_encode(uint64_t *value, uint8_t size, char *result_ptr):
    """
    Primitive encoding
    """
    if size == 1:  # 7 bit
        result_ptr[0] = value[0]
    elif size == 2:  # 15 bit
        result_ptr[0] = value[0] >> 8 & 0xFF
        result_ptr[1] = value[0] & 0xFF
    elif size == 3:  # 23 bit
        result_ptr[0] = value[0] >> 16 & 0xFF
        result_ptr[1] = value[0] >> 8 & 0xFF
        result_ptr[2] = value[0] & 0xFF
    elif size == 4:  # 31 bit
        result_ptr[0] = value[0] >> 24 & 0xFF
        result_ptr[1] = value[0] >> 16 & 0xFF
        result_ptr[2] = value[0] >> 8 & 0xFF
        result_ptr[3] = value[0] & 0xFF
    elif size == 5:  # 39 bit
        result_ptr[0] = value[0] >> 32 & 0xFF
        result_ptr[1] = value[0] >> 24 & 0xFF
        result_ptr[2] = value[0] >> 16 & 0xFF
        result_ptr[3] = value[0] >> 8 & 0xFF
        result_ptr[4] = value[0] & 0xFF
    elif size == 6:  # 47 bit
        result_ptr[0] = value[0] >> 40 & 0xFF
        result_ptr[1] = value[0] >> 32 & 0xFF
        result_ptr[2] = value[0] >> 24 & 0xFF
        result_ptr[3] = value[0] >> 16 & 0xFF
        result_ptr[4] = value[0] >> 8 & 0xFF
        result_ptr[5] = value[0] & 0xFF
    elif size == 7:  # 55 bit
        result_ptr[0] = value[0] >> 48 & 0xFF
        result_ptr[1] = value[0] >> 40 & 0xFF
        result_ptr[2] = value[0] >> 32 & 0xFF
        result_ptr[3] = value[0] >> 24 & 0xFF
        result_ptr[4] = value[0] >> 16 & 0xFF
        result_ptr[5] = value[0] >> 8 & 0xFF
        result_ptr[6] = value[0] & 0xFF
    elif size == 8:  # 63 bit
        result_ptr[0] = value[0] >> 56 & 0xFF
        result_ptr[1] = value[0] >> 48 & 0xFF
        result_ptr[2] = value[0] >> 40 & 0xFF
        result_ptr[3] = value[0] >> 32 & 0xFF
        result_ptr[4] = value[0] >> 24 & 0xFF
        result_ptr[5] = value[0] >> 16 & 0xFF
        result_ptr[6] = value[0] >> 8 & 0xFF
        result_ptr[7] = value[0] & 0xFF
    else:  # size == 9 64 bit
        result_ptr[0] = 0
        result_ptr[1] = value[0] >> 56 & 0xFF
        result_ptr[2] = value[0] >> 48 & 0xFF
        result_ptr[3] = value[0] >> 40 & 0xFF
        result_ptr[4] = value[0] >> 32 & 0xFF
        result_ptr[5] = value[0] >> 24 & 0xFF
        result_ptr[6] = value[0] >> 16 & 0xFF
        result_ptr[7] = value[0] >> 8 & 0xFF
        result_ptr[8] = value[0] & 0xFF


cdef inline int objectid_encode_array(uint64_t *subids, uint32_t subids_len,
                                      char *result, size_t *object_len):
    cdef uint32_t clen
    cdef uint64_t subid
    cdef size_t i
    cdef int retval = 0
    cdef size_t sid_len = 0
    cdef char *result_ptr

    if subids[0] == 2 and subids[1] > 39:
        return -3  # long SID1 is not supported

    if subids[0] > 2:
        return -1  # wrong SID1

    if subids[1] > 39:
        return -2  # wrong SID2

    result[0] = subids[0]*40 + subids[1]
    object_len[0] = 1
    result_ptr = result+1

    for i in range(2, subids_len):
        subid = subids[i]
        sid_len = primitive_encode7(&subid, result_ptr)
        object_len[0] += sid_len
        result_ptr = result_ptr+sid_len
    return retval

def objectid_encode(oid):
    """
    encode an ObjectID into stream
    X.690, chapter 8.19
    :param oid: OID
    :type oid: str
    :returns: stream
    :rtype: bytearray
    """
    cdef unsigned int number
    cdef uint64_t idlist[128]
    cdef list subidlist
    cdef size_t pos = 0
    cdef size_t object_len = 0
    cdef char result[256]
    cdef str subid
    for subid in oid.strip('.').split('.'):
        idlist[pos] = int(subid)
        pos += 1
    ret = objectid_encode_array(idlist, pos, result, &object_len)

    if ret != 0:
        if ret == -1:
            raise SNMPException("wrong SID1")
        elif ret == -2:
            raise SNMPException("wrong SID2")
        elif ret == -3:
            raise SNMPException("long SID1 is not supported")

    return <bytes>result[:object_len]

cdef inline bytes c_octetstring_decode(const unsigned char *data, size_t data_len):
    return <bytes> data[:data_len]

def octetstring_decode(bytes stream not None):
    return c_octetstring_decode(stream, len(stream))


def octetstring_encode(string):
    """
    encode an octetstring into string

    :param string: string
    :type string: string
    :returns: string
    :rtype: bytes
    """
    return bytes(string.encode('ascii'))

def integer_encode(const int64_t value):
    cdef char[MAX_INT_LEN] data
    cdef uint64_t data_len = 0
    integer_encode_c(value, data, &data_len)
    return <bytes> data[:data_len]

cdef inline void integer_encode_c(const int64_t value, char *data, uint64_t *data_len):
    # little -> big
    cdef uint64_t slen, i
    cdef uint64_t mod_value = value
    cdef uint8_t size
    if value < 0:
        mod_value = ~value + 1
    slen = primitive_size(mod_value)
    primitive_encode(<uint64_t*> &value, slen, data)
    data_len[0] = slen


def uinteger_encode(uint64_t value):
    # little -> big
    cdef size_t slen, i
    cdef char[MAX_INT_LEN] res
    slen = primitive_size(value)
    primitive_encode(&value, slen, res)
    return <bytes> res[:slen]

def uinteger_decode(bytes stream not None):
    """
    Decode input stream into a integer

    :param stream: encoded integer
    :type stream: bytes
    :returns: decoded integer
    :rtype: int
    """
    cdef uint64_t value = 0
    cdef size_t stream_len = len(stream)
    cdef const unsigned char *stream_char = stream
    return uinteger_decode_c(stream_char, &stream_len)

def integer_decode(bytes stream not None):
    """
    Decode input stream into a integer

    :param stream: encoded integer
    :type stream: bytes
    :returns: decoded integer
    :rtype: int
    """
    cdef int64_t value = 0
    cdef size_t stream_len = len(stream)
    cdef const unsigned char *stream_char = stream
    return integer_decode_c(stream_char, &stream_len)

cdef inline int64_t integer_decode_c(const unsigned char *stream, size_t *stream_len):
    cdef int64_t value = 0
    cdef size_t i

    if stream[0] & 0x80:  # copy sign bit into all bytes first
        value = INT64_MAX
    for i in range(stream_len[0]):
        value <<= 8
        value |= stream[i]
    return value

cdef inline uint64_t uinteger_decode_c(const unsigned char *stream, size_t *stream_len):
    cdef uint64_t value = 0
    cdef uint8_t i
    for i in range(stream_len[0]):
        value <<= 8
        value |= <uint8_t>stream[i]
    return value

def sequence_decode(bytes stream not None) -> list:
    cdef const unsigned char * stream_char = stream
    cdef size_t stream_len = len(stream)
    cdef list ret
    ret, ex = sequence_decode_c(stream_char, stream_len)
    if ex:
        raise ex
    return ret

cdef tuple sequence_decode_c(const unsigned char *stream, const size_t stream_len):
    """
    Decode input stream into as sequence

    :param stream: sequence
    :type stream: bytes
    :returns: decoded sequence
    :rtype: list
    """
    cdef uint64_t tag = 0, tmp_uint_val
    cdef int64_t tmp_int_val
    cdef size_t encode_length, length, current_stream_pos=0
    cdef bytes bytes_val
    cdef const unsigned char *stream_char = stream
    cdef list objects=[], tmp_list_val
    cdef tuple tmp_tuple_val
    cdef str object_str
    cdef int ret
    cdef Exception ex

    cdef char ret_str[MAX_OID_LEN_STR]
    cdef size_t ret_length
    # cdef char *result_str_ptr = result_str

    while current_stream_pos < stream_len:
        tag_decode_c(stream_char, &tag, &encode_length)
        stream_char += encode_length
        current_stream_pos += encode_length

        length_decode_c(stream_char, &length, &encode_length)
        stream_char += encode_length
        current_stream_pos += encode_length

        if (current_stream_pos + length) > stream_len:
            return objects, SNMPException("out of len. current_stream_pos=%s length=%s stream_len=%s tag=%s" %
                            (current_stream_pos, length, stream_len, tag))

        if tag == ASN_U_INTEGER:
            tmp_int_val = integer_decode_c(stream_char, &length)
            objects.append(tmp_int_val)
        elif tag == ASN_A_COUNTER32 or tag == ASN_A_UNSIGNED32 \
                or tag == ASN_A_GAUGE32 or tag == ASN_A_COUNTER64 or tag == ASN_A_TIMETICKS:
            tmp_uint_val = uinteger_decode_c(stream_char, &length)
            objects.append(tmp_uint_val)
        elif tag == ASN_U_OBJECTID:
            ret = objectid_decode_str(stream_char, length, ret_str, &ret_length)
            if ret != 0:
                return objects, SNMPException("invalid oid: objectid_decode_str err == %s" % (ret,))
            if ret_length > MAX_OID_LEN_STR:
                return objects, SNMPException("too long oid")
            object_str = PyUnicode_DecodeASCII(ret_str, ret_length, 'ignore')
            objects.append(object_str)
        elif tag == ASN_U_NULL:
            objects.append(None)
        elif tag == ASN_U_SEQUENCE or tag == ASN_SNMP_RESPONSE or tag == ASN_SNMP_GETBULK:
            tmp_list_val, ex = sequence_decode_c(stream_char, length)
            if tmp_list_val is not None:
                objects.append(tmp_list_val)
            if ex:
                return objects, ex
        elif tag == ASN_U_OCTETSTRING or tag == ASN_A_IPADDRESS:
            bytes_val = <bytes> stream_char[:length]
            objects.append(bytes_val)
        elif tag == ASN_A_OPAQUE:
            opaque_obj, ex = sequence_decode_c(stream_char, length)
            if opaque_obj and len(opaque_obj) != 1:
                return objects, SNMPException("opaque len %s != 1" % len(opaque_obj))
            objects.append(opaque_obj[0])
            if ex:
                return objects, ex
        elif tag == ASN_U_END_OF_MIB_VIEW:
            objects.append(end_of_mib_view)
        elif tag == ASN_OPAQUE_FLOAT:
            bytes_val = <bytes> stream_char[:length]
            if length == 4:
                objects.append(struct.unpack('>f', bytes_val)[0])
            else:
                return objects, NotImplementedError("unknown float len %s" % length)
        elif tag == ASN_OPAQUE_BOOL:
            if stream_char[0] == b'\x01':
                objects.append(True)
            else:
                objects.append(False)
        elif tag == ASN_U_NO_SUCH_OBJECT or tag == ASN_U_NO_SUCH_INSTANCE or tag == ASN_U_END_OF_MIB_VIEW:
            objects.append(None)
        elif tag == ASN_U_EOC:
            return objects, SNMPException("end of content")
        else:
            return objects, NotImplementedError("unknown tag=%s" % tag)

        current_stream_pos += length
        stream_char += length
    return objects, None


cdef inline int length_decode_c(const unsigned char *stream, size_t *length, size_t *enc_len):
    """
    X.690 8,1,3
    """
    length[0] = <uint8_t>stream[0]
    enc_len[0] = 1

    if length[0] & 0x80 == 0x80:  # 8.1.3.5
        enc_len[0] = length[0] & 0x7f
        length[0] = uinteger_decode_c(stream+1, enc_len)
        enc_len[0] += 1

    return 0


def length_decode(bytes data):
    cdef size_t encode_length, length
    length_decode_c(data, &length, &encode_length)
    return length, encode_length


def length_encode(uint64_t length):
    """
    Function takes the length of the contents and produces the encoding for that length.  Section 6.3 of ITU-T-X.209

    :param length: length
    :type length: int
    :returns: encoded length
    :rtype: bytes
    """
    if length in length_cache:
        return length_cache[length]
    cdef uint64_t tmp_length = length
    if length <= 127:
        result = bytes([length & 0xff])
    else:
        # Long form - Octet one is the number of octets used to
        # encode the length It has bit 8 set to 1 and the
        # remaining 7 bits are used to encode the number of octets
        # used to encode the length Each subsequent octet uses all
        # 8 bits to encode the length

        resultlist = bytearray()
        numOctets = 0
        while tmp_length > 0:
            resultlist.insert(0, tmp_length & 0xff)
            tmp_length >>= 8
            numOctets += 1
        # Add a 1 to the front of the octet
        numOctets |= 0x80
        resultlist.insert(0, numOctets & 0xff)
        result = bytes(resultlist)
    length_cache[length] = result
    return result

cdef inline int tag_decode_c(const unsigned char *stream, uint64_t *tag, size_t *enc_len) except -1:
    """
    X.690 8.1.2

    Decode a BER tag field, returning the tag and the remainder
    of the stream
    """
    cdef uint64_t htag=0
    cdef size_t henc_len=0
    cdef uint8_t tagp

    tag[0] = <uint8_t> stream[0]  # low-tag-number form
    enc_len[0] = 1
    if tag[0] & 0x1F == 0x1F: # 8.1.2.4 high-tag-number form
        htag = tag[0]
        while True:
            tagp = stream[henc_len + 1]
            htag <<= 8
            htag |= tagp
            henc_len += 1
            if tagp & 0x80 == 0:
                break
        enc_len[0] += henc_len
        tag[0] = htag

    return 0

def tag_decode(bytes stream not None):
    cdef uint64_t tag=0
    cdef size_t encode_length
    tag_decode_c(stream, &tag, &encode_length)
    return tag, encode_length

# TODO: implement more encoders
def value_encode(value=None, value_type='Null'):
    """
    Encoded value by ASN.1
    """
    if value_type == 'Null':
        if value is not None:
            raise SNMPException('value must be None for Null type!')
        return b''
    elif value_type == "Integer":
        return integer_encode(value)
    elif value_type == "Counter64":
        return uinteger_encode(value)
    elif value_type == "OctetString":
        return value.encode()
    else:
        raise NotImplementedError('not implement coder for %s' % type(value))


def encode_varbind(oid, value_type='Null', value=None):
    if value is None:
        value_type = 'Null'
    obj_id_value = objectid_encode(oid)
    obj_id_type = ASN_U_OBJECTID_BYTE
    obj_id_len = length_encode(len(obj_id_value))

    obj_value_value = value_encode(value, value_type)
    obj_value_type = TYPE_NAME_TO_TYPE[value_type]
    obj_value_len = length_encode(len(obj_value_value))

    varbinds_obj = obj_id_type + obj_id_len + obj_id_value + obj_value_type + obj_value_len + obj_value_value

    seq_tag = ASN_U_SEQUENCE_BYTE
    varbind_enc = seq_tag + length_encode(len(varbinds_obj)) + varbinds_obj
    return varbind_enc


def varbinds_encode(varbinds):
    res = bytearray()
    for varbind in varbinds:
        if isinstance(varbind, str):
            oid = varbind
            value = None
            value_type = "Null"
        elif len(varbind) == 3:
            oid, value_type, value = varbind
        elif len(varbind) == 2:
            oid, value_type = varbind
            value = None
        res += encode_varbind(oid, value_type, value)
    return res


def varbinds_encode_tlv(varbinds):
    varbinds_data = varbinds_encode(varbinds)
    varbinds_type = ASN_U_SEQUENCE_BYTE
    varbinds_len = length_encode(len(varbinds_data))
    return varbinds_type + varbinds_len + varbinds_data


def msg_encode(req_id, community, varbinds, msg_type="GetBulk", max_repetitions=10, non_repeaters=0):
    """
    Build SNMP-message

    :param req_id: request identifier
    :type req_id: int
    :param community: snmp community
    :type community: string
    :param varbinds: list of oid to encode or bytes if encoded
    :type varbinds: tuple
    :param msg_type: index of ASN_SNMP_MSG_TYPES
    :type msg_type: str
    :param max_repetitions: max repetitions
    :type community: int
    :param non_repeaters: non repeaters
    :type varbinds: int
    :returns: encoded message
    :rtype: bytes
    """
    if isinstance(varbinds, (list, tuple)):
        varbinds_tlv = varbinds_encode_tlv(varbinds)
    else:
        varbinds_tlv = varbinds
    request_id_type = ASN_U_INTEGER_BYTE
    request_id_value = integer_encode(req_id)
    request_id_len = length_encode(len(request_id_value))

    if msg_type == "GetBulk":
        if max_repetitions < 1:
            raise SNMPException("max_repetitions must be higher than %s" % max_repetitions)
        non_repeaters_value = integer_encode(non_repeaters)
        non_repeaters_type = ASN_U_INTEGER_BYTE
        non_repeaters_len = length_encode(len(non_repeaters_value))
        max_repetitions_value = integer_encode(max_repetitions)
        max_repetitions_type = ASN_U_INTEGER_BYTE
        max_repetitions_len = length_encode(len(max_repetitions_value))
        pdu = request_id_type + request_id_len + request_id_value + \
              non_repeaters_type + non_repeaters_len + non_repeaters_value + \
              max_repetitions_type + max_repetitions_len + max_repetitions_value + \
              varbinds_tlv
        pdu_type = ASN_SNMP_GETBULK_BYTE
    else:
        error_status_value = INT_0  # integer_encode(0)
        error_status_type = ASN_U_INTEGER_BYTE
        error_status_len = length_encode(len(error_status_value))
        error_index_value = INT_0  # integer_encode(0)
        error_index_type = ASN_U_INTEGER_BYTE
        error_index_len = length_encode(len(error_index_value))
        pdu = request_id_type + request_id_len + request_id_value + \
              error_status_type + error_status_len + error_status_value + \
              error_index_type + error_index_len + error_index_value + \
              varbinds_tlv
        pdu_type = ASN_SNMP_MSG_TYPES[msg_type]

    pdu_len = length_encode(len(pdu))

    community_value = octetstring_encode(community)
    community_type = ASN_U_OCTETSTRING_BYTE
    community_len = length_encode(len(community_value))

    version_value = INT_1  # v2c
    version_type = ASN_U_INTEGER_BYTE
    version_len = INT_1  # length_encode(len(version_value))

    snmp_message_type = ASN_U_SEQUENCE_BYTE
    snmp_message_value = version_type + version_len + version_value + \
                         community_type + community_len + community_value + \
                         pdu_type + pdu_len + pdu
    snmp_message_len = length_encode(len(snmp_message_value))
    return snmp_message_type + snmp_message_len + snmp_message_value


def msg_decode(stream):
    cdef uint64_t tag=0
    cdef size_t encode_length, length
    cdef const unsigned char *stream_char = stream
    cdef const unsigned char *stream_ptr = stream_char
    cdef size_t stream_len = len(stream)
    cdef list data

    tag_decode_c(stream_ptr, &tag, &encode_length)
    stream_ptr += encode_length
    length_decode_c(stream_ptr, &length, &encode_length)
    stream_ptr += encode_length
    ret, ex = sequence_decode_c(stream_ptr, length)
    try:
        snmp_ver, community, data = ret
        req_id, error_status, error_index, varbinds = data
    except:
        if ex:
            raise ex
        raise
    else:
        if ex:
            raise DecodeException(data) from ex

    return req_id, error_status, error_index, varbinds

def check_is_growing(str oid_start not None, str oid_finish not None):
    cdef bint is_growing = True
    if "." in oid_start:
        if [int(x) for x in oid_finish.split(".")] < [int(x) for x in oid_start.split(".")]:
            is_growing = False
    elif int(oid_finish) < int(oid_start):
        is_growing = False
    return is_growing

def parse_varbind(list var_bind_list not None, tuple orig_main_oids not None, tuple oids_to_poll not None):
    cdef str oid, main_oid, index_part
    cdef list result = [], item
    cdef list next_oids = list()
    cdef list orig_main_oids_doted = list()
    cdef list orig_main_oids_len = list()
    cdef object value
    cdef uint64_t main_oids_len
    rest_oids_positions = [x for x in range(len(oids_to_poll)) if oids_to_poll[x]]
    main_oids_len = len(rest_oids_positions)
    main_oids_positions = cycle(rest_oids_positions)
    var_bind_list_len = len(var_bind_list)

    for i in orig_main_oids:
        orig_main_oids_doted.append(i + ".")
        orig_main_oids_len.append(len(i))

    skip_column = {}
    # if some oid in requested oids is not supported, column with it is index will
    # be filled with another oid. need to skip
    last_seen_index = {}
    first_seen_index = {}

    for var_bind_pos in range(var_bind_list_len):
        item = var_bind_list[var_bind_pos]
        # if item is None:
        #     raise VarBindUnpackException("bad value in %s at %s" % (var_bind_list, var_bind_pos))
        try:
            oid, value = item
        except (ValueError, TypeError) as e:
            raise VarBindUnpackException("Exception='%s' item=%s" % (e, item))
        if not isinstance(oid, str):
            raise VarBindContentException("expected oid in str. got %r" % oid)
        main_oids_pos = next(main_oids_positions)
        if value is end_of_mib_view:
            skip_column[main_oids_pos] = True
        if main_oids_pos in skip_column:
            continue
        main_oid = orig_main_oids_doted[main_oids_pos]
        if oid.startswith(main_oid):
            index_part = oid[orig_main_oids_len[main_oids_pos]+1:]
            last_seen_index[main_oids_pos] = index_part
            if main_oids_pos not in first_seen_index:
                first_seen_index[main_oids_pos] = index_part

            result.append([orig_main_oids[main_oids_pos], index_part, value])
        else:
            skip_column[main_oids_pos] = True
            if len(skip_column) == var_bind_list_len:
                break
    if len(skip_column) < main_oids_len:
        if len(skip_column):
            next_oids = [None,] * len(orig_main_oids)
            for pos in rest_oids_positions:
                if pos in skip_column:
                    continue
                if not check_is_growing(first_seen_index[pos], last_seen_index[pos]):
                    return result, tuple(next_oids)
                    # raise SNMPException("not increasing %s vs %s for %s" % (last_seen_index[pos],
                    #                                                     first_seen_index[pos],
                    #                                                     orig_main_oids[pos]))
                next_oids[pos] = "%s.%s" % (orig_main_oids[pos], last_seen_index[pos])
        else:
            for pos in rest_oids_positions:
                if not check_is_growing(first_seen_index[pos], last_seen_index[pos]):
                    return result, tuple(next_oids)
                    # raise SNMPException("not increasing %s vs %s for %s" % (last_seen_index[pos],
                    #                                                     first_seen_index[pos],
                    #                                                     orig_main_oids[pos]))
            next_oids = [
                "%s.%s" % (orig_main_oids[p], last_seen_index[p]) for p in rest_oids_positions]
    return result, tuple(next_oids)

