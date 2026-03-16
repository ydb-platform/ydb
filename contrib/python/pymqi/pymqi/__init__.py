# Python MQI Class Wrappers. High level classes that for the MQI
# Extension. These present an object interface to MQI.
#
# Author: L. Smithson (lsmithson@open-networks.co.uk)
# Author: Dariusz Suchojad (dsuch at zato.io)
#
# DISCLAIMER
# You are free to use this code in any way you like, subject to the
# Python & IBM disclaimers & copyrights. I make no representations
# about the suitability of this software for any purpose. It is
# provided 'AS-IS' without warranty of any kind, either express or
# implied. So there.
#
"""
PyMQI - Python MQI Wrapper Classes

These classes wrap the pymqe low level MQI calls. They present an OO
interface with a passing resemblance MQI C++.

Classes are also provided for easy use of the MQI structure parameters
(MQMD, MQGMO etc.) from Python. These allow Python scripts to set/get
structure members by attribute, dictionary etc.

The classes are:

    * MQOpts - Base class for defining MQI parameter structures.
    * CD - MQI MQCD structure class
    * CMHO - MQI MQCMHO structure class
    * MD - MQI MQMD structure class
    * GMO - MQI MQGMO structure class.
    * IMPO - MQI MQIMPO structure class
    * OD - MQI MQOD structure class.
    * PD - MQI MQPD structure class.
    * PMO - MQI MQPMO structure class.
    * RFH2 - MQI MQRFH2 structure class.
    * SCO - MQI MQSCO structure class
    * SMPO - MQI MQSMPO structure class
    * SRO - MQI MQSRO structure class
    * SD - MQI MQSD structure class
    * TM - MQI MQTM structure class
    * TMC2- MQI MQTMC2 structure class
    * Filter/StringFilter/IntegerFilter - PCF/MQAI filters
    * QueueManager - Queue Manager operations
    * Queue - Queue operations
    * Topic - Publish/subscribe topic operations
    * Subscription - Publish/subscribe subscription operations
    * PCFExecute - Programmable Command Format operations
    * Error - Base class for pymqi errors.
    * MQMIError - MQI specific error
    * PYIFError - Pymqi error

The following MQI operations are supported:

    * MQCONN, MQDISC (QueueManager.connect()/QueueManager.disconnect())
    * MQCONNX (QueueManager.connectWithOptions())
    * MQOPEN/MQCLOSE (Queue.open(), Queue.close(), Topic.open(), Topic.close())
    * MQPUT/MQPUT1/MQGET (Queue.put(), QueueManager.put1(), Queue.get())
    * MQCMIT/MQBACK (QueueManager.commit()/QueueManager.backout())
    * MQBEGIN (QueueuManager.begin())
    * MQINQ (QueueManager.inquire(), Queue.inquire())
    * MQSET (Queue.set())
    * MQSUB (Subscription.sub())
    * And various MQAI PCF commands.

The supported command levels (from 5.0 onwards) for the version of MQI
linked with this module are available in the tuple pymqi.__mqlevels__.
For a client build, pymqi.__mqbuild__ is set to the string 'client',
otherwise it is set to 'server'.

To use this package, connect to the Queue Manager (using
QueueManager.connect()), then open a queue (using Queue.open()). You
may then put or get messages on the queue (using Queue.put(),
Queue.get()), as required.

Where possible, pymqi assumes the MQI defaults for all parameters.

Like MQI C++, pymqi can defer a queue connect until the put/get call.

Pymqi maps all MQI warning & error status to the MQMIError
exception. Errors detected by pymqi itself raise the PYIFError
exception. Both these exceptions are subclasses of the Error class.

MQI constants are defined in the CMQC module. PCF constants are
defined in CMQC.

PCF commands and inquiries are executed by calling a MQCMD_* method on
an instance of a PCFExecute object.

Pymqi is thread safe. Pymqi objects have the same thread scope as
their MQI counterparts.

"""

# stdlib
import ctypes
import struct
import sys
import threading

try:
    from typing import Any, Optional, Union, Dict, List
except ImportError:
    pass

# import xml parser.  lxml/etree only available since python 2.5
use_minidom = False
try:
    import lxml.etree # type: ignore
except ImportError:
    from xml.dom.minidom import parseString # type: ignore
    use_minidom = True

# Python 3.8+ DLL loading
try:
    from os import add_dll_directory  # type: ignore
    from os import environ
    from os.path import join
    from os.path import exists

    mq_home_path = environ.get('MQ_FILE_PATH')

    if mq_home_path:
        for dir in ['bin', 'bin64']:
            mq_dll_directory = join(mq_home_path, dir)
            if exists(mq_dll_directory):
                add_dll_directory(mq_dll_directory)
except ImportError:
    pass

# PyMQI
try:
    from . import pymqe # type: ignore
except ImportError:
    import pymqe # type: ignore # Backward compatibility
from pymqi import CMQCFC
from pymqi import CMQC, CMQXC, CMQZC

# ################################################################################################################################
# ################################################################################################################################

# For pyflakes
if 0:
    CMQZC = CMQZC
    unicode = object()

# ################################################################################################################################
# ################################################################################################################################

__version__ = '1.12.11'
__mqlevels__ = pymqe.__mqlevels__
__mqbuild__ = pymqe.__mqbuild__

# ################################################################################################################################
# ################################################################################################################################

#
# Python 2/3 compatibility
#

is_py2 = sys.version_info.major <= 2 # type: bool
is_py3 = not is_py2 # type: bool

# ################################################################################################################################
# ################################################################################################################################

class default:
    ccsid          = 1208
    bytes_encoding = 'utf8'

# ################################################################################################################################
# ################################################################################################################################

def py23long(x):
    """ Convert:
       py2 int -> py2 long
       py3 int -> py3 int (it's already a 'long')
    """
    return x + 0 * 0xffffffffffffffff  # multiplying by large enough number will force py2 to use long.

# ################################################################################################################################
# ################################################################################################################################

def is_unicode(s):
    """ Returns True if input arg is a Python 3 string (aka Python 2 unicode). False otherwise.
    """
    if isinstance(s, str) and not isinstance(s, bytes):
        return True
    else:
        return False

# ################################################################################################################################
# ################################################################################################################################

def ensure_not_unicode(value):
    if is_unicode(value):
        msg = 'Python 3 style string (unicode) found but not allowed here: `{0}`. Convert to bytes.'
        raise TypeError(msg.format(value))

# ################################################################################################################################
# ################################################################################################################################

def ensure_bytes(s, encoding='ascii'):
    if is_unicode(s):
        return s.encode(encoding)
    else:
        return s

# ################################################################################################################################
# ################################################################################################################################

def padded_count(count, boundary=4):
    # type: (int, int) -> int
    """Calculate padded bytes count
    """
    return count + ((boundary - count & (boundary - 1)) & (boundary - 1))

#
# 64bit suppport courtesy of Brent S. Elmer, Ph.D. (mailto:webe3vt@aim.com)
#
# On 64 bit machines when MQ is compiled 64bit, MQLONG is an int defined
# in /opt/mqm/inc/cmqc.h or wherever your MQ installs to.
#
# On 32 bit machines, MQLONG is a long and many other MQ data types are set to MQLONG
#
# So, set MQLONG_TYPE to 'i' for 64bit MQ and 'l' for 32bit MQ so that the
# conversion from the Python data types to C data types in the MQ structures
# will work correctly.
#

# Are we running 64 bit?
if struct.calcsize('P') == 8:
    MQLONG_TYPE = 'i'  # 64 bit
else:
    MQLONG_TYPE = 'l'  # 32 bit

INTEGER64_TYPE = 'q'

# ################################################################################################################################
# ################################################################################################################################

# MQI Python<->C Structure mapping. MQI uses lots of parameter passing
# structures in its API. These classes are used to set/get the
# parameters in the style of Python dictionaries & keywords. Pack &
# unpack calls are used to convert between python class attributes and
# 'C' structures, suitable for passing in/out of C functions.
#
# The MQOpts class defines common operations for structure definition,
# default values setting, member set/get and translation to & from 'C'
# structures. Specializations construct MQOpts with a list specifying
# structure member names, their default values, and pack/unpack
# formats. MQOpts uses this list to setup class attributes
# corresponding to the structure names, set up attribute defaults, and
# builds a format string usable by the struct package to translate to
# 'C' structures.

# ################################################################################################################################
# ################################################################################################################################

class MQOpts(object):
    """ Base class for packing/unpacking MQI Option structures. It is
    constructed with a list defining the member/attribute name,
    default value (from the CMQC module) and the member pack format
    (see the struct module for the formats). The list format is:

      [['Member0', CMQC.DEFAULT_VAL0, 'fmt1']
       ['Member1', CMQC.DEFAULT_VAL1, 'fmt2']
         ...
      ]

    MQOpts defines common methods to allow structure members to be
    set/get as attributes (foo.Member0 = 42), set/get as dictionary
    items (foo['Member0'] = 42) or set as keywords (foo.set(Member0 =
    42, Member1 = 'flipperhat'). The ctor can be passed an optional
    keyword list to initialize the structure members to non-default
    values. The get methods returns all attributes as a dictionary.

    The pack() method packs all members into a 'C' structure according
    to the format specifiers passed to the ctor. The packing order is
    as specified in the list passed to the ctor. Pack returns a string
    buffer, which can be passed directly to the MQI 'C' calls.

    The unpack() method does the opposite of pack. It unpacks a string
    buffer into an MQOpts instance.

    Applications are not expected to use MQOpts directly. Instead,
    MQOpts is sub-classed as particular MQI structures.
    """

    def __init__(self, memlist, **kw):
        # type: (Union[list,tuple], Any) -> None
        """ Initialise the option structure. 'list' is a list of structure
    member names, default values and pack/unpack formats. 'kw' is an
    optional keyword dictionary that may be used to override default
    values set by MQOpts sub-classes.
    """

        self.__list = memlist[:]
        self.__format = ''

        # Dict to store c_char arrays to prevent memory addresses
        # from getting overwritten
        self.__vs_ctype_store = {} # type: Dict[str, Any]

        # Create the structure members as instance attributes and build
        # the struct.pack/unpack format string. The attribute name is
        # identical to the 'C' structure member name.
        for i in memlist:
            setattr(self, i[0], i[1])
            try:
                i[3]
            except:
                i.append(1)
            self.__format = self.__format + i[2] * i[3]
        self.set(**kw)

    def pack(self):
        # type: () -> bytes
        """ Pack the attributes into a 'C' structure to be passed to MQI
        calls. The pack order is as defined to the MQOpts
        ctor. Returns the structure as a string buffer.
        """

        # Build tuple for struct.pack() argument. Start with format string.
        args = [self.__format]

        # Now add the current attribute values to the tuple
        for i in self.__list:
            v = getattr(self, i[0])
            # Flatten attribs that are arrays
            if isinstance(v, list):
                for x in v:
                    ensure_not_unicode(x)  # Python 3 bytes check
                    args.append(x)
            else:
                ensure_not_unicode(v)  # Python 3 bytes check
                args.append(v)

        return struct.pack(*args)

    def unpack(self, buff):
        # type (bytes)
        """ Unpack a 'C' structure 'buff' into self.
        """
        ensure_not_unicode(buff)  # Python 3 bytes check

        # Increase buff length to the current MQOpts structure size
        diff_length = self.get_length() - len(buff)
        if diff_length > 0:
            buff += b'\x00' * diff_length

        # Unpack returns a tuple of the unpacked data, in the same
        # order (I hope!) as in the ctor's list arg.
        r = struct.unpack(self.__format, buff)
        x = 0
        for i in self.__list:

            if isinstance(i[1], list):
                l = []
                for _j in range(i[3]):
                    ensure_not_unicode(r[x])  # Python 3 bytes check
                    l.append(r[x])
                    x = x + 1
                setattr(self, i[0], l)
            else:
                ensure_not_unicode(r[x])  # Python 3 bytes check
                setattr(self, i[0], r[x])
                x = x + 1

    def set(self, **kw):
        # type: (Dict[str, Any]) -> None
        """ Set a structure member using the keyword dictionary 'kw'.
        An AttributeError exception is raised for invalid member names.
        """

        for i in kw.keys():
            # Only set if the attribute already exists. getattr raises
            # an exception if it doesn't.
            getattr(self, str(i))
            ensure_not_unicode(kw[i])  # Python 3 bytes check
            setattr(self, str(i), kw[i])

    def __setitem__(self, key, value):
        # type: (str, Any) -> None
        """ Set the structure member attribute 'key' to 'value', as in obj['Attr'] = 42.
        """
        # Only set if the attribute already exists. getattr raises an
        # exception if it doesn't.
        getattr(self, key)
        ensure_not_unicode(value)  # Python 3 bytes check
        setattr(self, key, value)

    def get(self):
        # type: () -> dict
        """ Return a dictionary of the current structure member values. The dictionary is keyed by a 'C' member name.
        """
        d = {}
        for i in self.__list:
            d[i[0]] = getattr(self, i[0])
        return d

    def __getitem__(self, key):
        # type: (str) -> Any
        """Return the member value associated with key, as in print obj['Attr'].
        """
        return getattr(self, key)

    def __str__(self):
        # type: () -> str
        rv = ''
        for i in self.__list:
            rv = rv + str(i[0]) + ': ' + str(getattr(self, i[0])) + '\n'
        # Chop the trailing newline
        return rv[:-1]

    def __repr__(self):
        # type: () -> str
        """ Return the packed buffer as a printable string.
        """
        return str(self.pack())

    def get_length(self):
        # type: () -> int
        """ Returns the length of the (would be) packed buffer.
        """
        return struct.calcsize(self.__format)

    def set_vs(self, vs_name, vs_value=None, vs_offset=0, vs_buffer_size=0, vs_ccsid=0):
        # type: (str, Union[bytes, str, None], int, int, int) -> None
        """ This method aids in the setting of the MQCHARV (variable length
        string) types in MQ structures.  The type contains a pointer to a
        variable length string.  A common example of a MQCHARV type
        is the ObjectString in the MQOD structure.
        In pymqi the ObjectString is defined as 5 separate
        elements (as per MQCHARV):
        ObjectStringVSPtr - Pointer
        ObjectStringVSOffset - Long
        ObjectStringVSBufSize - Long
        ObjectStringVSLength - Long
        ObjectStringVSCCSID - Long
        """
        if vs_name in ['SubName',  # subject name
                       'ObjectString']:  # topic name
            vs_value = ensure_bytes(vs_value) # allow known args be a string in Py3
        else:
            ensure_not_unicode(vs_value)  # Python 3 bytes check

        # if the VSPtr name is passed - remove VSPtr to be left with name.
        if vs_name.endswith('VSPtr'):
            vs_name_vsptr = vs_name
        else:
            vs_name_vsptr = vs_name + 'VSPtr'

        vs_name_vsoffset = vs_name + 'VSOffset'
        vs_name_vsbuffsize = vs_name + 'VSBufSize'
        vs_name_vslength = vs_name + 'VSLength'
        vs_name_vsccsid = vs_name + 'VSCCSID'

        c_vs_value = None
        c_vs_value_p = 0 # type: Optional[int]

        if vs_value is not None:
            c_vs_value = ctypes.create_string_buffer(vs_value)
            c_vs_value_p = ctypes.cast(c_vs_value, ctypes.c_void_p).value

        self[vs_name_vsptr] = c_vs_value_p
        self[vs_name_vsoffset] = vs_offset
        self[vs_name_vsbuffsize] = vs_buffer_size
        self[vs_name_vslength] = len(vs_value)
        self[vs_name_vsccsid] = vs_ccsid

        # Store c_char array object so memory location does not get overwritten
        self.__vs_ctype_store[vs_name] = c_vs_value

    def get_vs(self, vs_name):
        # type: (str) -> Union[bytes, str, None]
        """ This method returns the string to which the VSPtr pointer points to.
        """
        # if the VSPtr name is passed - remove VSPtr to be left with name.
        if vs_name.endswith('VSPtr'):
            vs_name_vsptr = vs_name
        else:
            vs_name_vsptr = vs_name + 'VSPtr'

        c_vs_value = None
        c_vs_value_p = self[vs_name_vsptr]
        if c_vs_value_p != 0:
            c_vs_value = ctypes.cast(c_vs_value_p, ctypes.c_char_p).value

        return c_vs_value


# ################################################################################################################################
# ################################################################################################################################

#
# Sub-classes of MQOpts representing real MQI structures.
#

class GMO(MQOpts):
    """ Construct an MQGMO Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQGMO_STRUC_ID, '4s'],
                ['Version', CMQC.MQGMO_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQGMO_NO_WAIT, MQLONG_TYPE],
                ['WaitInterval', 0, MQLONG_TYPE],
                ['Signal1', 0, MQLONG_TYPE],
                ['Signal2', 0, MQLONG_TYPE],
                ['ResolvedQName', b'', '48s'],
                ['MatchOptions', CMQC.MQMO_MATCH_MSG_ID+CMQC.MQMO_MATCH_CORREL_ID, MQLONG_TYPE],
                ['GroupStatus', CMQC.MQGS_NOT_IN_GROUP, 'b'],
                ['SegmentStatus', CMQC.MQSS_NOT_A_SEGMENT, 'b'],
                ['Segmentation', CMQC.MQSEG_INHIBITED, 'b'],
                ['Reserved1', b' ', 'c'],
                ['MsgToken', b'', '16s'],
                ['ReturnedLength', CMQC.MQRL_UNDEFINED, MQLONG_TYPE], ]

        if '7.0' in pymqe.__mqlevels__:
            opts += [
                ['Reserved2', py23long(0), MQLONG_TYPE],
                ['MsgHandle', py23long(0), 'q']]

        super(GMO, self).__init__(tuple(opts), **kw)

# Backward compatibility
gmo = GMO

# ################################################################################################################################
# ################################################################################################################################

class PMO(MQOpts):
    """ Construct an MQPMO Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        opts = [
            ['StrucId', CMQC.MQPMO_STRUC_ID, '4s'],
            ['Version', CMQC.MQPMO_VERSION_1, MQLONG_TYPE],
            ['Options', CMQC.MQPMO_NONE, MQLONG_TYPE],
            ['Timeout', -1, MQLONG_TYPE],
            ['Context', 0, MQLONG_TYPE],
            ['KnownDestCount', 0, MQLONG_TYPE],
            ['UnknownDestCount', 0, MQLONG_TYPE],
            ['InvalidDestCount', 0, MQLONG_TYPE],
            ['ResolvedQName', b'', '48s'],
            ['ResolvedQMgrName', b'', '48s'],
            ['RecsPresent', 0, MQLONG_TYPE],
            ['PutMsgRecFields',  0, MQLONG_TYPE],
            ['PutMsgRecOffset', 0, MQLONG_TYPE],
            ['ResponseRecOffset', 0, MQLONG_TYPE],
            ['PutMsgRecPtr', 0, 'P'],
            ['ResponseRecPtr', 0, 'P']]

        if '7.0' in pymqe.__mqlevels__:
            opts += [
                ['OriginalMsgHandle', py23long(0), 'q'],
                ['NewMsgHandle', py23long(0), 'q'],
                ['Action', py23long(0), MQLONG_TYPE],
                ['PubLevel', py23long(0), MQLONG_TYPE]]

        super(PMO, self).__init__(tuple(opts), **kw)

# Backward compatibility
pmo = PMO

# ################################################################################################################################
# ################################################################################################################################

class OD(MQOpts):
    """ Construct an MQOD Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQOD_STRUC_ID, '4s'],
                ['Version', CMQC.MQOD_VERSION_1, MQLONG_TYPE],
                ['ObjectType', CMQC.MQOT_Q, MQLONG_TYPE],
                ['ObjectName', b'', '48s'],
                ['ObjectQMgrName', b'', '48s'],
                ['DynamicQName', b'AMQ.*', '48s'],
                ['AlternateUserId', b'', '12s'],
                ['RecsPresent', 0, MQLONG_TYPE],
                ['KnownDestCount', 0, MQLONG_TYPE],
                ['UnknownDestCount', 0, MQLONG_TYPE],
                ['InvalidDestCount', 0, MQLONG_TYPE],
                ['ObjectRecOffset', 0, MQLONG_TYPE],
                ['ResponseRecOffset', 0, MQLONG_TYPE],
                ['ObjectRecPtr', 0, 'P'],
                ['ResponseRecPtr', 0, 'P'],
                ['AlternateSecurityId', b'', '40s'],
                ['ResolvedQName', b'', '48s'],
                ['ResolvedQMgrName', b'', '48s'], ]

        if '7.0' in pymqe.__mqlevels__:
            opts += [

                # ObjectString
                ['ObjectStringVSPtr', 0, 'P'],
                ['ObjectStringVSOffset', py23long(0), MQLONG_TYPE],
                ['ObjectStringVSBufSize', py23long(0), MQLONG_TYPE],
                ['ObjectStringVSLength', py23long(0), MQLONG_TYPE],
                ['ObjectStringVSCCSID', py23long(0), MQLONG_TYPE],

                # SelectionString
                ['SelectionStringVSPtr', 0, 'P'],
                ['SelectionStringVSOffset', py23long(0), MQLONG_TYPE],
                ['SelectionStringVSBufSize', py23long(0), MQLONG_TYPE],
                ['SelectionStringVSLength', py23long(0), MQLONG_TYPE],
                ['SelectionStringVSCCSID', py23long(0), MQLONG_TYPE],

                # ResObjectString
                ['ResObjectStringVSPtr', 0, 'P'],
                ['ResObjectStringVSOffset', py23long(0), MQLONG_TYPE],
                ['ResObjectStringVSBufSize', py23long(0), MQLONG_TYPE],
                ['ResObjectStringVSLength', py23long(0), MQLONG_TYPE],
                ['ResObjectStringVSCCSID', py23long(0), MQLONG_TYPE],

                ['ResolvedType', py23long(-3), MQLONG_TYPE]]

            # For 64bit platforms MQLONG is an int and this pad
            # needs to be here for WMQ 7.0
            if MQLONG_TYPE == 'i':
                opts += [['pad', b'', '4s']]

        super(OD, self).__init__(tuple(opts), **kw)

# Backward compatibility
od = OD

# ################################################################################################################################
# ################################################################################################################################

class MD(MQOpts):
    """ Construct an MQMD Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        super(MD, self).__init__(tuple([
            ['StrucId', CMQC.MQMD_STRUC_ID, '4s'],
            ['Version', CMQC.MQMD_VERSION_1, MQLONG_TYPE],
            ['Report', CMQC.MQRO_NONE, MQLONG_TYPE],
            ['MsgType', CMQC.MQMT_DATAGRAM, MQLONG_TYPE],
            ['Expiry', CMQC.MQEI_UNLIMITED, MQLONG_TYPE],
            ['Feedback', CMQC.MQFB_NONE, MQLONG_TYPE],
            ['Encoding', CMQC.MQENC_NATIVE, MQLONG_TYPE],
            ['CodedCharSetId', CMQC.MQCCSI_Q_MGR, MQLONG_TYPE],
            ['Format', b'', '8s'],
            ['Priority', CMQC.MQPRI_PRIORITY_AS_Q_DEF, MQLONG_TYPE],
            ['Persistence', CMQC.MQPER_PERSISTENCE_AS_Q_DEF, MQLONG_TYPE],
            ['MsgId', b'', '24s'],
            ['CorrelId', b'', '24s'],
            ['BackoutCount', 0, MQLONG_TYPE],
            ['ReplyToQ', b'', '48s'],
            ['ReplyToQMgr', b'', '48s'],
            ['UserIdentifier', b'', '12s'],
            ['AccountingToken', b'', '32s'],
            ['ApplIdentityData', b'', '32s'],
            ['PutApplType', CMQC.MQAT_NO_CONTEXT, MQLONG_TYPE],
            ['PutApplName', b'', '28s'],
            ['PutDate', b'', '8s'],
            ['PutTime', b'', '8s'],
            ['ApplOriginData', b'', '4s'],
            ['GroupId', b'', '24s'],
            ['MsgSeqNumber', 1, MQLONG_TYPE],
            ['Offset', 0, MQLONG_TYPE],
            ['MsgFlags', CMQC.MQMF_NONE, MQLONG_TYPE],
            ['OriginalLength', CMQC.MQOL_UNDEFINED, MQLONG_TYPE]]), **kw)

# Backward compatibility
md = MD

# ################################################################################################################################
# ################################################################################################################################

# RFH2 Header parsing/creation Support - Hannes Wagener - 2010.
class RFH2(MQOpts):
    """ Construct a RFH2 Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    initial_opts = [['StrucId', CMQC.MQRFH_STRUC_ID, '4s'],
                    ['Version', CMQC.MQRFH_VERSION_2, MQLONG_TYPE],
                    ['StrucLength', 0, MQLONG_TYPE],
                    ['Encoding', CMQC.MQENC_NATIVE, MQLONG_TYPE],
                    ['CodedCharSetId', CMQC.MQCCSI_Q_MGR, MQLONG_TYPE],
                    ['Format', CMQC.MQFMT_NONE, '8s'],
                    ['Flags', 0, MQLONG_TYPE],
                    ['NameValueCCSID', CMQC.MQCCSI_Q_MGR, MQLONG_TYPE]] # type: List[List[Union[str, int, bytes]]]

    big_endian_encodings = [CMQC.MQENC_INTEGER_NORMAL,
                            CMQC.MQENC_DECIMAL_NORMAL,
                            CMQC.MQENC_FLOAT_IEEE_NORMAL,
                            CMQC.MQENC_FLOAT_S390,

                            # 17
                            CMQC.MQENC_INTEGER_NORMAL +
                            CMQC.MQENC_DECIMAL_NORMAL,

                            # 257
                            CMQC.MQENC_INTEGER_NORMAL +
                            CMQC.MQENC_FLOAT_IEEE_NORMAL,

                            # 272
                            CMQC.MQENC_DECIMAL_NORMAL +
                            CMQC.MQENC_FLOAT_IEEE_NORMAL,

                            # 273
                            CMQC.MQENC_INTEGER_NORMAL +
                            CMQC.MQENC_DECIMAL_NORMAL +
                            CMQC.MQENC_FLOAT_IEEE_NORMAL]

    def __init__(self, **kw):
        # Take a copy of private initial_opts
        self.opts = [list(x) for x in self.initial_opts]
        super(RFH2, self).__init__(tuple(self.opts), **kw)

    def add_folder(self, folder_data):
        """ Adds a new XML folder to the RFH2 header.
        Checks if the XML is well formed and updates self.StrucLength.
        """

        ensure_not_unicode(folder_data)  # Python 3 bytes check

        # Check that the folder is valid xml and get the root tag name.
        if use_minidom:
            try:
                folder_name = parseString(folder_data). documentElement.tagName
            except Exception as e:
                raise PYIFError('RFH2 - XML Folder not well formed. Exception: %s' % str(e))
        else:
            try:
                folder_name = lxml.etree.fromstring(folder_data).tag
            except Exception as e:
                raise PYIFError('RFH2 - XML Folder not well formed. Exception: %s' % str(e))

        # Make sure folder length divides by 4 - else add spaces
        folder_length = len(folder_data)
        remainder = folder_length % 4
        if remainder != 0:
            num_spaces = 4 - remainder
            folder_data = folder_data + b' ' * num_spaces
            folder_length = len(folder_data)

        self.opts.append([folder_name + 'Length', py23long(folder_length), MQLONG_TYPE])
        self.opts.append([folder_name, folder_data, '%is' % folder_length])

        # Save the current values
        saved_values = self.get()

        # Reinit MQOpts with new fields added
        super(RFH2, self).__init__(tuple(self.opts))

        # Reset the values to the saved values
        self.set(**saved_values)

        # Calculate the correct StrucLength
        self['StrucLength'] = self.get_length()

    def pack(self, encoding=None):
        """ Override pack in order to set correct numeric encoding in the format.
        """
        if encoding is not None:
            if encoding in self.big_endian_encodings:
                self.opts[0][2] = '>' + self.initial_opts[0][2]
                saved_values = self.get()

                # Apply the new opts
                super(RFH2, self).__init__(tuple(self.opts))

                # Set from saved values
                self.set(**saved_values)

        return super(RFH2, self).pack()

    def unpack(self, buff, encoding=None):
        """ Override unpack in order to extract and parse RFH2 folders.
        Encoding meant to come from the MQMD.
        """

        ensure_not_unicode(buff) # Python 3 bytes check

        if buff[0:4] != CMQC.MQRFH_STRUC_ID:
            raise PYIFError('RFH2 - StrucId not MQRFH_STRUC_ID. Value: %s' % buff[0:4])

        if len(buff) < 36:
            raise PYIFError('RFH2 - Buffer too short. Should be 36+ bytes instead of %s' % len(buff))
        # Take a copy of initial_opts and the lists inside
        self.opts = [list(x) for x in self.initial_opts]

        big_endian = False
        if encoding is not None:
            if encoding in self.big_endian_encodings:
                big_endian = True
        else:
            # If small endian first byte of version should be > 'x\00'
            if buff[4:5] == b'\x00':
                big_endian = True

        # Indicate bigendian in format
        if big_endian:
            self.opts[0][2] = '>' + self.opts[0][2]

        # Apply and parse the default header
        super(RFH2, self).__init__(tuple(self.opts))
        super(RFH2, self).unpack(buff[0:36])

        if self['StrucLength'] < 0:
            raise PYIFError('RFH2 - "StrucLength" is negative. Check numeric encoding.')

        if len(buff) > 36:
            if self['StrucLength'] > len(buff):
                raise PYIFError('RFH2 - Buffer too short. Expected: %s Buffer Length: %s'
                                % (self['StrucLength'], len(buff)))

        # Extract only the string containing the xml folders and loop
        s = buff[36:self['StrucLength']]

        while s:
            # First 4 bytes is the folder length. supposed to divide by 4.
            len_bytes = s[0:4]
            if big_endian:
                folder_length = struct.unpack('>l', len_bytes)[0]
            else:
                folder_length = struct.unpack('<l', len_bytes)[0]

            # Move on past four byte length
            s = s[4:]

            # Extract the folder string
            folder_data = s[:folder_length]

            # Check that the folder is valid xml and get the root tag name.
            if use_minidom:
                try:
                    folder_name = parseString(folder_data).documentElement.tagName
                except Exception as e:
                    raise PYIFError('RFH2 - XML Folder not well formed. Exception: %s' % str(e))
            else:
                try:
                    folder_name = lxml.etree.fromstring(folder_data).tag
                except Exception as e:
                    raise PYIFError('RFH2 - XML Folder not well formed. Exception: %s' % str(e))

            # Append folder length and folder string to self.opts types
            self.opts.append([folder_name + 'Length', py23long(folder_length), MQLONG_TYPE])
            self.opts.append([folder_name, folder_data, '%is' % folder_length])
            # Move on past the folder
            s = s[folder_length:]

        # Save the current values
        saved_values = self.get()

        # Apply the new opts
        super(RFH2, self).__init__(tuple(self.opts))

        # Set from saved values
        self.set(**saved_values)

        # unpack the buffer? - should get same result?
        # super(RFH2, self).unpack(buff[0:self['StrucLength']])

# ################################################################################################################################
# ################################################################################################################################

class TM(MQOpts):
    """ Construct an MQTM Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        super(TM, self).__init__(tuple([
            ['StrucId', CMQC.MQTM_STRUC_ID, '4s'],
            ['Version', CMQC.MQTM_VERSION_1, MQLONG_TYPE],
            ['QName', b'', '48s'],
            ['ProcessName', b'', '48s'],
            ['TriggerData', b'', '64s'],
            ['ApplType', 0, MQLONG_TYPE],
            ['ApplId', b'', '256s'],
            ['EnvData', b'', '128s'],
            ['UserData', b'', '128s']]), **kw)

# ################################################################################################################################
# ################################################################################################################################

class TMC2(MQOpts):
    """ Construct an MQTMC2 Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        super(TMC2, self).__init__(tuple([
            ['StrucId', CMQC.MQTMC_STRUC_ID, '4s'],
            ['Version', CMQC.MQTMC_VERSION_2, '4s'],
            ['QName', b'', '48s'],
            ['ProcessName', b'', '48s'],
            ['TriggerData', b'', '64s'],
            ['ApplType', b'', '4s'],
            ['ApplId', b'', '256s'],
            ['EnvData', b'', '128s'],
            ['UserData', b'', '128s'],
            ['QMgrName', b'', '48s']]), **kw)

# ################################################################################################################################
# ################################################################################################################################

# MQCONNX code courtesy of John OSullivan (mailto:jos@onebox.com)
# SSL additions courtesy of Brian Vicente (mailto:sailbv@netscape.net)

class CD(MQOpts):
    """ Construct an MQCD Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        """__init__(**kw)"""
        opts = []
        opts += [
            ['ChannelName', b'', '20s'],
            ['Version', CMQXC.MQCD_VERSION_6, MQLONG_TYPE],
            ['ChannelType', CMQC.MQCHT_CLNTCONN, MQLONG_TYPE],
            ['TransportType', CMQC.MQXPT_TCP, MQLONG_TYPE],
            ['Desc', b'', '64s'],
            ['QMgrName', b'', '48s'],
            ['XmitQName', b'', '48s'],
            ['ShortConnectionName', b'', '20s'],
            ['MCAName', b'', '20s'],
            ['ModeName', b'', '8s'],
            ['TpName', b'', '64s'],
            ['BatchSize', py23long(50), MQLONG_TYPE],
            ['DiscInterval', py23long(6000), MQLONG_TYPE],
            ['ShortRetryCount', py23long(10), MQLONG_TYPE],
            ['ShortRetryInterval', py23long(60), MQLONG_TYPE],
            ['LongRetryCount', py23long(999999999), MQLONG_TYPE],
            ['LongRetryInterval', py23long(1200), MQLONG_TYPE],
            ['SecurityExit', b'', '128s'],
            ['MsgExit', b'', '128s'],
            ['SendExit', b'', '128s'],
            ['ReceiveExit', b'', '128s'],
            ['SeqNumberWrap', py23long(999999999), MQLONG_TYPE],
            ['MaxMsgLength', py23long(4194304), MQLONG_TYPE],
            ['PutAuthority', CMQC.MQPA_DEFAULT, MQLONG_TYPE],
            ['DataConversion', CMQC.MQCDC_NO_SENDER_CONVERSION, MQLONG_TYPE],
            ['SecurityUserData', b'', '32s'],
            ['MsgUserData', b'', '32s'],
            ['SendUserData', b'', '32s'],
            ['ReceiveUserData', b'', '32s'],
            # Version 1
            ['UserIdentifier', b'', '12s'],
            ['Password', b'', '12s'],
            ['MCAUserIdentifier', b'', '12s'],
            ['MCAType', CMQC.MQMCAT_PROCESS, MQLONG_TYPE],
            ['ConnectionName', b'', '264s'],
            ['RemoteUserIdentifier', b'', '12s'],
            ['RemotePassword', b'', '12s'],
            # Version 2
            ['MsgRetryExit', b'', '128s'],
            ['MsgRetryUserData', b'', '32s'],
            ['MsgRetryCount', py23long(10), MQLONG_TYPE],
            ['MsgRetryInterval', py23long(1000), MQLONG_TYPE],
            # Version 3
            ['HeartbeatInterval', py23long(300), MQLONG_TYPE],
            ['BatchInterval', py23long(0), MQLONG_TYPE],
            ['NonPersistentMsgSpeed', CMQC.MQNPMS_FAST, MQLONG_TYPE],
            ['StrucLength', CMQXC.MQCD_CURRENT_LENGTH, MQLONG_TYPE],
            ['ExitNameLength', CMQC.MQ_EXIT_NAME_LENGTH, MQLONG_TYPE],
            ['ExitDataLength', CMQC.MQ_EXIT_DATA_LENGTH, MQLONG_TYPE],
            ['MsgExitsDefined', py23long(0), MQLONG_TYPE],
            ['SendExitsDefined', py23long(0), MQLONG_TYPE],
            ['ReceiveExitsDefined', py23long(0), MQLONG_TYPE],
            ['MsgExitPtr', 0, 'P'],
            ['MsgUserDataPtr', 0, 'P'],
            ['SendExitPtr', 0, 'P'],
            ['SendUserDataPtr', 0, 'P'],
            ['ReceiveExitPtr', 0, 'P'],
            ['ReceiveUserDataPtr', 0, 'P'],
            # Version 4
            ['ClusterPtr', 0, 'P'],
            ['ClustersDefined', py23long(0), MQLONG_TYPE],
            ['NetworkPriority', py23long(0), MQLONG_TYPE],
            ['LongMCAUserIdLength', py23long(0), MQLONG_TYPE],
            ['LongRemoteUserIdLength', py23long(0), MQLONG_TYPE],
            # Version 5
            ['LongMCAUserIdPtr', 0, 'P'],
            ['LongRemoteUserIdPtr', 0, 'P'],
            ['MCASecurityId', b'', '40s'],
            ['RemoteSecurityId', b'', '40s'],
            # Version 6
            ['SSLCipherSpec', b'', '32s'],
            ['SSLPeerNamePtr', 0, 'P'],
            ['SSLPeerNameLength', py23long(0), MQLONG_TYPE],
            ['SSLClientAuth', py23long(0), MQLONG_TYPE],
            ['KeepAliveInterval', -1, MQLONG_TYPE],
            ['LocalAddress', b'', '48s'],
            ['BatchHeartbeat', py23long(0), MQLONG_TYPE],
            # Version 7
            ['HdrCompList', [py23long(0), py23long(-1)], '2' + MQLONG_TYPE],
            ['MsgCompList', [0] + 15 * [py23long(-1)], '16' + MQLONG_TYPE],
            ['CLWLChannelRank', py23long(0), MQLONG_TYPE],
            ['CLWLChannelPriority', py23long(0), MQLONG_TYPE],
            ['CLWLChannelWeight', py23long(50), MQLONG_TYPE],
            ['ChannelMonitoring', py23long(0), MQLONG_TYPE],
            ['ChannelStatistics', py23long(0), MQLONG_TYPE],
            # Version 8
            ['SharingConversations', 10, MQLONG_TYPE],
            ['PropertyControl', 0, MQLONG_TYPE],      # 0 = MQPROP_COMPATIBILITY
            ['MaxInstances', 999999999, MQLONG_TYPE],
            ['MaxInstancesPerClient', 999999999, MQLONG_TYPE],
            ['ClientChannelWeight', 0, MQLONG_TYPE],
            ['ConnectionAffinity', 1, MQLONG_TYPE],  # 1 = MQCAFTY_PREFERRED
            # Version 9
            ['BatchDataLimit', 5000, MQLONG_TYPE],
            ['UseDLQ', 2, MQLONG_TYPE],
            ['DefReconnect', 0, MQLONG_TYPE],
            # Version 10
            ['CertificateLabel', b'', '64s'],
            # Version 11
            ['SPLProtection', 0, MQLONG_TYPE]
            # Version 12
        ]

        # In theory, the pad should've been placed right before the 'MsgExitPtr'
        # attribute, however setting it there makes no effect and that's why
        # it's being set here, as a last element in the list.
        if '7.1' not in pymqe.__mqlevels__:
            if MQLONG_TYPE == 'i':
                opts += [['pad', b'', '4s']]

        super(CD, self).__init__(tuple(opts), **kw)

# Backward compatibility
cd = CD

# ################################################################################################################################
# ################################################################################################################################

# SCO Class for SSL Support courtesy of Brian Vicente (mailto:sailbv@netscape.net)
class SCO(MQOpts):
    """ Construct an MQSCO Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):

        if '8.0.0' in pymqe.__mqlevels__:
            _mqcsco_version = CMQC.MQSCO_VERSION_5

        elif '7.1' in pymqe.__mqlevels__:
            _mqcsco_version = CMQC.MQSCO_VERSION_4

        elif '7.0' in pymqe.__mqlevels__:
            _mqcsco_version = CMQC.MQSCO_VERSION_3

        elif '6.0' in pymqe.__mqlevels__:
            _mqcsco_version = CMQC.MQSCO_VERSION_2

        else:
            _mqcsco_version = CMQC.MQSCO_VERSION_1

        opts = [
            ['StrucId', CMQC.MQSCO_STRUC_ID, '4s'],
            ['Version', _mqcsco_version, MQLONG_TYPE],
            ['KeyRepository', b'', '256s'],
            ['CryptoHardware', b'', '256s'],
            ['AuthInfoRecCount', py23long(0), MQLONG_TYPE],
            ['AuthInfoRecOffset', py23long(0), MQLONG_TYPE],
            ['AuthInfoRecPtr', 0, 'P']]

        # Add new SSL fields defined in 6.0 and update version to 2

        if '6.0' in pymqe.__mqlevels__:
            opts += [['KeyResetCount', py23long(0), MQLONG_TYPE],
                     ['FipsRequired', py23long(0), MQLONG_TYPE]]

        if '7.0' in pymqe.__mqlevels__:
            opts += [['EncryptionPolicySuiteB', [1, 0, 0, 0], '4' + MQLONG_TYPE]]

        if '7.1' in pymqe.__mqlevels__:
            opts += [['CertificateValPolicy', py23long(0), MQLONG_TYPE]]


        if '8.0.0' in pymqe.__mqlevels__:
            opts += [['CertificateLabel', b'', '64s']]

        super(SCO, self).__init__(tuple(opts), **kw)

# Backward compatibility
sco = SCO

# ################################################################################################################################
# ################################################################################################################################

class SD(MQOpts):
    """ Construct an MQSD Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQSD_STRUC_ID, '4s'],
                ['Version', CMQC.MQSD_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQSO_NON_DURABLE, MQLONG_TYPE],
                ['ObjectName', b'', '48s'],
                ['AlternateUserId', b'', '12s'],
                ['AlternateSecurityId', CMQC.MQSID_NONE, '40s'],
                ['SubExpiry', CMQC.MQEI_UNLIMITED, MQLONG_TYPE],

                # ObjectString
                ['ObjectStringVSPtr', 0, 'P'],
                ['ObjectStringVSOffset', py23long(0), MQLONG_TYPE],
                ['ObjectStringVSBufSize', py23long(0), MQLONG_TYPE],
                ['ObjectStringVSLength', py23long(0), MQLONG_TYPE],
                ['ObjectStringVSCCSID', py23long(0), MQLONG_TYPE],

                # Subname
                ['SubNameVSPtr', 0, 'P'],
                ['SubNameVSOffset', py23long(0), MQLONG_TYPE],
                ['SubNameVSBufSize', py23long(0), MQLONG_TYPE],
                ['SubNameVSLength', py23long(0), MQLONG_TYPE],
                ['SubNameVSCCSID', py23long(0), MQLONG_TYPE],

                # SubUserData
                ['SubUserDataVSPtr', 0, 'P'],
                ['SubUserDataVSOffset', py23long(0), MQLONG_TYPE],
                ['SubUserDataVSBufSize', py23long(0), MQLONG_TYPE],
                ['SubUserDataVSLength', py23long(0), MQLONG_TYPE],
                ['SubUserDataVSCCSID', py23long(0), MQLONG_TYPE],

                ['SubCorrelId', CMQC.MQCI_NONE, '24s'],
                ['PubPriority', CMQC.MQPRI_PRIORITY_AS_Q_DEF, MQLONG_TYPE],
                ['PubAccountingToken', CMQC.MQACT_NONE, '32s'],
                ['PubApplIdentityData', b'', '32s'],

                # SelectionString
                ['SelectionStringVSPtr', 0, 'P'],
                ['SelectionStringVSOffset', py23long(0), MQLONG_TYPE],
                ['SelectionStringVSBufSize', py23long(0), MQLONG_TYPE],
                ['SelectionStringVSLength', py23long(0), MQLONG_TYPE],
                ['SelectionStringVSCCSID', py23long(0), MQLONG_TYPE],

                ['SubLevel', 0, MQLONG_TYPE],

                # SelectionString
                ['ResObjectStringVSPtr', 0, 'P'],
                ['ResObjectStringVSOffset', py23long(0), MQLONG_TYPE],
                ['ResObjectStringVSBufSize', py23long(0), MQLONG_TYPE],
                ['ResObjectStringVSLength', py23long(0), MQLONG_TYPE],
                ['ResObjectStringVSCCSID', py23long(0), MQLONG_TYPE]]

        super(SD, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class SRO(MQOpts):
    """ Construct an MQSRO Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQSRO_STRUC_ID, '4s'],
                ['Version', CMQC.MQSRO_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQSRO_FAIL_IF_QUIESCING, MQLONG_TYPE],
                ['NumPubs', 0, MQLONG_TYPE]]

        super(SRO, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CMHO(MQOpts):
    """ Construct an MQCMHO Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQCMHO_STRUC_ID, '4s'],
                ['Version', CMQC.MQCMHO_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQCMHO_DEFAULT_VALIDATION, MQLONG_TYPE]]

        super(CMHO, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class PD(MQOpts):
    """ Construct an MQPD Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQPD_STRUC_ID, '4s'],
                ['Version', CMQC.MQPD_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQPD_NONE, MQLONG_TYPE],
                ['Support', CMQC.MQPD_SUPPORT_OPTIONAL, MQLONG_TYPE],
                ['Context', CMQC.MQPD_NO_CONTEXT, MQLONG_TYPE],
                ['CopyOptions', CMQC.MQCOPY_DEFAULT, MQLONG_TYPE]]

        super(PD, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class SMPO(MQOpts):
    """ Construct an MQSMPO Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQSMPO_STRUC_ID, '4s'],
                ['Version', CMQC.MQSMPO_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQSMPO_SET_FIRST, MQLONG_TYPE],
                ['ValueEncoding', CMQC.MQENC_NATIVE, MQLONG_TYPE],
                ['ValueCCSID', CMQC.MQCCSI_APPL, MQLONG_TYPE]]

        super(SMPO, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class IMPO(MQOpts):
    """ Construct an MQIMPO Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQIMPO_STRUC_ID, '4s'],
                ['Version', CMQC.MQIMPO_VERSION_1, MQLONG_TYPE],
                ['Options', CMQC.MQIMPO_INQ_FIRST, MQLONG_TYPE],
                ['RequestedEncoding', CMQC.MQENC_NATIVE, MQLONG_TYPE],
                ['RequestedCCSID', CMQC.MQCCSI_APPL, MQLONG_TYPE],
                ['ReturnedEncoding', CMQC.MQENC_NATIVE, MQLONG_TYPE],
                ['ReturnedCCSID', py23long(0), MQLONG_TYPE],
                ['Reserved1', py23long(0), MQLONG_TYPE],

                # ReturnedName
                ['ReturnedNameVSPtr', 0, 'P'],
                ['ReturnedNameVSOffset', py23long(0), MQLONG_TYPE],
                ['ReturnedNameVSBufSize', py23long(0), MQLONG_TYPE],
                ['ReturnedNameVSLength', py23long(0), MQLONG_TYPE],
                ['ReturnedNameVSCCSID', py23long(0), MQLONG_TYPE],

                ['TypeString', b'', '8s']]

        super(IMPO, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class XQH(MQOpts):
    """ Construct an MQXQH Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        opts = [['StrucId', CMQC.MQXQH_STRUC_ID, '4s'],
                ['Version', CMQC.MQXQH_VERSION_1, MQLONG_TYPE],
                ['RemoteQName', b'', '48s'],
                ['RemoteQMgrName', b'', '48s'], ]

        super(XQH, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CFH(MQOpts):
    """ Construct an MQCFH Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        # type: (int) -> None
        opts = [['Type', CMQCFC.MQCFT_COMMAND, MQLONG_TYPE],
                ['StrucLength', CMQCFC.MQCFH_STRUC_LENGTH, MQLONG_TYPE],
                ['Version', CMQCFC.MQCFH_VERSION_1, MQLONG_TYPE],
                ['Command', CMQCFC.MQCMD_NONE, MQLONG_TYPE],
                ['MsgSeqNumber', 1, MQLONG_TYPE],
                ['Control', CMQCFC.MQCFC_LAST, MQLONG_TYPE],
                ['CompCode', CMQC.MQCC_OK, MQLONG_TYPE],
                ['Reason', CMQC.MQRC_NONE, MQLONG_TYPE],
                ['ParameterCount', 0, MQLONG_TYPE]
               ]
        super(CFH, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CFBF(MQOpts):
    """ Construct an MQCFBF Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """

    def __init__(self, **kw):
        # type: (Dict[str, Any]) -> None
        filter_value = kw.pop('FilterValue', '')
        filter_value_length = kw.pop('FilterValueLength', len(filter_value))
        padded_filter_value_length = padded_count(filter_value_length) # type: int

        opts = [['Type', CMQCFC.MQCFT_BYTE_STRING_FILTER, MQLONG_TYPE],
                ['StrucLength',
                 CMQCFC.MQCFBF_STRUC_LENGTH_FIXED + padded_filter_value_length, MQLONG_TYPE],
                ['Parameter', 0, MQLONG_TYPE],
                ['Operator', 0, MQLONG_TYPE],
                ['FilterValueLength', filter_value_length, MQLONG_TYPE],
                ['FilterValue', filter_value, '{}s'.format(padded_filter_value_length)]
               ]

        super(CFBF, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CFBS(MQOpts):
    """ Construct an MQCFBS Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """

    def __init__(self, **kw):
        # type: (Dict[str, Any]) -> None
        string = kw.pop('String', '')
        string_length = kw.pop('StringLength', len(string))
        padded_string_length = padded_count(string_length)

        opts = [['Type', CMQCFC.MQCFT_BYTE_STRING, MQLONG_TYPE],
                ['StrucLength',
                 CMQCFC.MQCFBS_STRUC_LENGTH_FIXED + padded_string_length, MQLONG_TYPE],
                ['Parameter', 0, MQLONG_TYPE],
                ['StringLength', string_length, MQLONG_TYPE],
                ['String', string, '{}s'.format(padded_string_length)]
               ]

        super(CFBS, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CFGR(MQOpts):
    """ Construct an MQCFGR Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """

    def __init__(self, **kw):
        # type: (Dict[str, Any]) -> None
        count = kw.pop('ParameterCount', 0)

        opts = [['Type', CMQCFC.MQCFT_GROUP, MQLONG_TYPE],
                ['StrucLength', CMQCFC.MQCFGR_STRUC_LENGTH, MQLONG_TYPE],
                ['Parameter', 0, MQLONG_TYPE],
                ['ParameterCount', count, MQLONG_TYPE],
               ]
        super(CFGR, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CFIF(MQOpts):
    """ Construct an MQCFIF Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """

    def __init__(self, **kw):
        # type: (Dict[str, Any]) -> None
        opts = [['Type', CMQCFC.MQCFT_INTEGER_FILTER, MQLONG_TYPE],
                ['StrucLength', CMQCFC.MQCFIF_STRUC_LENGTH, MQLONG_TYPE],
                ['Parameter', 0, MQLONG_TYPE],
                ['Operator', 0, MQLONG_TYPE],
                ['FilterValue', 0, MQLONG_TYPE]
               ]

        super(CFIF, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CFIL(MQOpts):
    """ Construct an MQCFIL Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        # type: (Dict[str, Any]) -> None
        values = kw.pop('Values', []) # type: List[int]
        count = kw.pop('Count', len(values)) # type: int

        opts = [['Type', CMQCFC.MQCFT_INTEGER_LIST, MQLONG_TYPE],
                ['StrucLength', CMQCFC.MQCFIL_STRUC_LENGTH_FIXED + 4 * count, MQLONG_TYPE], # Check python 2
                ['Parameter', 0, MQLONG_TYPE],
                ['Count', count, MQLONG_TYPE],
                ['Values', values, MQLONG_TYPE, count],
               ]
        super(CFIL, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CFIL64(MQOpts):
    """ Construct an MQCFIL64 Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        # type: (Dict[str, Any]) -> None
        values = kw.pop('Values', []) # type: List[int]
        count = kw.pop('Count', len(values)) # type: int

        opts = [['Type', CMQCFC.MQCFT_INTEGER64_LIST, MQLONG_TYPE],
                ['StrucLength', CMQCFC.MQCFIL64_STRUC_LENGTH_FIXED + 8 * count, MQLONG_TYPE],
                ['Parameter', 0, MQLONG_TYPE],
                ['Count', count, MQLONG_TYPE],
                ['Values', values, INTEGER64_TYPE, count],
               ]
        super(CFIL64, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CFIN(MQOpts):
    """ Construct an MQCFIN Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        # type: (Dict[str, Any]) -> None

        opts = [['Type', CMQCFC.MQCFT_INTEGER, MQLONG_TYPE],
                ['StrucLength', CMQCFC.MQCFIN_STRUC_LENGTH, MQLONG_TYPE],
                ['Parameter', 0, MQLONG_TYPE],
                ['Value', 0, MQLONG_TYPE],
               ]
        super(CFIN, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CFIN64(MQOpts):
    """ Construct an MQCFIN64 Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """
    def __init__(self, **kw):
        # type: (Dict[str, Any]) -> None

        opts = [['Type', CMQCFC.MQCFT_INTEGER64, MQLONG_TYPE],
                ['StrucLength', CMQCFC.MQCFIN64_STRUC_LENGTH, MQLONG_TYPE],
                ['Parameter', 0, MQLONG_TYPE],
                ['Value', 0, INTEGER64_TYPE],
               ]
        super(CFIN64, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CFSF(MQOpts):
    """ Construct an MQCFSF Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """

    def __init__(self, **kw):
        # type: (Dict[str, Any]) -> None
        filter_value = kw.pop('FilterValue', '')
        filter_value_length = kw.pop('FilterValueLength', len(filter_value)) # type: int
        padded_filter_value_length = padded_count(filter_value_length)

        opts = [['Type', CMQCFC.MQCFT_STRING_FILTER, MQLONG_TYPE],
                ['StrucLength',
                 CMQCFC.MQCFSF_STRUC_LENGTH_FIXED + padded_filter_value_length, MQLONG_TYPE],
                ['Parameter', 0, MQLONG_TYPE],
                ['Operator', 0, MQLONG_TYPE],
                ['CodedCharSetId', CMQC.MQCCSI_DEFAULT, MQLONG_TYPE],
                ['FilterValueLength', filter_value_length, MQLONG_TYPE],
                ['FilterValue', filter_value, '{}s'.format(padded_filter_value_length)]
               ]

        super(CFSF, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CFSL(MQOpts):
    """ Construct an MQCFSL Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """

    def __init__(self, **kw):
        # type: (Dict[str, Any]) -> None
        strings = kw.pop('Strings', []) # type: List[str]
        string_length = kw.pop('StringLength', len(max(strings, key=len)) if strings else 0)

        strings_count = len(strings)
        count = kw.pop('Count', strings_count)

        max_string_length = padded_count(string_length) if count else 0
        padded_strings_length = (max_string_length) * strings_count

        opts = [['Type', CMQCFC.MQCFT_STRING_LIST, MQLONG_TYPE],
                ['StrucLength', CMQCFC.MQCFSL_STRUC_LENGTH_FIXED + padded_strings_length, MQLONG_TYPE],
                ['Parameter', 0, MQLONG_TYPE],
                ['CodedCharSetId', CMQC.MQCCSI_DEFAULT, MQLONG_TYPE],
                ['Count', count, MQLONG_TYPE],
                ['StringLength', max_string_length, MQLONG_TYPE],
                ['Strings', strings if strings else [b''], '{}s'.format(max_string_length), (count if count else 1)]
               ]

        super(CFSL, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

class CFST(MQOpts):
    """ Construct an MQCFST Structure with default values as per MQI.
    The default values may be overridden by the optional keyword arguments 'kw'.
    """

    def __init__(self, **kw):
        # type: (Dict[str, Any]) -> None
        string = kw.pop('String', '')
        string_length = kw.pop('StringLength', len(string))
        padded_string_length = padded_count(string_length)

        opts = [['Type', CMQCFC.MQCFT_STRING, MQLONG_TYPE],
                ['StrucLength', CMQCFC.MQCFST_STRUC_LENGTH_FIXED + padded_string_length, MQLONG_TYPE],
                ['Parameter', 0, MQLONG_TYPE],
                ['CodedCharSetId', CMQC.MQCCSI_DEFAULT, MQLONG_TYPE],
                ['StringLength', string_length, MQLONG_TYPE],
                ['String', string, '{}s'.format(padded_string_length)]
               ]

        super(CFST, self).__init__(tuple(opts), **kw)

# ################################################################################################################################
# ################################################################################################################################

#
# A utility to convert a MQ constant to its string mnemonic by groping
# a module dictonary
#

class _MQConst2String(object):

    def __init__(self, module, prefix):
        self.__module = module
        self.__prefix = prefix
        self.__stringDict = {}
        self.__lock = threading.Lock()

    def __build(self):
        # Lazily build the dictionary of consts vs. their
        # mnemonic strings from the given module dict. Only those
        # attribute that begins with the prefix are considered.
        _ = self.__lock.acquire()
        if len(self.__stringDict) == 0:
            pfxlen = len(self.__prefix)
            for i in self.__module.__dict__.keys():
                if i[0:pfxlen] == self.__prefix:
                    new_key, new_val = self.__module.__dict__[i], i
                    self.__stringDict[new_key] = new_val
        self.__lock.release()

    def __getitem__(self, code):
        self.__build()
        return self.__stringDict[code]

    def __contains__(self, key):
        self.__build()
        return key in self.__stringDict

    def has_key(self, key):
        """'Deprecated. Use 'in' operator instead."""
        return key in self

# ################################################################################################################################
# ################################################################################################################################

#######################################################################
#
# Exception class that encapsulates MQI completion/reason codes.
#
#######################################################################

class Error(Exception):
    """ Base class for all PyMQI exceptions.
    """

# ################################################################################################################################
# ################################################################################################################################

class MQMIError(Error):
    """ Exception class for MQI low level errors.
    """
    errStringDicts = (_MQConst2String(CMQC, 'MQRC_'), _MQConst2String(CMQCFC, 'MQRCCF_'),)
    comp = CMQC.MQCC_OK
    reason = CMQC.MQRC_NONE

    def __init__(self, comp, reason, **kw):
        # type: (int, int, Dict[str, Any]) -> None
        """ Construct the error object with MQI completion code 'comp' and reason code 'reason'.
        """
        self.comp, self.reason = comp, reason

        for key in kw:
            setattr(self, key, kw[key])

    def __str__(self):
        # type: () -> str
        return 'MQI Error. Comp: %d, Reason %d: %s' % (self.comp, self.reason, self.errorAsString())

    def errorAsString(self):
        # type: () -> str
        """ Return the exception object MQI warning/failed reason as its mnemonic string.
        """
        if self.comp == CMQC.MQCC_OK:
            return 'OK'
        elif self.comp == CMQC.MQCC_WARNING:
            pfx = 'WARNING: '
        else:
            pfx = 'FAILED: '

        for d in MQMIError.errStringDicts:
            if self.reason in d:
                return pfx + d[self.reason]
        return pfx + 'Error code ' + str(self.reason) + ' not defined'

# ################################################################################################################################
# ################################################################################################################################

class PYIFError(Error):
    """ Exception class for errors generated by pymqi.
    """
    def __init__(self, e):
        self.error = e

    def __str__(self):
        return 'PYMQI Error: ' + str(self.error)

# ################################################################################################################################
# ################################################################################################################################

#######################################################################
#
# MQI Verbs
#
#######################################################################

class QueueManager(object):
    """ QueueManager encapsulates the connection to the Queue Manager. By
    default, the Queue Manager is implicitly connected. If required,
    the connection may be deferred until a call to connect().
    """
    def __init__(self, name='', disconnect_on_exit=True,
                 bytes_encoding=default.bytes_encoding, default_ccsid=default.ccsid):
        # type: (Optional[str], bool, str, int) -> None
        """ Connect to the Queue Manager 'name' (default value '').
        If 'name' is None, don't connect now, but defer the connection until connect() is called.
        Input 'bytes_encoding'  and 'default_ccsid' are the encodings that will be used in PCF, MQPUT and MQPUT1 calls
        using this MQ connection in case Unicode objects should be given on input.
        """
        name = ensure_bytes(name)  # Python 3 strings to be converted to bytes

        self.__handle = None
        self.__name = name
        self.__disconnect_on_exit = disconnect_on_exit
        self.__qmobj = None
        self.bytes_encoding = bytes_encoding
        self.default_ccsid = default_ccsid

        if name is not None:
            self.connect(name)

    def __del__(self):
        # type: () -> None
        """ Disconnect from the queue Manager, if connected.
        """
        if self.__handle:
            if self.__qmobj:
                try:
                    pymqe.MQCLOSE(self.__handle, self.__qmobj, CMQC.MQCO_NONE)
                except Exception:
                    pass

            if self.__disconnect_on_exit:
                try:
                    self.disconnect()
                except Exception:
                    pass

    def connect(self, name):
        # type: (str) -> None
        """connect(name)

        Connect immediately to the Queue Manager 'name'."""

        rv = pymqe.MQCONN(name)
        if rv[1]:
            raise MQMIError(rv[1], rv[2])
        self.__handle = rv[0]
        self.__name = name

# MQCONNX code courtesy of John OSullivan (mailto:jos@onebox.com)
# SSL additions courtesy of Brian Vicente (mailto:sailbv@netscape.net)
# Connect options suggested by Jaco Smuts (mailto:JSmuts@clover.co.za)

    def connect_with_options(self, name, *args, **kwargs):
        # type: (str, Any, Dict[str, Any]) -> None
        """connect_with_options(name [, opts=cnoopts][ ,cd=mqcd][ ,sco=mqsco])
           connect_with_options(name, cd, [sco])

        Connect immediately to the Queue Manager 'name', using the
        optional MQCNO Options opts, the optional MQCD connection
        descriptor cd and the optional MQSCO SSL options sco.

        The second form is defined for backward compatibility with
        older (broken) versions of pymqi. It connects immediately to
        the Queue Manager 'name', using the MQCD connection descriptor
        cd and the optional MQSCO SSL options sco.
        """
        name = ensure_bytes(name)  # Python 3 strings to be converted to bytes

        # Deal with old style args
        len_args = len(args)
        if len_args:
            if len_args > 2:
                raise TypeError('Invalid options: %s' % args)
            if len_args >= 1:
                kwargs['cd'] = args[0]
            if len_args == 2:
                kwargs['sco'] = args[1]

        user_password = {} # type: Dict[str, str]
        user = kwargs.get('user')
        password = kwargs.get('password')

        if user:
            # We check for None because password can be an empty string
            if password is None:
                raise ValueError('Password must not be None if user is provided')

            if not (isinstance(user, (str, bytes)) and isinstance(password, (str, bytes))):
                raise ValueError('Both user and password must be instances of str or bytes')

            user_password['user'] = ensure_bytes(user, 'utf-8')
            user_password['password'] = ensure_bytes(password, 'utf-8')

        if 'cd' in kwargs:
            cd = kwargs['cd']

            # TLS encryption requires MQCD of version at least 7.
            # Thus, if someone uses TLS and the version is lower than that,
            # we can just increase it ourselves.
            if cd.SSLCipherSpec and cd.Version < CMQC.MQCD_VERSION_7:
                cd.Version = CMQC.MQCD_VERSION_7

            cd = cd.pack()
        else:
            cd = None

        options = kwargs['opts'] if 'opts' in kwargs else CMQC.MQCNO_NONE
        sco     = kwargs['sco']  if 'sco'  in kwargs else SCO()

        rv = pymqe.MQCONNX(name, options, cd, user_password, sco.pack())

        if rv[1] <= CMQC.MQCC_WARNING:
            self.__handle = rv[0]
            self.__name = name

        if rv[1]:
            raise MQMIError(rv[1], rv[2])


    # Backward compatibility
    connectWithOptions = connect_with_options

    def connect_tcp_client(self, name, cd, channel, conn_name, user=None, password=None):
        # type: (str, CD, str, str, str, str) -> None
        """ Connect immediately to the remote Queue Manager 'name', using
        a TCP Client connection, with channnel 'channel' and the
        TCP connection string 'conn_name'. All other connection
        optons come from 'cd'.
        """

        cd.ChannelName = ensure_bytes(channel)
        cd.ConnectionName = ensure_bytes(conn_name)
        cd.ChannelType = CMQC.MQCHT_CLNTCONN
        cd.TransportType = CMQC.MQXPT_TCP

        kwargs = {
            'user': user,
            'password': password,
            'cd': cd
        }

        self.connect_with_options(name, **kwargs)

    # Backward compatibility
    connectTCPClient = connect_tcp_client

    def disconnect(self):
        # type: () -> None
        """ Disconnect from queue manager, if connected.
        """
        if not self.__handle:
            raise PYIFError('not connected')
        pymqe.MQDISC(self.__handle)
        self.__handle = self.__qmobj = None

    def get_handle(self):
        # type: () -> None
        """ Get the queue manager handle. The handle is used for other pymqi calls.
        """
        if self.__handle:
            return self.__handle
        else:
            raise PYIFError('not connected')

    # Backward compatibility
    getHandle = get_handle

    def begin(self):
        # type: () -> None
        """ Begin a new global transaction.
        """
        rv = pymqe.MQBEGIN(self.__handle)
        if rv[0]:
            raise MQMIError(rv[0], rv[1])

    def commit(self):
        # type: () -> None
        """ Commits any outstanding gets/puts in the current unit of work.
        """
        rv = pymqe.MQCMIT(self.__handle)
        if rv[0]:
            raise MQMIError(rv[0], rv[1])

    def backout(self):
        # type: () -> None
        """ Backout any outstanding gets/puts in the current unit of work.
        """
        rv = pymqe.MQBACK(self.__handle)
        if rv[0]:
            raise MQMIError(rv[0], rv[1])

    def put1(self, qDesc, msg, *opts):
        # type: (Union[str, bytes, OD], Optional[bytes], Union[MD, OD]) -> None
        """ Put the single message in string buffer 'msg' on the queue
        using the MQI PUT1 call. This encapsulates calls to MQOPEN,
        MQPUT and MQCLOSE. put1 is the optimal way to put a single
        message on a queue.

        qDesc identifies the Queue either by name (if its a string),
        or by MQOD (if its a pymqi.od() instance).

        mDesc is the pymqi.md() MQMD Message Descriptor for the
        message. If it is not passed, or is None, then a default md()
        object is used.

        putOpts is the pymqi.pmo() MQPMO Put Message Options structure
        for the put1 call. If it is not passed, or is None, then a
        default pmo() object is used.

        If mDesc and/or putOpts arguments were supplied, they may be
        updated by the put1 operation.
        """
        m_desc, put_opts = common_q_args(*opts)

        if not isinstance(msg, bytes):
            if (
                    (is_py3 and isinstance(msg, str))  # Python 3 string is unicode
                    or
                    (is_py2 and isinstance(msg, unicode)) # type: ignore # Python 2.7 string can be unicode
                ):
                msg = msg.encode(self.bytes_encoding)
                m_desc.CodedCharSetId = self.default_ccsid
                m_desc.Format = CMQC.MQFMT_STRING
            else:
                error_message = 'Message type is {0}. Convert to bytes.'
                raise TypeError(error_message.format(type(msg)))

        if put_opts is None:
            put_opts = PMO()

        # Now send the message
        rv = pymqe.MQPUT1(self.__handle, make_q_desc(qDesc).pack(), m_desc.pack(), put_opts.pack(), msg)
        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])
        _ = m_desc.unpack(rv[0])
        _ = put_opts.unpack(rv[1])

    def inquire(self, attribute):
        # type: (str) -> Any
        """ Inquire on queue manager 'attribute'. Returns either the integer or string value for the attribute.
        """
        attribute = ensure_bytes(attribute)  # Python 3 strings to be converted to bytes

        if self.__qmobj is None:
            # Make an od for the queue manager, open the qmgr & cache result
            qmod = od(ObjectType=CMQC.MQOT_Q_MGR, ObjectQMgrName=self.__name)
            rv = pymqe.MQOPEN(self.__handle, qmod.pack(), CMQC.MQOO_INQUIRE)
            if rv[-2]:
                raise MQMIError(rv[-2], rv[-1])
            self.__qmobj = rv[0]
        rv = pymqe.MQINQ(self.__handle, self.__qmobj, attribute)
        if rv[1]:
            raise MQMIError(rv[-2], rv[-1])
        return rv[0]

    def _is_connected(self):
        # type: () -> bool
        """ Try pinging the queue manager in order to see whether the application
        is connected to it. Note that the method is merely a convienece wrapper
        around MQCMD_PING_Q_MGR, in particular, there's still possibility that
        the app will disconnect between checking QueueManager.is_connected
        and the next MQ call.
        """

        try:
            pcf = PCFExecute(self)
            _ = pcf.MQCMD_PING_Q_MGR()
        except Exception:
            return False
        else:
            return True

    is_connected = property(_is_connected)

# ################################################################################################################################
# ################################################################################################################################

# Some support functions for Queue ops.
def make_q_desc(qDescOrString):
    # type: (Union[str, bytes, OD]) -> OD
    """Maybe make MQOD from string. Module Private"""
    if isinstance(qDescOrString, (str, bytes)):
        return OD(ObjectName=ensure_bytes(qDescOrString))  # Python 3 strings to be converted to bytes
    else:
        return qDescOrString

# Backward compatibility
makeQDesc = make_q_desc

# ################################################################################################################################
# ################################################################################################################################

def common_q_args(*opts):
    """ Process args common to put/get/put1. Module Private.
    """
    ln = len(opts)
    if ln > 2:
        raise TypeError('Too many args')
    m_desc = None
    pg_opts = None
    if ln > 0:
        m_desc = opts[0]
    if ln == 2:
        pg_opts = opts[1]
    if m_desc is None:
        m_desc = md()
    return m_desc, pg_opts

# Backward compatibility
commonQArgs = common_q_args

# ################################################################################################################################
# ################################################################################################################################

class Queue:
    """ Queue encapsulates all the Queue I/O operations, including
    open/close and get/put. A QueueManager object must be already
    connected. The Queue may be opened implicitly on construction, or
    the open may be deferred until a call to open(), put() or
    get(). The Queue to open is identified either by a queue name
    string (in which case a default MQOD structure is created using
    that name), or by passing a ready constructed MQOD class.
    """
    def __realOpen(self):
        """Really open the queue."""
        if self.__qDesc is None:
            raise PYIFError('The Queue Descriptor has not been set.')
        rv = pymqe.MQOPEN(self.__qMgr.getHandle(), self.__qDesc.pack(), self.__openOpts)
        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])
        self.__qHandle = rv[0]
        self.__qDesc.unpack(rv[1])

    def __init__(self, qMgr, *opts):
        """ Associate a Queue instance with the QueueManager object 'qMgr'
        and optionally open the Queue.

        If q_desc is passed, it identifies the Queue either by name (if
        its a string), or by MQOD (if its a pymqi.od() instance). If
        q_desc is not defined, then the Queue is not opened
        immediately, but deferred to a subsequent call to open().

        If openOpts is passed, it specifies queue open options, and
        the queue is opened immediately. If open_opts is not passed,
        the queue open is deferred to a subsequent call to open(),
        put() or get().

        The following table clarifies when the Queue is opened:

           qDesc  openOpts   When opened
             N       N       open()
             Y       N       open() or get() or put()
             Y       Y       Immediately
        """

        self.__qMgr = qMgr # type: QueueManager
        self.__qHandle = self.__qDesc = self.__openOpts = None
        ln = len(opts)
        if ln > 2:
            raise TypeError('Too many args')
        if ln > 0:
            self.__qDesc = make_q_desc(opts[0])
        if ln == 2:
            self.__openOpts = opts[1]
            self.__realOpen()

    def __del__(self):
        """ Close the queue, if it has been opened.
        """
        if self.__qHandle:
            try:
                self.close()
            except Exception:
                pass

    def open(self, qDesc, *opts):
        """ Open the queue specified by qDesc. qDesc identifies the Queue
        either by name (if its a string), or by MQOD (if its a
        pymqi.od() instance). If openOpts is passed, it defines the
        queue open options, and the Queue is opened immediately. If
        openOpts is not passed, the Queue open is deferred until a
        subsequent put() or get() call.
        """
        ln = len(opts)
        if ln > 1:
            raise TypeError('Too many args')
        if self.__qHandle:
            raise PYIFError('The Queue is already open')
        self.__qDesc = make_q_desc(qDesc)
        if ln == 1:
            self.__openOpts = opts[0]
            self.__realOpen()

    def put(self, msg, *opts):
        """ Put the string buffer 'msg' on the queue. If the queue is not
        already open, it is opened now with the option 'MQOO_OUTPUT'.

        m_desc is the pymqi.md() MQMD Message Descriptor for the
        message. If it is not passed, or is None, then a default md()
        object is used.

        put_opts is the pymqi.pmo() MQPMO Put Message Options structure
        for the put call. If it is not passed, or is None, then a
        default pmo() object is used.

        If m_desc and/or put_opts arguments were supplied, they may be
        updated by the put operation.
        """
        m_desc, put_opts = common_q_args(*opts)

        if not isinstance(msg, bytes):
            if (
                    (is_py3 and isinstance(msg, str))  # Python 3 string is unicode
                    or
                    (is_py2 and isinstance(msg, unicode)) # Python 2.7 string can be unicode
                ):
                msg = msg.encode(self.__qMgr.bytes_encoding)
                m_desc.CodedCharSetId = self.__qMgr.default_ccsid
                m_desc.Format = CMQC.MQFMT_STRING
            else:
                error_message = 'Message type is {0}. Convert to bytes.'
                raise TypeError(error_message.format(type(msg)))

        if put_opts is None:
            put_opts = PMO()

        # If queue open was deferred, open it for put now
        if not self.__qHandle:
            self.__openOpts = CMQC.MQOO_OUTPUT
            self.__realOpen()

        # Now send the message
        rv = pymqe.MQPUT(self.__qMgr.get_handle(), self.__qHandle, m_desc.pack(), put_opts.pack(), msg)
        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])
        _ = m_desc.unpack(rv[0])
        _ = put_opts.unpack(rv[1])

    def put_rfh2(self, msg, *opts):
        """ Put a RFH2 message. opts[2] is a list of RFH2 headers. MQMD and RFH2's must be correct.
        """
        ensure_not_unicode(msg)  # Python 3 bytes check

        rfh2_buff = b''
        if len(opts) >= 3:
            if opts[2] is not None:
                if not isinstance(opts[2], list):
                    raise TypeError('Third item of opts should be a list.')
                encoding = CMQC.MQENC_NATIVE
                if opts[0] is not None:
                    mqmd = opts[0]
                    encoding = mqmd['Encoding']

                for rfh2_header in opts[2]:
                    if rfh2_header is not None:
                        rfh2_buff = rfh2_buff + rfh2_header.pack(encoding)
                        encoding = rfh2_header['Encoding']

                msg = rfh2_buff + msg
            self.put(msg, *opts[0:2])
        else:
            self.put(msg, *opts)

    def get(self, maxLength=None, *opts):
        """ Return a message from the queue. If the queue is not already
        open, it is opened now with the option 'MQOO_INPUT_AS_Q_DEF'.

        maxLength, if present, specifies the maximum length for the
        message. If the message received exceeds maxLength, then the
        behavior is as defined by MQI and the get_opts argument.

        If maxLength is not specified, or is None, then the entire
        message is returned regardless of its size. This may require
        multiple calls to the underlying MQGET API.

        m_desc is the pymqi.md() MQMD Message Descriptor for receiving
        the message. If it is not passed, or is None, then a default
        md() object is used.

        get_opts is the pymqi.gmo() MQGMO Get Message Options
        structure for the get call. If it is not passed, or is None,
        then a default gmo() object is used.

        If m_desc and/or get_opts arguments were supplied, they may be
        updated by the get operation.
        """

        m_desc, get_opts = common_q_args(*opts)
        if get_opts is None:
            get_opts = gmo()

        # If queue open was deferred, open it for put now
        if not self.__qHandle:
            self.__openOpts = CMQC.MQOO_INPUT_AS_Q_DEF
            self.__realOpen()

        if maxLength is None:
            if get_opts.Options & CMQC.MQGMO_ACCEPT_TRUNCATED_MSG:
                length = 0
            else:
                length = 4096 # Try to read short message in one call
        else:
            length = maxLength

        rv = pymqe.MQGET(self.__qMgr.getHandle(), self.__qHandle, m_desc.pack(), get_opts.pack(), length)

        if not rv[-2]:
            # Everything is OK
            _ = m_desc.unpack(rv[1])
            _ = get_opts.unpack(rv[2])
            return rv[0]

        # Accept truncated message
        if ((rv[-1] == CMQC.MQRC_TRUNCATED_MSG_ACCEPTED) or
                # Do not reread message with original length
                (rv[-1] == CMQC.MQRC_TRUNCATED_MSG_FAILED and maxLength is not None) or
                # Other errors
                (rv[-1] != CMQC.MQRC_TRUNCATED_MSG_FAILED)):
            if rv[-2] == CMQC.MQCC_WARNING:
                _ = m_desc.unpack(rv[1])
                _ = get_opts.unpack(rv[2])

            raise MQMIError(rv[-2], rv[-1], message=rv[0], original_length=rv[-3])

        # Message truncated, but we know its size. Do another MQGET
        # to retrieve it from the queue.
        rv = pymqe.MQGET(self.__qMgr.getHandle(), self.__qHandle, m_desc.pack(), get_opts.pack(), rv[-3])
        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])

        _ = m_desc.unpack(rv[1])
        _ = get_opts.unpack(rv[2])

        return rv[0]

    def get_no_jms(self, max_length=None, *args):
        md, gmo = common_q_args(*args)
        if not gmo:
            gmo = GMO()
        gmo.Options = gmo.Options | CMQC.MQGMO_NO_PROPERTIES | CMQC.MQGMO_FAIL_IF_QUIESCING

        return self.get(max_length, md, gmo)

    get_no_rfh2 = get_no_jms

    def get_rfh2(self, max_length=None, *opts):
        """ Get a message and attempt to unpack the rfh2 headers.
        opts[2] should be a empty list.
        Unpacking only attempted if Format in previous header is
        CMQC.MQFMT_RF_HEADER_2.
        """
        if len(opts) >= 3:
            if opts[2] is not None:
                if not isinstance(opts[2], list):
                    raise TypeError('Third item of opts should be a list.')

                msg = self.get(max_length, *opts[0:2])
                mqmd = opts[0]
                rfh2_headers = []
                # If format is not CMQC.MQFMT_RF_HEADER_2 then do not parse.
                frmt = mqmd['Format']
                while frmt == CMQC.MQFMT_RF_HEADER_2:
                    rfh2_header = RFH2()
                    rfh2_header.unpack(msg)
                    rfh2_headers.append(rfh2_header)
                    msg = msg[rfh2_header['StrucLength']:]
                    frmt = rfh2_header['Format']
                opts[2].extend(rfh2_headers)
            else:
                raise AttributeError('get_opts cannot be None if passed.')
        else:
            msg = self.get(max_length, *opts)

        return msg

    def close(self, options=CMQC.MQCO_NONE):
        # type: (int) -> None
        """ Close a queue, using options.
        """
        if not self.__qHandle:
            raise PYIFError('not open')
        rv = pymqe.MQCLOSE(self.__qMgr.getHandle(), self.__qHandle, options)
        if rv[0]:
            raise MQMIError(rv[-2], rv[-1])
        self.__qHandle = self.__qDesc = self.__openOpts = None

    def inquire(self, attribute):
        """ Inquire on queue 'attribute'. If the queue is not already
        open, it is opened for Inquire. Returns either the integer or
        string value for the attribute.
        """
        attribute = ensure_bytes(attribute)  # Python 3 strings to be converted to bytes

        if not self.__qHandle:
            self.__openOpts = CMQC.MQOO_INQUIRE
            self.__realOpen()
        rv = pymqe.MQINQ(self.__qMgr.getHandle(), self.__qHandle, attribute)
        if rv[1]:
            raise MQMIError(rv[-2], rv[-1])
        return rv[0]

    def set(self, attribute, arg):
        """ Sets the Queue attribute to arg.
        """
        attribute = ensure_bytes(attribute)  # Python 3 strings to be converted to bytes
        ensure_not_unicode(arg)  # Python 3 bytes check

        if not self.__qHandle:
            self.__openOpts = CMQC.MQOO_SET
            self.__realOpen()
        rv = pymqe.MQSET(self.__qMgr.getHandle(), self.__qHandle, attribute, arg)
        if rv[1]:
            raise MQMIError(rv[-2], rv[-1])

    def set_handle(self, queue_handle):
        """ Sets the queue handle in the case when a handle was returned from a previous MQ call.
        """
        self.__qHandle = queue_handle

    def get_handle(self): # type: () -> Queue
        """ Get the queue handle.
        """
        return self.__qHandle

# ################################################################################################################################
# ################################################################################################################################

# Publish Subscribe support - Hannes Wagener 2011
class Topic:
    """ Topic encapsulates all the Topic I/O operations, including
    publish/subscribe.  A QueueManager object must be already
    connected. The Topic may be opened implicitly on construction, or
    the open may be deferred until a call to open(), pub() or
    (The same as for Queue).

    The Topic to open is identified either by a topic name and/or a topic
    string (in which case a default MQOD structure is created using
    those names), or by passing a ready constructed MQOD class.
    Refer to the 'Using topic strings' section in the MQ7 Information Center
    for an explanation of how the topic name and topic string is combined
    to identify a particular topic.
    """

    def __real_open(self):
        """ Really open the topic.  Only do this in pub()?
        """
        if self.__topic_desc is None:
            raise PYIFError('The Topic Descriptor has not been set.')

        rv = pymqe.MQOPEN(self.__queue_manager.getHandle(),
                          self.__topic_desc.pack(), self.__open_opts)
        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])

        _ = self.__topic_handle = rv[0]
        _ = self.__topic_desc.unpack(rv[1])

    def __init__(self, queue_manager, topic_name=None, topic_string=None, topic_desc=None, open_opts=None):
        """ Associate a Topic instance with the QueueManager object 'queue_manager'
        and optionally open the Topic.

        If topic_desc is passed ignore topic_string and topic_name.

        If open_opts is passed, it specifies topic open options, and
        the topic is opened immediately. If open_opts is not passed,
        the queue open is deferred to a subsequent call to open(),
        pub().

        The following table clarifies when the Topic is opened:

        topic_desc  open_opts   When opened
             N       N       open()
             Y       N       open() or pub()
             Y       Y       Immediately
        """

        queue_manager = ensure_bytes(queue_manager)  # Python 3 strings to be converted to bytes
        topic_name = ensure_bytes(topic_name)  # Python 3 strings to be converted to bytes
        topic_string = ensure_bytes(topic_string)  # Python 3 strings to be converted to bytes

        self.__queue_manager = queue_manager
        self.__topic_handle = None
        self.__topic_desc = topic_desc
        self.__open_opts = open_opts

        self.topic_name = topic_name
        self.topic_string = topic_string

        if self.__topic_desc:
            if self.__topic_desc['ObjectType'] is not CMQC.MQOT_TOPIC:
                raise PYIFError('The Topic Descriptor ObjectType is not MQOT_TOPIC.')
            if self.__topic_desc['Version'] is not CMQC.MQOD_VERSION_4:
                raise PYIFError('The Topic Descriptor Version is not MQOD_VERSION_4.')
        else:
            self.__topic_desc = self.__create_topic_desc(topic_name, topic_string)

        if self.__open_opts:
            self.__real_open()

    @staticmethod
    def __create_topic_desc(topic_name, topic_string):
        """ Creates a topic object descriptor from a given topic_name/topic_string.
        """
        topic_name = ensure_bytes(topic_name)  # Python 3 strings to be converted to bytes
        topic_string = ensure_bytes(topic_string)  # Python 3 strings to be converted to bytes

        topic_desc = OD()
        topic_desc['ObjectType'] = CMQC.MQOT_TOPIC
        topic_desc['Version'] = CMQC.MQOD_VERSION_4

        if topic_name:
            topic_desc['ObjectName'] = topic_name

        if topic_string:
            topic_desc.set_vs('ObjectString', topic_string, 0, 0, 0)

        return topic_desc

    def __del__(self):
        """ Close the Topic, if it has been opened.
        """
        try:
            if self.__topic_handle:
                self.close()
        except Exception:
            pass

    def open(self, topic_name=None, topic_string=None, topic_desc=None, open_opts=None):
        """ Open the Topic specified by topic_desc or create a object descriptor
        from topic_name and topic_string.
        If open_opts is passed, it defines the
        Topic open options, and the Topic is opened immediately. If
        open_opts is not passed, the Topic open is deferred until a
        subsequent pub() call.
        """
        topic_name = ensure_bytes(topic_name)  # Python 3 strings to be converted to bytes
        topic_string = ensure_bytes(topic_string)  # Python 3 strings to be converted to bytes

        if self.__topic_handle:
            raise PYIFError('The Topic is already open.')

        if topic_name:
            self.topic_name = topic_name

        if topic_string:
            self.topic_string = topic_string

        if topic_desc:
            self.__topic_desc = topic_desc
        else:
            self.__topic_desc = self.__create_topic_desc(self.topic_name, self.topic_string)

        if open_opts:
            self.__open_opts = open_opts
            self.__real_open()

    def pub(self, msg, *opts):
        """ Publish the string buffer 'msg' to the Topic. If the Topic is not
        already open, it is opened now. with the option 'MQOO_OUTPUT'.

        msg_desc is the pymqi.md() MQMD Message Descriptor for the
        message. If it is not passed, or is None, then a default md()
        object is used.

        put_opts is the pymqi.pmo() MQPMO Put Message Options structure
        for the put call. If it is not passed, or is None, then a
        default pmo() object is used.

        If msg_desc and/or put_opts arguments were supplied, they may be
        updated by the put operation.
        """
        ensure_not_unicode(msg)  # Python 3 bytes check

        msg_desc, put_opts = common_q_args(*opts)

        if put_opts is None:
            put_opts = pmo()

        # If queue open was deferred, open it for put now
        if not self.__topic_handle:
            self.__open_opts = CMQC.MQOO_OUTPUT
            self.__real_open()
        # Now send the message
        rv = pymqe.MQPUT(self.__queue_manager.getHandle(), self.__topic_handle, msg_desc.pack(), put_opts.pack(), msg)
        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])

        _ = msg_desc.unpack(rv[0])
        _ = put_opts.unpack(rv[1])

    def pub_rfh2(self, msg, *opts):
        # type: (bytes, *MQOpts) -> None
        """pub_rfh2(msg[, mDesc ,putOpts, [rfh2_header, ]])
        Put a RFH2 message. opts[2] is a list of RFH2 headers.
        MQMD and RFH2's must be correct.
        """
        ensure_not_unicode(msg)  # Python 3 bytes check

        rfh2_buff = b''
        if len(opts) >= 3:
            if opts[2] is not None:
                if not isinstance(opts[2], list):
                    raise TypeError('Third item of opts should be a list.')
                encoding = CMQC.MQENC_NATIVE
                if opts[0] is not None:
                    mqmd = opts[0]
                    encoding = mqmd['Encoding']

                for rfh2_header in opts[2]:
                    if rfh2_header is not None:
                        rfh2_buff = rfh2_buff + rfh2_header.pack(encoding)
                        encoding = rfh2_header['Encoding']

                msg = rfh2_buff + msg
            self.pub(msg, *opts[0:2])
        else:
            self.pub(msg, *opts)

    def sub(self, *opts):
        """ Subscribe to the topic and return a Subscription object.
        A subscription to a topic can be made using an existing queue, either
        by pasing a Queue object or a string at which case the queue will
        be opened with default options.
        """
        sub_desc = None
        if len(opts) > 0:
            sub_desc = opts[0]

        sub_queue = None
        if len(opts) > 1:
            sub_queue = ensure_bytes(opts[1])  # Python 3 strings to be converted to bytes

        sub = Subscription(self.__queue_manager)
        sub.sub(sub_desc=sub_desc, sub_queue=sub_queue, topic_name=self.topic_name, topic_string=self.topic_string)

        return sub

    def close(self, options=CMQC.MQCO_NONE):
        """ Close the topic, using options.
        """
        if not self.__topic_handle:
            raise PYIFError('Topic not open.')

        rv = pymqe.MQCLOSE(self.__queue_manager.getHandle(), self.__topic_handle, options)
        if rv[0]:
            raise MQMIError(rv[-2], rv[-1])

        self.__topic_handle = None
        self.__topic_desc = None
        self.__open_opts = None

# ################################################################################################################################
# ################################################################################################################################

class Subscription:
    """ Encapsulates a subscription to a topic.
    """
    def __init__(self, queue_manager, sub_desc=None, sub_name=None,
                 sub_queue=None, sub_opts=None, topic_name=None, topic_string=None):

        queue_manager = ensure_bytes(queue_manager)  # Python 3 strings to be converted to bytes
        sub_name = ensure_bytes(sub_name)  # Python 3 strings to be converted to bytes
        topic_name = ensure_bytes(topic_name)  # Python 3 strings to be converted to bytes
        topic_string = ensure_bytes(topic_string)  # Python 3 strings to be converted to bytes

        self.__queue_manager = queue_manager
        self.sub_queue = sub_queue
        self.__sub_desc = sub_desc
        self.sub_name = sub_name
        self.sub_opts = sub_opts
        self.topic_name = topic_name
        self.topic_string = topic_string
        self.__sub_handle = None
        self.__open_opts = None

        if self.__sub_desc:
            self.sub(sub_desc=self.__sub_desc)

    def get_sub_queue(self):
        """ Return the subscription queue.
        """
        return self.sub_queue

    def get(self, max_length=None, *opts):
        """ Get a publication from the Queue.
        """
        return self.sub_queue.get(max_length, *opts)

    def get_rfh2(self, max_length=None, *opts):
        # type: (int, *MQOpts) -> bytes
        """ Get a publication from the Queue.
        """
        return self.sub_queue.get_rfh2(max_length, *opts)

    def sub(self, sub_desc=None, sub_queue=None, sub_name=None, sub_opts=None,
            topic_name=None, topic_string=None):
        """ Subscribe to a topic, alter or resume a subscription.
        Executes the MQSUB call with parameters.
        The subscription queue can be either passed as a Queue object or a
        Queue object handle.
        """
        sub_queue = ensure_bytes(sub_queue)  # Python 3 strings to be converted to bytes
        sub_name = ensure_bytes(sub_name)  # Python 3 strings to be converted to bytes
        topic_name = ensure_bytes(topic_name)  # Python 3 strings to be converted to bytes
        topic_string = ensure_bytes(topic_string)  # Python 3 strings to be converted to bytes

        if topic_name:
            self.topic_name = topic_name
        if topic_string:
            self.topic_string = topic_string
        if sub_name:
            self.sub_name = sub_name

        if sub_desc:
            if not isinstance(sub_desc, SD):
                raise TypeError('sub_desc must be a SD(sub descriptor) object.')
        else:
            sub_desc = SD()
            if sub_opts:
                sub_desc['Options'] = sub_opts
            else:
                sub_desc['Options'] = CMQC.MQSO_CREATE + CMQC.MQSO_NON_DURABLE + CMQC.MQSO_MANAGED
            if self.sub_name:
                sub_desc.set_vs('SubName', self.sub_name)
            if self.topic_name:
                sub_desc['ObjectName'] = self.topic_name
            if self.topic_string:
                sub_desc.set_vs('ObjectString', self.topic_string)
        self.__sub_desc = sub_desc

        sub_queue_handle = CMQC.MQHO_NONE
        if sub_queue:
            if isinstance(sub_queue, Queue):
                sub_queue_handle = sub_queue.get_handle()
            else:
                sub_queue_handle = sub_queue

        rv = pymqe.MQSUB(self.__queue_manager.getHandle(), sub_desc.pack(), sub_queue_handle)

        if rv[-2]:
            raise MQMIError(rv[-2], rv[-1])

        sub_desc.unpack(rv[0])
        self.__sub_desc = sub_desc
        self.sub_queue = Queue(self.__queue_manager)
        self.sub_queue.set_handle(rv[1])
        self.__sub_handle = rv[2]

    def close(self, sub_close_options=CMQC.MQCO_NONE, close_sub_queue=False, close_sub_queue_options=CMQC.MQCO_NONE):

        if not self.__sub_handle:
            raise PYIFError('Subscription not open.')

        rv = pymqe.MQCLOSE(self.__queue_manager.getHandle(), self.__sub_handle, sub_close_options)
        if rv[0]:
            raise MQMIError(rv[-2], rv[-1])

        self.__sub_handle = None
        self.__sub_desc = None
        self.__open_opts = None

        if close_sub_queue:
            _ = self.sub_queue.close(close_sub_queue_options)

    def __del__(self):
        """ Close the Subscription, if it has been opened.
        """
        try:
            if self.__sub_handle:
                self.close()
        except PYIFError:
            pass

# ################################################################################################################################
# ################################################################################################################################

class MessageHandle(object):
    """ A higher-level wrapper around the MQI's native MQCMHO structure.
    """
    # When accessing message properties, this will be the maximum number
    # of characters a value will be able to hold. If it's not enough
    # an exception will be raised and its 'actual_value_length' will be
    # filled in with the information of how many characters there are actually
    # so that an application may re-issue the call.
    default_value_length = 64

    class _Properties(object):
        """ Encapsulates access to message properties.
        """
        def __init__(self, conn_handle, msg_handle):
            self.conn_handle = conn_handle
            self.msg_handle = msg_handle

        def __getitem__(self, name):
            """ Allows for a dict-like access to properties,
            handle.properties[name]
            """
            value = self.get(name)
            if not value:
                raise KeyError('No such property [%s]' % name)

            return value

        def __setitem__(self, name, value):
            """ Implements 'handle.properties[name] = value'.
            """
            return self.set(name, value)

        def get(self, name, default=None, max_value_length=None,
                impo_options=CMQC.MQIMPO_INQ_FIRST, pd=CMQC.MQPD_NONE,
                property_type=CMQC.MQTYPE_AS_SET):
            """ Returns the value of message property 'name'. 'default' is the
            value to return if the property is missing. 'max_value_length'
            is the maximum number of characters the underlying pymqe function
            is allowed to allocate for fetching the value
            (defaults to MessageHandle.default_value_length). 'impo_options'
            and 'pd_options' describe options of MQPD and MQIMPO structures
            to be used and 'property_type' points to the expected data type
            of the property.
            """
            if not max_value_length:
                max_value_length = MessageHandle.default_value_length

            value, dataLength, comp_code, comp_reason  = pymqe.MQINQMP(
                self.conn_handle, self.msg_handle, impo_options, name, pd, property_type, max_value_length)

            if comp_code != CMQC.MQCC_OK:
                raise MQMIError(comp_code, comp_reason, value=value, dataLength=dataLength)

            return value

        def set(self, name, value, property_type=CMQC.MQTYPE_STRING,
                value_length=CMQC.MQVL_NULL_TERMINATED, pd=None, smpo=None):
            """ Allows for setting arbitrary properties of a message. 'name'
            and 'value' are mandatory. All other parameters are OK to use as-is
            if 'value' is a string. If it isn't a string, the 'property_type'
            and 'value_length' should be set accordingly. For further
            customization, you can also use 'pd' and 'smpo' parameters for
            passing in MQPD and MQSMPO structures.
            """

            #name = ensure_bytes(name)  # Python 3 strings to be converted to bytes
            #ensure_not_unicode(value)  # Python 3 only bytes allowed

            pd = pd if pd else PD()
            smpo = smpo if smpo else SMPO()

            comp_code, comp_reason = pymqe.MQSETMP(
                self.conn_handle, self.msg_handle, smpo.pack(), name, pd.pack(), property_type, value, value_length)

            if comp_code != CMQC.MQCC_OK:
                raise MQMIError(comp_code, comp_reason)

    def __init__(self, qmgr=None, cmho=None):
        self.conn_handle = qmgr.get_handle() if qmgr else CMQC.MQHO_NONE
        cmho = cmho if cmho else CMHO()

        self.msg_handle, comp_code, comp_reason = pymqe.MQCRTMH(self.conn_handle, cmho.pack())

        if comp_code != CMQC.MQCC_OK:
            raise MQMIError(comp_code, comp_reason)

        self.properties = self._Properties(self.conn_handle, self.msg_handle)

# ################################################################################################################################
# ################################################################################################################################

class _Filter(object):
    """ The base class for MQAI filters. The initializer expectes user to provide
    the selector, value and the operator to use. For instance, the can be respectively
    MQCA_Q_DESC, 'MY.QUEUE.*', MQCFOP_LIKE. Compare with the pymqi.Filter class.
    """
    _pymqi_filter_type = None # type: Optional[str]

    def __init__(self, selector, value, operator):
        self.selector = selector  # this is int
        self.value = ensure_bytes(value)  # Python 3 strings to be converted to bytes
        self.operator = operator  # this is int

    def __repr__(self):
        msg = '<%s at %s %s:%s:%s>'
        return msg % (self.__class__.__name__, hex(id(self)), self.selector, self.value, self.operator)

# ################################################################################################################################
# ################################################################################################################################

class StringFilter(_Filter):
    """ A subclass of pymqi._Filter suitable for passing MQAI string filters around.
    """
    _pymqi_filter_type = 'string'

# ################################################################################################################################
# ################################################################################################################################

class IntegerFilter(_Filter):
    """ A subclass of pymqi._Filter suitable for passing MQAI integer filters around.
    """
    _pymqi_filter_type = 'integer'

# ################################################################################################################################
# ################################################################################################################################

class FilterOperator(object):
    """ Creates low-level filters basing on what's been provided in the high-level
    pymqi.Filter object.
    """
    operator_mapping = {
        'less': CMQCFC.MQCFOP_LESS,
        'equal': CMQCFC.MQCFOP_EQUAL,
        'greater': CMQCFC.MQCFOP_GREATER,
        'not_less': CMQCFC.MQCFOP_NOT_LESS,
        'not_equal': CMQCFC.MQCFOP_NOT_EQUAL,
        'not_greater': CMQCFC.MQCFOP_NOT_GREATER,
        'like': CMQCFC.MQCFOP_LIKE,
        'not_like': CMQCFC.MQCFOP_NOT_LIKE,
        'contains': CMQCFC.MQCFOP_CONTAINS,
        'excludes': CMQCFC.MQCFOP_EXCLUDES,
        'contains_gen': CMQCFC.MQCFOP_CONTAINS_GEN,
        'excludes_gen': CMQCFC.MQCFOP_EXCLUDES_GEN,
        } # type: Dict[str, int]

    def __init__(self, selector, operator_name):
        # type: (int, str) -> None
        # Do we support the given attribute filter?
        if CMQC.MQIA_FIRST <= selector <= CMQC.MQIA_LAST:
            self.filter_cls = IntegerFilter
        elif CMQC.MQCA_FIRST <= selector <= CMQC.MQCA_LAST:
            self.filter_cls = StringFilter
        else:
            msg = 'selector [%s] is of an unsupported type (neither integer ' + \
                'nor a string attribute). Please see' + \
                'https://dsuch.github.io/pymqi/support.html'
            raise Error(msg % selector)
        self.selector = selector
        self.operator = self.operator_mapping.get(operator_name)
        # Do we support the operator?
        if not self.operator:
            msg = 'Operator [%s] is not supported.'
            raise Error(msg % operator_name)

    def __call__(self, value):
        # type: (Union[str, int]) -> Union[IntegerFilter, StringFilter]
        ensure_not_unicode(value)  # Python 3 bytes accepted here
        return (self.filter_cls)(self.selector, value, self.operator)

# ################################################################################################################################
# ################################################################################################################################

class Filter(object):
    """ The user-facing MQAI filtering class which provides syntactic sugar
    on top of pymqi._Filter and its base classes.
    """
    def __init__(self, selector):
        # type: (int) -> None
        self.selector = selector

    def __getattribute__(self, name):
        # type: (str) -> FilterOperator
        """ A generic method for either fetching the pymqi.Filter object's
        attributes or calling magic methods like 'like', 'contains' etc.
        """
        if name=='selector':
            return object.__getattribute__(self, name)

        return FilterOperator(self.selector, name)

# ################################################################################################################################
# ################################################################################################################################

#
# This piece of magic shamelessly plagiarised from xmlrpclib.py. It
# works a bit like a C++ STL functor.
#
class _Method:
    def __init__(self, pcf, name):
        # type: (PCFExecute, str) -> None
        self.__pcf = pcf
        self.__name = name

    def __getattr__(self, name):
        # type: (str) -> _Method
        return _Method(self.__pcf, '%s.%s' % (self.__name, name))

    def __call__(self, *args):
        # type: (Union[dict, list, _Filter]) -> list
        if self.__name[0:7] == 'CMQCFC.':
            self.__name = self.__name[7:]
        if self.__pcf.qm:
            bytes_encoding = self.__pcf.bytes_encoding
            _ = self.__pcf.qm.getHandle()
        else:
            bytes_encoding = 'utf8'
            _ = self.__pcf.getHandle()

        len_args = len(args)

        if len_args == 2:
            args_dict, filters = args

        elif len_args == 1:
            args_dict, filters = args[0], []

        else:
            args_dict, filters = {}, []

        mqcfh = CFH(Version=CMQCFC.MQCFH_VERSION_3,
                    Command=CMQCFC.__dict__[self.__name],
                    Type=CMQCFC.MQCFT_COMMAND_XR,
                    ParameterCount=len(args_dict) + len(filters))
        message = mqcfh.pack()

        if args_dict:
            if isinstance(args_dict, dict):
                for key, value in args_dict.items():
                    if isinstance(value, (str, bytes)):
                        if is_unicode(value):
                            value = value.encode(bytes_encoding)
                        parameter = CFST(Parameter=key,
                                         String=value)
                    elif (isinstance(value, ByteString)):
                        parameter = CFBS(Parameter=key,
                                         String=value.value)
                    elif isinstance(value, int):
                        # Backward compatibility for MQAI behaviour
                        # for single value instead of list
                        is_list = False
                        for item in CMQCFC.__dict__:
                            if ((item[:7] == 'MQIACF_'
                                or
                                item[:7] == 'MQIACH_')
                                and item[-6:] == '_ATTRS'
                                and CMQCFC.__dict__[item] == key):
                                    is_list = True
                                    break
                        if not is_list:
                            parameter = CFIN(Parameter=key,
                                            Value=value)
                        else:
                            parameter = CFIL(Parameter=key,
                                            Values=[value])
                    elif (isinstance(value, list)):
                        if isinstance(value[0], int):
                            parameter = CFIL(Parameter=key, Values=value)
                        elif isinstance(value[0], (str, bytes)):
                            _value = []
                            for item in value:
                                if is_unicode(item):
                                    item = item.encode(bytes_encoding)
                                _value.append(item)
                            value = _value

                            parameter = CFSL(Parameter=key, Strings=value)

                    message = message + parameter.pack()
            elif isinstance(args_dict, list):
                for parameter in args_dict:
                    message = message + parameter.pack()

        if filters:
            for pcf_filter in filters:
                if isinstance(pcf_filter, _Filter):
                    if pcf_filter._pymqi_filter_type == 'string':
                        pcf_filter = CFSF(Parameter=pcf_filter.selector,
                                          Operator=pcf_filter.operator,
                                          FilterValue=pcf_filter.value)
                    elif pcf_filter._pymqi_filter_type == 'integer':
                        pcf_filter = CFIF(Parameter=pcf_filter.selector,
                                          Operator=pcf_filter.operator,
                                          FilterValue=pcf_filter.value)

                message = message + pcf_filter.pack()

        command_queue = Queue(self.__pcf.qm,
                              self.__pcf.command_queue_name,
                              CMQC.MQOO_OUTPUT)

        put_md = MD(Format=CMQC.MQFMT_ADMIN,
                    MsgType=CMQC.MQMT_REQUEST,
                    ReplyToQ=self.__pcf.reply_queue_name,
                    Feedback=CMQC.MQFB_NONE,
                    Expiry=self.__pcf.response_wait_interval // 100,
                    Report=CMQC.MQRO_PASS_DISCARD_AND_EXPIRY | CMQC.MQRO_DISCARD_MSG)
        put_opts = PMO(Options=CMQC.MQPMO_NO_SYNCPOINT)

        command_queue.put(message, put_md, put_opts)
        command_queue.close()

        gmo_options = CMQC.MQGMO_NO_SYNCPOINT + CMQC.MQGMO_FAIL_IF_QUIESCING + \
                      CMQC.MQGMO_WAIT

        if self.__pcf.convert:
            gmo_options = gmo_options + CMQC.MQGMO_CONVERT

        get_opts = GMO(
            Options=gmo_options,
            Version=CMQC.MQGMO_VERSION_2,
            MatchOptions=CMQC.MQMO_MATCH_CORREL_ID,
            WaitInterval=self.__pcf.response_wait_interval)
        get_md = MD(CorrelId=put_md.MsgId)

        ress = []
        while True:
            message = self.__pcf.reply_queue.get(None, get_md, get_opts)
            res, mqcfh_response = self.__pcf.unpack(message)

            ress.append(res)

            if mqcfh_response.Control == CMQCFC.MQCFC_LAST:
                break

        return ress

# ################################################################################################################################
# ################################################################################################################################

#
# Execute a PCF commmand. Inspired by Maas-Maarten Zeeman
#

class PCFExecute(QueueManager):

    """Send PCF commands or inquiries using the MQAI
    interface. PCFExecute must be connected to the Queue Manager
    (using one of the techniques inherited from QueueManager) before
    its used. PCF commands are executed by calling a CMQC defined
    MQCMD_* method on the object.  """

    qm = None # type: Optional[QueueManager]

    iaStringDict = _MQConst2String(CMQC, 'MQIA_')
    caStringDict = _MQConst2String(CMQC, 'MQCA_')

    def __init__(self, name=None,
                 disconnect_on_exit=True,
                 model_queue_name=b'SYSTEM.DEFAULT.MODEL.QUEUE',
                 dynamic_queue_name=b'PYMQPCF.*',
                 command_queue_name=b'SYSTEM.ADMIN.COMMAND.QUEUE',
                 response_wait_interval=5000,
                 convert=False):
        # type: (Any, bool, bytes, bytes, bytes, int, bool) -> None
        """PCFExecute(name = '')

        Connect to the Queue Manager 'name' (default value '') ready
        for a PCF command. If name is a QueueManager instance, it is
        used for the connection, otherwise a new connection is made """

        self.__command_queue_name = command_queue_name
        self.__convert = convert
        self.__response_wait_interval = response_wait_interval


        if isinstance(name, QueueManager):
            self.qm = name
            super(PCFExecute, self).__init__(None)
        else:
            self.qm = None
            super(PCFExecute, self).__init__(name)

        od = OD(ObjectName=model_queue_name,
                DynamicQName=dynamic_queue_name)

        self.__reply_queue = Queue(self.qm, od, CMQC.MQOO_INPUT_EXCLUSIVE)
        self.__reply_queue_name = od.ObjectName.strip()

    @property
    def command_queue_name(self):
        return self.__command_queue_name

    @property
    def convert(self):
        return self.__convert

    @property
    def reply_queue(self):
        return self.__reply_queue

    @property
    def reply_queue_name(self):
        return self.__reply_queue_name

    @property
    def response_wait_interval(self):
        return self.__response_wait_interval

    def __getattr__(self, name):
        """MQCMD_*(attrDict)

        Execute the PCF command or inquiry, passing an an optional
        dictionary of MQ attributes.  The command must begin with
        MQCMD_, and must be one of those defined in the CMQC
        module. If attrDict is passed, its keys must be an attribute
        as defined in the CMQC or CMQC modules (MQCA_*, MQIA_*,
        MQCACH_* or MQIACH_*). The key value must be an int or string,
        as appropriate.

        If an inquiry is executed, a list of dictionaries (one per
        matching query) is returned. Each dictionary encodes the
        attributes and values of the object queried. The keys are as
        defined in the CMQC module (MQIA_*, MQCA_*), The values are
        strings or ints, as appropriate.

        If a command was executed, or no inquiry results are
        available, an empty listis returned.  """

        return _Method(self, name)

    @staticmethod
    def stringify_keys(rawDict):
        """stringifyKeys(rawDict)

        Return a copy of rawDict with its keys converted to string
        mnemonics, as defined in CMQC. """

        rv = {}
        for k in rawDict.keys():
            if isinstance(rawDict[k], bytes):
                d = PCFExecute.caStringDict
            elif isinstance(rawDict[k], str):
                raise TypeError('In Python 3 use bytes, not str (found "{0}":"{1}")'.format(k, rawDict[k]))
            else:
                d = PCFExecute.iaStringDict
            try:
                rv[d[k]] = rawDict[k]
            except KeyError:
                rv[k] = rawDict[k]
        return rv

    # Backward compatibility
    stringifyKeys = stringify_keys

    def disconnect(self):
        """ Disconnect from reply_queue
        """
        try:
            if self.__reply_queue and self.__reply_queue.get_handle():
                self.__reply_queue.close()
        except MQMIError:
            pass
        finally:
            self.__reply_queue = None
            self.__reply_queue_name = None

    @staticmethod
    def unpack(message): # type: (bytes) -> dict
        """Unpack PCF message to dictionary
        """

        mqcfh = CFH(Version=CMQCFC.MQCFH_VERSION_1)
        mqcfh.unpack(message[:CMQCFC.MQCFH_STRUC_LENGTH])

        if mqcfh.Version != CMQCFC.MQCFH_VERSION_1:
            mqcfh = CFH(Version=mqcfh.Version)
            mqcfh.unpack(message[:CMQCFC.MQCFH_STRUC_LENGTH])

        if mqcfh.CompCode:
            raise MQMIError(mqcfh.CompCode, mqcfh.Reason)

        res = {}  # type: Dict[str, Union[int, str, bool, Dict]]
        index = mqcfh.ParameterCount
        cursor = CMQCFC.MQCFH_STRUC_LENGTH
        parameter = None  # type: Optional[MQOpts]
        group = None  # type: Union[None, Dict[str, Union[str, int, bool]]]
        group_count = 0

        while (index > 0):
            parameter_type = struct.unpack(MQLONG_TYPE, message[cursor:cursor + 4])[0]
            if group_count == 0:
                group = None
            if group is not None:
                group_count -= 1
            if parameter_type == CMQCFC.MQCFT_STRING:
                parameter = CFST()
                parameter.unpack(message[cursor:cursor + CMQCFC.MQCFST_STRUC_LENGTH_FIXED])
                if parameter.StringLength > 1:
                    parameter = CFST(StringLength=parameter.StringLength)
                    parameter.unpack(message[cursor:cursor + parameter.StrucLength])
                value = parameter.String
            elif parameter_type == CMQCFC.MQCFT_STRING_LIST:
                parameter = CFSL()
                parameter.unpack(message[cursor:cursor + CMQCFC.MQCFSL_STRUC_LENGTH_FIXED])
                if parameter.StringLength > 1:
                    parameter = CFSL(StringLength=parameter.StringLength,
                                     Count=parameter.Count,
                                     StrucLength=parameter.StrucLength)
                    parameter.unpack(message[cursor:cursor + parameter.StrucLength])
                value = parameter.Strings
            elif parameter_type == CMQCFC.MQCFT_INTEGER:
                parameter = CFIN()
                parameter.unpack(message[cursor:cursor + CMQCFC.MQCFIN_STRUC_LENGTH])
                value = parameter.Value
            elif parameter_type == CMQCFC.MQCFT_INTEGER64:
                parameter = CFIN64()
                parameter.unpack(message[cursor:cursor + CMQCFC.MQCFIN64_STRUC_LENGTH])
                value = parameter.Value
            elif parameter_type == CMQCFC.MQCFT_INTEGER_LIST:
                parameter = CFIL()
                parameter.unpack(message[cursor:cursor + CMQCFC.MQCFIL_STRUC_LENGTH_FIXED])
                if parameter.Count > 0:
                    parameter = CFIL(Count=parameter.Count,
                                     StrucLength=parameter.StrucLength)
                    parameter.unpack(message[cursor:cursor + parameter.StrucLength])
                value = parameter.Values
            elif parameter_type == CMQCFC.MQCFT_INTEGER64_LIST:
                parameter = CFIL64()
                parameter.unpack(message[cursor:cursor + CMQCFC.MQCFIL64_STRUC_LENGTH_FIXED])
                if parameter.Count > 0:
                    parameter = CFIL64(Count=parameter.Count,
                                       StrucLength=parameter.StrucLength)
                    parameter.unpack(message[cursor:cursor + parameter.StrucLength])
                value = parameter.Values
            elif parameter_type == CMQCFC.MQCFT_GROUP:
                parameter = CFGR()
                parameter.unpack(message[cursor:cursor + parameter.StrucLength])
                group_count = parameter.ParameterCount
                index += group_count
                group = {}
                res[parameter.Parameter] = res.get(parameter.Parameter, [])
                res[parameter.Parameter].append(group)
            elif parameter_type == CMQCFC.MQCFT_BYTE_STRING:
                parameter = CFBS()
                parameter.unpack(message[cursor:cursor + CMQCFC.MQCFBS_STRUC_LENGTH_FIXED])
                if parameter.StringLength > 1:
                    parameter = CFBS(StringLength=parameter.StringLength)
                    parameter.unpack(message[cursor:cursor + parameter.StrucLength])
                value = parameter.String
            elif parameter_type == CMQCFC.MQCFT_STRING_FILTER:
                parameter = CFSF()
                parameter.unpack(message[cursor:cursor + CMQCFC.MQCFSF_STRUC_LENGTH_FIXED])
                if parameter.FilterValueLength > 0:
                    parameter = CFSF(FilterValueLength=parameter.FilterValueLength)
                    parameter.unpack(message[cursor:cursor + parameter.StrucLength])
                value = (parameter.Operator, parameter.FilterValue)
            elif parameter_type == CMQCFC.MQCFT_BYTE_STRING_FILTER:
                parameter = CFBF()
                parameter.unpack(message[cursor:cursor + CMQCFC.MQCFBF_STRUC_LENGTH_FIXED])
                if parameter.FilterValueLength > 0:
                    parameter = CFBF(FilterValueLength=parameter.FilterValueLength)
                    parameter.unpack(message[cursor:cursor + parameter.StrucLength])
                value = (parameter.Operator, parameter.FilterValue)
            elif parameter_type == CMQCFC.MQCFT_INTEGER_FILTER:
                parameter = CFIF()
                parameter.unpack(message[cursor:cursor + CMQCFC.MQCFIF_STRUC_LENGTH])
                value = (parameter.Operator, parameter.FilterValue)
            else:
                pcf_type = struct.unpack(MQLONG_TYPE, message[cursor:cursor + 4])
                raise NotImplementedError('Unpack for type ({}) not implemented'.format(pcf_type))
            index -= 1
            cursor += parameter.StrucLength
            if parameter.Type == CMQCFC.MQCFT_GROUP:
                continue
            if group is not None:
                group[parameter.Parameter] = value
            else:
                res[parameter.Parameter] = value

        return res, mqcfh

# ################################################################################################################################
# ################################################################################################################################

class ByteString(object):
    """ A simple wrapper around string values, suitable for passing into PyMQI
    calls wherever IBM's docs state a 'byte string' object should be passed in.
    """
    def __init__(self, value): # type: (bytes) -> None
        self.value = value
        self.pymqi_byte_string = True

    def __len__(self): # type: () -> int
        return len(self.value)

# ################################################################################################################################
# ################################################################################################################################

def connect(queue_manager, channel=None, conn_info=None, user=None, password=None, disconnect_on_exit=True,
            bytes_encoding=default.bytes_encoding, default_ccsid=default.ccsid):
    """ A convenience wrapper for connecting to MQ queue managers. If given the
    'queue_manager' parameter only, will try connecting to it in bindings mode.
    If given both 'channel' and 'conn_info' will connect in client mode.
    A pymqi.QueueManager is returned on successfully establishing a connection.
    """
    if channel and conn_info:
        qmgr = QueueManager(None, disconnect_on_exit, bytes_encoding=bytes_encoding, default_ccsid=default.ccsid)
        qmgr.connect_tcp_client(queue_manager or '', CD(), channel, conn_info, user, password)
        return qmgr

    elif queue_manager:
        qmgr = QueueManager(queue_manager, disconnect_on_exit,
                            bytes_encoding=bytes_encoding, default_ccsid=default.ccsid)
        return qmgr

    else:
        raise TypeError('Invalid arguments: %r' % [queue_manager, channel, conn_info, user, password])

# ################################################################################################################################
# ################################################################################################################################
