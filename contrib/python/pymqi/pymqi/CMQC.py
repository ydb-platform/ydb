from struct import calcsize


def py23long(x):
    # this will convert py2's int to py2's long without need to suffix it with L (which doesn't work in py3).
    # (note the recommended way is to use "from builtins import int",
    #     as per http://python-future.org/compatible_idioms.html#long-integers),
    # however "builtins" is in the "futures" package which should not be dependency of pymqi.
    return x + 0 * 0xffffffffffffffff

# Generated from 9.2.5 CD Header File


# 64bit
if calcsize("P") == 8:
    MQAIR_LENGTH_1 = 328
    MQAIR_LENGTH_2 = 584
    MQCBC_LENGTH_1 = 56
    MQCBC_LENGTH_2 = 64
    MQCBD_LENGTH_1 = 168
    MQCTLO_LENGTH_1 = 24
    MQSCO_LENGTH_1 = 536
    MQSCO_LENGTH_2 = 544
    MQSCO_LENGTH_3 = 560
    MQSCO_LENGTH_4 = 568
    MQSCO_LENGTH_5 = 632
    MQCSP_LENGTH_1 = 56
    MQCNO_LENGTH_1 = 12
    MQCNO_LENGTH_2 = 24
    MQCNO_LENGTH_3 = 152
    MQCNO_LENGTH_4 = 168
    MQCNO_LENGTH_5 = 200
    MQCNO_LENGTH_6 = 224
    MQCNO_LENGTH_7 = 256
    MQCNO_LENGTH_8 = 272
    MQIMPO_LENGTH_1 = 64
    MQOD_LENGTH_1 = 168
    MQOD_LENGTH_2 = 208
    MQOD_LENGTH_3 = 344
    MQOD_LENGTH_4 = 424
    MQPMO_LENGTH_1 = 128
    MQPMO_LENGTH_2 = 160
    MQPMO_LENGTH_3 = 184
    MQSD_LENGTH_1 = 344
    MQSTS_LENGTH_1 = 224
    MQSTS_LENGTH_2 = 280
# 32bit
else:
    MQAIR_LENGTH_1 = 320
    MQAIR_LENGTH_2 = 576
    MQCBC_LENGTH_1 = 48
    MQCBC_LENGTH_2 = 52
    MQCBD_LENGTH_1 = 156
    MQCTLO_LENGTH_1 = 20
    MQSCO_LENGTH_1 = 532
    MQSCO_LENGTH_2 = 540
    MQSCO_LENGTH_3 = 556
    MQSCO_LENGTH_4 = 560
    MQSCO_LENGTH_5 = 624
    MQCSP_LENGTH_1 = 48
    MQCNO_LENGTH_1 = 12
    MQCNO_LENGTH_2 = 20
    MQCNO_LENGTH_3 = 148
    MQCNO_LENGTH_4 = 156
    MQCNO_LENGTH_5 = 188
    MQCNO_LENGTH_6 = 208
    MQCNO_LENGTH_7 = 240
    MQCNO_LENGTH_8 = 252
    MQIMPO_LENGTH_1 = 60
    MQOD_LENGTH_1 = 168
    MQOD_LENGTH_2 = 200
    MQOD_LENGTH_3 = 336
    MQOD_LENGTH_4 = 400
    MQPMO_LENGTH_1 = 128
    MQPMO_LENGTH_2 = 152
    MQPMO_LENGTH_3 = 176
    MQSD_LENGTH_1 = 312
    MQSTS_LENGTH_1 = 224
    MQSTS_LENGTH_2 = 272





##################################################################
# Values related to MQAIR Structure                              #
##################################################################

# Structure Identifier
MQAIR_STRUC_ID = b"AIR "

# Structure Identifier (array form)
MQAIR_STRUC_ID_ARRAY = [b"A", b"I", b"R", b" "]

# Structure Version Number
MQAIR_VERSION_1 = 1
MQAIR_VERSION_2 = 2
MQAIR_CURRENT_VERSION = 2

# Structure Length - Moved to if statement at top
# due to 32/64 bit differences
MQAIR_CURRENT_LENGTH = MQAIR_LENGTH_2

# Authentication Information Type
MQAIT_ALL = 0
MQAIT_CRL_LDAP = 1
MQAIT_OCSP = 2
MQAIT_IDPW_OS = 3
MQAIT_IDPW_LDAP = 4


##################################################################
# Values related to MQBNO Structure                              #
##################################################################

# Structure Identifier
MQBNO_STRUC_ID = b"BNO "

# Structure Identifier (array form)
MQBNO_STRUC_ID_ARRAY = [b"B", b"N", b"O", b" "]

# Structure Version Number
MQBNO_VERSION_1 = 1
MQBNO_CURRENT_VERSION = 1

# Structure Length
MQBNO_LENGTH_1 = 20
MQBNO_CURRENT_LENGTH = 20

# MQ Balancing Options
MQBNO_OPTIONS_NONE = 0x00000000
MQBNO_OPTIONS_IGNORE_TRANS = 0x00000001

# MQ Balancing Application Type
MQBNO_BALTYPE_SIMPLE = 0x00000000
MQBNO_BALTYPE_REQREP = 0x00000001
MQBNO_BALTYPE_RA_MANAGED = 0x00010000

# MQ Balancing Timeout
MQBNO_TIMEOUT_AS_DEFAULT = (-1)
MQBNO_TIMEOUT_IMMEDIATE = 0
MQBNO_TIMEOUT_NEVER = (-2)


##################################################################
# Values related to MQBMHO Structure                             #
##################################################################

# Structure Identifier
MQBMHO_STRUC_ID = b"BMHO"

# Structure Identifier (array form)
MQBMHO_STRUC_ID_ARRAY = [b"B", b"M", b"H", b"O"]

# Structure Version Number
MQBMHO_VERSION_1 = 1
MQBMHO_CURRENT_VERSION = 1

# Structure Length
MQBMHO_LENGTH_1 = 12
MQBMHO_CURRENT_LENGTH = 12

# Buffer to Message Handle Options
MQBMHO_NONE = 0x00000000
MQBMHO_DELETE_PROPERTIES = 0x00000001


##################################################################
# Values related to MQBO Structure                               #
##################################################################

# Structure Identifier
MQBO_STRUC_ID = b"BO  "

# Structure Identifier (array form)
MQBO_STRUC_ID_ARRAY = [b"B", b"O", b" ", b" "]

# Structure Version Number
MQBO_VERSION_1 = 1
MQBO_CURRENT_VERSION = 1

# Structure Length
MQBO_LENGTH_1 = 12
MQBO_CURRENT_LENGTH = 12

# Begin Options
MQBO_NONE = 0x00000000


##################################################################
# Values Related to MQCBC Structure - Callback Context           #
##################################################################

# Structure Identifier
MQCBC_STRUC_ID = b"CBC "

# Structure Identifier (array form)
MQCBC_STRUC_ID_ARRAY = [b"C", b"B", b"C", b" "]

# Structure Version Number
MQCBC_VERSION_1 = 1
MQCBC_VERSION_2 = 2
MQCBC_CURRENT_VERSION = 2

# Structure Length - Moved to if statement at top
# due to 32/64 bit differences
MQCBC_CURRENT_LENGTH = MQCBC_LENGTH_2

# Flags
MQCBCF_NONE = 0x00000000
MQCBCF_READA_BUFFER_EMPTY = 0x00000001

# Callback Type
MQCBCT_START_CALL = 1
MQCBCT_STOP_CALL = 2
MQCBCT_REGISTER_CALL = 3
MQCBCT_DEREGISTER_CALL = 4
MQCBCT_EVENT_CALL = 5
MQCBCT_MSG_REMOVED = 6
MQCBCT_MSG_NOT_REMOVED = 7
MQCBCT_MC_EVENT_CALL = 8

# Consumer State
MQCS_NONE = 0
MQCS_SUSPENDED_TEMPORARY = 1
MQCS_SUSPENDED_USER_ACTION = 2
MQCS_SUSPENDED = 3
MQCS_STOPPED = 4

# Reconnect Delay
MQRD_NO_RECONNECT = (-1)
MQRD_NO_DELAY = 0


##################################################################
# Values Related to MQCBD Structure - Callback Descriptor        #
##################################################################

# Structure Identifier
MQCBD_STRUC_ID = b"CBD "

# Structure Identifier (array form)
MQCBD_STRUC_ID_ARRAY = [b"C", b"B", b"D", b" "]

# Structure Version Number
MQCBD_VERSION_1 = 1
MQCBD_CURRENT_VERSION = 1

# Structure Length - Moved to if statement at top
# due to 32/64 bit differences
MQCBD_CURRENT_LENGTH = MQCBD_LENGTH_1

# Callback Options
MQCBDO_NONE = 0x00000000
MQCBDO_START_CALL = 0x00000001
MQCBDO_STOP_CALL = 0x00000004
MQCBDO_REGISTER_CALL = 0x00000100
MQCBDO_DEREGISTER_CALL = 0x00000200
MQCBDO_FAIL_IF_QUIESCING = 0x00002000
MQCBDO_EVENT_CALL = 0x00004000
MQCBDO_MC_EVENT_CALL = 0x00008000

# This is the type of the Callback Function
MQCBT_MESSAGE_CONSUMER = 0x00000001
MQCBT_EVENT_HANDLER = 0x00000002

# Buffer size values
MQCBD_FULL_MSG_LENGTH = (-1)


##################################################################
# Values Related to MQCHARV Structure                            #
##################################################################

# Variable String Length
MQVS_NULL_TERMINATED = (-1)


##################################################################
# Values Related to MQCIH Structure                              #
##################################################################

# Structure Identifier
MQCIH_STRUC_ID = b"CIH "

# Structure Identifier (array form)
MQCIH_STRUC_ID_ARRAY = [b"C", b"I", b"H", b" "]

# Structure Version Number
MQCIH_VERSION_1 = 1
MQCIH_VERSION_2 = 2
MQCIH_CURRENT_VERSION = 2

# Structure Length
MQCIH_LENGTH_1 = 164
MQCIH_LENGTH_2 = 180
MQCIH_CURRENT_LENGTH = 180

# Flags
MQCIH_NONE = 0x00000000
MQCIH_PASS_EXPIRATION = 0x00000001
MQCIH_UNLIMITED_EXPIRATION = 0x00000000
MQCIH_REPLY_WITHOUT_NULLS = 0x00000002
MQCIH_REPLY_WITH_NULLS = 0x00000000
MQCIH_SYNC_ON_RETURN = 0x00000004
MQCIH_NO_SYNC_ON_RETURN = 0x00000000

# Return Codes
MQCRC_OK = 0
MQCRC_CICS_EXEC_ERROR = 1
MQCRC_MQ_API_ERROR = 2
MQCRC_BRIDGE_ERROR = 3
MQCRC_BRIDGE_ABEND = 4
MQCRC_APPLICATION_ABEND = 5
MQCRC_SECURITY_ERROR = 6
MQCRC_PROGRAM_NOT_AVAILABLE = 7
MQCRC_BRIDGE_TIMEOUT = 8
MQCRC_TRANSID_NOT_AVAILABLE = 9

# Unit-of-Work Controls
MQCUOWC_ONLY = 0x00000111
MQCUOWC_CONTINUE = 0x00010000
MQCUOWC_FIRST = 0x00000011
MQCUOWC_MIDDLE = 0x00000010
MQCUOWC_LAST = 0x00000110
MQCUOWC_COMMIT = 0x00000100
MQCUOWC_BACKOUT = 0x00001100

# Get Wait Interval
MQCGWI_DEFAULT = (-2)

# Link Types
MQCLT_PROGRAM = 1
MQCLT_TRANSACTION = 2

# Output Data Length
MQCODL_AS_INPUT = (-1)

# ADS Descriptors
MQCADSD_NONE = 0x00000000
MQCADSD_SEND = 0x00000001
MQCADSD_RECV = 0x00000010
MQCADSD_MSGFORMAT = 0x00000100

# Conversational Task Options
MQCCT_YES = 0x00000001
MQCCT_NO = 0x00000000

# Task End Status
MQCTES_NOSYNC = 0x00000000
MQCTES_COMMIT = 0x00000100
MQCTES_BACKOUT = 0x00001100
MQCTES_ENDTASK = 0x00010000

# Facility
MQCFAC_NONE = b"\0\0\0\0\0\0\0\0"

# Facility (array form)
MQCFAC_NONE_ARRAY = [b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0"]

# Functions
MQCFUNC_MQCONN = b"CONN"
MQCFUNC_MQGET = b"GET "
MQCFUNC_MQINQ = b"INQ "
MQCFUNC_MQOPEN = b"OPEN"
MQCFUNC_MQPUT = b"PUT "
MQCFUNC_MQPUT1 = b"PUT1"
MQCFUNC_NONE = b"    "

# Functions (array form)
MQCFUNC_MQCONN_ARRAY = [b"C", b"O", b"N", b"N"]
MQCFUNC_MQGET_ARRAY = [b"G", b"E", b"T", b" "]
MQCFUNC_MQINQ_ARRAY = [b"I", b"N", b"Q", b" "]
MQCFUNC_MQOPEN_ARRAY = [b"O", b"P", b"E", b"N"]
MQCFUNC_MQPUT_ARRAY = [b"P", b"U", b"T", b" "]
MQCFUNC_MQPUT1_ARRAY = [b"P", b"U", b"T", b"1"]
MQCFUNC_NONE_ARRAY = [b" ", b" ", b" ", b" "]

# Start Codes
MQCSC_START = b"S   "
MQCSC_STARTDATA = b"SD  "
MQCSC_TERMINPUT = b"TD  "
MQCSC_NONE = b"    "

# Start Codes (array form)
MQCSC_START_ARRAY = [b"S", b" ", b" ", b" "]
MQCSC_STARTDATA_ARRAY = [b"S", b"D", b" ", b" "]
MQCSC_TERMINPUT_ARRAY = [b"T", b"D", b" ", b" "]
MQCSC_NONE_ARRAY = [b" ", b" ", b" ", b" "]


##################################################################
# Values Related to MQCMHO Structure                             #
##################################################################

# Structure Identifier
MQCMHO_STRUC_ID = b"CMHO"

# Structure Identifier (array form)
MQCMHO_STRUC_ID_ARRAY = [b"C", b"M", b"H", b"O"]

# Structure Version Number
MQCMHO_VERSION_1 = 1
MQCMHO_CURRENT_VERSION = 1

# Structure Length
MQCMHO_LENGTH_1 = 12
MQCMHO_CURRENT_LENGTH = 12

# Create Message Handle Options
MQCMHO_DEFAULT_VALIDATION = 0x00000000
MQCMHO_NO_VALIDATION = 0x00000001
MQCMHO_VALIDATE = 0x00000002
MQCMHO_NONE = 0x00000000


##################################################################
# Values Related to MQCTLO Structure                             #
##################################################################

# Structure Identifier
MQCTLO_STRUC_ID = b"CTLO"

# Structure Version Number
MQCTLO_VERSION_1 = 1
MQCTLO_CURRENT_VERSION = 1

# Structure Length - Moved to if statement at top
# due to 32/64 bit differences
MQCTLO_CURRENT_LENGTH = MQCTLO_LENGTH_1

# Consumer Control Options
MQCTLO_NONE = 0x00000000
MQCTLO_THREAD_AFFINITY = 0x00000001
MQCTLO_FAIL_IF_QUIESCING = 0x00002000


##################################################################
# Values Related to MQSCO Structure                              #
##################################################################

# Structure Identifier
MQSCO_STRUC_ID = b"SCO "

# Structure Identifier (array form)
MQSCO_STRUC_ID_ARRAY = [b"S", b"C", b"O", b" "]

# Structure Version Number
MQSCO_VERSION_1 = 1
MQSCO_VERSION_2 = 2
MQSCO_VERSION_3 = 3
MQSCO_VERSION_4 = 4
MQSCO_VERSION_5 = 5
MQSCO_CURRENT_VERSION = 5

# Structure Length - Moved to if statement at top
# due to 32/64 bit differences
MQSCO_CURRENT_LENGTH = MQSCO_LENGTH_5

# SuiteB Type
MQ_SUITE_B_NOT_AVAILABLE = 0
MQ_SUITE_B_NONE = 1
MQ_SUITE_B_128_BIT = 2
MQ_SUITE_B_192_BIT = 4

# Key Reset Count
MQSCO_RESET_COUNT_DEFAULT = 0

# Certificate Validation Policy Type
MQ_CERT_VAL_POLICY_DEFAULT = 0
MQ_CERT_VAL_POLICY_ANY = 0
MQ_CERT_VAL_POLICY_RFC5280 = 1


##################################################################
# Values Related to MQCSP Structure                              #
##################################################################

# Structure Identifier
MQCSP_STRUC_ID = b"CSP "

# Structure Identifier (array form)
MQCSP_STRUC_ID_ARRAY = [b"C", b"S", b"P", b" "]

# Structure Length - Moved to if statement at top
# due to 32/64 bit differences
MQCSP_CURRENT_LENGTH = MQCSP_LENGTH_1

# Authentication Types
MQCSP_AUTH_NONE = 0
MQCSP_AUTH_USER_ID_AND_PWD = 1

##################################################################
# Values Related to MQCNO Structure                              #
##################################################################

# Structure Identifier
MQCNO_STRUC_ID = b"CNO "

# Structure Identifier (array form)
MQCNO_STRUC_ID_ARRAY = [b"C", b"N", b"O", b" "]

# Structure Version Number
MQCNO_VERSION_1 = 1
MQCNO_VERSION_2 = 2
MQCNO_VERSION_3 = 3
MQCNO_VERSION_4 = 4
MQCNO_VERSION_5 = 5
MQCNO_VERSION_6 = 6
MQCNO_VERSION_7 = 7
MQCNO_VERSION_8 = 8
MQCNO_CURRENT_VERSION = 8

# Structure Length - Moved to if statement at top
# due to 32/64 bit differences
MQCNO_CURRENT_LENGTH = MQCNO_LENGTH_8

# Connect Options
MQCNO_STANDARD_BINDING = 0x00000000
MQCNO_FASTPATH_BINDING = 0x00000001
MQCNO_SERIALIZE_CONN_TAG_Q_MGR = 0x00000002
MQCNO_SERIALIZE_CONN_TAG_QSG = 0x00000004
MQCNO_RESTRICT_CONN_TAG_Q_MGR = 0x00000008
MQCNO_RESTRICT_CONN_TAG_QSG = 0x00000010
MQCNO_HANDLE_SHARE_NONE = 0x00000020
MQCNO_HANDLE_SHARE_BLOCK = 0x00000040
MQCNO_HANDLE_SHARE_NO_BLOCK = 0x00000080
MQCNO_SHARED_BINDING = 0x00000100
MQCNO_ISOLATED_BINDING = 0x00000200
MQCNO_LOCAL_BINDING = 0x00000400
MQCNO_CLIENT_BINDING = 0x00000800
MQCNO_ACCOUNTING_MQI_ENABLED = 0x00001000
MQCNO_ACCOUNTING_MQI_DISABLED = 0x00002000
MQCNO_ACCOUNTING_Q_ENABLED = 0x00004000
MQCNO_ACCOUNTING_Q_DISABLED = 0x00008000
MQCNO_NO_CONV_SHARING = 0x00010000
MQCNO_ALL_CONVS_SHARE = 0x00040000
MQCNO_CD_FOR_OUTPUT_ONLY = 0x00080000
MQCNO_USE_CD_SELECTION = 0x00100000
MQCNO_GENERATE_CONN_TAG = 0x00200000
MQCNO_RECONNECT_AS_DEF = 0x00000000
MQCNO_RECONNECT = 0x01000000
MQCNO_RECONNECT_DISABLED = 0x02000000
MQCNO_RECONNECT_Q_MGR = 0x04000000
MQCNO_ACTIVITY_TRACE_ENABLED = 0x08000000
MQCNO_ACTIVITY_TRACE_DISABLED = 0x10000000
MQCNO_NONE = 0x00000000

# Queue Manager Connection Tag
MQCT_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"\
            b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"\
            b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"\
            b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"\
            b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"\
            b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"\
            b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"\
            b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
            
# Queue Manager Connection Tag (array form)
MQCT_NONE_ARRAY = [b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0"]

# Connection Identifier
MQCONNID_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0"\
                b"\0\0\0\0\0\0\0\0\0\0\0\0"
                
# Connection Identifier (array form)
MQCONNID_NONE_ARRAY = [b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                       b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                       b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0"]

# Application Name
MQAN_NONE = b"                            "

# Application Name (array form)
MQAN_NONE_ARRAY = [b" ", b" ", b" ", b" ", b" ", b" ", b" ", b" ",
                   b" ", b" ", b" ", b" ", b" ", b" ", b" ", b" ",
                   b" ", b" ", b" ", b" ", b" ", b" ", b" ", b" ",
                   b" ", b" ", b" ", b" "]


##################################################################
# Values Related to MQDH Structure                               #
##################################################################

# Structure Identifier
MQDH_STRUC_ID = b"DH  "

# Structure Identifier (array form)
MQDH_STRUC_ID_ARRAY = [b"D", b"H", b" ", b" "]

# Structure Version Number
MQDH_VERSION_1 = 1
MQDH_CURRENT_VERSION = 1

# Structure Length
MQDH_LENGTH_1 = 48
MQDH_CURRENT_LENGTH = 48

# Flags
MQDHF_NEW_MSG_IDS = 0x00000001
MQDHF_NONE = 0x00000000


##################################################################
# Values Related to MQDLH Structure                              #
##################################################################

# Structure Identifier
MQDLH_STRUC_ID = b"DLH "

# Structure Identifier (array form)
MQDLH_STRUC_ID_ARRAY = [b"D", b"L", b"H", b" "]

# Structure Version Number
MQDLH_VERSION_1 = 1
MQDLH_CURRENT_VERSION = 1

# Structure Length
MQDLH_LENGTH_1 = 172
MQDLH_CURRENT_LENGTH = 172


##################################################################
# Values Related to MQDMHO Structure                             #
##################################################################

# Structure Identifier
MQDMHO_STRUC_ID = b"DMHO"

# Structure Identifier (array form)
MQDMHO_STRUC_ID_ARRAY = [b"D", b"M", b"H", b"O"]

# Structure Version Number
MQDMHO_VERSION_1 = 1
MQDMHO_CURRENT_VERSION = 1

# Structure Length
MQDMHO_LENGTH_1 = 12
MQDMHO_CURRENT_LENGTH = 12

# Delete Message Handle Options
MQDMHO_NONE = 0x00000000


##################################################################
# Values Related to MQDMPO Structure                             #
##################################################################

# Structure Identifier
MQDMPO_STRUC_ID = b"DMPO"

# Structure Identifier (array form)
MQDMPO_STRUC_ID_ARRAY = [b"D", b"M", b"P", b"O"]

# Structure Version Number
MQDMPO_VERSION_1 = 1
MQDMPO_CURRENT_VERSION = 1

# Structure Length
MQDMPO_LENGTH_1 = 12
MQDMPO_CURRENT_LENGTH = 12

# Delete Message Property Options
MQDMPO_DEL_FIRST = 0x00000000
MQDMPO_DEL_PROP_UNDER_CURSOR = 0x00000001
MQDMPO_NONE = 0x00000000


##################################################################
# Values Related to MQGMO Structure                              #
##################################################################

# Structure Identifier
MQGMO_STRUC_ID = b"GMO "

# Structure Identifier (array form)
MQGMO_STRUC_ID_ARRAY = [b"G", b"M", b"O", b" "]

# Structure Version Number
MQGMO_VERSION_1 = 1
MQGMO_VERSION_2 = 2
MQGMO_VERSION_3 = 3
MQGMO_VERSION_4 = 4
MQGMO_CURRENT_VERSION = 4

# Structure Length
MQGMO_LENGTH_1 = 72
MQGMO_LENGTH_2 = 80
MQGMO_LENGTH_3 = 100
MQGMO_LENGTH_4 = 112
MQGMO_CURRENT_LENGTH = 112

# Get Message Options
MQGMO_WAIT = 0x00000001
MQGMO_NO_WAIT = 0x00000000
MQGMO_SET_SIGNAL = 0x00000008
MQGMO_FAIL_IF_QUIESCING = 0x00002000
MQGMO_SYNCPOINT = 0x00000002
MQGMO_SYNCPOINT_IF_PERSISTENT = 0x00001000
MQGMO_NO_SYNCPOINT = 0x00000004
MQGMO_MARK_SKIP_BACKOUT = 0x00000080
MQGMO_BROWSE_FIRST = 0x00000010
MQGMO_BROWSE_NEXT = 0x00000020
MQGMO_BROWSE_MSG_UNDER_CURSOR = 0x00000800
MQGMO_MSG_UNDER_CURSOR = 0x00000100
MQGMO_LOCK = 0x00000200
MQGMO_UNLOCK = 0x00000400
MQGMO_ACCEPT_TRUNCATED_MSG = 0x00000040
MQGMO_CONVERT = 0x00004000
MQGMO_LOGICAL_ORDER = 0x00008000
MQGMO_COMPLETE_MSG = 0x00010000
MQGMO_ALL_MSGS_AVAILABLE = 0x00020000
MQGMO_ALL_SEGMENTS_AVAILABLE = 0x00040000
MQGMO_MARK_BROWSE_HANDLE = 0x00100000
MQGMO_MARK_BROWSE_CO_OP = 0x00200000
MQGMO_UNMARK_BROWSE_CO_OP = 0x00400000
MQGMO_UNMARK_BROWSE_HANDLE = 0x00800000
MQGMO_UNMARKED_BROWSE_MSG = 0x01000000
MQGMO_PROPERTIES_FORCE_MQRFH2 = 0x02000000
MQGMO_NO_PROPERTIES = 0x04000000
MQGMO_PROPERTIES_IN_HANDLE = 0x08000000
MQGMO_PROPERTIES_COMPATIBILITY = 0x10000000
MQGMO_PROPERTIES_AS_Q_DEF = 0x00000000
MQGMO_NONE = 0x00000000
MQGMO_BROWSE_HANDLE = MQGMO_BROWSE_FIRST | MQGMO_UNMARKED_BROWSE_MSG | MQGMO_MARK_BROWSE_HANDLE
MQGMO_BROWSE_CO_OP = MQGMO_BROWSE_FIRST | MQGMO_UNMARKED_BROWSE_MSG | MQGMO_MARK_BROWSE_CO_OP

# Wait Interval
MQWI_UNLIMITED = (-1)

# Signal Values
MQEC_MSG_ARRIVED = 2
MQEC_WAIT_INTERVAL_EXPIRED = 3
MQEC_WAIT_CANCELED = 4
MQEC_Q_MGR_QUIESCING = 5
MQEC_CONNECTION_QUIESCING = 6

# Match Options
MQMO_MATCH_MSG_ID = 0x00000001
MQMO_MATCH_CORREL_ID = 0x00000002
MQMO_MATCH_GROUP_ID = 0x00000004
MQMO_MATCH_MSG_SEQ_NUMBER = 0x00000008
MQMO_MATCH_OFFSET = 0x00000010
MQMO_MATCH_MSG_TOKEN = 0x00000020
MQMO_NONE = 0x00000000

# Group Status
MQGS_NOT_IN_GROUP = ord(" ")
MQGS_MSG_IN_GROUP = ord("G")
MQGS_LAST_MSG_IN_GROUP = ord("L")

# Segment Status
MQSS_NOT_A_SEGMENT = ord(" ")
MQSS_SEGMENT = ord("S")
MQSS_LAST_SEGMENT = ord("L")

# Segmentation
MQSEG_INHIBITED = ord(" ")
MQSEG_ALLOWED = ord("A")

# Message Token
MQMTOK_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"

# Message Token (array form)
MQMTOK_NONE_ARRAY = [b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                     b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0"]
                     
# Returned Length
MQRL_UNDEFINED = (-1)


##################################################################
# Values Related to MQIIH Structure                              #
##################################################################

# Structure Identifier
MQIIH_STRUC_ID = b"IIH "

# Structure Identifier (array form)
MQIIH_STRUC_ID_ARRAY = [b"I", b"I", b"H", b" "]

# Structure Version Number
MQIIH_VERSION_1 = 1
MQIIH_CURRENT_VERSION = 1

# Flags
MQIIH_NONE = 0x00000000
MQIIH_PASS_EXPIRATION = 0x00000001
MQIIH_UNLIMITED_EXPIRATION = 0x00000000
MQIIH_REPLY_FORMAT_NONE = 0x00000008
MQIIH_IGNORE_PURG = 0x00000010
MQIIH_CM0_REQUEST_RESPONSE = 0x00000020

# Authenticator
MQIAUT_NONE = b"        "

# Authenticator (array form)
MQIAUT_NONE_ARRAY = [b" ", b" ", b" ", b" ", b" ", b" ", b" ", b" "]

# Transaction Instance Identifier
MQITII_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"

# Transaction Instance Identifier (array form)
MQITII_NONE_ARRAY = [b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                     b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0"]

# Transaction States
MQITS_IN_CONVERSATION = b"C"
MQITS_NOT_IN_CONVERSATION = b" "
MQITS_ARCHITECTED = b"A"

# Commit Modes
MQICM_COMMIT_THEN_SEND = b"0"
MQICM_SEND_THEN_COMMIT = b"1"

# Security Scopes
MQISS_CHECK = b"C"
MQISS_FULL = b"F"


##################################################################
# Values Related to MQIMPO Structure                             #
##################################################################

# Structure Identifier
MQIMPO_STRUC_ID = b"IMPO"

# Structure Identifier (array form)
MQIMPO_STRUC_ID_ARRAY = [b"I", b"M", b"P", b"O"]

# Structure Version Number
MQIMPO_VERSION_1 = 1
MQIMPO_CURRENT_VERSION = 1

# Structure Length - Moved to if statement at top
# due to 32/64 bit differences
MQIMPO_CURRENT_LENGTH = MQIMPO_LENGTH_1

# Inquire Message Property Options
MQIMPO_CONVERT_TYPE = 0x00000002
MQIMPO_QUERY_LENGTH = 0x00000004
MQIMPO_INQ_FIRST = 0x00000000
MQIMPO_INQ_NEXT = 0x00000008
MQIMPO_INQ_PROP_UNDER_CURSOR = 0x00000010
MQIMPO_CONVERT_VALUE = 0x00000020
MQIMPO_NONE = 0x00000000


##################################################################
# Values Related to MQMD Structure                               #
##################################################################

# Structure Identifier
MQMD_STRUC_ID = b"MD  "

# Structure Identifier (array form)
MQMD_STRUC_ID_ARRAY = [b"M", b"D", b" ", b" "]

# Structure Version Number
MQMD_VERSION_1 = 1
MQMD_VERSION_2 = 2
MQMD_CURRENT_VERSION = 2

# Structure Length
MQMD_LENGTH_1 = 324
MQMD_LENGTH_2 = 364
MQMD_CURRENT_LENGTH = 364

# Report Options
MQRO_EXCEPTION = 0x01000000
MQRO_EXCEPTION_WITH_DATA = 0x03000000
MQRO_EXCEPTION_WITH_FULL_DATA = 0x07000000
MQRO_EXPIRATION = 0x00200000
MQRO_EXPIRATION_WITH_DATA = 0x00600000
MQRO_EXPIRATION_WITH_FULL_DATA = 0x00E00000
MQRO_COA = 0x00000100
MQRO_COA_WITH_DATA = 0x00000300
MQRO_COA_WITH_FULL_DATA = 0x00000700
MQRO_COD = 0x00000800
MQRO_COD_WITH_DATA = 0x00001800
MQRO_COD_WITH_FULL_DATA = 0x00003800
MQRO_PAN = 0x00000001
MQRO_NAN = 0x00000002
MQRO_ACTIVITY = 0x00000004
MQRO_NEW_MSG_ID = 0x00000000
MQRO_PASS_MSG_ID = 0x00000080
MQRO_COPY_MSG_ID_TO_CORREL_ID = 0x00000000
MQRO_PASS_CORREL_ID = 0x00000040
MQRO_DEAD_LETTER_Q = 0x00000000
MQRO_DISCARD_MSG = 0x08000000
MQRO_PASS_DISCARD_AND_EXPIRY = 0x00004000
MQRO_NONE = 0x00000000

# Report Options Masks
MQRO_REJECT_UNSUP_MASK = 0x101C0000
MQRO_ACCEPT_UNSUP_MASK = 0xEFE000FF
MQRO_ACCEPT_UNSUP_IF_XMIT_MASK = 0x0003FF00

# Message Types
MQMT_SYSTEM_FIRST = 1
MQMT_REQUEST = 1
MQMT_REPLY = 2
MQMT_DATAGRAM = 8
MQMT_REPORT = 4
MQMT_MQE_FIELDS_FROM_MQE = 112
MQMT_MQE_FIELDS = 113
MQMT_SYSTEM_LAST = 65535
MQMT_APPL_FIRST = 65536
MQMT_APPL_LAST = 999999999

# Expiry
MQEI_UNLIMITED = (-1)

# Feedback Values
MQFB_NONE = 0
MQFB_SYSTEM_FIRST = 1
MQFB_QUIT = 256
MQFB_EXPIRATION = 258
MQFB_COA = 259
MQFB_COD = 260
MQFB_CHANNEL_COMPLETED = 262
MQFB_CHANNEL_FAIL_RETRY = 263
MQFB_CHANNEL_FAIL = 264
MQFB_APPL_CANNOT_BE_STARTED = 265
MQFB_TM_ERROR = 266
MQFB_APPL_TYPE_ERROR = 267
MQFB_STOPPED_BY_MSG_EXIT = 268
MQFB_ACTIVITY = 269
MQFB_XMIT_Q_MSG_ERROR = 271
MQFB_PAN = 275
MQFB_NAN = 276
MQFB_STOPPED_BY_CHAD_EXIT = 277
MQFB_STOPPED_BY_PUBSUB_EXIT = 279
MQFB_NOT_A_REPOSITORY_MSG = 280
MQFB_BIND_OPEN_CLUSRCVR_DEL = 281
MQFB_MAX_ACTIVITIES = 282
MQFB_NOT_FORWARDED = 283
MQFB_NOT_DELIVERED = 284
MQFB_UNSUPPORTED_FORWARDING = 285
MQFB_UNSUPPORTED_DELIVERY = 286
MQFB_DATA_LENGTH_ZERO = 291
MQFB_DATA_LENGTH_NEGATIVE = 292
MQFB_DATA_LENGTH_TOO_BIG = 293
MQFB_BUFFER_OVERFLOW = 294
MQFB_LENGTH_OFF_BY_ONE = 295
MQFB_IIH_ERROR = 296
MQFB_NOT_AUTHORIZED_FOR_IMS = 298
MQFB_IMS_ERROR = 300
MQFB_IMS_FIRST = 301
MQFB_IMS_LAST = 399
MQFB_CICS_INTERNAL_ERROR = 401
MQFB_CICS_NOT_AUTHORIZED = 402
MQFB_CICS_BRIDGE_FAILURE = 403
MQFB_CICS_CORREL_ID_ERROR = 404
MQFB_CICS_CCSID_ERROR = 405
MQFB_CICS_ENCODING_ERROR = 406
MQFB_CICS_CIH_ERROR = 407
MQFB_CICS_UOW_ERROR = 408
MQFB_CICS_COMMAREA_ERROR = 409
MQFB_CICS_APPL_NOT_STARTED = 410
MQFB_CICS_APPL_ABENDED = 411
MQFB_CICS_DLQ_ERROR = 412
MQFB_CICS_UOW_BACKED_OUT = 413
MQFB_PUBLICATIONS_ON_REQUEST = 501
MQFB_SUBSCRIBER_IS_PUBLISHER = 502
MQFB_MSG_SCOPE_MISMATCH = 503
MQFB_SELECTOR_MISMATCH = 504
MQFB_NOT_A_GROUPUR_MSG = 505
MQFB_IMS_NACK_1A_REASON_FIRST = 600
MQFB_IMS_NACK_1A_REASON_LAST = 855
MQFB_SYSTEM_LAST = 65535
MQFB_APPL_FIRST = 65536
MQFB_APPL_LAST = 999999999

# Encoding
MQENC_NATIVE = 0x00000222

# Encoding Masks
MQENC_INTEGER_MASK = 0x0000000F
MQENC_DECIMAL_MASK = 0x000000F0
MQENC_FLOAT_MASK = 0x00000F00
MQENC_RESERVED_MASK = 0xFFFFF000

# Encodings for Binary Integers
MQENC_INTEGER_UNDEFINED = 0x00000000
MQENC_INTEGER_NORMAL = 0x00000001
MQENC_INTEGER_REVERSED = 0x00000002

# Encodings for Packed Decimal Integers
MQENC_DECIMAL_UNDEFINED = 0x00000000
MQENC_DECIMAL_NORMAL = 0x00000010
MQENC_DECIMAL_REVERSED = 0x00000020

# Encodings for Floating Point Numbers
MQENC_FLOAT_UNDEFINED = 0x00000000
MQENC_FLOAT_IEEE_NORMAL = 0x00000100
MQENC_FLOAT_IEEE_REVERSED = 0x00000200
MQENC_FLOAT_S390 = 0x00000300
MQENC_FLOAT_TNS = 0x00000400

# Encodings for Multicast
MQENC_NORMAL = MQENC_FLOAT_IEEE_NORMAL | MQENC_DECIMAL_NORMAL | MQENC_INTEGER_NORMAL
MQENC_REVERSED = MQENC_FLOAT_IEEE_REVERSED | MQENC_DECIMAL_REVERSED | MQENC_INTEGER_REVERSED
MQENC_S390 = MQENC_FLOAT_S390 | MQENC_DECIMAL_NORMAL | MQENC_INTEGER_NORMAL
MQENC_TNS = MQENC_FLOAT_TNS | MQENC_DECIMAL_NORMAL | MQENC_INTEGER_NORMAL
MQENC_AS_PUBLISHED = (-1)

# Coded Character Set Identifiers
MQCCSI_UNDEFINED = 0
MQCCSI_DEFAULT = 0
MQCCSI_Q_MGR = 0
MQCCSI_INHERIT = (-2)
MQCCSI_EMBEDDED = (-1)
MQCCSI_APPL = (-3)
MQCCSI_AS_PUBLISHED = (-4)

# Formats
MQFMT_NONE = b"        "
MQFMT_ADMIN = b"MQADMIN "
MQFMT_AMQP = b"MQAMQP  "
MQFMT_CHANNEL_COMPLETED = b"MQCHCOM "
MQFMT_CICS = b"MQCICS  "
MQFMT_COMMAND_1 = b"MQCMD1  "
MQFMT_COMMAND_2 = b"MQCMD2  "
MQFMT_DEAD_LETTER_HEADER = b"MQDEAD  "
MQFMT_DIST_HEADER = b"MQHDIST "
MQFMT_EMBEDDED_PCF = b"MQHEPCF "
MQFMT_EVENT = b"MQEVENT "
MQFMT_IMS = b"MQIMS   "
MQFMT_IMS_VAR_STRING = b"MQIMSVS "
MQFMT_MD_EXTENSION = b"MQHMDE  "
MQFMT_PCF = b"MQPCF   "
MQFMT_REF_MSG_HEADER = b"MQHREF  "
MQFMT_RF_HEADER = b"MQHRF   "
MQFMT_RF_HEADER_1 = b"MQHRF   "
MQFMT_RF_HEADER_2 = b"MQHRF2  "
MQFMT_STRING = b"MQSTR   "
MQFMT_TRIGGER = b"MQTRIG  "
MQFMT_WORK_INFO_HEADER = b"MQHWIH  "
MQFMT_XMIT_Q_HEADER = b"MQXMIT  "

# Formats (array form)
MQFMT_NONE_ARRAY = [b" ", b" ", b" ", b" ", b" ", b" ", b" ", b" "]
MQFMT_ADMIN_ARRAY = [b"M", b"Q", b"A", b"D", b"M", b"I", b"N", b" "]
MQFMT_AMQP_ARRAY = [b"M", b"Q", b"A", b"M", b"Q", b"P", b" ", b" "]
MQFMT_CHANNEL_COMPLETED_ARRAY = [b"M", b"Q", b"C", b"H", b"C", b"O", b"M", b" "]
MQFMT_CICS_ARRAY = [b"M", b"Q", b"C", b"I", b"C", b"S", b" ", b" "]
MQFMT_COMMAND_1_ARRAY = [b"M", b"Q", b"C", b"M", b"D", b"1", b" ", b" "]
MQFMT_COMMAND_2_ARRAY = [b"M", b"Q", b"C", b"M", b"D", b"2", b" ", b" "]
MQFMT_DEAD_LETTER_HEADER_ARRAY = [b"M", b"Q", b"D", b"E", b"A", b"D", b" ", b" "]
MQFMT_DIST_HEADER_ARRAY = [b"M", b"Q", b"H", b"D", b"I", b"S", b"T", b" "]
MQFMT_EMBEDDED_PCF_ARRAY = [b"M", b"Q", b"H", b"E", b"P", b"C", b"F", b" "]
MQFMT_EVENT_ARRAY = [b"M", b"Q", b"E", b"V", b"E", b"N", b"T", b" "]
MQFMT_IMS_ARRAY = [b"M", b"Q", b"I", b"M", b"S", b" ", b" ", b" "]
MQFMT_IMS_VAR_STRING_ARRAY = [b"M", b"Q", b"I", b"M", b"S", b"V", b"S", b" "]
MQFMT_MD_EXTENSION_ARRAY = [b"M", b"Q", b"H", b"M", b"D", b"E", b" ", b" "]
MQFMT_PCF_ARRAY = [b"M", b"Q", b"P", b"C", b"F", b" ", b" ", b" "]
MQFMT_REF_MSG_HEADER_ARRAY = [b"M", b"Q", b"H", b"R", b"E", b"F", b" ", b" "]
MQFMT_RF_HEADER_ARRAY = [b"M", b"Q", b"H", b"R", b"F", b" ", b" ", b" "]
MQFMT_RF_HEADER_1_ARRAY = [b"M", b"Q", b"H", b"R", b"F", b" ", b" ", b" "]
MQFMT_RF_HEADER_2_ARRAY = [b"M", b"Q", b"H", b"R", b"F", b"2", b" ", b" "]
MQFMT_STRING_ARRAY = [b"M", b"Q", b"S", b"T", b"R", b" ", b" ", b" "]
MQFMT_TRIGGER_ARRAY = [b"M", b"Q", b"T", b"R", b"I", b"G", b" ", b" "]
MQFMT_WORK_INFO_HEADER_ARRAY = [b"M", b"Q", b"H", b"W", b"I", b"H", b" ", b" "]
MQFMT_XMIT_Q_HEADER_ARRAY = [b"M", b"Q", b"X", b"M", b"I", b"T", b" ", b" "]

# Priority
MQPRI_PRIORITY_AS_Q_DEF = (-1)
MQPRI_PRIORITY_AS_PARENT = (-2)
MQPRI_PRIORITY_AS_PUBLISHED = (-3)
MQPRI_PRIORITY_AS_TOPIC_DEF = (-1)

# Persistence Values
MQPER_PERSISTENCE_AS_PARENT = (-1)
MQPER_NOT_PERSISTENT = 0
MQPER_PERSISTENT = 1
MQPER_PERSISTENCE_AS_Q_DEF = 2
MQPER_PERSISTENCE_AS_TOPIC_DEF = 2

# Put Response Values
MQPRT_RESPONSE_AS_PARENT = 0
MQPRT_SYNC_RESPONSE = 1
MQPRT_ASYNC_RESPONSE = 2

# Message Identifier
MQMI_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"

# Message Identifier (array form)
MQMI_NONE_ARRAY = [b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0"]
                   
# Correlation Identifier
MQCI_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0"\
            b"\0\0\0\0\0\0\0\0\0\0\0\0"
            
MQCI_NEW_SESSION = b"\x41\x4D\x51\x21\x4E\x45\x57\x5F"\
                   b"\x53\x45\x53\x53\x49\x4F\x4E\x5F"\
                   b"\x43\x4F\x52\x52\x45\x4C\x49\x44"
                   
# Correlation Identifier (array form)
MQCI_NONE_ARRAY = [b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0"]

MQCI_NEW_SESSION_ARRAY = [b"\x41", b"\x4D", b"\x51", b"\x21",
                          b"\x4E", b"\x45", b"\x57", b"\x5F",
                          b"\x53", b"\x45", b"\x53", b"\x53",
                          b"\x49", b"\x4F", b"\x4E", b"\x5F",
                          b"\x43", b"\x4F", b"\x52", b"\x52",
                          b"\x45", b"\x4C", b"\x49", b"\x44"]

# Accounting Token
MQACT_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"\
             b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
             
# Accounting Token (array form)
MQACT_NONE_ARRAY = [b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0"]
                   
# Accounting Token Types
MQACTT_UNKNOWN = b"\x00"
MQACTT_CICS_LUOW_ID = b"\x01"
MQACTT_OS2_DEFAULT = b"\x04"
MQACTT_DOS_DEFAULT = b"\x05"
MQACTT_UNIX_NUMERIC_ID = b"\x06"
MQACTT_OS400_ACCOUNT_TOKEN = b"\x08"
MQACTT_WINDOWS_DEFAULT = b"\x09"
MQACTT_NT_SECURITY_ID = b"\x0B"
MQACTT_AZUREAD_SECURITY_ID = b"\x0C"
MQACTT_MS_ACC_AUTH_SECURITY_ID = b"\x0D"
MQACTT_USER = b"\x19"

# Put Application Types
MQAT_UNKNOWN = (-1)
MQAT_NO_CONTEXT = 0
MQAT_CICS = 1
MQAT_MVS = 2
MQAT_OS390 = 2
MQAT_ZOS = 2
MQAT_IMS = 3
MQAT_OS2 = 4
MQAT_DOS = 5
MQAT_AIX = 6
MQAT_UNIX = 6
MQAT_QMGR = 7
MQAT_OS400 = 8
MQAT_WINDOWS = 9
MQAT_CICS_VSE = 10
MQAT_WINDOWS_NT = 11
MQAT_VMS = 12
MQAT_GUARDIAN = 13
MQAT_NSK = 13
MQAT_VOS = 14
MQAT_OPEN_TP1 = 15
MQAT_VM = 18
MQAT_IMS_BRIDGE = 19
MQAT_XCF = 20
MQAT_CICS_BRIDGE = 21
MQAT_NOTES_AGENT = 22
MQAT_TPF = 23
MQAT_USER = 25
MQAT_BROKER = 26
MQAT_QMGR_PUBLISH = 26
MQAT_JAVA = 28
MQAT_DQM = 29
MQAT_CHANNEL_INITIATOR = 30
MQAT_WLM = 31
MQAT_BATCH = 32
MQAT_RRS_BATCH = 33
MQAT_SIB = 34
MQAT_SYSTEM_EXTENSION = 35
MQAT_MCAST_PUBLISH = 36
MQAT_AMQP = 37
MQAT_DEFAULT = 11
MQAT_USER_FIRST = 65536
MQAT_USER_LAST = 999999999

# Group Identifier
MQGI_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"

# Group Identifier (array form)
MQGI_NONE_ARRAY = [b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                   b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0"]
                   
# Message Flags
MQMF_SEGMENTATION_INHIBITED = 0x00000000
MQMF_SEGMENTATION_ALLOWED = 0x00000001
MQMF_MSG_IN_GROUP = 0x00000008
MQMF_LAST_MSG_IN_GROUP = 0x00000010
MQMF_SEGMENT = 0x00000002
MQMF_LAST_SEGMENT = 0x00000004
MQMF_NONE = 0x00000000

# Message Flags Masks
MQMF_REJECT_UNSUP_MASK = 0x00000FFF
MQMF_ACCEPT_UNSUP_MASK = 0xFFF00000
MQMF_ACCEPT_UNSUP_IF_XMIT_MASK = 0x000FF000

# Original Length
MQOL_UNDEFINED = (-1)



##################################################################
# Values Related to MQMDE Structure                              #
##################################################################

# Structure Identifier
MQMDE_STRUC_ID = b"MDE "

# Structure Identifier (array form)
MQMDE_STRUC_ID_ARRAY = [b"M", b"D", b"E", b" "]

# Structure Version Number
MQMDE_VERSION_2 = 2
MQMDE_CURRENT_VERSION = 2

# Structure Length
MQMDE_LENGTH_2 = 72
MQMDE_CURRENT_LENGTH = 72

# Flags
MQMDEF_NONE = 0x00000000


##################################################################
# Values Related to MQMD1 Structure                              #
##################################################################

# Structure Length
MQMD1_LENGTH_1 = 324
MQMD1_CURRENT_LENGTH = 324


##################################################################
# Values Related to MQMD2 Structure                              #
##################################################################

# Structure Length
MQMD2_LENGTH_1 = 324
MQMD2_LENGTH_2 = 364
MQMD2_CURRENT_LENGTH = 364


##################################################################
# Values Related to MQMHBO Structure                             #
##################################################################

# Structure Identifier
MQMHBO_STRUC_ID = b"MHBO"

# Structure Identifier (array form)
MQMHBO_STRUC_ID_ARRAY = [b"M", b"H", b"B", b"O"]

# Structure Version Number
MQMHBO_VERSION_1 = 1
MQMHBO_CURRENT_VERSION = 1

# Structure Length
MQMHBO_LENGTH_1 = 12
MQMHBO_CURRENT_LENGTH = 12

# Message Handle to buffer Options
MQMHBO_PROPERTIES_IN_MQRFH2 = 0x00000001
MQMHBO_DELETE_PROPERTIES = 0x00000002
MQMHBO_NONE = 0x00000000


##################################################################
# Values Related to MQOD Structure                               #
##################################################################

# Structure Identifier
MQOD_STRUC_ID = b"OD  "

# Structure Identifier (array form)
MQOD_STRUC_ID_ARRAY = [b"O", b"D", b" ", b" "]

# Structure Version Number
MQOD_VERSION_1 = 1
MQOD_VERSION_2 = 2
MQOD_VERSION_3 = 3
MQOD_VERSION_4 = 4
MQOD_CURRENT_VERSION = 4

# Structure Length - Moved to if statement at top
# due to 32/64 bit differences
MQOD_CURRENT_LENGTH = MQOD_LENGTH_4

# Obsolete DB2 Messages Options on Inquire Group
MQOM_NO = 0
MQOM_YES = 1

# Object Types
MQOT_NONE = 0
MQOT_Q = 1
MQOT_NAMELIST = 2
MQOT_PROCESS = 3
MQOT_STORAGE_CLASS = 4
MQOT_Q_MGR = 5
MQOT_CHANNEL = 6
MQOT_AUTH_INFO = 7
MQOT_TOPIC = 8
MQOT_COMM_INFO = 9
MQOT_CF_STRUC = 10
MQOT_LISTENER = 11
MQOT_SERVICE = 12
MQOT_RESERVED_1 = 999

# Extended Object Types
MQOT_ALL = 1001
MQOT_ALIAS_Q = 1002
MQOT_MODEL_Q = 1003
MQOT_LOCAL_Q = 1004
MQOT_REMOTE_Q = 1005
MQOT_SENDER_CHANNEL = 1007
MQOT_SERVER_CHANNEL = 1008
MQOT_REQUESTER_CHANNEL = 1009
MQOT_RECEIVER_CHANNEL = 1010
MQOT_CURRENT_CHANNEL = 1011
MQOT_SAVED_CHANNEL = 1012
MQOT_SVRCONN_CHANNEL = 1013
MQOT_CLNTCONN_CHANNEL = 1014
MQOT_SHORT_CHANNEL = 1015
MQOT_CHLAUTH = 1016
MQOT_REMOTE_Q_MGR_NAME = 1017
MQOT_PROT_POLICY = 1019
MQOT_TT_CHANNEL = 1020
MQOT_AMQP_CHANNEL = 1021
MQOT_AUTH_REC = 1022


##################################################################
# Values Related to MQPD Structure                               #
##################################################################

# Structure Identifier
MQPD_STRUC_ID = b"PD  "

# Structure Identifier (array form)
MQPD_STRUC_ID_ARRAY = [b"P", b"D", b" ", b" "]

# Structure Version Number
MQPD_VERSION_1 = 1
MQPD_CURRENT_VERSION = 1

# Structure Length
MQPD_LENGTH_1 = 24
MQPD_CURRENT_LENGTH = 24

# Property Descriptor Options
MQPD_NONE = 0x00000000

# Property Support Options
MQPD_SUPPORT_OPTIONAL = 0x00000001
MQPD_SUPPORT_REQUIRED = 0x00100000
MQPD_SUPPORT_REQUIRED_IF_LOCAL = 0x00000400
MQPD_REJECT_UNSUP_MASK = 0xFFF00000
MQPD_ACCEPT_UNSUP_IF_XMIT_MASK = 0x000FFC00
MQPD_ACCEPT_UNSUP_MASK = 0x000003FF

# Property Context
MQPD_NO_CONTEXT = 0x00000000
MQPD_USER_CONTEXT = 0x00000001

# Property Copy Options
MQCOPY_NONE = 0x00000000
MQCOPY_ALL = 0x00000001
MQCOPY_FORWARD = 0x00000002
MQCOPY_PUBLISH = 0x00000004
MQCOPY_REPLY = 0x00000008
MQCOPY_REPORT = 0x00000010
MQCOPY_DEFAULT = 0x00000016


##################################################################
# Values Related to MQPMO Structure                              #
##################################################################

# Structure Identifier
MQPMO_STRUC_ID = b"PMO "

# Structure Identifier (array form)
MQPMO_STRUC_ID_ARRAY = [b"P", b"M", b"O", b" "]

# Structure Version Number
MQPMO_VERSION_1 = 1
MQPMO_VERSION_2 = 2
MQPMO_VERSION_3 = 3
MQPMO_CURRENT_VERSION = 3

# Structure Length - Moved to if statement at top
# due to 32/64 bit differences
MQPMO_CURRENT_LENGTH = MQPMO_LENGTH_3

# Put Message Options
MQPMO_SYNCPOINT = 0x00000002
MQPMO_NO_SYNCPOINT = 0x00000004
MQPMO_DEFAULT_CONTEXT = 0x00000020
MQPMO_NEW_MSG_ID = 0x00000040
MQPMO_NEW_CORREL_ID = 0x00000080
MQPMO_PASS_IDENTITY_CONTEXT = 0x00000100
MQPMO_PASS_ALL_CONTEXT = 0x00000200
MQPMO_SET_IDENTITY_CONTEXT = 0x00000400
MQPMO_SET_ALL_CONTEXT = 0x00000800
MQPMO_ALTERNATE_USER_AUTHORITY = 0x00001000
MQPMO_FAIL_IF_QUIESCING = 0x00002000
MQPMO_NO_CONTEXT = 0x00004000
MQPMO_LOGICAL_ORDER = 0x00008000
MQPMO_ASYNC_RESPONSE = 0x00010000
MQPMO_SYNC_RESPONSE = 0x00020000
MQPMO_RESOLVE_LOCAL_Q = 0x00040000
MQPMO_WARN_IF_NO_SUBS_MATCHED = 0x00080000
MQPMO_RETAIN = 0x00200000
MQPMO_MD_FOR_OUTPUT_ONLY = 0x00800000
MQPMO_SCOPE_QMGR = 0x04000000
MQPMO_SUPPRESS_REPLYTO = 0x08000000
MQPMO_NOT_OWN_SUBS = 0x10000000
MQPMO_RESPONSE_AS_Q_DEF = 0x00000000
MQPMO_RESPONSE_AS_TOPIC_DEF = 0x00000000
MQPMO_NONE = 0x00000000

# Put Message Options for Publish Mask
MQPMO_PUB_OPTIONS_MASK = 0x00200000

# Put Message Record Fields
MQPMRF_MSG_ID = 0x00000001
MQPMRF_CORREL_ID = 0x00000002
MQPMRF_GROUP_ID = 0x00000004
MQPMRF_FEEDBACK = 0x00000008
MQPMRF_ACCOUNTING_TOKEN = 0x00000010
MQPMRF_NONE = 0x00000000

# Action
MQACTP_NEW = 0
MQACTP_FORWARD = 1
MQACTP_REPLY = 2
MQACTP_REPORT = 3


##################################################################
# Values Related to MQRFH Structure                              #
##################################################################

# Structure Identifier
MQRFH_STRUC_ID = b"RFH "

# Structure Identifier (array form)
MQRFH_STRUC_ID_ARRAY = [b"R", b"F", b"H", b" "]

# Structure Version Number
MQRFH_VERSION_1 = 1
MQRFH_VERSION_2 = 2

# Structure Length
MQRFH_STRUC_LENGTH_FIXED = 32
MQRFH_STRUC_LENGTH_FIXED_2 = 36
MQRFH_LENGTH_1 = 32
MQRFH_CURRENT_LENGTH = 32

# Flags
MQRFH_NONE = 0x00000000
MQRFH_NO_FLAGS = 0
MQRFH_FLAGS_RESTRICTED_MASK = 0xFFFF0000

# MQRFH2 flags in the restricted mask are reserved for MQ use:

# 0x80000000 - MQRFH_INTERNAL - This flag indicates the RFH2 header
# was created by IBM MQ for internal use.

# Names for Name/Value String
MQNVS_APPL_TYPE = b"OPT_APP_GRP "
MQNVS_MSG_TYPE = b"OPT_MSG_TYPE "


##################################################################
# Values Related to MQRFH2 Structure                             #
##################################################################

# Structure Length
MQRFH2_LENGTH_2 = 36
MQRFH2_CURRENT_LENGTH = 36


##################################################################
# Values Related to MQRMH Structure                              #
##################################################################

# Structure Identifier
MQRMH_STRUC_ID = b"RMH "

# Structure Identifier (array form)
MQRMH_STRUC_ID_ARRAY = [b"R", b"M", b"H", b" "]

# Structure Version Number
MQRMH_VERSION_1 = 1
MQRMH_CURRENT_VERSION = 1

# Structure Length
MQRMH_LENGTH_1 = 108
MQRMH_CURRENT_LENGTH = 108

# Flags
MQRMHF_LAST = 0x00000001
MQRMHF_NOT_LAST = 0x00000000

# Object Instance Identifier
MQOII_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"

# Object Instance Identifier (array form)
MQOII_NONE_ARRAY = [b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                    b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                    b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0"]
                    

##################################################################
# Values Related to MQSD Structure                               #
##################################################################

# Structure Identifier
MQSD_STRUC_ID = b"SD  "

# Structure Identifier (array form)
MQSD_STRUC_ID_ARRAY = [b"S", b"D", b" ", b" "]

# Structure Version Number
MQSD_VERSION_1 = 1
MQSD_CURRENT_VERSION = 1

# Structure Length - Moved to if statement at top
# due to 32/64 bit differences
MQSD_CURRENT_LENGTH = MQSD_LENGTH_1

# Security Identifier
MQSID_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"\
             b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
             
# Security Identifier (array form)
MQSID_NONE_ARRAY = [b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                    b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                    b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                    b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0",
                    b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0", b"\0"]
                    
# Security Identifier Types
MQSIDT_NONE = b"\x00"
MQSIDT_NT_SECURITY_ID = b"\x01"
MQSIDT_WAS_SECURITY_ID = b"\x02"


##################################################################
# Values Related to MQSMPO Structure                             #
##################################################################

# Structure Identifier
MQSMPO_STRUC_ID = b"SMPO"

# Structure Identifier (array form)
MQSMPO_STRUC_ID_ARRAY = [b"S", b"M", b"P", b"O"]

# Structure Version Number
MQSMPO_VERSION_1 = 1
MQSMPO_CURRENT_VERSION = 1

# Structure Length
MQSMPO_LENGTH_1 = 20
MQSMPO_CURRENT_LENGTH = 20

# Set Message Property Options
MQSMPO_SET_FIRST = 0x00000000
MQSMPO_SET_PROP_UNDER_CURSOR = 0x00000001
MQSMPO_SET_PROP_AFTER_CURSOR = 0x00000002
MQSMPO_APPEND_PROPERTY = 0x00000004
MQSMPO_SET_PROP_BEFORE_CURSOR = 0x00000008
MQSMPO_NONE = 0x00000000


##################################################################
# Values Related to MQSRO Structure                              #
##################################################################

# Structure Identifier
MQSRO_STRUC_ID = b"SRO "

# Structure Identifier (array form)
MQSRO_STRUC_ID_ARRAY = [b"S", b"R", b"O", b" "]

# Structure Version Number
MQSRO_VERSION_1 = 1
MQSRO_CURRENT_VERSION = 1

# Structure Length
MQSRO_LENGTH_1 = 16
MQSRO_CURRENT_LENGTH = 16

# Subscription Request Options
MQSRO_NONE = 0x00000000
MQSRO_FAIL_IF_QUIESCING = 0x00002000


##################################################################
# Values Related to MQSTS Structure                              #
##################################################################

# Structure Identifier
MQSTS_STRUC_ID = b"STAT"

# Structure Identifier (array form)
MQSTS_STRUC_ID_ARRAY = [b"S", b"T", b"A", b"T"]

# Structure Version Number
MQSTS_VERSION_1 = 1
MQSTS_VERSION_2 = 2
MQSTS_CURRENT_VERSION = 2

# Structure Length - Moved to if statement at top
# due to 32/64 bit differences
MQSTS_CURRENT_LENGTH = MQSTS_LENGTH_2


##################################################################
# Values Related to MQTM Structure                               #
##################################################################

# Structure Identifier
MQTM_STRUC_ID = b"TM  "

# Structure Identifier (array form)
MQTM_STRUC_ID_ARRAY = [b"T", b"M", b" ", b" "]

# Structure Version Number
MQTM_VERSION_1 = 1
MQTM_CURRENT_VERSION = 1

# Structure Length
MQTM_LENGTH_1 = 684
MQTM_CURRENT_LENGTH = 684


##################################################################
# Values Related to MQTMC2 Structure                             #
##################################################################

# Structure Identifier
MQTMC_STRUC_ID = b"TMC "

# Structure Identifier (array form)
MQTMC_STRUC_ID_ARRAY = [b"T", b"M", b"C", b" "]

# Structure Version Number
MQTMC_VERSION_1 = b"   1"
MQTMC_VERSION_2 = b"   2"
MQTMC_CURRENT_VERSION = b"   2"

# Structure Version Number (array form)
MQTMC_VERSION_1_ARRAY = [b" ", b" ", b" ", b"1"]
MQTMC_VERSION_2_ARRAY = [b" ", b" ", b" ", b"2"]
MQTMC_CURRENT_VERSION_ARRAY = [b" ", b" ", b" ", b"2"]

# Structure Length
MQTMC2_LENGTH_1 = 684
MQTMC2_LENGTH_2 = 732
MQTMC2_CURRENT_LENGTH = 732


##################################################################
# Values Related to MQWIH Structure                              #
##################################################################

# Structure Identifier
MQWIH_STRUC_ID = b"WIH "

# Structure Identifier (array form)
MQWIH_STRUC_ID_ARRAY = [b"W", b"I", b"H", b" "]

# Structure Version Number
MQWIH_VERSION_1 = 1
MQWIH_CURRENT_VERSION = 1

# Structure Length
MQWIH_LENGTH_1 = 120
MQWIH_CURRENT_LENGTH = 120

# Flags
MQWIH_NONE = 0x00000000


##################################################################
# Values Related to MQXQH Structure                              #
##################################################################

# Structure Identifier
MQXQH_STRUC_ID = b"XQH "

# Structure Identifier (array form)
MQXQH_STRUC_ID_ARRAY = [b"X", b"Q", b"H", b" "]

# Structure Version Number
MQXQH_VERSION_1 = 1
MQXQH_CURRENT_VERSION = 1

# Structure Length
MQXQH_LENGTH_1 = 428
MQXQH_CURRENT_LENGTH = 428


##################################################################
# Values Related to All Functions                                #
##################################################################

# Connection Handles
MQHC_DEF_HCONN = 0
MQHC_UNUSABLE_HCONN = (-1)
MQHC_UNASSOCIATED_HCONN = (-3)

# String Lengths
MQ_OPERATOR_MESSAGE_LENGTH = 4
MQ_ABEND_CODE_LENGTH = 4
MQ_ACCOUNTING_TOKEN_LENGTH = 32
MQ_APPL_DESC_LENGTH = 64
MQ_APPL_IDENTITY_DATA_LENGTH = 32
MQ_APPL_NAME_LENGTH = 28
MQ_APPL_ORIGIN_DATA_LENGTH = 4
MQ_APPL_TAG_LENGTH = 28
MQ_ARM_SUFFIX_LENGTH = 2
MQ_ATTENTION_ID_LENGTH = 4
MQ_AUTH_INFO_CONN_NAME_LENGTH = 264
MQ_AUTH_INFO_DESC_LENGTH = 64
MQ_AUTH_INFO_NAME_LENGTH = 48
MQ_AUTH_INFO_OCSP_URL_LENGTH = 256
MQ_AUTHENTICATOR_LENGTH = 8
MQ_AUTO_REORG_CATALOG_LENGTH = 44
MQ_AUTO_REORG_TIME_LENGTH = 4
MQ_BATCH_INTERFACE_ID_LENGTH = 8
MQ_BRIDGE_NAME_LENGTH = 24
MQ_CANCEL_CODE_LENGTH = 4
MQ_CF_STRUC_DESC_LENGTH = 64
MQ_CF_STRUC_NAME_LENGTH = 12
MQ_CHANNEL_DATE_LENGTH = 12
MQ_CHANNEL_DESC_LENGTH = 64
MQ_CHANNEL_NAME_LENGTH = 20
MQ_CHANNEL_TIME_LENGTH = 8
MQ_CHINIT_SERVICE_PARM_LENGTH = 32
MQ_CICS_FILE_NAME_LENGTH = 8
MQ_AMQP_CLIENT_ID_LENGTH = 256
MQ_CLIENT_ID_LENGTH = 23
MQ_CLIENT_USER_ID_LENGTH = 1024
MQ_CLUSTER_NAME_LENGTH = 48
MQ_COMM_INFO_DESC_LENGTH = 64
MQ_COMM_INFO_NAME_LENGTH = 48
MQ_CONN_NAME_LENGTH = 264
MQ_CONN_TAG_LENGTH = 128
MQ_CONNECTION_ID_LENGTH = 24
MQ_CORREL_ID_LENGTH = 24
MQ_CREATION_DATE_LENGTH = 12
MQ_CREATION_TIME_LENGTH = 8
MQ_CSP_PASSWORD_LENGTH = 256
MQ_DATE_LENGTH = 12
MQ_DISTINGUISHED_NAME_LENGTH = 1024
MQ_DNS_GROUP_NAME_LENGTH = 18
MQ_EXIT_DATA_LENGTH = 32
MQ_EXIT_INFO_NAME_LENGTH = 48
MQ_EXIT_NAME_LENGTH = 128
MQ_EXIT_PD_AREA_LENGTH = 48
MQ_EXIT_USER_AREA_LENGTH = 16
MQ_FACILITY_LENGTH = 8
MQ_FACILITY_LIKE_LENGTH = 4
MQ_FORMAT_LENGTH = 8
MQ_FUNCTION_LENGTH = 4
MQ_GROUP_ID_LENGTH = 24
MQ_APPL_FUNCTION_NAME_LENGTH = 10
MQ_INSTALLATION_DESC_LENGTH = 64
MQ_INSTALLATION_NAME_LENGTH = 16
MQ_INSTALLATION_PATH_LENGTH = 256
MQ_JAAS_CONFIG_LENGTH = 1024
MQ_LDAP_PASSWORD_LENGTH = 32
MQ_LDAP_BASE_DN_LENGTH = 1024
MQ_LDAP_FIELD_LENGTH = 128
MQ_LDAP_CLASS_LENGTH = 128
MQ_LISTENER_NAME_LENGTH = 48
MQ_LISTENER_DESC_LENGTH = 64
MQ_LOCAL_ADDRESS_LENGTH = 48
MQ_LTERM_OVERRIDE_LENGTH = 8
MQ_LU_NAME_LENGTH = 8
MQ_LUWID_LENGTH = 16
MQ_MAX_EXIT_NAME_LENGTH = 128
MQ_MAX_MCA_USER_ID_LENGTH = 64
MQ_MAX_LDAP_MCA_USER_ID_LENGTH = 1024
MQ_MAX_PROPERTY_NAME_LENGTH = 4095
MQ_MAX_USER_ID_LENGTH = 64
MQ_MCA_JOB_NAME_LENGTH = 28
MQ_MCA_NAME_LENGTH = 20
MQ_MCA_USER_DATA_LENGTH = 32
MQ_MCA_USER_ID_LENGTH = 64
MQ_LDAP_MCA_USER_ID_LENGTH = 1024
MQ_MFS_MAP_NAME_LENGTH = 8
MQ_MODE_NAME_LENGTH = 8
MQ_MSG_HEADER_LENGTH = 4000
MQ_MSG_ID_LENGTH = 24
MQ_MSG_TOKEN_LENGTH = 16
MQ_NAMELIST_DESC_LENGTH = 64
MQ_NAMELIST_NAME_LENGTH = 48
MQ_NHA_INSTANCE_NAME_LENGTH = 48
MQ_OBJECT_INSTANCE_ID_LENGTH = 24
MQ_OBJECT_NAME_LENGTH = 48
MQ_PASS_TICKET_APPL_LENGTH = 8
MQ_PASSWORD_LENGTH = 12
MQ_PROCESS_APPL_ID_LENGTH = 256
MQ_PROCESS_DESC_LENGTH = 64
MQ_PROCESS_ENV_DATA_LENGTH = 128
MQ_PROCESS_NAME_LENGTH = 48
MQ_PROCESS_USER_DATA_LENGTH = 128
MQ_PROGRAM_NAME_LENGTH = 20
MQ_PUT_APPL_NAME_LENGTH = 28
MQ_PUT_DATE_LENGTH = 8
MQ_PUT_TIME_LENGTH = 8
MQ_Q_DESC_LENGTH = 64
MQ_Q_MGR_DESC_LENGTH = 64
MQ_Q_MGR_IDENTIFIER_LENGTH = 48
MQ_Q_MGR_NAME_LENGTH = 48
MQ_Q_NAME_LENGTH = 48
MQ_QSG_NAME_LENGTH = 4
MQ_REMOTE_SYS_ID_LENGTH = 4
MQ_SECURITY_ID_LENGTH = 40
MQ_SELECTOR_LENGTH = 10240
MQ_SERVICE_ARGS_LENGTH = 255
MQ_SERVICE_COMMAND_LENGTH = 255
MQ_SERVICE_DESC_LENGTH = 64
MQ_SERVICE_NAME_LENGTH = 32
MQ_SERVICE_PATH_LENGTH = 255
MQ_SERVICE_STEP_LENGTH = 8
MQ_SHORT_CONN_NAME_LENGTH = 20
MQ_SHORT_DNAME_LENGTH = 256
MQ_SSL_CIPHER_SPEC_LENGTH = 32
MQ_SSL_CIPHER_SUITE_LENGTH = 32
MQ_SSL_CRYPTO_HARDWARE_LENGTH = 256
MQ_SSL_HANDSHAKE_STAGE_LENGTH = 32
MQ_SSL_KEY_LIBRARY_LENGTH = 44
MQ_SSL_KEY_MEMBER_LENGTH = 8
MQ_SSL_KEY_REPOSITORY_LENGTH = 256
MQ_SSL_PEER_NAME_LENGTH = 1024
MQ_SSL_SHORT_PEER_NAME_LENGTH = 256
MQ_START_CODE_LENGTH = 4
MQ_STORAGE_CLASS_DESC_LENGTH = 64
MQ_STORAGE_CLASS_LENGTH = 8
MQ_SUB_IDENTITY_LENGTH = 128
MQ_SUB_POINT_LENGTH = 128
MQ_TCP_NAME_LENGTH = 8
MQ_TEMPORARY_Q_PREFIX_LENGTH = 32
MQ_TIME_LENGTH = 8
MQ_TOPIC_DESC_LENGTH = 64
MQ_TOPIC_NAME_LENGTH = 48
MQ_TOPIC_STR_LENGTH = 10240
MQ_TOTAL_EXIT_DATA_LENGTH = 999
MQ_TOTAL_EXIT_NAME_LENGTH = 999
MQ_TP_NAME_LENGTH = 64
MQ_TPIPE_NAME_LENGTH = 8
MQ_TRAN_INSTANCE_ID_LENGTH = 16
MQ_TRANSACTION_ID_LENGTH = 4
MQ_TRIGGER_DATA_LENGTH = 64
MQ_TRIGGER_PROGRAM_NAME_LENGTH = 8
MQ_TRIGGER_TERM_ID_LENGTH = 4
MQ_TRIGGER_TRANS_ID_LENGTH = 4
MQ_USER_ID_LENGTH = 12
MQ_VERSION_LENGTH = 8
MQ_XCF_GROUP_NAME_LENGTH = 8
MQ_XCF_MEMBER_NAME_LENGTH = 16
MQ_SMDS_NAME_LENGTH = 4
MQ_CHLAUTH_DESC_LENGTH = 64
MQ_CUSTOM_LENGTH = 128
MQ_SUITE_B_SIZE = 4
MQ_CERT_LABEL_LENGTH = 64

# Completion Codes
MQCC_OK = 0
MQCC_WARNING = 1
MQCC_FAILED = 2
MQCC_UNKNOWN = (-1)

# Reason Codes
MQRC_NONE = 0
MQRC_APPL_FIRST = 900
MQRC_APPL_LAST = 999
MQRC_ALIAS_BASE_Q_TYPE_ERROR = 2001
MQRC_ALREADY_CONNECTED = 2002
MQRC_BACKED_OUT = 2003
MQRC_BUFFER_ERROR = 2004
MQRC_BUFFER_LENGTH_ERROR = 2005
MQRC_CHAR_ATTR_LENGTH_ERROR = 2006
MQRC_CHAR_ATTRS_ERROR = 2007
MQRC_CHAR_ATTRS_TOO_SHORT = 2008
MQRC_CONNECTION_BROKEN = 2009
MQRC_DATA_LENGTH_ERROR = 2010
MQRC_DYNAMIC_Q_NAME_ERROR = 2011
MQRC_ENVIRONMENT_ERROR = 2012
MQRC_EXPIRY_ERROR = 2013
MQRC_FEEDBACK_ERROR = 2014
MQRC_GET_INHIBITED = 2016
MQRC_HANDLE_NOT_AVAILABLE = 2017
MQRC_HCONN_ERROR = 2018
MQRC_HOBJ_ERROR = 2019
MQRC_INHIBIT_VALUE_ERROR = 2020
MQRC_INT_ATTR_COUNT_ERROR = 2021
MQRC_INT_ATTR_COUNT_TOO_SMALL = 2022
MQRC_INT_ATTRS_ARRAY_ERROR = 2023
MQRC_SYNCPOINT_LIMIT_REACHED = 2024
MQRC_MAX_CONNS_LIMIT_REACHED = 2025
MQRC_MD_ERROR = 2026
MQRC_MISSING_REPLY_TO_Q = 2027
MQRC_MSG_TYPE_ERROR = 2029
MQRC_MSG_TOO_BIG_FOR_Q = 2030
MQRC_MSG_TOO_BIG_FOR_Q_MGR = 2031
MQRC_NO_MSG_AVAILABLE = 2033
MQRC_NO_MSG_UNDER_CURSOR = 2034
MQRC_NOT_AUTHORIZED = 2035
MQRC_NOT_OPEN_FOR_BROWSE = 2036
MQRC_NOT_OPEN_FOR_INPUT = 2037
MQRC_NOT_OPEN_FOR_INQUIRE = 2038
MQRC_NOT_OPEN_FOR_OUTPUT = 2039
MQRC_NOT_OPEN_FOR_SET = 2040
MQRC_OBJECT_CHANGED = 2041
MQRC_OBJECT_IN_USE = 2042
MQRC_OBJECT_TYPE_ERROR = 2043
MQRC_OD_ERROR = 2044
MQRC_OPTION_NOT_VALID_FOR_TYPE = 2045
MQRC_OPTIONS_ERROR = 2046
MQRC_PERSISTENCE_ERROR = 2047
MQRC_PERSISTENT_NOT_ALLOWED = 2048
MQRC_PRIORITY_EXCEEDS_MAXIMUM = 2049
MQRC_PRIORITY_ERROR = 2050
MQRC_PUT_INHIBITED = 2051
MQRC_Q_DELETED = 2052
MQRC_Q_FULL = 2053
MQRC_Q_NOT_EMPTY = 2055
MQRC_Q_SPACE_NOT_AVAILABLE = 2056
MQRC_Q_TYPE_ERROR = 2057
MQRC_Q_MGR_NAME_ERROR = 2058
MQRC_Q_MGR_NOT_AVAILABLE = 2059
MQRC_REPORT_OPTIONS_ERROR = 2061
MQRC_SECOND_MARK_NOT_ALLOWED = 2062
MQRC_SECURITY_ERROR = 2063
MQRC_SELECTOR_COUNT_ERROR = 2065
MQRC_SELECTOR_LIMIT_EXCEEDED = 2066
MQRC_SELECTOR_ERROR = 2067
MQRC_SELECTOR_NOT_FOR_TYPE = 2068
MQRC_SIGNAL_OUTSTANDING = 2069
MQRC_SIGNAL_REQUEST_ACCEPTED = 2070
MQRC_STORAGE_NOT_AVAILABLE = 2071
MQRC_SYNCPOINT_NOT_AVAILABLE = 2072
MQRC_TRIGGER_CONTROL_ERROR = 2075
MQRC_TRIGGER_DEPTH_ERROR = 2076
MQRC_TRIGGER_MSG_PRIORITY_ERR = 2077
MQRC_TRIGGER_TYPE_ERROR = 2078
MQRC_TRUNCATED_MSG_ACCEPTED = 2079
MQRC_TRUNCATED_MSG_FAILED = 2080
MQRC_UNKNOWN_ALIAS_BASE_Q = 2082
MQRC_UNKNOWN_OBJECT_NAME = 2085
MQRC_UNKNOWN_OBJECT_Q_MGR = 2086
MQRC_UNKNOWN_REMOTE_Q_MGR = 2087
MQRC_WAIT_INTERVAL_ERROR = 2090
MQRC_XMIT_Q_TYPE_ERROR = 2091
MQRC_XMIT_Q_USAGE_ERROR = 2092
MQRC_NOT_OPEN_FOR_PASS_ALL = 2093
MQRC_NOT_OPEN_FOR_PASS_IDENT = 2094
MQRC_NOT_OPEN_FOR_SET_ALL = 2095
MQRC_NOT_OPEN_FOR_SET_IDENT = 2096
MQRC_CONTEXT_HANDLE_ERROR = 2097
MQRC_CONTEXT_NOT_AVAILABLE = 2098
MQRC_SIGNAL1_ERROR = 2099
MQRC_OBJECT_ALREADY_EXISTS = 2100
MQRC_OBJECT_DAMAGED = 2101
MQRC_RESOURCE_PROBLEM = 2102
MQRC_ANOTHER_Q_MGR_CONNECTED = 2103
MQRC_UNKNOWN_REPORT_OPTION = 2104
MQRC_STORAGE_CLASS_ERROR = 2105
MQRC_COD_NOT_VALID_FOR_XCF_Q = 2106
MQRC_XWAIT_CANCELED = 2107
MQRC_XWAIT_ERROR = 2108
MQRC_SUPPRESSED_BY_EXIT = 2109
MQRC_FORMAT_ERROR = 2110
MQRC_SOURCE_CCSID_ERROR = 2111
MQRC_SOURCE_INTEGER_ENC_ERROR = 2112
MQRC_SOURCE_DECIMAL_ENC_ERROR = 2113
MQRC_SOURCE_FLOAT_ENC_ERROR = 2114
MQRC_TARGET_CCSID_ERROR = 2115
MQRC_TARGET_INTEGER_ENC_ERROR = 2116
MQRC_TARGET_DECIMAL_ENC_ERROR = 2117
MQRC_TARGET_FLOAT_ENC_ERROR = 2118
MQRC_NOT_CONVERTED = 2119
MQRC_CONVERTED_MSG_TOO_BIG = 2120
MQRC_TRUNCATED = 2120
MQRC_NO_EXTERNAL_PARTICIPANTS = 2121
MQRC_PARTICIPANT_NOT_AVAILABLE = 2122
MQRC_OUTCOME_MIXED = 2123
MQRC_OUTCOME_PENDING = 2124
MQRC_BRIDGE_STARTED = 2125
MQRC_BRIDGE_STOPPED = 2126
MQRC_ADAPTER_STORAGE_SHORTAGE = 2127
MQRC_UOW_IN_PROGRESS = 2128
MQRC_ADAPTER_CONN_LOAD_ERROR = 2129
MQRC_ADAPTER_SERV_LOAD_ERROR = 2130
MQRC_ADAPTER_DEFS_ERROR = 2131
MQRC_ADAPTER_DEFS_LOAD_ERROR = 2132
MQRC_ADAPTER_CONV_LOAD_ERROR = 2133
MQRC_BO_ERROR = 2134
MQRC_DH_ERROR = 2135
MQRC_MULTIPLE_REASONS = 2136
MQRC_OPEN_FAILED = 2137
MQRC_ADAPTER_DISC_LOAD_ERROR = 2138
MQRC_CNO_ERROR = 2139
MQRC_CICS_WAIT_FAILED = 2140
MQRC_DLH_ERROR = 2141
MQRC_HEADER_ERROR = 2142
MQRC_SOURCE_LENGTH_ERROR = 2143
MQRC_TARGET_LENGTH_ERROR = 2144
MQRC_SOURCE_BUFFER_ERROR = 2145
MQRC_TARGET_BUFFER_ERROR = 2146
MQRC_INCOMPLETE_TRANSACTION = 2147
MQRC_IIH_ERROR = 2148
MQRC_PCF_ERROR = 2149
MQRC_DBCS_ERROR = 2150
MQRC_OBJECT_NAME_ERROR = 2152
MQRC_OBJECT_Q_MGR_NAME_ERROR = 2153
MQRC_RECS_PRESENT_ERROR = 2154
MQRC_OBJECT_RECORDS_ERROR = 2155
MQRC_RESPONSE_RECORDS_ERROR = 2156
MQRC_ASID_MISMATCH = 2157
MQRC_PMO_RECORD_FLAGS_ERROR = 2158
MQRC_PUT_MSG_RECORDS_ERROR = 2159
MQRC_CONN_ID_IN_USE = 2160
MQRC_Q_MGR_QUIESCING = 2161
MQRC_Q_MGR_STOPPING = 2162
MQRC_DUPLICATE_RECOV_COORD = 2163
MQRC_PMO_ERROR = 2173
MQRC_API_EXIT_NOT_FOUND = 2182
MQRC_API_EXIT_LOAD_ERROR = 2183
MQRC_REMOTE_Q_NAME_ERROR = 2184
MQRC_INCONSISTENT_PERSISTENCE = 2185
MQRC_GMO_ERROR = 2186
MQRC_CICS_BRIDGE_RESTRICTION = 2187
MQRC_STOPPED_BY_CLUSTER_EXIT = 2188
MQRC_CLUSTER_RESOLUTION_ERROR = 2189
MQRC_CONVERTED_STRING_TOO_BIG = 2190
MQRC_TMC_ERROR = 2191
MQRC_STORAGE_MEDIUM_FULL = 2192
MQRC_PAGESET_FULL = 2192
MQRC_PAGESET_ERROR = 2193
MQRC_NAME_NOT_VALID_FOR_TYPE = 2194
MQRC_UNEXPECTED_ERROR = 2195
MQRC_UNKNOWN_XMIT_Q = 2196
MQRC_UNKNOWN_DEF_XMIT_Q = 2197
MQRC_DEF_XMIT_Q_TYPE_ERROR = 2198
MQRC_DEF_XMIT_Q_USAGE_ERROR = 2199
MQRC_MSG_MARKED_BROWSE_CO_OP = 2200
MQRC_NAME_IN_USE = 2201
MQRC_CONNECTION_QUIESCING = 2202
MQRC_CONNECTION_STOPPING = 2203
MQRC_ADAPTER_NOT_AVAILABLE = 2204
MQRC_MSG_ID_ERROR = 2206
MQRC_CORREL_ID_ERROR = 2207
MQRC_FILE_SYSTEM_ERROR = 2208
MQRC_NO_MSG_LOCKED = 2209
MQRC_SOAP_DOTNET_ERROR = 2210
MQRC_SOAP_AXIS_ERROR = 2211
MQRC_SOAP_URL_ERROR = 2212
MQRC_FILE_NOT_AUDITED = 2216
MQRC_CONNECTION_NOT_AUTHORIZED = 2217
MQRC_MSG_TOO_BIG_FOR_CHANNEL = 2218
MQRC_CALL_IN_PROGRESS = 2219
MQRC_RMH_ERROR = 2220
MQRC_Q_MGR_ACTIVE = 2222
MQRC_Q_MGR_NOT_ACTIVE = 2223
MQRC_Q_DEPTH_HIGH = 2224
MQRC_Q_DEPTH_LOW = 2225
MQRC_Q_SERVICE_INTERVAL_HIGH = 2226
MQRC_Q_SERVICE_INTERVAL_OK = 2227
MQRC_RFH_HEADER_FIELD_ERROR = 2228
MQRC_RAS_PROPERTY_ERROR = 2229
MQRC_UNIT_OF_WORK_NOT_STARTED = 2232
MQRC_CHANNEL_AUTO_DEF_OK = 2233
MQRC_CHANNEL_AUTO_DEF_ERROR = 2234
MQRC_CFH_ERROR = 2235
MQRC_CFIL_ERROR = 2236
MQRC_CFIN_ERROR = 2237
MQRC_CFSL_ERROR = 2238
MQRC_CFST_ERROR = 2239
MQRC_INCOMPLETE_GROUP = 2241
MQRC_INCOMPLETE_MSG = 2242
MQRC_INCONSISTENT_CCSIDS = 2243
MQRC_INCONSISTENT_ENCODINGS = 2244
MQRC_INCONSISTENT_UOW = 2245
MQRC_INVALID_MSG_UNDER_CURSOR = 2246
MQRC_MATCH_OPTIONS_ERROR = 2247
MQRC_MDE_ERROR = 2248
MQRC_MSG_FLAGS_ERROR = 2249
MQRC_MSG_SEQ_NUMBER_ERROR = 2250
MQRC_OFFSET_ERROR = 2251
MQRC_ORIGINAL_LENGTH_ERROR = 2252
MQRC_SEGMENT_LENGTH_ZERO = 2253
MQRC_UOW_NOT_AVAILABLE = 2255
MQRC_WRONG_GMO_VERSION = 2256
MQRC_WRONG_MD_VERSION = 2257
MQRC_GROUP_ID_ERROR = 2258
MQRC_INCONSISTENT_BROWSE = 2259
MQRC_XQH_ERROR = 2260
MQRC_SRC_ENV_ERROR = 2261
MQRC_SRC_NAME_ERROR = 2262
MQRC_DEST_ENV_ERROR = 2263
MQRC_DEST_NAME_ERROR = 2264
MQRC_TM_ERROR = 2265
MQRC_CLUSTER_EXIT_ERROR = 2266
MQRC_CLUSTER_EXIT_LOAD_ERROR = 2267
MQRC_CLUSTER_PUT_INHIBITED = 2268
MQRC_CLUSTER_RESOURCE_ERROR = 2269
MQRC_NO_DESTINATIONS_AVAILABLE = 2270
MQRC_CONN_TAG_IN_USE = 2271
MQRC_PARTIALLY_CONVERTED = 2272
MQRC_CONNECTION_ERROR = 2273
MQRC_OPTION_ENVIRONMENT_ERROR = 2274
MQRC_CD_ERROR = 2277
MQRC_CLIENT_CONN_ERROR = 2278
MQRC_CHANNEL_STOPPED_BY_USER = 2279
MQRC_HCONFIG_ERROR = 2280
MQRC_FUNCTION_ERROR = 2281
MQRC_CHANNEL_STARTED = 2282
MQRC_CHANNEL_STOPPED = 2283
MQRC_CHANNEL_CONV_ERROR = 2284
MQRC_SERVICE_NOT_AVAILABLE = 2285
MQRC_INITIALIZATION_FAILED = 2286
MQRC_TERMINATION_FAILED = 2287
MQRC_UNKNOWN_Q_NAME = 2288
MQRC_SERVICE_ERROR = 2289
MQRC_Q_ALREADY_EXISTS = 2290
MQRC_USER_ID_NOT_AVAILABLE = 2291
MQRC_UNKNOWN_ENTITY = 2292
MQRC_UNKNOWN_AUTH_ENTITY = 2293
MQRC_UNKNOWN_REF_OBJECT = 2294
MQRC_CHANNEL_ACTIVATED = 2295
MQRC_CHANNEL_NOT_ACTIVATED = 2296
MQRC_UOW_CANCELED = 2297
MQRC_FUNCTION_NOT_SUPPORTED = 2298
MQRC_SELECTOR_TYPE_ERROR = 2299
MQRC_COMMAND_TYPE_ERROR = 2300
MQRC_MULTIPLE_INSTANCE_ERROR = 2301
MQRC_SYSTEM_ITEM_NOT_ALTERABLE = 2302
MQRC_BAG_CONVERSION_ERROR = 2303
MQRC_SELECTOR_OUT_OF_RANGE = 2304
MQRC_SELECTOR_NOT_UNIQUE = 2305
MQRC_INDEX_NOT_PRESENT = 2306
MQRC_STRING_ERROR = 2307
MQRC_ENCODING_NOT_SUPPORTED = 2308
MQRC_SELECTOR_NOT_PRESENT = 2309
MQRC_OUT_SELECTOR_ERROR = 2310
MQRC_STRING_TRUNCATED = 2311
MQRC_SELECTOR_WRONG_TYPE = 2312
MQRC_INCONSISTENT_ITEM_TYPE = 2313
MQRC_INDEX_ERROR = 2314
MQRC_SYSTEM_BAG_NOT_ALTERABLE = 2315
MQRC_ITEM_COUNT_ERROR = 2316
MQRC_FORMAT_NOT_SUPPORTED = 2317
MQRC_SELECTOR_NOT_SUPPORTED = 2318
MQRC_ITEM_VALUE_ERROR = 2319
MQRC_HBAG_ERROR = 2320
MQRC_PARAMETER_MISSING = 2321
MQRC_CMD_SERVER_NOT_AVAILABLE = 2322
MQRC_STRING_LENGTH_ERROR = 2323
MQRC_INQUIRY_COMMAND_ERROR = 2324
MQRC_NESTED_BAG_NOT_SUPPORTED = 2325
MQRC_BAG_WRONG_TYPE = 2326
MQRC_ITEM_TYPE_ERROR = 2327
MQRC_SYSTEM_BAG_NOT_DELETABLE = 2328
MQRC_SYSTEM_ITEM_NOT_DELETABLE = 2329
MQRC_CODED_CHAR_SET_ID_ERROR = 2330
MQRC_MSG_TOKEN_ERROR = 2331
MQRC_MISSING_WIH = 2332
MQRC_WIH_ERROR = 2333
MQRC_RFH_ERROR = 2334
MQRC_RFH_STRING_ERROR = 2335
MQRC_RFH_COMMAND_ERROR = 2336
MQRC_RFH_PARM_ERROR = 2337
MQRC_RFH_DUPLICATE_PARM = 2338
MQRC_RFH_PARM_MISSING = 2339
MQRC_CHAR_CONVERSION_ERROR = 2340
MQRC_UCS2_CONVERSION_ERROR = 2341
MQRC_DB2_NOT_AVAILABLE = 2342
MQRC_OBJECT_NOT_UNIQUE = 2343
MQRC_CONN_TAG_NOT_RELEASED = 2344
MQRC_CF_NOT_AVAILABLE = 2345
MQRC_CF_STRUC_IN_USE = 2346
MQRC_CF_STRUC_LIST_HDR_IN_USE = 2347
MQRC_CF_STRUC_AUTH_FAILED = 2348
MQRC_CF_STRUC_ERROR = 2349
MQRC_CONN_TAG_NOT_USABLE = 2350
MQRC_GLOBAL_UOW_CONFLICT = 2351
MQRC_LOCAL_UOW_CONFLICT = 2352
MQRC_HANDLE_IN_USE_FOR_UOW = 2353
MQRC_UOW_ENLISTMENT_ERROR = 2354
MQRC_UOW_MIX_NOT_SUPPORTED = 2355
MQRC_WXP_ERROR = 2356
MQRC_CURRENT_RECORD_ERROR = 2357
MQRC_NEXT_OFFSET_ERROR = 2358
MQRC_NO_RECORD_AVAILABLE = 2359
MQRC_OBJECT_LEVEL_INCOMPATIBLE = 2360
MQRC_NEXT_RECORD_ERROR = 2361
MQRC_BACKOUT_THRESHOLD_REACHED = 2362
MQRC_MSG_NOT_MATCHED = 2363
MQRC_JMS_FORMAT_ERROR = 2364
MQRC_SEGMENTS_NOT_SUPPORTED = 2365
MQRC_WRONG_CF_LEVEL = 2366
MQRC_CONFIG_CREATE_OBJECT = 2367
MQRC_CONFIG_CHANGE_OBJECT = 2368
MQRC_CONFIG_DELETE_OBJECT = 2369
MQRC_CONFIG_REFRESH_OBJECT = 2370
MQRC_CHANNEL_SSL_ERROR = 2371
MQRC_PARTICIPANT_NOT_DEFINED = 2372
MQRC_CF_STRUC_FAILED = 2373
MQRC_API_EXIT_ERROR = 2374
MQRC_API_EXIT_INIT_ERROR = 2375
MQRC_API_EXIT_TERM_ERROR = 2376
MQRC_EXIT_REASON_ERROR = 2377
MQRC_RESERVED_VALUE_ERROR = 2378
MQRC_NO_DATA_AVAILABLE = 2379
MQRC_SCO_ERROR = 2380
MQRC_KEY_REPOSITORY_ERROR = 2381
MQRC_CRYPTO_HARDWARE_ERROR = 2382
MQRC_AUTH_INFO_REC_COUNT_ERROR = 2383
MQRC_AUTH_INFO_REC_ERROR = 2384
MQRC_AIR_ERROR = 2385
MQRC_AUTH_INFO_TYPE_ERROR = 2386
MQRC_AUTH_INFO_CONN_NAME_ERROR = 2387
MQRC_LDAP_USER_NAME_ERROR = 2388
MQRC_LDAP_USER_NAME_LENGTH_ERR = 2389
MQRC_LDAP_PASSWORD_ERROR = 2390
MQRC_SSL_ALREADY_INITIALIZED = 2391
MQRC_SSL_CONFIG_ERROR = 2392
MQRC_SSL_INITIALIZATION_ERROR = 2393
MQRC_Q_INDEX_TYPE_ERROR = 2394
MQRC_CFBS_ERROR = 2395
MQRC_SSL_NOT_ALLOWED = 2396
MQRC_JSSE_ERROR = 2397
MQRC_SSL_PEER_NAME_MISMATCH = 2398
MQRC_SSL_PEER_NAME_ERROR = 2399
MQRC_UNSUPPORTED_CIPHER_SUITE = 2400
MQRC_SSL_CERTIFICATE_REVOKED = 2401
MQRC_SSL_CERT_STORE_ERROR = 2402
MQRC_CLIENT_EXIT_LOAD_ERROR = 2406
MQRC_CLIENT_EXIT_ERROR = 2407
MQRC_UOW_COMMITTED = 2408
MQRC_SSL_KEY_RESET_ERROR = 2409
MQRC_UNKNOWN_COMPONENT_NAME = 2410
MQRC_LOGGER_STATUS = 2411
MQRC_COMMAND_MQSC = 2412
MQRC_COMMAND_PCF = 2413
MQRC_CFIF_ERROR = 2414
MQRC_CFSF_ERROR = 2415
MQRC_CFGR_ERROR = 2416
MQRC_MSG_NOT_ALLOWED_IN_GROUP = 2417
MQRC_FILTER_OPERATOR_ERROR = 2418
MQRC_NESTED_SELECTOR_ERROR = 2419
MQRC_EPH_ERROR = 2420
MQRC_RFH_FORMAT_ERROR = 2421
MQRC_CFBF_ERROR = 2422
MQRC_CLIENT_CHANNEL_CONFLICT = 2423
MQRC_SD_ERROR = 2424
MQRC_TOPIC_STRING_ERROR = 2425
MQRC_STS_ERROR = 2426
MQRC_NO_SUBSCRIPTION = 2428
MQRC_SUBSCRIPTION_IN_USE = 2429
MQRC_STAT_TYPE_ERROR = 2430
MQRC_SUB_USER_DATA_ERROR = 2431
MQRC_SUB_ALREADY_EXISTS = 2432
MQRC_IDENTITY_MISMATCH = 2434
MQRC_ALTER_SUB_ERROR = 2435
MQRC_DURABILITY_NOT_ALLOWED = 2436
MQRC_NO_RETAINED_MSG = 2437
MQRC_SRO_ERROR = 2438
MQRC_SUB_NAME_ERROR = 2440
MQRC_OBJECT_STRING_ERROR = 2441
MQRC_PROPERTY_NAME_ERROR = 2442
MQRC_SEGMENTATION_NOT_ALLOWED = 2443
MQRC_CBD_ERROR = 2444
MQRC_CTLO_ERROR = 2445
MQRC_NO_CALLBACKS_ACTIVE = 2446
MQRC_CALLBACK_NOT_REGISTERED = 2448
MQRC_OPTIONS_CHANGED = 2457
MQRC_READ_AHEAD_MSGS = 2458
MQRC_SELECTOR_SYNTAX_ERROR = 2459
MQRC_HMSG_ERROR = 2460
MQRC_CMHO_ERROR = 2461
MQRC_DMHO_ERROR = 2462
MQRC_SMPO_ERROR = 2463
MQRC_IMPO_ERROR = 2464
MQRC_PROPERTY_NAME_TOO_BIG = 2465
MQRC_PROP_VALUE_NOT_CONVERTED = 2466
MQRC_PROP_TYPE_NOT_SUPPORTED = 2467
MQRC_PROPERTY_VALUE_TOO_BIG = 2469
MQRC_PROP_CONV_NOT_SUPPORTED = 2470
MQRC_PROPERTY_NOT_AVAILABLE = 2471
MQRC_PROP_NUMBER_FORMAT_ERROR = 2472
MQRC_PROPERTY_TYPE_ERROR = 2473
MQRC_PROPERTIES_TOO_BIG = 2478
MQRC_PUT_NOT_RETAINED = 2479
MQRC_ALIAS_TARGTYPE_CHANGED = 2480
MQRC_DMPO_ERROR = 2481
MQRC_PD_ERROR = 2482
MQRC_CALLBACK_TYPE_ERROR = 2483
MQRC_CBD_OPTIONS_ERROR = 2484
MQRC_MAX_MSG_LENGTH_ERROR = 2485
MQRC_CALLBACK_ROUTINE_ERROR = 2486
MQRC_CALLBACK_LINK_ERROR = 2487
MQRC_OPERATION_ERROR = 2488
MQRC_BMHO_ERROR = 2489
MQRC_UNSUPPORTED_PROPERTY = 2490
MQRC_PROP_NAME_NOT_CONVERTED = 2492
MQRC_GET_ENABLED = 2494
MQRC_MODULE_NOT_FOUND = 2495
MQRC_MODULE_INVALID = 2496
MQRC_MODULE_ENTRY_NOT_FOUND = 2497
MQRC_MIXED_CONTENT_NOT_ALLOWED = 2498
MQRC_MSG_HANDLE_IN_USE = 2499
MQRC_HCONN_ASYNC_ACTIVE = 2500
MQRC_MHBO_ERROR = 2501
MQRC_PUBLICATION_FAILURE = 2502
MQRC_SUB_INHIBITED = 2503
MQRC_SELECTOR_ALWAYS_FALSE = 2504
MQRC_XEPO_ERROR = 2507
MQRC_DURABILITY_NOT_ALTERABLE = 2509
MQRC_TOPIC_NOT_ALTERABLE = 2510
MQRC_SUBLEVEL_NOT_ALTERABLE = 2512
MQRC_PROPERTY_NAME_LENGTH_ERR = 2513
MQRC_DUPLICATE_GROUP_SUB = 2514
MQRC_GROUPING_NOT_ALTERABLE = 2515
MQRC_SELECTOR_INVALID_FOR_TYPE = 2516
MQRC_HOBJ_QUIESCED = 2517
MQRC_HOBJ_QUIESCED_NO_MSGS = 2518
MQRC_SELECTION_STRING_ERROR = 2519
MQRC_RES_OBJECT_STRING_ERROR = 2520
MQRC_CONNECTION_SUSPENDED = 2521
MQRC_INVALID_DESTINATION = 2522
MQRC_INVALID_SUBSCRIPTION = 2523
MQRC_SELECTOR_NOT_ALTERABLE = 2524
MQRC_RETAINED_MSG_Q_ERROR = 2525
MQRC_RETAINED_NOT_DELIVERED = 2526
MQRC_RFH_RESTRICTED_FORMAT_ERR = 2527
MQRC_CONNECTION_STOPPED = 2528
MQRC_ASYNC_UOW_CONFLICT = 2529
MQRC_ASYNC_XA_CONFLICT = 2530
MQRC_PUBSUB_INHIBITED = 2531
MQRC_MSG_HANDLE_COPY_FAILURE = 2532
MQRC_DEST_CLASS_NOT_ALTERABLE = 2533
MQRC_OPERATION_NOT_ALLOWED = 2534
MQRC_ACTION_ERROR = 2535
MQRC_CHANNEL_NOT_AVAILABLE = 2537
MQRC_HOST_NOT_AVAILABLE = 2538
MQRC_CHANNEL_CONFIG_ERROR = 2539
MQRC_UNKNOWN_CHANNEL_NAME = 2540
MQRC_LOOPING_PUBLICATION = 2541
MQRC_ALREADY_JOINED = 2542
MQRC_STANDBY_Q_MGR = 2543
MQRC_RECONNECTING = 2544
MQRC_RECONNECTED = 2545
MQRC_RECONNECT_QMID_MISMATCH = 2546
MQRC_RECONNECT_INCOMPATIBLE = 2547
MQRC_RECONNECT_FAILED = 2548
MQRC_CALL_INTERRUPTED = 2549
MQRC_NO_SUBS_MATCHED = 2550
MQRC_SELECTION_NOT_AVAILABLE = 2551
MQRC_CHANNEL_SSL_WARNING = 2552
MQRC_OCSP_URL_ERROR = 2553
MQRC_CONTENT_ERROR = 2554
MQRC_RECONNECT_Q_MGR_REQD = 2555
MQRC_RECONNECT_TIMED_OUT = 2556
MQRC_PUBLISH_EXIT_ERROR = 2557
MQRC_COMMINFO_ERROR = 2558
MQRC_DEF_SYNCPOINT_INHIBITED = 2559
MQRC_MULTICAST_ONLY = 2560
MQRC_DATA_SET_NOT_AVAILABLE = 2561
MQRC_GROUPING_NOT_ALLOWED = 2562
MQRC_GROUP_ADDRESS_ERROR = 2563
MQRC_MULTICAST_CONFIG_ERROR = 2564
MQRC_MULTICAST_INTERFACE_ERROR = 2565
MQRC_MULTICAST_SEND_ERROR = 2566
MQRC_MULTICAST_INTERNAL_ERROR = 2567
MQRC_CONNECTION_NOT_AVAILABLE = 2568
MQRC_SYNCPOINT_NOT_ALLOWED = 2569
MQRC_SSL_ALT_PROVIDER_REQUIRED = 2570
MQRC_MCAST_PUB_STATUS = 2571
MQRC_MCAST_SUB_STATUS = 2572
MQRC_PRECONN_EXIT_LOAD_ERROR = 2573
MQRC_PRECONN_EXIT_NOT_FOUND = 2574
MQRC_PRECONN_EXIT_ERROR = 2575
MQRC_CD_ARRAY_ERROR = 2576
MQRC_CHANNEL_BLOCKED = 2577
MQRC_CHANNEL_BLOCKED_WARNING = 2578
MQRC_SUBSCRIPTION_CREATE = 2579
MQRC_SUBSCRIPTION_DELETE = 2580
MQRC_SUBSCRIPTION_CHANGE = 2581
MQRC_SUBSCRIPTION_REFRESH = 2582
MQRC_INSTALLATION_MISMATCH = 2583
MQRC_NOT_PRIVILEGED = 2584
MQRC_PROPERTIES_DISABLED = 2586
MQRC_HMSG_NOT_AVAILABLE = 2587
MQRC_EXIT_PROPS_NOT_SUPPORTED = 2588
MQRC_INSTALLATION_MISSING = 2589
MQRC_FASTPATH_NOT_AVAILABLE = 2590
MQRC_CIPHER_SPEC_NOT_SUITE_B = 2591
MQRC_SUITE_B_ERROR = 2592
MQRC_CERT_VAL_POLICY_ERROR = 2593
MQRC_PASSWORD_PROTECTION_ERROR = 2594
MQRC_CSP_ERROR = 2595
MQRC_CERT_LABEL_NOT_ALLOWED = 2596
MQRC_ADMIN_TOPIC_STRING_ERROR = 2598
MQRC_AMQP_NOT_AVAILABLE = 2599
MQRC_CCDT_URL_ERROR = 2600
MQRC_Q_MGR_RECONNECT_REQUESTED = 2601
MQRC_BNO_ERROR = 2602
MQRC_OUTBOUND_SNI_NOT_VALID = 2603
MQRC_REOPEN_EXCL_INPUT_ERROR = 6100
MQRC_REOPEN_INQUIRE_ERROR = 6101
MQRC_REOPEN_SAVED_CONTEXT_ERR = 6102
MQRC_REOPEN_TEMPORARY_Q_ERROR = 6103
MQRC_ATTRIBUTE_LOCKED = 6104
MQRC_CURSOR_NOT_VALID = 6105
MQRC_ENCODING_ERROR = 6106
MQRC_STRUC_ID_ERROR = 6107
MQRC_NULL_POINTER = 6108
MQRC_NO_CONNECTION_REFERENCE = 6109
MQRC_NO_BUFFER = 6110
MQRC_BINARY_DATA_LENGTH_ERROR = 6111
MQRC_BUFFER_NOT_AUTOMATIC = 6112
MQRC_INSUFFICIENT_BUFFER = 6113
MQRC_INSUFFICIENT_DATA = 6114
MQRC_DATA_TRUNCATED = 6115
MQRC_ZERO_LENGTH = 6116
MQRC_NEGATIVE_LENGTH = 6117
MQRC_NEGATIVE_OFFSET = 6118
MQRC_INCONSISTENT_FORMAT = 6119
MQRC_INCONSISTENT_OBJECT_STATE = 6120
MQRC_CONTEXT_OBJECT_NOT_VALID = 6121
MQRC_CONTEXT_OPEN_ERROR = 6122
MQRC_STRUC_LENGTH_ERROR = 6123
MQRC_NOT_CONNECTED = 6124
MQRC_NOT_OPEN = 6125
MQRC_DISTRIBUTION_LIST_EMPTY = 6126
MQRC_INCONSISTENT_OPEN_OPTIONS = 6127
MQRC_WRONG_VERSION = 6128
MQRC_REFERENCE_ERROR = 6129
MQRC_XR_NOT_AVAILABLE = 6130
MQRC_SUB_JOIN_NOT_ALTERABLE = 29440


##################################################################
# Values Related to Queue Attributes                             #
##################################################################

# Queue Types
MQQT_LOCAL = 1
MQQT_MODEL = 2
MQQT_ALIAS = 3
MQQT_REMOTE = 6
MQQT_CLUSTER = 7

# Cluster Queue Types
MQCQT_LOCAL_Q = 1
MQCQT_ALIAS_Q = 2
MQCQT_REMOTE_Q = 3
MQCQT_Q_MGR_ALIAS = 4

# Extended Queue Types
MQQT_ALL = 1001

# Queue Definition Types
MQQDT_PREDEFINED = 1
MQQDT_PERMANENT_DYNAMIC = 2
MQQDT_TEMPORARY_DYNAMIC = 3
MQQDT_SHARED_DYNAMIC = 4

# Inhibit Get Values
MQQA_GET_INHIBITED = 1
MQQA_GET_ALLOWED = 0

# Inhibit Put Values
MQQA_PUT_INHIBITED = 1
MQQA_PUT_ALLOWED = 0

# Queue Shareability
MQQA_SHAREABLE = 1
MQQA_NOT_SHAREABLE = 0

# Back-Out Hardening
MQQA_BACKOUT_HARDENED = 1
MQQA_BACKOUT_NOT_HARDENED = 0

# Message Delivery Sequence
MQMDS_PRIORITY = 0
MQMDS_FIFO = 1

# Nonpersistent Message Class
MQNPM_CLASS_NORMAL = 0
MQNPM_CLASS_HIGH = 10

# Trigger Controls
MQTC_OFF = 0
MQTC_ON = 1

# Trigger Types
MQTT_NONE = 0
MQTT_FIRST = 1
MQTT_EVERY = 2
MQTT_DEPTH = 3

# Trigger Restart
MQTRIGGER_RESTART_NO = 0
MQTRIGGER_RESTART_YES = 1

# Queue Usages
MQUS_NORMAL = 0
MQUS_TRANSMISSION = 1

# Distribution Lists
MQDL_SUPPORTED = 1
MQDL_NOT_SUPPORTED = 0

# Index Types
MQIT_NONE = 0
MQIT_MSG_ID = 1
MQIT_CORREL_ID = 2
MQIT_MSG_TOKEN = 4
MQIT_GROUP_ID = 5

# Default Bindings
MQBND_BIND_ON_OPEN = 0
MQBND_BIND_NOT_FIXED = 1
MQBND_BIND_ON_GROUP = 2

# Queue Sharing Group Dispositions
MQQSGD_ALL = (-1)
MQQSGD_Q_MGR = 0
MQQSGD_COPY = 1
MQQSGD_SHARED = 2
MQQSGD_GROUP = 3
MQQSGD_PRIVATE = 4
MQQSGD_LIVE = 6

# Reorganization Controls
MQREORG_DISABLED = 0
MQREORG_ENABLED = 1

# Max queue file size values
MQQFS_DEFAULT = (-1)

# Read Ahead Values
MQREADA_NO = 0
MQREADA_YES = 1
MQREADA_DISABLED = 2
MQREADA_INHIBITED = 3
MQREADA_BACKLOG = 4

# Queue and Channel Property Control Values
MQPROP_COMPATIBILITY = 0
MQPROP_NONE = 1
MQPROP_ALL = 2
MQPROP_FORCE_MQRFH2 = 3
MQPROP_V6COMPAT = 4

# Streaming Queue Quality of Service Values
MQST_BEST_EFFORT = 0
MQST_MUST_DUP = 1


##################################################################
# Values Related to Namelist Attributes                          #
##################################################################

# Name Count
MQNC_MAX_NAMELIST_NAME_COUNT = 256

# Namelist Types
MQNT_NONE = 0
MQNT_Q = 1
MQNT_CLUSTER = 2
MQNT_AUTH_INFO = 4
MQNT_ALL = 1001


##################################################################
# Values Related to CF-Structure Attributes                      #
##################################################################

# CF Recoverability
MQCFR_YES = 1
MQCFR_NO = 0

# CF Automatic Recovery
MQRECAUTO_NO = 0
MQRECAUTO_YES = 1

# CF Loss of Connectivity Action
MQCFCONLOS_TERMINATE = 0
MQCFCONLOS_TOLERATE = 1
MQCFCONLOS_ASQMGR = 2


##################################################################
# Values Related to Service Attributes                           #
##################################################################

# Service Types
MQSVC_TYPE_COMMAND = 0
MQSVC_TYPE_SERVER = 1


##################################################################
# Values Related to QueueManager Attributes                      #
##################################################################

# Adopt New MCA Checks
MQADOPT_CHECK_NONE = 0
MQADOPT_CHECK_ALL = 1
MQADOPT_CHECK_Q_MGR_NAME = 2
MQADOPT_CHECK_NET_ADDR = 4
MQADOPT_CHECK_CHANNEL_NAME = 8

# Adopt New MCA Types
MQADOPT_TYPE_NO = 0
MQADOPT_TYPE_ALL = 1
MQADOPT_TYPE_SVR = 2
MQADOPT_TYPE_SDR = 4
MQADOPT_TYPE_RCVR = 8
MQADOPT_TYPE_CLUSRCVR = 16

# Autostart
MQCHAD_DISABLED = 0
MQCHAD_ENABLED = 1

# Cluster Workload
MQCLWL_USEQ_LOCAL = 0
MQCLWL_USEQ_ANY = 1
MQCLWL_USEQ_AS_Q_MGR = (-3)

# Command Levels
MQCMDL_LEVEL_1 = 100
MQCMDL_LEVEL_101 = 101
MQCMDL_LEVEL_110 = 110
MQCMDL_LEVEL_114 = 114
MQCMDL_LEVEL_120 = 120
MQCMDL_LEVEL_200 = 200
MQCMDL_LEVEL_201 = 201
MQCMDL_LEVEL_210 = 210
MQCMDL_LEVEL_211 = 211
MQCMDL_LEVEL_220 = 220
MQCMDL_LEVEL_221 = 221
MQCMDL_LEVEL_230 = 230
MQCMDL_LEVEL_320 = 320
MQCMDL_LEVEL_420 = 420
MQCMDL_LEVEL_500 = 500
MQCMDL_LEVEL_510 = 510
MQCMDL_LEVEL_520 = 520
MQCMDL_LEVEL_530 = 530
MQCMDL_LEVEL_531 = 531
MQCMDL_LEVEL_600 = 600
MQCMDL_LEVEL_700 = 700
MQCMDL_LEVEL_701 = 701
MQCMDL_LEVEL_710 = 710
MQCMDL_LEVEL_711 = 711
MQCMDL_LEVEL_750 = 750
MQCMDL_LEVEL_800 = 800
MQCMDL_LEVEL_801 = 801
MQCMDL_LEVEL_802 = 802
MQCMDL_LEVEL_900 = 900
MQCMDL_LEVEL_901 = 901
MQCMDL_LEVEL_902 = 902
MQCMDL_LEVEL_903 = 903
MQCMDL_LEVEL_904 = 904
MQCMDL_LEVEL_905 = 905
MQCMDL_LEVEL_910 = 910
MQCMDL_LEVEL_911 = 911
MQCMDL_LEVEL_912 = 912
MQCMDL_LEVEL_913 = 913
MQCMDL_LEVEL_914 = 914
MQCMDL_LEVEL_915 = 915
MQCMDL_LEVEL_920 = 920
MQCMDL_LEVEL_921 = 921
MQCMDL_LEVEL_922 = 922
MQCMDL_LEVEL_923 = 923
MQCMDL_LEVEL_924 = 924
MQCMDL_LEVEL_925 = 925
MQCMDL_CURRENT_LEVEL = 925

# Command Server Options
MQCSRV_CONVERT_NO = 0
MQCSRV_CONVERT_YES = 1
MQCSRV_DLQ_NO = 0
MQCSRV_DLQ_YES = 1

# DNS WLM
MQDNSWLM_NO = 0
MQDNSWLM_YES = 1

# Expiration Scan Interval
MQEXPI_OFF = 0

# Intra-Group Queuing
MQIGQ_DISABLED = 0
MQIGQ_ENABLED = 1

# Intra-Group Queuing Put Authority
MQIGQPA_DEFAULT = 1
MQIGQPA_CONTEXT = 2
MQIGQPA_ONLY_IGQ = 3
MQIGQPA_ALTERNATE_OR_IGQ = 4

# IP Address Versions
MQIPADDR_IPV4 = 0
MQIPADDR_IPV6 = 1

# Message Mark-Browse Interval
MQMMBI_UNLIMITED = (-1)

# Monitoring Values
MQMON_NOT_AVAILABLE = (-1)
MQMON_NONE = (-1)
MQMON_Q_MGR = (-3)
MQMON_OFF = 0
MQMON_ON = 1
MQMON_DISABLED = 0
MQMON_ENABLED = 1
MQMON_LOW = 17
MQMON_MEDIUM = 33
MQMON_HIGH = 65

# Application Function Types
MQFUN_TYPE_UNKNOWN = 0
MQFUN_TYPE_JVM = 1
MQFUN_TYPE_PROGRAM = 2
MQFUN_TYPE_PROCEDURE = 3
MQFUN_TYPE_USERDEF = 4
MQFUN_TYPE_COMMAND = 5

# Application Activity Trace Detail
MQACTV_DETAIL_LOW = 1
MQACTV_DETAIL_MEDIUM = 2
MQACTV_DETAIL_HIGH = 3

# Platforms
MQPL_MVS = 1
MQPL_OS390 = 1
MQPL_ZOS = 1
MQPL_OS2 = 2
MQPL_AIX = 3
MQPL_UNIX = 3
MQPL_OS400 = 4
MQPL_WINDOWS = 5
MQPL_WINDOWS_NT = 11
MQPL_VMS = 12
MQPL_NSK = 13
MQPL_NSS = 13
MQPL_OPEN_TP1 = 15
MQPL_VM = 18
MQPL_TPF = 23
MQPL_VSE = 27
MQPL_APPLIANCE = 28
MQPL_NATIVE = 11

# Maximum Properties Length
MQPROP_UNRESTRICTED_LENGTH = (-1)

# Pub/Sub Mode
MQPSM_DISABLED = 0
MQPSM_COMPAT = 1
MQPSM_ENABLED = 2

# Pub/Sub Clusters
MQPSCLUS_DISABLED = 0
MQPSCLUS_ENABLED = 1

# Control Options
MQQMOPT_DISABLED = 0
MQQMOPT_ENABLED = 1
MQQMOPT_REPLY = 2

# Receive Timeout Types
MQRCVTIME_MULTIPLY = 0
MQRCVTIME_ADD = 1
MQRCVTIME_EQUAL = 2

# Recording Options
MQRECORDING_DISABLED = 0
MQRECORDING_Q = 1
MQRECORDING_MSG = 2

# Security Case
MQSCYC_UPPER = 0
MQSCYC_MIXED = 1

# Shared Queue Queue Manager Name
MQSQQM_USE = 0
MQSQQM_IGNORE = 1

# SSL FIPS Requirements
MQSSL_FIPS_NO = 0
MQSSL_FIPS_YES = 1

# Syncpoint Availability
MQSP_AVAILABLE = 1
MQSP_NOT_AVAILABLE = 0

# Service Controls
MQSVC_CONTROL_Q_MGR = 0
MQSVC_CONTROL_Q_MGR_START = 1
MQSVC_CONTROL_MANUAL = 2

# TCP Keepalive
MQTCPKEEP_NO = 0
MQTCPKEEP_YES = 1

# TCP Stack Types
MQTCPSTACK_SINGLE = 0
MQTCPSTACK_MULTIPLE = 1

# Channel Initiator Trace Autostart
MQTRAXSTR_NO = 0
MQTRAXSTR_YES = 1

# Capability
MQCAP_NOT_SUPPORTED = 0
MQCAP_SUPPORTED = 1
MQCAP_EXPIRED = 2

# Media Image Scheduling
MQMEDIMGSCHED_MANUAL = 0
MQMEDIMGSCHED_AUTO = 1

# Automatic Media Image Interval
MQMEDIMGINTVL_OFF = 0

# Automatic Media Image Log Length
MQMEDIMGLOGLN_OFF = 0

# Media Image Recoverability
MQIMGRCOV_NO = 0
MQIMGRCOV_YES = 1
MQIMGRCOV_AS_Q_MGR = 2


##################################################################
# Values Related to Topic Attributes                             #
##################################################################

# Persistent/Non-persistent Message Delivery
MQDLV_AS_PARENT = 0
MQDLV_ALL = 1
MQDLV_ALL_DUR = 2
MQDLV_ALL_AVAIL = 3

# Master Administration
MQMASTER_NO = 0
MQMASTER_YES = 1

# Publish Scope
MQSCOPE_ALL = 0
MQSCOPE_AS_PARENT = 1
MQSCOPE_QMGR = 4

# Durable Subscriptions
MQSUB_DURABLE_AS_PARENT = 0
MQSUB_DURABLE_ALLOWED = 1
MQSUB_DURABLE_INHIBITED = 2

# Wildcards
MQTA_BLOCK = 1
MQTA_PASSTHRU = 2

# Subscriptions Allowed
MQTA_SUB_AS_PARENT = 0
MQTA_SUB_INHIBITED = 1
MQTA_SUB_ALLOWED = 2

# Proxy Sub Propagation
MQTA_PROXY_SUB_FORCE = 1
MQTA_PROXY_SUB_FIRSTUSE = 2

# Publications Allowed
MQTA_PUB_AS_PARENT = 0
MQTA_PUB_INHIBITED = 1
MQTA_PUB_ALLOWED = 2

# Topic Type
MQTOPT_LOCAL = 0
MQTOPT_CLUSTER = 1
MQTOPT_ALL = 2

# Multicast
MQMC_AS_PARENT = 0
MQMC_ENABLED = 1
MQMC_DISABLED = 2
MQMC_ONLY = 3

# CommInfo Type
MQCIT_MULTICAST = 1


##################################################################
# Values Related to Subscription Attributes                      #
##################################################################

# Destination Class
MQDC_MANAGED = 1
MQDC_PROVIDED = 2

# Pub/Sub Message Properties
MQPSPROP_NONE = 0
MQPSPROP_COMPAT = 1
MQPSPROP_RFH2 = 2
MQPSPROP_MSGPROP = 3

# Request Only
MQRU_PUBLISH_ON_REQUEST = 1
MQRU_PUBLISH_ALL = 2

# Durable Subscriptions
MQSUB_DURABLE_ALL = (-1)
MQSUB_DURABLE_YES = 1
MQSUB_DURABLE_NO = 2

# Subscription Scope
MQTSCOPE_QMGR = 1
MQTSCOPE_ALL = 2

# Variable User ID
MQVU_FIXED_USER = 1
MQVU_ANY_USER = 2

# Wildcard Schema
MQWS_DEFAULT = 0
MQWS_CHAR = 1
MQWS_TOPIC = 2


##################################################################
# Values Related to Channel Authentication Configuration         #
# Attributes                                                     #
##################################################################

# User Source Options
MQUSRC_MAP = 0
MQUSRC_NOACCESS = 1
MQUSRC_CHANNEL = 2

# Warn Options
MQWARN_YES = 1
MQWARN_NO = 0

# DSBlock Options
MQDSB_DEFAULT = 0
MQDSB_8K = 1
MQDSB_16K = 2
MQDSB_32K = 3
MQDSB_64K = 4
MQDSB_128K = 5
MQDSB_256K = 6
MQDSB_512K = 7
MQDSB_1024K = 8
MQDSB_1M = 8

# DSExpand Options
MQDSE_DEFAULT = 0
MQDSE_YES = 1
MQDSE_NO = 2

# OffldUse Options
MQCFOFFLD_NONE = 0
MQCFOFFLD_SMDS = 1
MQCFOFFLD_DB2 = 2
MQCFOFFLD_BOTH = 3

# Use Dead Letter Queue Options
MQUSEDLQ_AS_PARENT = 0
MQUSEDLQ_NO = 1
MQUSEDLQ_YES = 2


##################################################################
# Constants for MQ Extended Reach                                #
##################################################################

# General Constants
MQ_MQTT_MAX_KEEP_ALIVE = 65536
MQ_SSL_KEY_PASSPHRASE_LENGTH = 1024


##################################################################
# Values Related to MQCLOSE Function                             #
##################################################################

# Object Handle
MQHO_UNUSABLE_HOBJ = (-1)
MQHO_NONE = 0

# Close Options
MQCO_IMMEDIATE = 0x00000000
MQCO_NONE = 0x00000000
MQCO_DELETE = 0x00000001
MQCO_DELETE_PURGE = 0x00000002
MQCO_KEEP_SUB = 0x00000004
MQCO_REMOVE_SUB = 0x00000008
MQCO_QUIESCE = 0x00000020


##################################################################
# Values Related to MQCTL and MQCB Functions                     #
##################################################################

# Operation codes for MQCTL
MQOP_START = 0x00000001
MQOP_START_WAIT = 0x00000002
MQOP_STOP = 0x00000004

# Operation codes for MQCB
MQOP_REGISTER = 0x00000100
MQOP_DEREGISTER = 0x00000200

# Operation codes for MQCTL and MQCB
MQOP_SUSPEND = 0x00010000
MQOP_RESUME = 0x00020000


##################################################################
# Values Related to MQDLTMH Function                             #
##################################################################

# Message handle
MQHM_UNUSABLE_HMSG = (-1)
MQHM_NONE = 0


##################################################################
# Values Related to MQINQ Function                               #
##################################################################

# Byte Attribute Selectors
MQBA_FIRST = 6001
MQBA_LAST = 8000

# Character Attribute Selectors
MQCA_ADMIN_TOPIC_NAME = 2105
MQCA_ALTERATION_DATE = 2027
MQCA_ALTERATION_TIME = 2028
MQCA_AMQP_SSL_CIPHER_SUITES = 2137
MQCA_AMQP_VERSION = 2136
MQCA_APPL_ID = 2001
MQCA_AUTH_INFO_CONN_NAME = 2053
MQCA_AUTH_INFO_DESC = 2046
MQCA_AUTH_INFO_NAME = 2045
MQCA_AUTH_INFO_OCSP_URL = 2109
MQCA_AUTO_REORG_CATALOG = 2091
MQCA_AUTO_REORG_START_TIME = 2090
MQCA_BACKOUT_REQ_Q_NAME = 2019
MQCA_BASE_OBJECT_NAME = 2002
MQCA_BASE_Q_NAME = 2002
MQCA_BATCH_INTERFACE_ID = 2068
MQCA_CERT_LABEL = 2121
MQCA_CF_STRUC_DESC = 2052
MQCA_CF_STRUC_NAME = 2039
MQCA_CHANNEL_AUTO_DEF_EXIT = 2026
MQCA_CHILD = 2101
MQCA_CHINIT_SERVICE_PARM = 2076
MQCA_CHLAUTH_DESC = 2118
MQCA_CICS_FILE_NAME = 2060
MQCA_CLUSTER_DATE = 2037
MQCA_CLUSTER_NAME = 2029
MQCA_CLUSTER_NAMELIST = 2030
MQCA_CLUSTER_Q_MGR_NAME = 2031
MQCA_CLUSTER_TIME = 2038
MQCA_CLUSTER_WORKLOAD_DATA = 2034
MQCA_CLUSTER_WORKLOAD_EXIT = 2033
MQCA_CLUS_CHL_NAME = 2124
MQCA_COMMAND_INPUT_Q_NAME = 2003
MQCA_COMMAND_REPLY_Q_NAME = 2067
MQCA_COMM_INFO_DESC = 2111
MQCA_COMM_INFO_NAME = 2110
MQCA_CONN_AUTH = 2125
MQCA_CREATION_DATE = 2004
MQCA_CREATION_TIME = 2005
MQCA_CUSTOM = 2119
MQCA_DEAD_LETTER_Q_NAME = 2006
MQCA_DEF_XMIT_Q_NAME = 2025
MQCA_DNS_GROUP = 2071
MQCA_ENV_DATA = 2007
MQCA_FIRST = 2001
MQCA_IGQ_USER_ID = 2041
MQCA_INITIATION_Q_NAME = 2008
MQCA_INSTALLATION_DESC = 2115
MQCA_INSTALLATION_NAME = 2116
MQCA_INSTALLATION_PATH = 2117
MQCA_LAST = 4000
MQCA_LAST_USED = 2138
MQCA_LDAP_BASE_DN_GROUPS = 2132
MQCA_LDAP_BASE_DN_USERS = 2126
MQCA_LDAP_FIND_GROUP_FIELD = 2135
MQCA_LDAP_GROUP_ATTR_FIELD = 2134
MQCA_LDAP_GROUP_OBJECT_CLASS = 2133
MQCA_LDAP_PASSWORD = 2048
MQCA_LDAP_SHORT_USER_FIELD = 2127
MQCA_LDAP_USER_ATTR_FIELD = 2129
MQCA_LDAP_USER_NAME = 2047
MQCA_LDAP_USER_OBJECT_CLASS = 2128
MQCA_LU62_ARM_SUFFIX = 2074
MQCA_LU_GROUP_NAME = 2072
MQCA_LU_NAME = 2073
MQCA_MODEL_DURABLE_Q = 2096
MQCA_MODEL_NON_DURABLE_Q = 2097
MQCA_MONITOR_Q_NAME = 2066
MQCA_NAMELIST_DESC = 2009
MQCA_NAMELIST_NAME = 2010
MQCA_NAMES = 2020
MQCA_PARENT = 2102
MQCA_PASS_TICKET_APPL = 2086
MQCA_POLICY_NAME = 2112
MQCA_PROCESS_DESC = 2011
MQCA_PROCESS_NAME = 2012
MQCA_QSG_CERT_LABEL = 2131
MQCA_QSG_NAME = 2040
MQCA_Q_DESC = 2013
MQCA_Q_MGR_DESC = 2014
MQCA_Q_MGR_IDENTIFIER = 2032
MQCA_Q_MGR_NAME = 2015
MQCA_Q_NAME = 2016
MQCA_RECIPIENT_DN = 2114
MQCA_REMOTE_Q_MGR_NAME = 2017
MQCA_REMOTE_Q_NAME = 2018
MQCA_REPOSITORY_NAME = 2035
MQCA_REPOSITORY_NAMELIST = 2036
MQCA_RESUME_DATE = 2098
MQCA_RESUME_TIME = 2099
MQCA_SERVICE_DESC = 2078
MQCA_SERVICE_NAME = 2077
MQCA_SERVICE_START_ARGS = 2080
MQCA_SERVICE_START_COMMAND = 2079
MQCA_SERVICE_STOP_ARGS = 2082
MQCA_SERVICE_STOP_COMMAND = 2081
MQCA_SIGNER_DN = 2113
MQCA_SSL_CERT_ISSUER_NAME = 2130
MQCA_SSL_CRL_NAMELIST = 2050
MQCA_SSL_CRYPTO_HARDWARE = 2051
MQCA_SSL_KEY_LIBRARY = 2069
MQCA_SSL_KEY_MEMBER = 2070
MQCA_SSL_KEY_REPOSITORY = 2049
MQCA_STDERR_DESTINATION = 2084
MQCA_STDOUT_DESTINATION = 2083
MQCA_STORAGE_CLASS = 2022
MQCA_STORAGE_CLASS_DESC = 2042
MQCA_STREAM_QUEUE_NAME = 2138
MQCA_SYSTEM_LOG_Q_NAME = 2065
MQCA_TCP_NAME = 2075
MQCA_TOPIC_DESC = 2093
MQCA_TOPIC_NAME = 2092
MQCA_TOPIC_STRING = 2094
MQCA_TOPIC_STRING_FILTER = 2108
MQCA_TPIPE_NAME = 2085
MQCA_TRIGGER_CHANNEL_NAME = 2064
MQCA_TRIGGER_DATA = 2023
MQCA_TRIGGER_PROGRAM_NAME = 2062
MQCA_TRIGGER_TERM_ID = 2063
MQCA_TRIGGER_TRANS_ID = 2061
MQCA_USER_DATA = 2021
MQCA_USER_LIST = 4000
MQCA_VERSION = 2120
MQCA_XCF_GROUP_NAME = 2043
MQCA_XCF_MEMBER_NAME = 2044
MQCA_XMIT_Q_NAME = 2024
MQCA_XR_SSL_CIPHER_SUITES = 2123
MQCA_XR_VERSION = 2122

# Integer Attribute Selectors
MQIA_ACCOUNTING_CONN_OVERRIDE = 136
MQIA_ACCOUNTING_INTERVAL = 135
MQIA_ACCOUNTING_MQI = 133
MQIA_ACCOUNTING_Q = 134
MQIA_ACTIVE_CHANNELS = 100
MQIA_ACTIVITY_CONN_OVERRIDE = 239
MQIA_ACTIVITY_RECORDING = 138
MQIA_ACTIVITY_TRACE = 240
MQIA_ADOPTNEWMCA_CHECK = 102
MQIA_ADOPTNEWMCA_INTERVAL = 104
MQIA_ADOPTNEWMCA_TYPE = 103
MQIA_ADOPT_CONTEXT = 260
MQIA_ADVANCED_CAPABILITY = 273
MQIA_AMQP_CAPABILITY = 265
MQIA_APPL_TYPE = 1
MQIA_ARCHIVE = 60
MQIA_AUTHENTICATION_FAIL_DELAY = 259
MQIA_AUTHENTICATION_METHOD = 266
MQIA_AUTHORITY_EVENT = 47
MQIA_AUTH_INFO_TYPE = 66
MQIA_AUTO_REORGANIZATION = 173
MQIA_AUTO_REORG_INTERVAL = 174
MQIA_BACKOUT_THRESHOLD = 22
MQIA_BASE_TYPE = 193
MQIA_BATCH_INTERFACE_AUTO = 86
MQIA_BRIDGE_EVENT = 74
MQIA_CERT_VAL_POLICY = 252
MQIA_CF_CFCONLOS = 246
MQIA_CF_LEVEL = 70
MQIA_CF_OFFLDUSE = 229
MQIA_CF_OFFLOAD = 224
MQIA_CF_OFFLOAD_THRESHOLD1 = 225
MQIA_CF_OFFLOAD_THRESHOLD2 = 226
MQIA_CF_OFFLOAD_THRESHOLD3 = 227
MQIA_CF_RECAUTO = 244
MQIA_CF_RECOVER = 71
MQIA_CF_SMDS_BUFFERS = 228
MQIA_CHANNEL_AUTO_DEF = 55
MQIA_CHANNEL_AUTO_DEF_EVENT = 56
MQIA_CHANNEL_EVENT = 73
MQIA_CHECK_CLIENT_BINDING = 258
MQIA_CHECK_LOCAL_BINDING = 257
MQIA_CHINIT_ADAPTERS = 101
MQIA_CHINIT_CONTROL = 119
MQIA_CHINIT_DISPATCHERS = 105
MQIA_CHINIT_TRACE_AUTO_START = 117
MQIA_CHINIT_TRACE_TABLE_SIZE = 118
MQIA_CHLAUTH_RECORDS = 248
MQIA_CLUSTER_OBJECT_STATE = 256
MQIA_CLUSTER_PUB_ROUTE = 255
MQIA_CLUSTER_Q_TYPE = 59
MQIA_CLUSTER_WORKLOAD_LENGTH = 58
MQIA_CLWL_MRU_CHANNELS = 97
MQIA_CLWL_Q_PRIORITY = 96
MQIA_CLWL_Q_RANK = 95
MQIA_CLWL_USEQ = 98
MQIA_CMD_SERVER_AUTO = 87
MQIA_CMD_SERVER_CONTROL = 120
MQIA_CMD_SERVER_CONVERT_MSG = 88
MQIA_CMD_SERVER_DLQ_MSG = 89
MQIA_CODED_CHAR_SET_ID = 2
MQIA_COMMAND_EVENT = 99
MQIA_COMMAND_LEVEL = 31
MQIA_COMM_EVENT = 232
MQIA_COMM_INFO_TYPE = 223
MQIA_CONFIGURATION_EVENT = 51
MQIA_CPI_LEVEL = 27
MQIA_CURRENT_Q_DEPTH = 3
MQIA_DEFINITION_TYPE = 7
MQIA_DEF_BIND = 61
MQIA_DEF_CLUSTER_XMIT_Q_TYPE = 250
MQIA_DEF_INPUT_OPEN_OPTION = 4
MQIA_DEF_PERSISTENCE = 5
MQIA_DEF_PRIORITY = 6
MQIA_DEF_PUT_RESPONSE_TYPE = 184
MQIA_DEF_READ_AHEAD = 188
MQIA_DISPLAY_TYPE = 262
MQIA_DIST_LISTS = 34
MQIA_DNS_WLM = 106
MQIA_DURABLE_SUB = 175
MQIA_ENCRYPTION_ALGORITHM = 237
MQIA_EXPIRY_INTERVAL = 39
MQIA_FIRST = 1
MQIA_GROUP_UR = 221
MQIA_HARDEN_GET_BACKOUT = 8
MQIA_HIGH_Q_DEPTH = 36
MQIA_IGQ_PUT_AUTHORITY = 65
MQIA_INDEX_TYPE = 57
MQIA_INHIBIT_EVENT = 48
MQIA_INHIBIT_GET = 9
MQIA_INHIBIT_PUB = 181
MQIA_INHIBIT_PUT = 10
MQIA_INHIBIT_SUB = 182
MQIA_INTRA_GROUP_QUEUING = 64
MQIA_IP_ADDRESS_VERSION = 93
MQIA_KEY_REUSE_COUNT = 267
MQIA_LAST = 2000
MQIA_LAST_USED = 275
MQIA_LDAP_AUTHORMD = 263
MQIA_LDAP_NESTGRP = 264
MQIA_LDAP_SECURE_COMM = 261
MQIA_LISTENER_PORT_NUMBER = 85
MQIA_LISTENER_TIMER = 107
MQIA_LOCAL_EVENT = 49
MQIA_LOGGER_EVENT = 94
MQIA_LU62_CHANNELS = 108
MQIA_MASTER_ADMIN = 186
MQIA_MAX_CHANNELS = 109
MQIA_MAX_CLIENTS = 172
MQIA_MAX_GLOBAL_LOCKS = 83
MQIA_MAX_HANDLES = 11
MQIA_MAX_LOCAL_LOCKS = 84
MQIA_MAX_MSG_LENGTH = 13
MQIA_MAX_OPEN_Q = 80
MQIA_MAX_PRIORITY = 14
MQIA_MAX_PROPERTIES_LENGTH = 192
MQIA_MAX_Q_DEPTH = 15
MQIA_MAX_Q_FILE_SIZE = 274
MQIA_MAX_Q_TRIGGERS = 90
MQIA_MAX_RECOVERY_TASKS = 171
MQIA_MAX_RESPONSES = 230
MQIA_MAX_UNCOMMITTED_MSGS = 33
MQIA_MCAST_BRIDGE = 233
MQIA_MEDIA_IMAGE_INTERVAL = 269
MQIA_MEDIA_IMAGE_LOG_LENGTH = 270
MQIA_MEDIA_IMAGE_RECOVER_OBJ = 271
MQIA_MEDIA_IMAGE_RECOVER_Q = 272
MQIA_MEDIA_IMAGE_SCHEDULING = 268
MQIA_MONITORING_AUTO_CLUSSDR = 124
MQIA_MONITORING_CHANNEL = 122
MQIA_MONITORING_Q = 123
MQIA_MONITOR_INTERVAL = 81
MQIA_MSG_DELIVERY_SEQUENCE = 16
MQIA_MSG_DEQ_COUNT = 38
MQIA_MSG_ENQ_COUNT = 37
MQIA_MSG_MARK_BROWSE_INTERVAL = 68
MQIA_MULTICAST = 176
MQIA_NAMELIST_TYPE = 72
MQIA_NAME_COUNT = 19
MQIA_NPM_CLASS = 78
MQIA_NPM_DELIVERY = 196
MQIA_OPEN_INPUT_COUNT = 17
MQIA_OPEN_OUTPUT_COUNT = 18
MQIA_OUTBOUND_PORT_MAX = 140
MQIA_OUTBOUND_PORT_MIN = 110
MQIA_PAGESET_ID = 62
MQIA_PERFORMANCE_EVENT = 53
MQIA_PLATFORM = 32
MQIA_PM_DELIVERY = 195
MQIA_POLICY_VERSION = 238
MQIA_PROPERTY_CONTROL = 190
MQIA_PROT_POLICY_CAPABILITY = 251
MQIA_PROXY_SUB = 199
MQIA_PUBSUB_CLUSTER = 249
MQIA_PUBSUB_MAXMSG_RETRY_COUNT = 206
MQIA_PUBSUB_MODE = 187
MQIA_PUBSUB_NP_MSG = 203
MQIA_PUBSUB_NP_RESP = 205
MQIA_PUBSUB_SYNC_PT = 207
MQIA_PUB_COUNT = 215
MQIA_PUB_SCOPE = 219
MQIA_QMGR_CFCONLOS = 245
MQIA_QMOPT_CONS_COMMS_MSGS = 155
MQIA_QMOPT_CONS_CRITICAL_MSGS = 154
MQIA_QMOPT_CONS_ERROR_MSGS = 153
MQIA_QMOPT_CONS_INFO_MSGS = 151
MQIA_QMOPT_CONS_REORG_MSGS = 156
MQIA_QMOPT_CONS_SYSTEM_MSGS = 157
MQIA_QMOPT_CONS_WARNING_MSGS = 152
MQIA_QMOPT_CSMT_ON_ERROR = 150
MQIA_QMOPT_INTERNAL_DUMP = 170
MQIA_QMOPT_LOG_COMMS_MSGS = 162
MQIA_QMOPT_LOG_CRITICAL_MSGS = 161
MQIA_QMOPT_LOG_ERROR_MSGS = 160
MQIA_QMOPT_LOG_INFO_MSGS = 158
MQIA_QMOPT_LOG_REORG_MSGS = 163
MQIA_QMOPT_LOG_SYSTEM_MSGS = 164
MQIA_QMOPT_LOG_WARNING_MSGS = 159
MQIA_QMOPT_TRACE_COMMS = 166
MQIA_QMOPT_TRACE_CONVERSION = 168
MQIA_QMOPT_TRACE_MQI_CALLS = 165
MQIA_QMOPT_TRACE_REORG = 167
MQIA_QMOPT_TRACE_SYSTEM = 169
MQIA_QSG_DISP = 63
MQIA_Q_DEPTH_HIGH_EVENT = 43
MQIA_Q_DEPTH_HIGH_LIMIT = 40
MQIA_Q_DEPTH_LOW_EVENT = 44
MQIA_Q_DEPTH_LOW_LIMIT = 41
MQIA_Q_DEPTH_MAX_EVENT = 42
MQIA_Q_SERVICE_INTERVAL = 54
MQIA_Q_SERVICE_INTERVAL_EVENT = 46
MQIA_Q_TYPE = 20
MQIA_Q_USERS = 82
MQIA_READ_AHEAD = 189
MQIA_RECEIVE_TIMEOUT = 111
MQIA_RECEIVE_TIMEOUT_MIN = 113
MQIA_RECEIVE_TIMEOUT_TYPE = 112
MQIA_REMOTE_EVENT = 50
MQIA_RESPONSE_RESTART_POINT = 231
MQIA_RETENTION_INTERVAL = 21
MQIA_REVERSE_DNS_LOOKUP = 254
MQIA_SCOPE = 45
MQIA_SECURITY_CASE = 141
MQIA_SERVICE_CONTROL = 139
MQIA_SERVICE_TYPE = 121
MQIA_SHAREABILITY = 23
MQIA_SHARED_Q_Q_MGR_NAME = 77
MQIA_SIGNATURE_ALGORITHM = 236
MQIA_SSL_EVENT = 75
MQIA_SSL_FIPS_REQUIRED = 92
MQIA_SSL_RESET_COUNT = 76
MQIA_SSL_TASKS = 69
MQIA_START_STOP_EVENT = 52
MQIA_STATISTICS_AUTO_CLUSSDR = 130
MQIA_STATISTICS_CHANNEL = 129
MQIA_STATISTICS_INTERVAL = 131
MQIA_STATISTICS_MQI = 127
MQIA_STATISTICS_Q = 128
MQIA_STREAM_QUEUE_QOS = 275
MQIA_SUB_CONFIGURATION_EVENT = 242
MQIA_SUB_COUNT = 204
MQIA_SUB_SCOPE = 218
MQIA_SUITE_B_STRENGTH = 247
MQIA_SYNCPOINT = 30
MQIA_TCP_CHANNELS = 114
MQIA_TCP_KEEP_ALIVE = 115
MQIA_TCP_STACK_TYPE = 116
MQIA_TIME_SINCE_RESET = 35
MQIA_TOLERATE_UNPROTECTED = 235
MQIA_TOPIC_DEF_PERSISTENCE = 185
MQIA_TOPIC_NODE_COUNT = 253
MQIA_TOPIC_TYPE = 208
MQIA_TRACE_ROUTE_RECORDING = 137
MQIA_TREE_LIFE_TIME = 183
MQIA_TRIGGER_CONTROL = 24
MQIA_TRIGGER_DEPTH = 29
MQIA_TRIGGER_INTERVAL = 25
MQIA_TRIGGER_MSG_PRIORITY = 26
MQIA_TRIGGER_RESTART = 91
MQIA_TRIGGER_TYPE = 28
MQIA_UR_DISP = 222
MQIA_USAGE = 12
MQIA_USER_LIST = 2000
MQIA_USE_DEAD_LETTER_Q = 234
MQIA_WILDCARD_OPERATION = 216
MQIA_XR_CAPABILITY = 243

# Integer Attribute Values
MQIAV_NOT_APPLICABLE = (-1)
MQIAV_UNDEFINED = (-2)

# CommInfo Bridge
MQMCB_DISABLED = 0
MQMCB_ENABLED = 1

# Key Reuse Count
MQKEY_REUSE_DISABLED = 0
MQKEY_REUSE_UNLIMITED = (-1)

# Group Attribute Selectors
MQGA_FIRST = 8001
MQGA_LAST = 9000


##################################################################
# Values Related to MQINQMP Function                             #
##################################################################

# Not 100% sure on how to handle these in Python. Advice sought.

#/* Inquire on all properties -  "%" */
# #define MQPROP_INQUIRE_ALL     (MQPTR)(char*)"%",\
#                                 0,\
#                                 0,\
#                                 1,\
#                                MQCCSI_APPL
#
# /* Inquire on all "usr" properties - "usr.%" */
# #define MQPROP_INQUIRE_ALL_USR (MQPTR)(char*)"usr.%",\
#                                 0,\
#                                 0,\
#                                 5,\
#                                 MQCCSI_APPL

##################################################################
# Values Related to MQOPEN Function                              #
##################################################################

# Open Options
MQOO_BIND_AS_Q_DEF = 0x00000000
MQOO_READ_AHEAD_AS_Q_DEF = 0x00000000
MQOO_INPUT_AS_Q_DEF = 0x00000001
MQOO_INPUT_SHARED = 0x00000002
MQOO_INPUT_EXCLUSIVE = 0x00000004
MQOO_BROWSE = 0x00000008
MQOO_OUTPUT = 0x00000010
MQOO_INQUIRE = 0x00000020
MQOO_SET = 0x00000040
MQOO_SAVE_ALL_CONTEXT = 0x00000080
MQOO_PASS_IDENTITY_CONTEXT = 0x00000100
MQOO_PASS_ALL_CONTEXT = 0x00000200
MQOO_SET_IDENTITY_CONTEXT = 0x00000400
MQOO_SET_ALL_CONTEXT = 0x00000800
MQOO_ALTERNATE_USER_AUTHORITY = 0x00001000
MQOO_FAIL_IF_QUIESCING = 0x00002000
MQOO_BIND_ON_OPEN = 0x00004000
MQOO_BIND_ON_GROUP = 0x00400000
MQOO_BIND_NOT_FIXED = 0x00008000
MQOO_CO_OP = 0x00020000
MQOO_NO_READ_AHEAD = 0x00080000
MQOO_READ_AHEAD = 0x00100000
MQOO_NO_MULTICAST = 0x00200000
MQOO_RESOLVE_LOCAL_Q = 0x00040000
MQOO_RESOLVE_LOCAL_TOPIC = 0x00040000


##################################################################
# Values Related to MQSETMP Function                             #
##################################################################

# Property data types
MQTYPE_AS_SET = 0x00000000
MQTYPE_NULL = 0x00000002
MQTYPE_BOOLEAN = 0x00000004
MQTYPE_BYTE_STRING = 0x00000008
MQTYPE_INT8 = 0x00000010
MQTYPE_INT16 = 0x00000020
MQTYPE_INT32 = 0x00000040
MQTYPE_LONG = 0x00000040
MQTYPE_INT64 = 0x00000080
MQTYPE_FLOAT32 = 0x00000100
MQTYPE_FLOAT64 = 0x00000200
MQTYPE_STRING = 0x00000400

# Property value lengths
MQVL_NULL_TERMINATED = (-1)
MQVL_EMPTY_STRING = 0


##################################################################
# Values Related to MQSTAT Function                              #
##################################################################

# Stat Options
MQSTAT_TYPE_ASYNC_ERROR = 0
MQSTAT_TYPE_RECONNECTION = 1
MQSTAT_TYPE_RECONNECTION_ERROR = 2


##################################################################
# Values Related to MQSUB Function                               #
##################################################################

# Subscribe Options
MQSO_NONE = 0x00000000
MQSO_NON_DURABLE = 0x00000000
MQSO_READ_AHEAD_AS_Q_DEF = 0x00000000
MQSO_ALTER = 0x00000001
MQSO_CREATE = 0x00000002
MQSO_RESUME = 0x00000004
MQSO_DURABLE = 0x00000008
MQSO_GROUP_SUB = 0x00000010
MQSO_MANAGED = 0x00000020
MQSO_SET_IDENTITY_CONTEXT = 0x00000040
MQSO_NO_MULTICAST = 0x00000080
MQSO_FIXED_USERID = 0x00000100
MQSO_ANY_USERID = 0x00000200
MQSO_PUBLICATIONS_ON_REQUEST = 0x00000800
MQSO_NEW_PUBLICATIONS_ONLY = 0x00001000
MQSO_FAIL_IF_QUIESCING = 0x00002000
MQSO_ALTERNATE_USER_AUTHORITY = 0x00040000
MQSO_WILDCARD_CHAR = 0x00100000
MQSO_WILDCARD_TOPIC = 0x00200000
MQSO_SET_CORREL_ID = 0x00400000
MQSO_SCOPE_QMGR = 0x04000000
MQSO_NO_READ_AHEAD = 0x08000000
MQSO_READ_AHEAD = 0x10000000


##################################################################
# Values Related to MQSUBRQ Function                             #
##################################################################

# Action
MQSR_ACTION_PUBLICATION = 1

##### LEGACY DEFINITIONS BELOW HERE - CANNOT REMOVE THESE WITHOUT TESTS FAILING

#
# MQCD defines courtesy of John OSullivan (mailto:jos@onebox.com)
#

#
# ======================================================================
#                                WARNING
# ======================================================================
# The following MQ constants are kept here only for compatibility with
# PyMQI versions prior to 1.0 and they will be removed in the future. Any new
# development should use the very same constants from pymqi.CMQXC which are
# always up to date with current WebSphere MQ versions and which also take into
# account differences in 32bit vs. 64bit modes.
#

MQCD_VERSION_1 = py23long(1)
MQCD_VERSION_2 = py23long(2)
MQCD_VERSION_3 = py23long(3)
MQCD_VERSION_4 = py23long(4)
MQCD_VERSION_5 = py23long(5)
MQCD_VERSION_6 = py23long(6)
MQCD_VERSION_7 = py23long(7)
MQCD_VERSION_8 = py23long(8)
MQCD_VERSION_9 = py23long(9)
MQCD_CURRENT_VERSION = py23long(9)

MQCD_LENGTH_4 = py23long(1540)
MQCD_LENGTH_5 = py23long(1552)
MQCD_LENGTH_6 = py23long(1648)
MQCD_CURRENT_LENGTH = py23long(1648)

MQCHT_SENDER = py23long(1)
MQCHT_SERVER = py23long(2)
MQCHT_RECEIVER = py23long(3)
MQCHT_REQUESTER = py23long(4)
MQCHT_ALL = py23long(5)
MQCHT_CLNTCONN = py23long(6)
MQCHT_SVRCONN = py23long(7)
MQCHT_CLUSRCVR = py23long(8)
MQCHT_CLUSSDR = py23long(9)

MQXPT_LOCAL = py23long(0)
MQXPT_LU62 = py23long(1)
MQXPT_TCP = py23long(2)
MQXPT_NETBIOS = py23long(3)
MQXPT_SPX = py23long(4)
MQXPT_DECNET = py23long(5)
MQXPT_UDP = py23long(6)

MQPA_DEFAULT = py23long(1)
MQPA_CONTEXT = py23long(2)
MQPA_ONLY_MCA = py23long(3)
MQPA_ALTERNATE_OR_MCA = py23long(4)

MQCDC_SENDER_CONVERSION = py23long(1)
MQCDC_NO_SENDER_CONVERSION = py23long(0)

MQMCAT_PROCESS = py23long(1)
MQMCAT_THREAD = py23long(2)

MQNPMS_NORMAL = py23long(1)
MQNPMS_FAST = py23long(2)

MQCXP_STRUC_ID = b"CXP "

MQCXP_STRUC_ID_ARRAY = [b'C', b'X', b'P', b' ']

MQCXP_VERSION_1 = py23long(1)
MQCXP_VERSION_2 = py23long(2)
MQCXP_VERSION_3 = py23long(3)
MQCXP_VERSION_4 = py23long(4)
MQCXP_CURRENT_VERSION = py23long(4)

MQXR2_PUT_WITH_DEF_ACTION = py23long(0)
MQXR2_PUT_WITH_DEF_USERID = py23long(1)
MQXR2_PUT_WITH_MSG_USERID = py23long(2)
MQXR2_USE_AGENT_BUFFER = py23long(0)
MQXR2_USE_EXIT_BUFFER = py23long(4)
MQXR2_DEFAULT_CONTINUATION = py23long(0)
MQXR2_CONTINUE_CHAIN = py23long(8)
MQXR2_SUPPRESS_CHAIN = py23long(16)

MQCF_NONE = py23long(0x00000000)
MQCF_DIST_LISTS = py23long(0x00000001)


MQDXP_STRUC_ID = b"DXP "

MQDXP_STRUC_ID_ARRAY = [b'D', b'X', b'P', b' ']

MQDXP_VERSION_1 = py23long(1)
MQDXP_CURRENT_VERSION = py23long(1)

MQXDR_OK = py23long(0)
MQXDR_CONVERSION_FAILED = py23long(1)

MQPXP_STRUC_ID = b"PXP "

MQPXP_STRUC_ID_ARRAY = [b'P', b'X', b'P', b' ']

MQPXP_VERSION_1 = py23long(1)
MQPXP_CURRENT_VERSION = py23long(1)

MQDT_APPL = py23long(1)
MQDT_BROKER = py23long(2)


MQWDR_STRUC_ID = b"WDR "

MQWDR_STRUC_ID_ARRAY = [b'W', b'D', b'R', b' ']

MQWDR_VERSION_1 = py23long(1)
MQWDR_CURRENT_VERSION = py23long(1)

MQWDR_LENGTH_1 = py23long(124)
MQWDR_CURRENT_LENGTH = py23long(124)

MQQMF_REPOSITORY_Q_MGR = py23long(0x00000002)
MQQMF_CLUSSDR_USER_DEFINED = py23long(0x00000008)
MQQMF_CLUSSDR_AUTO_DEFINED = py23long(0x00000010)
MQQMF_AVAILABLE = py23long(0x00000020)


MQWQR_STRUC_ID = b"WQR "

MQWQR_STRUC_ID_ARRAY = [b'W', b'Q', b'R', b' ']

MQWQR_VERSION_1 = py23long(1)
MQWQR_CURRENT_VERSION = py23long(1)

MQWQR_LENGTH_1 = py23long(200)
MQWQR_CURRENT_LENGTH = py23long(200)

MQQF_LOCAL_Q = py23long(0x00000001)

MQWXP_STRUC_ID = b"WXP "

MQWXP_STRUC_ID_ARRAY = [b'W', b'X', b'P', b' ']

MQWXP_VERSION_1 = py23long(1)
MQWXP_CURRENT_VERSION = py23long(1)
MQXT_CHANNEL_SEC_EXIT = py23long(11)
MQXT_CHANNEL_MSG_EXIT = py23long(12)
MQXT_CHANNEL_SEND_EXIT = py23long(13)
MQXT_CHANNEL_RCV_EXIT = py23long(14)
MQXT_CHANNEL_MSG_RETRY_EXIT = py23long(15)
MQXT_CHANNEL_AUTO_DEF_EXIT = py23long(16)
MQXT_CLUSTER_WORKLOAD_EXIT = py23long(20)
MQXT_PUBSUB_ROUTING_EXIT = py23long(21)

MQXR_INIT = py23long(11)
MQXR_TERM = py23long(12)
MQXR_MSG = py23long(13)
MQXR_XMIT = py23long(14)
MQXR_SEC_MSG = py23long(15)
MQXR_INIT_SEC = py23long(16)
MQXR_RETRY = py23long(17)
MQXR_AUTO_CLUSSDR = py23long(18)
MQXR_AUTO_RECEIVER = py23long(19)
MQXR_CLWL_OPEN = py23long(20)
MQXR_CLWL_PUT = py23long(21)
MQXR_CLWL_MOVE = py23long(22)
MQXR_CLWL_REPOS = py23long(23)
MQXR_CLWL_REPOS_MOVE = py23long(24)
MQXR_AUTO_SVRCONN = py23long(27)
MQXR_AUTO_CLUSRCVR = py23long(28)

MQXCC_OK = py23long(0)
MQXCC_SUPPRESS_FUNCTION = py23long(-1)
MQXCC_SKIP_FUNCTION = py23long(-2)
MQXCC_SEND_AND_REQUEST_SEC_MSG = py23long(-3)
MQXCC_SEND_SEC_MSG = py23long(-4)
MQXCC_SUPPRESS_EXIT = py23long(-5)
MQXCC_CLOSE_CHANNEL = py23long(-6)

MQXUA_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"

MQXUA_NONE_ARRAY = [b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0',
                    b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0']


MQDCC_DEFAULT_CONVERSION = py23long(0x00000001)
MQDCC_FILL_TARGET_BUFFER = py23long(0x00000002)
MQDCC_SOURCE_ENC_NATIVE = py23long(0x00000020)
MQDCC_SOURCE_ENC_NORMAL = py23long(0x00000010)
MQDCC_SOURCE_ENC_REVERSED = py23long(0x00000020)
MQDCC_SOURCE_ENC_UNDEFINED = py23long(0x00000000)
MQDCC_TARGET_ENC_NATIVE = py23long(0x00000200)
MQDCC_TARGET_ENC_NORMAL = py23long(0x00000100)
MQDCC_TARGET_ENC_REVERSED = py23long(0x00000200)
MQDCC_TARGET_ENC_UNDEFINED = py23long(0x00000000)
MQDCC_NONE = py23long(0x00000000)

MQDCC_SOURCE_ENC_MASK = py23long(0x000000f0)
MQDCC_TARGET_ENC_MASK = py23long(0x00000f00)
MQDCC_SOURCE_ENC_FACTOR = py23long(16)
MQDCC_TARGET_ENC_FACTOR = py23long(256)

# ======================================================================
