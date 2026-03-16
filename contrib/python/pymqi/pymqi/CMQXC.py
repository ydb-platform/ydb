from struct import calcsize

# CMQXC - Generated from V9.2.5 CD C Header File

# 64bit
if calcsize("P") == 8:
    MQCD_LENGTH_4 = 1568
    MQCD_LENGTH_5 = 1584
    MQCD_LENGTH_6 = 1688
    MQCD_LENGTH_7 = 1792
    MQCD_LENGTH_8 = 1888
    MQCD_LENGTH_9 = 1912
    MQCD_LENGTH_10 = 1920
    MQCD_LENGTH_11 = 1984
    MQCD_LENGTH_12 = 1992
    MQACH_LENGTH_1 = 72
    MQAXC_LENGTH_1 = 392
    MQAXC_LENGTH_2 = 424
    MQAXP_LENGTH_1 = 256
    MQCXP_LENGTH_6 = 200
    MQCXP_LENGTH_7 = 208
    MQCXP_LENGTH_8 = 224
    MQCXP_LENGTH_9 = 240
    MQDXP_LENGTH_2 = 56
    MQNXP_LENGTH_1 = 64
    MQNXP_LENGTH_2 = 72
    MQPBC_LENGTH_1 = 32
    MQPBC_LENGTH_2 = 40
    MQPSXP_LENGTH_1 = 176
    MQPSXP_LENGTH_2 = 184
    MQSBC_LENGTH_1 = 288
    MQWXP_LENGTH_1 = 224
    MQWXP_LENGTH_2 = 240
    MQWXP_LENGTH_3 = 240
    MQWXP_LENGTH_4 = 248
    MQWXP1_LENGTH_1 = 224
    MQWXP2_LENGTH_1 = 224
    MQWXP2_LENGTH_2 = 240
    MQWXP3_LENGTH_1 = 224
    MQWXP3_LENGTH_2 = 240
    MQWXP3_LENGTH_3 = 240
    MQWXP4_LENGTH_1 = 224
    MQWXP4_LENGTH_2 = 240
    MQWXP4_LENGTH_3 = 240
    MQWXP4_LENGTH_4 = 248
    MQXEPO_LENGTH_1 = 40

else:
    MQCD_LENGTH_4 = 1540
    MQCD_LENGTH_5 = 1552
    MQCD_LENGTH_6 = 1648
    MQCD_LENGTH_7 = 1748
    MQCD_LENGTH_8 = 1840
    MQCD_LENGTH_9 = 1864
    MQCD_LENGTH_10 = 1876
    MQCD_LENGTH_11 = 1940
    MQCD_LENGTH_12 = 1944
    MQACH_LENGTH_1 = 68
    MQAXC_LENGTH_1 = 384
    MQAXC_LENGTH_2 = 412
    MQAXP_LENGTH_1 = 244
    MQCXP_LENGTH_6 = 192
    MQCXP_LENGTH_7 = 200
    MQCXP_LENGTH_8 = 208
    MQCXP_LENGTH_9 = 220
    MQDXP_LENGTH_2 = 48
    MQNXP_LENGTH_1 = 52
    MQNXP_LENGTH_2 = 56
    MQPBC_LENGTH_1 = 28
    MQPBC_LENGTH_2 = 32
    MQPSXP_LENGTH_1 = 156
    MQPSXP_LENGTH_2 = 160
    MQSBC_LENGTH_1 = 272
    MQWXP_LENGTH_1 = 208
    MQWXP_LENGTH_2 = 216
    MQWXP_LENGTH_3 = 220
    MQWXP_LENGTH_4 = 224
    MQWXP1_LENGTH_1 = 208
    MQWXP2_LENGTH_1 = 208
    MQWXP2_LENGTH_2 = 216
    MQWXP3_LENGTH_1 = 208
    MQWXP3_LENGTH_2 = 216
    MQWXP3_LENGTH_3 = 220
    MQWXP4_LENGTH_1 = 208
    MQWXP4_LENGTH_2 = 216
    MQWXP4_LENGTH_3 = 220
    MQWXP4_LENGTH_4 = 224
    MQXEPO_LENGTH_1 = 32

###################################################################
# Values Related to MQCD Structure
###################################################################

# Structure Version Number
MQCD_VERSION_1 = 1
MQCD_VERSION_2 = 2
MQCD_VERSION_3 = 3
MQCD_VERSION_4 = 4
MQCD_VERSION_5 = 5
MQCD_VERSION_6 = 6
MQCD_VERSION_7 = 7
MQCD_VERSION_8 = 8
MQCD_VERSION_9 = 9
MQCD_VERSION_10 = 10
MQCD_VERSION_11 = 11
MQCD_VERSION_12 = 12
MQCD_CURRENT_VERSION = 12

# Structure Length
MQCD_LENGTH_1 = 984
MQCD_LENGTH_2 = 1312
MQCD_LENGTH_3 = 1480
MQCD_CURRENT_LENGTH = MQCD_LENGTH_12

# Channel Types
MQCHT_SENDER = 1
MQCHT_SERVER = 2
MQCHT_RECEIVER = 3
MQCHT_REQUESTER = 4
MQCHT_ALL = 5
MQCHT_CLNTCONN = 6
MQCHT_SVRCONN = 7
MQCHT_CLUSRCVR = 8
MQCHT_CLUSSDR = 9
MQCHT_MQTT = 10
MQCHT_AMQP = 11

# Channel Compression
MQCOMPRESS_NOT_AVAILABLE = (-1)
MQCOMPRESS_NONE = 0
MQCOMPRESS_RLE = 1
MQCOMPRESS_ZLIBFAST = 2
MQCOMPRESS_ZLIBHIGH = 4
MQCOMPRESS_SYSTEM = 8
MQCOMPRESS_ANY = 0x0FFFFFFF

# Transport Types
MQXPT_ALL = (-1)
MQXPT_LOCAL = 0
MQXPT_LU62 = 1
MQXPT_TCP = 2
MQXPT_NETBIOS = 3
MQXPT_SPX = 4
MQXPT_DECNET = 5
MQXPT_UDP = 6

# Put Authority
MQPA_DEFAULT = 1
MQPA_CONTEXT = 2
MQPA_ONLY_MCA = 3
MQPA_ALTERNATE_OR_MCA = 4

# Channel Data Conversion
MQCDC_SENDER_CONVERSION = 1
MQCDC_NO_SENDER_CONVERSION = 0

# MCA Types
MQMCAT_PROCESS = 1
MQMCAT_THREAD = 2

# NonPersistent-Message Speeds
MQNPMS_NORMAL = 1
MQNPMS_FAST = 2

# SSL Client Authentication
MQSCA_REQUIRED = 0
MQSCA_OPTIONAL = 1
MQSCA_NEVER_REQUIRED = 2

# KeepAlive Interval
MQKAI_AUTO = (-1)

# Connection Affinity Values
MQCAFTY_NONE = 0
MQCAFTY_PREFERRED = 1

# Client Reconnect
MQRCN_NO = 0
MQRCN_YES = 1
MQRCN_Q_MGR = 2
MQRCN_DISABLED = 3

# Protocol
MQPROTO_MQTTV3 = 1
MQPROTO_HTTP = 2
MQPROTO_AMQP = 3
MQPROTO_MQTTV311 = 4

# Security Protocol
MQSECPROT_NONE = 0
MQSECPROT_SSLV30 = 1
MQSECPROT_TLSV10 = 2
MQSECPROT_TLSV12 = 4
MQSECPROT_TLSV13 = 8

# SPL Protection
MQSPL_PASSTHRU = 0
MQSPL_REMOVE = 1
MQSPL_AS_POLICY = 2

###################################################################
# Values Related to MQACH Structure
###################################################################

# Structure Identifier
MQACH_STRUC_ID = b"ACH "

# Structure Identifier (array form)
MQACH_STRUC_ID_ARRAY = [b'A', b'C', b'H', b' ']

# Structure Version Number
MQACH_VERSION_1 = 1
MQACH_CURRENT_VERSION = 1

# Structure Length
MQACH_CURRENT_LENGTH = MQACH_LENGTH_1

###################################################################
# Values Related to MQAXC Structure
###################################################################

# Structure Identifier
MQAXC_STRUC_ID = b"AXC "

# Structure Identifier (array form)
MQAXC_STRUC_ID_ARRAY = [b'A', b'X', b'C', b' ']

# Structure Version Number
MQAXC_VERSION_1 = 1
MQAXC_VERSION_2 = 2
MQAXC_CURRENT_VERSION = 2

# Structure Length
MQAXC_CURRENT_LENGTH = MQAXC_LENGTH_2

# Environments
MQXE_OTHER = 0
MQXE_MCA = 1
MQXE_MCA_SVRCONN = 2
MQXE_COMMAND_SERVER = 3
MQXE_MQSC = 4
MQXE_MCA_CLNTCONN = 5

###################################################################
# Values Related to MQAXP Structure
###################################################################

# Structure Identifier
MQAXP_STRUC_ID = b"AXP "

# Structure Identifier (array form)
MQAXP_STRUC_ID_ARRAY = [b'A', b'X', b'P', b' ']

# Structure Version Number
MQAXP_VERSION_1 = 1
MQAXP_VERSION_2 = 2
MQAXP_CURRENT_VERSION = 2

# Structure Length
MQAXP_CURRENT_LENGTH = MQAXP_LENGTH_1

# API Caller Types
MQXACT_EXTERNAL = 1
MQXACT_INTERNAL = 2

# Problem Determination Area
MQXPDA_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"\
              b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"\
              b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"

# Problem Determination Area (array form)
MQXPDA_NONE_ARRAY = [b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0',
                     b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0',
                     b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0',
                     b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0',
                     b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0',
                     b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0',
                     b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0',
                     b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0']

# API Function Identifiers
MQXF_INIT = 1
MQXF_TERM = 2
MQXF_CONN = 3
MQXF_CONNX = 4
MQXF_DISC = 5
MQXF_OPEN = 6
MQXF_CLOSE = 7
MQXF_PUT1 = 8
MQXF_PUT = 9
MQXF_GET = 10
MQXF_DATA_CONV_ON_GET = 11
MQXF_INQ = 12
MQXF_SET = 13
MQXF_BEGIN = 14
MQXF_CMIT = 15
MQXF_BACK = 16
MQXF_STAT = 18
MQXF_CB = 19
MQXF_CTL = 20
MQXF_CALLBACK = 21
MQXF_SUB = 22
MQXF_SUBRQ = 23
MQXF_XACLOSE = 24
MQXF_XACOMMIT = 25
MQXF_XACOMPLETE = 26
MQXF_XAEND = 27
MQXF_XAFORGET = 28
MQXF_XAOPEN = 29
MQXF_XAPREPARE = 30
MQXF_XARECOVER = 31
MQXF_XAROLLBACK = 32
MQXF_XASTART = 33
MQXF_AXREG = 34
MQXF_AXUNREG = 35

###################################################################
# Values Related to MQCXP Structure
###################################################################

# Structure Identifier
MQCXP_STRUC_ID = b"CXP "

# Structure Identifier (array form)
MQCXP_STRUC_ID_ARRAY = [b'C', b'X', b'P', b' ']

# Structure Version Number
MQCXP_VERSION_1 = 1
MQCXP_VERSION_2 = 2
MQCXP_VERSION_3 = 3
MQCXP_VERSION_4 = 4
MQCXP_VERSION_5 = 5
MQCXP_VERSION_6 = 6
MQCXP_VERSION_7 = 7
MQCXP_VERSION_8 = 8
MQCXP_VERSION_9 = 9
MQCXP_CURRENT_VERSION = 9

# Structure Length
MQCXP_LENGTH_3 = 156
MQCXP_LENGTH_4 = 156
MQCXP_LENGTH_5 = 160
MQCXP_CURRENT_LENGTH = MQCXP_LENGTH_9

# Exit Response 2
MQXR2_PUT_WITH_DEF_ACTION = 0
MQXR2_PUT_WITH_DEF_USERID = 1
MQXR2_PUT_WITH_MSG_USERID = 2
MQXR2_USE_AGENT_BUFFER = 0
MQXR2_USE_EXIT_BUFFER = 4
MQXR2_DEFAULT_CONTINUATION = 0
MQXR2_CONTINUE_CHAIN = 8
MQXR2_SUPPRESS_CHAIN = 16
MQXR2_STATIC_CACHE = 0
MQXR2_DYNAMIC_CACHE = 32

# Capability Flags
MQCF_NONE = 0x00000000
MQCF_DIST_LISTS = 0x00000001

###################################################################
# Values Related to MQDXP Structure
###################################################################

# Structure Identifier
MQDXP_STRUC_ID = b"DXP "

# Structure Identifier (array form)
MQDXP_STRUC_ID_ARRAY = [b'D', b'X', b'P', b' ']

# Structure Version Number
MQDXP_VERSION_1 = 1
MQDXP_VERSION_2 = 2
MQDXP_CURRENT_VERSION = 2

# Structure Length
MQDXP_LENGTH_1 = 44
MQDXP_CURRENT_LENGTH = MQDXP_LENGTH_2

# Exit Response
MQXDR_OK = 0
MQXDR_CONVERSION_FAILED = 1

###################################################################
# Values Related to MQNXP Structure
###################################################################

# Structure Identifier
MQNXP_STRUC_ID = b"NXP "

# Structure Identifier (array form)
MQNXP_STRUC_ID_ARRAY = [b'N', b'X', b'P', b' ']

# Structure Version Number
MQNXP_VERSION_1 = 1
MQNXP_VERSION_2 = 2
MQNXP_CURRENT_VERSION = 2

# Structure Length
MQNXP_CURRENT_LENGTH = MQNXP_LENGTH_2

###################################################################
# Values Related to MQPBC Structure
###################################################################

# Structure Identifier
MQPBC_STRUC_ID = b"PBC "

# Structure Identifier (array form)
MQPBC_STRUC_ID_ARRAY = [b'P', b'B', b'C', b' ']

# Structure Version Number
MQPBC_VERSION_1 = 1
MQPBC_VERSION_2 = 2
MQPBC_CURRENT_VERSION = 2

# Structure Length
MQPBC_CURRENT_LENGTH = MQPBC_LENGTH_2

##################################################################
# Values Related to MQPSXP Structure
###################################################################

# Structure Identifier
MQPSXP_STRUC_ID = b"PSXP"

# Structure Identifier (array form)
MQPSXP_STRUC_ID_ARRAY = [b'P', b'S', b'X', b'P']

# Structure Version Number
MQPSXP_VERSION_1 = 1
MQPSXP_VERSION_2 = 2
MQPSXP_CURRENT_VERSION = 2

# Structure Length
MQPSXP_CURRENT_LENGTH = MQPSXP_LENGTH_2

###################################################################
# Values Related to MQSBC Structure
###################################################################

# Structure Identifier
MQSBC_STRUC_ID = b"SBC "

# Structure Identifier (array form)
MQSBC_STRUC_ID_ARRAY = [b'S', b'B', b'C', b' ']

# Structure Version Number
MQSBC_VERSION_1 = 1
MQSBC_CURRENT_VERSION = 1

# Structure Length
MQSBC_CURRENT_LENGTH = MQSBC_LENGTH_1

###################################################################
# Values Related to MQWDR Structure
###################################################################

# Structure Identifier
MQWDR_STRUC_ID = b"WDR "

# Structure Identifier (array form)
MQWDR_STRUC_ID_ARRAY = [b'W', b'D', b'R', b' ']

# Structure Version Number
MQWDR_VERSION_1 = 1
MQWDR_VERSION_2 = 2
MQWDR_CURRENT_VERSION = 2

# Structure Length
MQWDR_LENGTH_1 = 124
MQWDR_LENGTH_2 = 136
MQWDR_CURRENT_LENGTH = 136

# Queue Manager Flags
MQQMF_REPOSITORY_Q_MGR = 0x00000002
MQQMF_CLUSSDR_USER_DEFINED = 0x00000008
MQQMF_CLUSSDR_AUTO_DEFINED = 0x00000010
MQQMF_AVAILABLE = 0x00000020

###################################################################
# Values Related to MQWDR Structure
###################################################################

# Structure Length
MQWDR1_LENGTH_1 = 124
MQWDR1_CURRENT_LENGTH = 124

###################################################################
# Values Related to MQWDR2 Structure
###################################################################

# Structure Length
MQWDR2_LENGTH_1 = 124
MQWDR2_LENGTH_2 = 136
MQWDR2_CURRENT_LENGTH = 136

###################################################################
# Values Related to MQWQR Structure
###################################################################

# Structure Identifier
MQWQR_STRUC_ID = b"WQR "

# Structure Identifier (array form)
MQWQR_STRUC_ID_ARRAY = [b'W', b'Q', b'R', b' ']

# Structure Version Number
MQWQR_VERSION_1 = 1
MQWQR_VERSION_2 = 2
MQWQR_VERSION_3 = 3
MQWQR_CURRENT_VERSION = 3

# Structure Length
MQWQR_LENGTH_1 = 200
MQWQR_LENGTH_2 = 208
MQWQR_LENGTH_3 = 212
MQWQR_CURRENT_LENGTH = 212

# Queue Flags
MQQF_LOCAL_Q = 0x00000001
MQQF_CLWL_USEQ_ANY = 0x00000040
MQQF_CLWL_USEQ_LOCAL = 0x00000080

###################################################################
# Values Related to MQWQR1 Structure
###################################################################

# Structure Length
MQWQR1_LENGTH_1 = 200
MQWQR1_CURRENT_LENGTH = 200

###################################################################
# Values Related to MQWQR2 Structure
###################################################################

# Structure Length
MQWQR2_LENGTH_1 = 200
MQWQR2_LENGTH_2 = 208
MQWQR2_CURRENT_LENGTH = 208

###################################################################
# Values Related to MQWQR3 Structure
###################################################################

# Structure Length
MQWQR3_LENGTH_1 = 200
MQWQR3_LENGTH_2 = 208
MQWQR3_LENGTH_3 = 212
MQWQR3_CURRENT_LENGTH = 212

###################################################################
# Values Related to MQWXP Structure
###################################################################

# Structure Identifier
MQWXP_STRUC_ID = b"WXP "

# Structure Identifier (array form)
MQWXP_STRUC_ID_ARRAY = [b'W', b'X', b'P', b' ']

# Structure Version Number
MQWXP_VERSION_1 = 1
MQWXP_VERSION_2 = 2
MQWXP_VERSION_3 = 3
MQWXP_VERSION_4 = 4
MQWXP_CURRENT_VERSION = 4

# Structure Length
MQWXP_CURRENT_LENGTH = MQWXP_LENGTH_4

# Cluster Workload Flags
MQWXP_PUT_BY_CLUSTER_CHL = 0x00000002

###################################################################
# Values Related to MQWXP1 Structure
###################################################################

# Structure Length
MQWXP1_CURRENT_LENGTH = MQWXP1_LENGTH_1

###################################################################
# Values Related to MQWXP2 Structure
###################################################################

# Structure Length
MQWXP2_CURRENT_LENGTH = MQWXP2_LENGTH_2

###################################################################
# Values Related to MQWXP3 Structure
###################################################################

# Structure Length
MQWXP3_CURRENT_LENGTH = MQWXP3_LENGTH_3

###################################################################
# Values Related to MQWXP4 Structure
###################################################################

# Structure Length
MQWXP4_CURRENT_LENGTH = MQWXP4_LENGTH_4

###################################################################
# Values Related to MQXEPO Structure
###################################################################

# Structure Identifier
MQXEPO_STRUC_ID = b"XEPO"

# Structure Identifier (array form)
MQXEPO_STRUC_ID_ARRAY = [b'X', b'E', b'P', b'O']

# Structure Version Number
MQXEPO_VERSION_1 = 1
MQXEPO_CURRENT_VERSION = 1

# Structure Length
MQXEPO_CURRENT_LENGTH = MQXEPO_LENGTH_1

# Exit Options
MQXEPO_NONE = 0x00000000

###################################################################
# General Values Related to Exits
###################################################################

# Exit Identifiers
MQXT_API_CROSSING_EXIT = 1
MQXT_API_EXIT = 2
MQXT_CHANNEL_SEC_EXIT = 11
MQXT_CHANNEL_MSG_EXIT = 12
MQXT_CHANNEL_SEND_EXIT = 13
MQXT_CHANNEL_RCV_EXIT = 14
MQXT_CHANNEL_MSG_RETRY_EXIT = 15
MQXT_CHANNEL_AUTO_DEF_EXIT = 16
MQXT_CLUSTER_WORKLOAD_EXIT = 20
MQXT_PUBSUB_ROUTING_EXIT = 21
MQXT_PUBLISH_EXIT = 22
MQXT_PRECONNECT_EXIT = 23

# Exit Reasons
MQXR_BEFORE = 1
MQXR_AFTER = 2
MQXR_CONNECTION = 3
MQXR_BEFORE_CONVERT = 4
MQXR_INIT = 11
MQXR_TERM = 12
MQXR_MSG = 13
MQXR_XMIT = 14
MQXR_SEC_MSG = 15
MQXR_INIT_SEC = 16
MQXR_RETRY = 17
MQXR_AUTO_CLUSSDR = 18
MQXR_AUTO_RECEIVER = 19
MQXR_CLWL_OPEN = 20
MQXR_CLWL_PUT = 21
MQXR_CLWL_MOVE = 22
MQXR_CLWL_REPOS = 23
MQXR_CLWL_REPOS_MOVE = 24
MQXR_END_BATCH = 25
MQXR_ACK_RECEIVED = 26
MQXR_AUTO_SVRCONN = 27
MQXR_AUTO_CLUSRCVR = 28
MQXR_SEC_PARMS = 29
MQXR_PUBLICATION = 30
MQXR_PRECONNECT = 31

# Exit Responses
MQXCC_OK = 0
MQXCC_SUPPRESS_FUNCTION = (-1)
MQXCC_SKIP_FUNCTION = (-2)
MQXCC_SEND_AND_REQUEST_SEC_MSG = (-3)
MQXCC_SEND_SEC_MSG = (-4)
MQXCC_SUPPRESS_EXIT = (-5)
MQXCC_CLOSE_CHANNEL = (-6)
MQXCC_REQUEST_ACK = (-7)
MQXCC_FAILED = (-8)

# Exit User Area Value
MQXUA_NONE = b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"

# Exit User Area Value (array form)
MQXUA_NONE_ARRAY = [b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0',
                    b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0', b'\0']

# Cluster Cache Types
MQCLCT_STATIC = 0
MQCLCT_DYNAMIC = 1

# Multicast Events
MQMCEV_PACKET_LOSS = 1
MQMCEV_HEARTBEAT_TIMEOUT = 2
MQMCEV_VERSION_CONFLICT = 3
MQMCEV_RELIABILITY = 4
MQMCEV_CLOSED_TRANS = 5
MQMCEV_STREAM_ERROR = 6
MQMCEV_NEW_SOURCE = 10
MQMCEV_RECEIVE_QUEUE_TRIMMED = 11
MQMCEV_PACKET_LOSS_NACK_EXPIRE = 12
MQMCEV_ACK_RETRIES_EXCEEDED = 13
MQMCEV_STREAM_SUSPEND_NACK = 14
MQMCEV_STREAM_RESUME_NACK = 15
MQMCEV_STREAM_EXPELLED = 16
MQMCEV_FIRST_MESSAGE = 20
MQMCEV_LATE_JOIN_FAILURE = 21
MQMCEV_MESSAGE_LOSS = 22
MQMCEV_SEND_PACKET_FAILURE = 23
MQMCEV_REPAIR_DELAY = 24
MQMCEV_MEMORY_ALERT_ON = 25
MQMCEV_MEMORY_ALERT_OFF = 26
MQMCEV_NACK_ALERT_ON = 27
MQMCEV_NACK_ALERT_OFF = 28
MQMCEV_REPAIR_ALERT_ON = 29
MQMCEV_REPAIR_ALERT_OFF = 30
MQMCEV_RELIABILITY_CHANGED = 31
MQMCEV_SHM_DEST_UNUSABLE = 80
MQMCEV_SHM_PORT_UNUSABLE = 81
MQMCEV_CCT_GETTIME_FAILED = 110
MQMCEV_DEST_INTERFACE_FAILURE = 120
MQMCEV_DEST_INTERFACE_FAILOVER = 121
MQMCEV_PORT_INTERFACE_FAILURE = 122
MQMCEV_PORT_INTERFACE_FAILOVER = 123

##################################################################
# Values Related to MQXCNVC Function
###################################################################

# Conversion Options
MQDCC_DEFAULT_CONVERSION = 0x00000001
MQDCC_FILL_TARGET_BUFFER = 0x00000002
MQDCC_INT_DEFAULT_CONVERSION = 0x00000004
MQDCC_SOURCE_ENC_NATIVE = 0x00000020
MQDCC_SOURCE_ENC_NORMAL = 0x00000010
MQDCC_SOURCE_ENC_REVERSED = 0x00000020
MQDCC_SOURCE_ENC_UNDEFINED = 0x00000000
MQDCC_TARGET_ENC_NATIVE = 0x00000200
MQDCC_TARGET_ENC_NORMAL = 0x00000100
MQDCC_TARGET_ENC_REVERSED = 0x00000200
MQDCC_TARGET_ENC_UNDEFINED = 0x00000000
MQDCC_NONE = 0x00000000

# Conversion Options Masks and Factors
MQDCC_SOURCE_ENC_MASK = 0x000000F0
MQDCC_TARGET_ENC_MASK = 0x00000F00
MQDCC_SOURCE_ENC_FACTOR = 16
MQDCC_TARGET_ENC_FACTOR = 256
