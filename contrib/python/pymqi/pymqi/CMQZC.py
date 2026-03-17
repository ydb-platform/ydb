from struct import calcsize

# CMQZC - Generated from 9.2.5 CD header file

# 64bit
if calcsize("P") == 8:
    MQZED_LENGTH_1 = 64
    MQZED_LENGTH_2 = 72
    MQZAD_LENGTH_1 = 80
    MQZAD_LENGTH_2 = 80
    MQZFP_LENGTH_1 = 24
else:
    MQZED_LENGTH_1 = 56
    MQZED_LENGTH_2 = 60
    MQZAD_LENGTH_1 = 72
    MQZAD_LENGTH_2 = 76
    MQZFP_LENGTH_1 = 20

###################################################################
# Values Related to MQZED Structure
###################################################################

# Structure Identifier
MQZED_STRUC_ID = b"ZED "

# Structure Identifier (array form)
MQZED_STRUC_ID_ARRAY = [b'Z', b'E', b'D', b' ']

# Structure Version Number
MQZED_VERSION_1 = 1
MQZED_VERSION_2 = 2
MQZED_CURRENT_VERSION = 2

# Structure Length
MQZED_CURRENT_LENGTH = MQZED_LENGTH_2

###################################################################
# Values Related to MQZAC Structure
###################################################################

# Structure Identifier
MQZAC_STRUC_ID = b"ZAC "

# Structure Identifier (array form)
MQZAC_STRUC_ID_ARRAY = [b'Z', b'A', b'C', b' ']

# Structure Version Number
MQZAC_VERSION_1 = 1
MQZAC_CURRENT_VERSION = 1

# Structure Length
MQZAC_LENGTH_1 = 84
MQZAC_CURRENT_LENGTH = 84

# Authentication Types
MQZAT_INITIAL_CONTEXT = 0
MQZAT_CHANGE_CONTEXT = 1

###################################################################
# Values Related to MQZAD Structure
###################################################################

# Structure Identifier
MQZAD_STRUC_ID = b"ZAD "

# Structure Identifier (array form)
MQZAD_STRUC_ID_ARRAY = [b'Z', b'A', b'D', b' ']

# Structure Version Number
MQZAD_VERSION_1 = 1
MQZAD_VERSION_2 = 2
MQZAD_CURRENT_VERSION = 2

# Structure Length
MQZAD_CURRENT_LENGTH = MQZAD_LENGTH_2

###################################################################
# Values Related to MQZFP Structure
###################################################################

# Structure Identifier
MQZFP_STRUC_ID = b"ZFP "

# Structure Identifier (array form)
MQZFP_STRUC_ID_ARRAY = [b'Z', b'F', b'P', b' ']

# Structure Version Number
MQZFP_VERSION_1 = 1
MQZFP_CURRENT_VERSION = 1

# Structure Length
MQZFP_CURRENT_LENGTH = MQZFP_LENGTH_1

###################################################################
# Values Related to MQZIC Structure
###################################################################

# Structure Identifier
MQZIC_STRUC_ID = b"ZIC "

# Structure Identifier (array form)
MQZIC_STRUC_ID_ARRAY = [b'Z', b'I', b'C', b' ']

# Structure Version Number
MQZIC_VERSION_1 = 1
MQZIC_CURRENT_VERSION = 1

# Structure Length
MQZIC_LENGTH_1 = 84
MQZIC_CURRENT_LENGTH = 84

###################################################################
# Values Related to All Services
###################################################################

# Initialization Options
MQZIO_PRIMARY = 0
MQZIO_SECONDARY = 1

# Termination Options
MQZTO_PRIMARY = 0
MQZTO_SECONDARY = 1

# Continuation Indicator
MQZCI_DEFAULT = 0
MQZCI_CONTINUE = 0
MQZCI_STOP = 1

###################################################################
# Values Related to Authority Service
###################################################################

# Service Interface Version
MQZAS_VERSION_1 = 1
MQZAS_VERSION_2 = 2
MQZAS_VERSION_3 = 3
MQZAS_VERSION_4 = 4
MQZAS_VERSION_5 = 5
MQZAS_VERSION_6 = 6

# Authorizations
MQZAO_CONNECT = 0x00000001
MQZAO_BROWSE = 0x00000002
MQZAO_INPUT = 0x00000004
MQZAO_OUTPUT = 0x00000008
MQZAO_INQUIRE = 0x00000010
MQZAO_SET = 0x00000020
MQZAO_PASS_IDENTITY_CONTEXT = 0x00000040
MQZAO_PASS_ALL_CONTEXT = 0x00000080
MQZAO_SET_IDENTITY_CONTEXT = 0x00000100
MQZAO_SET_ALL_CONTEXT = 0x00000200
MQZAO_ALTERNATE_USER_AUTHORITY = 0x00000400
MQZAO_PUBLISH = 0x00000800
MQZAO_SUBSCRIBE = 0x00001000
MQZAO_RESUME = 0x00002000
MQZAO_ALL_MQI = 0x00003FFF
MQZAO_CREATE = 0x00010000
MQZAO_DELETE = 0x00020000
MQZAO_DISPLAY = 0x00040000
MQZAO_CHANGE = 0x00080000
MQZAO_CLEAR = 0x00100000
MQZAO_CONTROL = 0x00200000
MQZAO_CONTROL_EXTENDED = 0x00400000
MQZAO_AUTHORIZE = 0x00800000
MQZAO_ALL_ADMIN = 0x00FE0000
MQZAO_SYSTEM = 0x02000000
MQZAO_ALL = 0x02FE3FFF
MQZAO_REMOVE = 0x01000000
MQZAO_NONE = 0x00000000
MQZAO_CREATE_ONLY = 0x04000000

# Entity Types
MQZAET_NONE = 0x00000000
MQZAET_PRINCIPAL = 0x00000001
MQZAET_GROUP = 0x00000002
MQZAET_UNKNOWN = 0x00000003

# Start-Enumeration Indicator
MQZSE_START = 1
MQZSE_CONTINUE = 0

# Selector Indicator
MQZSL_NOT_RETURNED = 0
MQZSL_RETURNED = 1

###################################################################
# Values Related to Name Service
###################################################################

# Service Interface Version
MQZNS_VERSION_1 = 1

###################################################################
# Values Related to Userid Service
###################################################################

# Service Interface Version
MQZUS_VERSION_1 = 1

###################################################################
# Values Related to MQZEP Function
###################################################################

# Function ids common to all services
MQZID_INIT = 0
MQZID_TERM = 1

# Function ids for Authority service
MQZID_INIT_AUTHORITY = 0
MQZID_TERM_AUTHORITY = 1
MQZID_CHECK_AUTHORITY = 2
MQZID_COPY_ALL_AUTHORITY = 3
MQZID_DELETE_AUTHORITY = 4
MQZID_SET_AUTHORITY = 5
MQZID_GET_AUTHORITY = 6
MQZID_GET_EXPLICIT_AUTHORITY = 7
MQZID_REFRESH_CACHE = 8
MQZID_ENUMERATE_AUTHORITY_DATA = 9
MQZID_AUTHENTICATE_USER = 10
MQZID_FREE_USER = 11
MQZID_INQUIRE = 12
MQZID_CHECK_PRIVILEGED = 13

# Function ids for Name service
MQZID_INIT_NAME = 0
MQZID_TERM_NAME = 1
MQZID_LOOKUP_NAME = 2
MQZID_INSERT_NAME = 3
MQZID_DELETE_NAME = 4

# Function ids for Userid service
MQZID_INIT_USERID = 0
MQZID_TERM_USERID = 1
MQZID_FIND_USERID = 2