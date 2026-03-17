'''
NL80211 module
==============

TODO
'''
import struct
import datetime
from pyroute2.common import map_namespace
from pyroute2.netlink import genlmsg
from pyroute2.netlink.generic import GenericNetlinkSocket
from pyroute2.netlink.nlsocket import Marshal
from pyroute2.netlink import nla
from pyroute2.netlink import nla_base

# nl80211 commands

NL80211_CMD_UNSPEC = 0
NL80211_CMD_GET_WIPHY = 1
NL80211_CMD_SET_WIPHY = 2
NL80211_CMD_NEW_WIPHY = 3
NL80211_CMD_DEL_WIPHY = 4
NL80211_CMD_GET_INTERFACE = 5
NL80211_CMD_SET_INTERFACE = 6
NL80211_CMD_NEW_INTERFACE = 7
NL80211_CMD_DEL_INTERFACE = 8
NL80211_CMD_GET_KEY = 9
NL80211_CMD_SET_KEY = 10
NL80211_CMD_NEW_KEY = 11
NL80211_CMD_DEL_KEY = 12
NL80211_CMD_GET_BEACON = 13
NL80211_CMD_SET_BEACON = 14
NL80211_CMD_START_AP = 15
NL80211_CMD_NEW_BEACON = NL80211_CMD_START_AP
NL80211_CMD_STOP_AP = 16
NL80211_CMD_DEL_BEACON = NL80211_CMD_STOP_AP
NL80211_CMD_GET_STATION = 17
NL80211_CMD_SET_STATION = 18
NL80211_CMD_NEW_STATION = 19
NL80211_CMD_DEL_STATION = 20
NL80211_CMD_GET_MPATH = 21
NL80211_CMD_SET_MPATH = 22
NL80211_CMD_NEW_MPATH = 23
NL80211_CMD_DEL_MPATH = 24
NL80211_CMD_SET_BSS = 25
NL80211_CMD_SET_REG = 26
NL80211_CMD_REQ_SET_REG = 27
NL80211_CMD_GET_MESH_CONFIG = 28
NL80211_CMD_SET_MESH_CONFIG = 29
NL80211_CMD_SET_MGMT_EXTRA_IE = 30
NL80211_CMD_GET_REG = 31
NL80211_CMD_GET_SCAN = 32
NL80211_CMD_TRIGGER_SCAN = 33
NL80211_CMD_NEW_SCAN_RESULTS = 34
NL80211_CMD_SCAN_ABORTED = 35
NL80211_CMD_REG_CHANGE = 36
NL80211_CMD_AUTHENTICATE = 37
NL80211_CMD_ASSOCIATE = 38
NL80211_CMD_DEAUTHENTICATE = 39
NL80211_CMD_DISASSOCIATE = 40
NL80211_CMD_MICHAEL_MIC_FAILURE = 41
NL80211_CMD_REG_BEACON_HINT = 42
NL80211_CMD_JOIN_IBSS = 43
NL80211_CMD_LEAVE_IBSS = 44
NL80211_CMD_TESTMODE = 45
NL80211_CMD_CONNECT = 46
NL80211_CMD_ROAM = 47
NL80211_CMD_DISCONNECT = 48
NL80211_CMD_SET_WIPHY_NETNS = 49
NL80211_CMD_GET_SURVEY = 50
NL80211_CMD_NEW_SURVEY_RESULTS = 51
NL80211_CMD_SET_PMKSA = 52
NL80211_CMD_DEL_PMKSA = 53
NL80211_CMD_FLUSH_PMKSA = 54
NL80211_CMD_REMAIN_ON_CHANNEL = 55
NL80211_CMD_CANCEL_REMAIN_ON_CHANNEL = 56
NL80211_CMD_SET_TX_BITRATE_MASK = 57
NL80211_CMD_REGISTER_FRAME = 58
NL80211_CMD_REGISTER_ACTION = NL80211_CMD_REGISTER_FRAME
NL80211_CMD_FRAME = 59
NL80211_CMD_ACTION = NL80211_CMD_FRAME
NL80211_CMD_FRAME_TX_STATUS = 60
NL80211_CMD_ACTION_TX_STATUS = NL80211_CMD_FRAME_TX_STATUS
NL80211_CMD_SET_POWER_SAVE = 61
NL80211_CMD_GET_POWER_SAVE = 62
NL80211_CMD_SET_CQM = 63
NL80211_CMD_NOTIFY_CQM = 64
NL80211_CMD_SET_CHANNEL = 65
NL80211_CMD_SET_WDS_PEER = 66
NL80211_CMD_FRAME_WAIT_CANCEL = 67
NL80211_CMD_JOIN_MESH = 68
NL80211_CMD_LEAVE_MESH = 69
NL80211_CMD_UNPROT_DEAUTHENTICATE = 70
NL80211_CMD_UNPROT_DISASSOCIATE = 71
NL80211_CMD_NEW_PEER_CANDIDATE = 72
NL80211_CMD_GET_WOWLAN = 73
NL80211_CMD_SET_WOWLAN = 74
NL80211_CMD_START_SCHED_SCAN = 75
NL80211_CMD_STOP_SCHED_SCAN = 76
NL80211_CMD_SCHED_SCAN_RESULTS = 77
NL80211_CMD_SCHED_SCAN_STOPPED = 78
NL80211_CMD_SET_REKEY_OFFLOAD = 79
NL80211_CMD_PMKSA_CANDIDATE = 80
NL80211_CMD_TDLS_OPER = 81
NL80211_CMD_TDLS_MGMT = 82
NL80211_CMD_UNEXPECTED_FRAME = 83
NL80211_CMD_PROBE_CLIENT = 84
NL80211_CMD_REGISTER_BEACONS = 85
NL80211_CMD_UNEXPECTED_4ADDR_FRAME = 86
NL80211_CMD_SET_NOACK_MAP = 87
NL80211_CMD_CH_SWITCH_NOTIFY = 88
NL80211_CMD_START_P2P_DEVICE = 89
NL80211_CMD_STOP_P2P_DEVICE = 90
NL80211_CMD_CONN_FAILED = 91
NL80211_CMD_SET_MCAST_RATE = 92
NL80211_CMD_SET_MAC_ACL = 93
NL80211_CMD_RADAR_DETECT = 94
NL80211_CMD_GET_PROTOCOL_FEATURES = 95
NL80211_CMD_UPDATE_FT_IES = 96
NL80211_CMD_FT_EVENT = 97
NL80211_CMD_CRIT_PROTOCOL_START = 98
NL80211_CMD_CRIT_PROTOCOL_STOP = 99
NL80211_CMD_GET_COALESCE = 100
NL80211_CMD_SET_COALESCE = 101
NL80211_CMD_CHANNEL_SWITCH = 102
NL80211_CMD_VENDOR = 103
NL80211_CMD_SET_QOS_MAP = 104
NL80211_CMD_ADD_TX_TS = 105
NL80211_CMD_DEL_TX_TS = 106
NL80211_CMD_GET_MPP = 107
NL80211_CMD_JOIN_OCB = 108
NL80211_CMD_LEAVE_OCB = 109
NL80211_CMD_CH_SWITCH_STARTED_NOTIFY = 110
NL80211_CMD_TDLS_CHANNEL_SWITCH = 111
NL80211_CMD_TDLS_CANCEL_CHANNEL_SWITCH = 112
NL80211_CMD_WIPHY_REG_CHANGE = 113
NL80211_CMD_MAX = NL80211_CMD_WIPHY_REG_CHANGE
(NL80211_NAMES, NL80211_VALUES) = map_namespace('NL80211_CMD_', globals())

NL80211_BSS_ELEMENTS_SSID = 0
NL80211_BSS_ELEMENTS_SUPPORTED_RATES = 1
NL80211_BSS_ELEMENTS_CHANNEL = 3
NL80211_BSS_ELEMENTS_TIM = 5
NL80211_BSS_ELEMENTS_RSN = 48
NL80211_BSS_ELEMENTS_EXTENDED_RATE = 50
NL80211_BSS_ELEMENTS_VENDOR = 221

BSS_MEMBERSHIP_SELECTOR_HT_PHY = 127
BSS_MEMBERSHIP_SELECTOR_VHT_PHY = 126

# interface types
NL80211_IFTYPE_UNSPECIFIED = 0
NL80211_IFTYPE_ADHOC = 1
NL80211_IFTYPE_STATION = 2
NL80211_IFTYPE_AP = 3
NL80211_IFTYPE_AP_VLAN = 4
NL80211_IFTYPE_WDS = 5
NL80211_IFTYPE_MONITOR = 6
NL80211_IFTYPE_MESH_POINT = 7
NL80211_IFTYPE_P2P_CLIENT = 8
NL80211_IFTYPE_P2P_GO = 9
NL80211_IFTYPE_P2P_DEVICE = 10
NL80211_IFTYPE_OCB = 11
(IFTYPE_NAMES, IFTYPE_VALUES) = map_namespace('NL80211_IFTYPE_',
                                              globals(),
                                              normalize=True)

# channel width
NL80211_CHAN_WIDTH_20_NOHT = 0  # 20 MHz non-HT channel
NL80211_CHAN_WIDTH_20 = 1       # 20 MHz HT channel
NL80211_CHAN_WIDTH_40 = 2       # 40 MHz HT channel
NL80211_CHAN_WIDTH_80 = 3       # 80 MHz channel
NL80211_CHAN_WIDTH_80P80 = 4    # 80+80 MHz channel
NL80211_CHAN_WIDTH_160 = 5      # 160 MHz channel
NL80211_CHAN_WIDTH_5 = 6        # 5 MHz OFDM channel
NL80211_CHAN_WIDTH_10 = 7       # 10 MHz OFDM channel
(CHAN_WIDTH, WIDTH_VALUES) = map_namespace('NL80211_CHAN_WIDTH_',
                                           globals(),
                                           normalize=True)

# BSS "status"
NL80211_BSS_STATUS_AUTHENTICATED = 0  # Authenticated with this BS
NL80211_BSS_STATUS_ASSOCIATED = 1     # Associated with this BSS
NL80211_BSS_STATUS_IBSS_JOINED = 2    # Joined to this IBSS
(BSS_STATUS_NAMES, BSS_STATUS_VALUES) = map_namespace('NL80211_BSS_STATUS_',
                                                      globals(),
                                                      normalize=True)

NL80211_SCAN_FLAG_LOW_PRIORITY = 1 << 0
NL80211_SCAN_FLAG_FLUSH = 1 << 1
NL80211_SCAN_FLAG_AP = 1 << 2
NL80211_SCAN_FLAG_RANDOM_ADDR = 1 << 3
NL80211_SCAN_FLAG_FILS_MAX_CHANNEL_TIME = 1 << 4
NL80211_SCAN_FLAG_ACCEPT_BCAST_PROBE_RESP = 1 << 5
NL80211_SCAN_FLAG_OCE_PROBE_REQ_HIGH_TX_RATE = 1 << 6
NL80211_SCAN_FLAG_OCE_PROBE_REQ_DEFERRAL_SUPPRESSION = 1 << 7
(SCAN_FLAGS_NAMES, SCAN_FLAGS_VALUES) = map_namespace('NL80211_SCAN_FLAG_',
                                                      globals())

NL80211_STA_FLAG_AUTHORIZED = 1
NL80211_STA_FLAG_SHORT_PREAMBLE = 2
NL80211_STA_FLAG_WME = 3
NL80211_STA_FLAG_MFP = 4
NL80211_STA_FLAG_AUTHENTICATED = 5
NL80211_STA_FLAG_TDLS_PEER = 6
NL80211_STA_FLAG_ASSOCIATED = 7
(STA_FLAG_NAMES, STA_FLAG_VALUES) = map_namespace('NL80211_STA_FLAG_',
                                                  globals())


class nl80211cmd(genlmsg):
    prefix = 'NL80211_ATTR_'
    nla_map = (('NL80211_ATTR_UNSPEC', 'none'),
               ('NL80211_ATTR_WIPHY', 'uint32'),
               ('NL80211_ATTR_WIPHY_NAME', 'asciiz'),
               ('NL80211_ATTR_IFINDEX', 'uint32'),
               ('NL80211_ATTR_IFNAME', 'asciiz'),
               ('NL80211_ATTR_IFTYPE', 'uint32'),
               ('NL80211_ATTR_MAC', 'l2addr'),
               ('NL80211_ATTR_KEY_DATA', 'hex'),
               ('NL80211_ATTR_KEY_IDX', 'hex'),
               ('NL80211_ATTR_KEY_CIPHER', 'uint32'),
               ('NL80211_ATTR_KEY_SEQ', 'hex'),
               ('NL80211_ATTR_KEY_DEFAULT', 'hex'),
               ('NL80211_ATTR_BEACON_INTERVAL', 'hex'),
               ('NL80211_ATTR_DTIM_PERIOD', 'hex'),
               ('NL80211_ATTR_BEACON_HEAD', 'hex'),
               ('NL80211_ATTR_BEACON_TAIL', 'hex'),
               ('NL80211_ATTR_STA_AID', 'hex'),
               ('NL80211_ATTR_STA_FLAGS', 'hex'),
               ('NL80211_ATTR_STA_LISTEN_INTERVAL', 'hex'),
               ('NL80211_ATTR_STA_SUPPORTED_RATES', 'hex'),
               ('NL80211_ATTR_STA_VLAN', 'hex'),
               ('NL80211_ATTR_STA_INFO', 'STAInfo'),
               ('NL80211_ATTR_WIPHY_BANDS', 'hex'),
               ('NL80211_ATTR_MNTR_FLAGS', 'hex'),
               ('NL80211_ATTR_MESH_ID', 'hex'),
               ('NL80211_ATTR_STA_PLINK_ACTION', 'hex'),
               ('NL80211_ATTR_MPATH_NEXT_HOP', 'hex'),
               ('NL80211_ATTR_MPATH_INFO', 'hex'),
               ('NL80211_ATTR_BSS_CTS_PROT', 'hex'),
               ('NL80211_ATTR_BSS_SHORT_PREAMBLE', 'hex'),
               ('NL80211_ATTR_BSS_SHORT_SLOT_TIME', 'hex'),
               ('NL80211_ATTR_HT_CAPABILITY', 'hex'),
               ('NL80211_ATTR_SUPPORTED_IFTYPES', 'hex'),
               ('NL80211_ATTR_REG_ALPHA2', 'hex'),
               ('NL80211_ATTR_REG_RULES', 'hex'),
               ('NL80211_ATTR_MESH_CONFIG', 'hex'),
               ('NL80211_ATTR_BSS_BASIC_RATES', 'hex'),
               ('NL80211_ATTR_WIPHY_TXQ_PARAMS', 'hex'),
               ('NL80211_ATTR_WIPHY_FREQ', 'uint32'),
               ('NL80211_ATTR_WIPHY_CHANNEL_TYPE', 'hex'),
               ('NL80211_ATTR_KEY_DEFAULT_MGMT', 'hex'),
               ('NL80211_ATTR_MGMT_SUBTYPE', 'hex'),
               ('NL80211_ATTR_IE', 'hex'),
               ('NL80211_ATTR_MAX_NUM_SCAN_SSIDS', 'uint8'),
               ('NL80211_ATTR_SCAN_FREQUENCIES', 'hex'),
               ('NL80211_ATTR_SCAN_SSIDS', '*string'),
               ('NL80211_ATTR_GENERATION', 'uint32'),
               ('NL80211_ATTR_BSS', 'bss'),
               ('NL80211_ATTR_REG_INITIATOR', 'hex'),
               ('NL80211_ATTR_REG_TYPE', 'hex'),
               ('NL80211_ATTR_SUPPORTED_COMMANDS', 'hex'),
               ('NL80211_ATTR_FRAME', 'hex'),
               ('NL80211_ATTR_SSID', 'string'),
               ('NL80211_ATTR_AUTH_TYPE', 'uint32'),
               ('NL80211_ATTR_REASON_CODE', 'uint16'),
               ('NL80211_ATTR_KEY_TYPE', 'hex'),
               ('NL80211_ATTR_MAX_SCAN_IE_LEN', 'uint16'),
               ('NL80211_ATTR_CIPHER_SUITES', 'hex'),
               ('NL80211_ATTR_FREQ_BEFORE', 'hex'),
               ('NL80211_ATTR_FREQ_AFTER', 'hex'),
               ('NL80211_ATTR_FREQ_FIXED', 'hex'),
               ('NL80211_ATTR_WIPHY_RETRY_SHORT', 'uint8'),
               ('NL80211_ATTR_WIPHY_RETRY_LONG', 'uint8'),
               ('NL80211_ATTR_WIPHY_FRAG_THRESHOLD', 'hex'),
               ('NL80211_ATTR_WIPHY_RTS_THRESHOLD', 'hex'),
               ('NL80211_ATTR_TIMED_OUT', 'hex'),
               ('NL80211_ATTR_USE_MFP', 'hex'),
               ('NL80211_ATTR_STA_FLAGS2', 'hex'),
               ('NL80211_ATTR_CONTROL_PORT', 'hex'),
               ('NL80211_ATTR_TESTDATA', 'hex'),
               ('NL80211_ATTR_PRIVACY', 'hex'),
               ('NL80211_ATTR_DISCONNECTED_BY_AP', 'hex'),
               ('NL80211_ATTR_STATUS_CODE', 'hex'),
               ('NL80211_ATTR_CIPHER_SUITES_PAIRWISE', 'hex'),
               ('NL80211_ATTR_CIPHER_SUITE_GROUP', 'hex'),
               ('NL80211_ATTR_WPA_VERSIONS', 'hex'),
               ('NL80211_ATTR_AKM_SUITES', 'hex'),
               ('NL80211_ATTR_REQ_IE', 'hex'),
               ('NL80211_ATTR_RESP_IE', 'hex'),
               ('NL80211_ATTR_PREV_BSSID', 'hex'),
               ('NL80211_ATTR_KEY', 'hex'),
               ('NL80211_ATTR_KEYS', 'hex'),
               ('NL80211_ATTR_PID', 'hex'),
               ('NL80211_ATTR_4ADDR', 'hex'),
               ('NL80211_ATTR_SURVEY_INFO', 'hex'),
               ('NL80211_ATTR_PMKID', 'hex'),
               ('NL80211_ATTR_MAX_NUM_PMKIDS', 'uint8'),
               ('NL80211_ATTR_DURATION', 'hex'),
               ('NL80211_ATTR_COOKIE', 'hex'),
               ('NL80211_ATTR_WIPHY_COVERAGE_CLASS', 'uint8'),
               ('NL80211_ATTR_TX_RATES', 'hex'),
               ('NL80211_ATTR_FRAME_MATCH', 'hex'),
               ('NL80211_ATTR_ACK', 'hex'),
               ('NL80211_ATTR_PS_STATE', 'hex'),
               ('NL80211_ATTR_CQM', 'hex'),
               ('NL80211_ATTR_LOCAL_STATE_CHANGE', 'hex'),
               ('NL80211_ATTR_AP_ISOLATE', 'hex'),
               ('NL80211_ATTR_WIPHY_TX_POWER_SETTING', 'hex'),
               ('NL80211_ATTR_WIPHY_TX_POWER_LEVEL', 'hex'),
               ('NL80211_ATTR_TX_FRAME_TYPES', 'hex'),
               ('NL80211_ATTR_RX_FRAME_TYPES', 'hex'),
               ('NL80211_ATTR_FRAME_TYPE', 'hex'),
               ('NL80211_ATTR_CONTROL_PORT_ETHERTYPE', 'hex'),
               ('NL80211_ATTR_CONTROL_PORT_NO_ENCRYPT', 'hex'),
               ('NL80211_ATTR_SUPPORT_IBSS_RSN', 'hex'),
               ('NL80211_ATTR_WIPHY_ANTENNA_TX', 'hex'),
               ('NL80211_ATTR_WIPHY_ANTENNA_RX', 'hex'),
               ('NL80211_ATTR_MCAST_RATE', 'hex'),
               ('NL80211_ATTR_OFFCHANNEL_TX_OK', 'hex'),
               ('NL80211_ATTR_BSS_HT_OPMODE', 'hex'),
               ('NL80211_ATTR_KEY_DEFAULT_TYPES', 'hex'),
               ('NL80211_ATTR_MAX_REMAIN_ON_CHANNEL_DURATION', 'hex'),
               ('NL80211_ATTR_MESH_SETUP', 'hex'),
               ('NL80211_ATTR_WIPHY_ANTENNA_AVAIL_TX', 'uint32'),
               ('NL80211_ATTR_WIPHY_ANTENNA_AVAIL_RX', 'uint32'),
               ('NL80211_ATTR_SUPPORT_MESH_AUTH', 'hex'),
               ('NL80211_ATTR_STA_PLINK_STATE', 'hex'),
               ('NL80211_ATTR_WOWLAN_TRIGGERS', 'hex'),
               ('NL80211_ATTR_WOWLAN_TRIGGERS_SUPPORTED', 'hex'),
               ('NL80211_ATTR_SCHED_SCAN_INTERVAL', 'hex'),
               ('NL80211_ATTR_INTERFACE_COMBINATIONS', 'hex'),
               ('NL80211_ATTR_SOFTWARE_IFTYPES', 'hex'),
               ('NL80211_ATTR_REKEY_DATA', 'hex'),
               ('NL80211_ATTR_MAX_NUM_SCHED_SCAN_SSIDS', 'uint8'),
               ('NL80211_ATTR_MAX_SCHED_SCAN_IE_LEN', 'uint16'),
               ('NL80211_ATTR_SCAN_SUPP_RATES', 'hex'),
               ('NL80211_ATTR_HIDDEN_SSID', 'hex'),
               ('NL80211_ATTR_IE_PROBE_RESP', 'hex'),
               ('NL80211_ATTR_IE_ASSOC_RESP', 'hex'),
               ('NL80211_ATTR_STA_WME', 'hex'),
               ('NL80211_ATTR_SUPPORT_AP_UAPSD', 'hex'),
               ('NL80211_ATTR_ROAM_SUPPORT', 'hex'),
               ('NL80211_ATTR_SCHED_SCAN_MATCH', 'hex'),
               ('NL80211_ATTR_MAX_MATCH_SETS', 'uint8'),
               ('NL80211_ATTR_PMKSA_CANDIDATE', 'hex'),
               ('NL80211_ATTR_TX_NO_CCK_RATE', 'hex'),
               ('NL80211_ATTR_TDLS_ACTION', 'hex'),
               ('NL80211_ATTR_TDLS_DIALOG_TOKEN', 'hex'),
               ('NL80211_ATTR_TDLS_OPERATION', 'hex'),
               ('NL80211_ATTR_TDLS_SUPPORT', 'hex'),
               ('NL80211_ATTR_TDLS_EXTERNAL_SETUP', 'hex'),
               ('NL80211_ATTR_DEVICE_AP_SME', 'hex'),
               ('NL80211_ATTR_DONT_WAIT_FOR_ACK', 'hex'),
               ('NL80211_ATTR_FEATURE_FLAGS', 'hex'),
               ('NL80211_ATTR_PROBE_RESP_OFFLOAD', 'hex'),
               ('NL80211_ATTR_PROBE_RESP', 'hex'),
               ('NL80211_ATTR_DFS_REGION', 'hex'),
               ('NL80211_ATTR_DISABLE_HT', 'hex'),
               ('NL80211_ATTR_HT_CAPABILITY_MASK', 'hex'),
               ('NL80211_ATTR_NOACK_MAP', 'hex'),
               ('NL80211_ATTR_INACTIVITY_TIMEOUT', 'hex'),
               ('NL80211_ATTR_RX_SIGNAL_DBM', 'hex'),
               ('NL80211_ATTR_BG_SCAN_PERIOD', 'hex'),
               ('NL80211_ATTR_WDEV', 'uint64'),
               ('NL80211_ATTR_USER_REG_HINT_TYPE', 'hex'),
               ('NL80211_ATTR_CONN_FAILED_REASON', 'hex'),
               ('NL80211_ATTR_SAE_DATA', 'hex'),
               ('NL80211_ATTR_VHT_CAPABILITY', 'hex'),
               ('NL80211_ATTR_SCAN_FLAGS', 'uint32'),
               ('NL80211_ATTR_CHANNEL_WIDTH', 'uint32'),
               ('NL80211_ATTR_CENTER_FREQ1', 'uint32'),
               ('NL80211_ATTR_CENTER_FREQ2', 'uint32'),
               ('NL80211_ATTR_P2P_CTWINDOW', 'hex'),
               ('NL80211_ATTR_P2P_OPPPS', 'hex'),
               ('NL80211_ATTR_LOCAL_MESH_POWER_MODE', 'hex'),
               ('NL80211_ATTR_ACL_POLICY', 'hex'),
               ('NL80211_ATTR_MAC_ADDRS', 'hex'),
               ('NL80211_ATTR_MAC_ACL_MAX', 'hex'),
               ('NL80211_ATTR_RADAR_EVENT', 'hex'),
               ('NL80211_ATTR_EXT_CAPA', 'array(uint8)'),
               ('NL80211_ATTR_EXT_CAPA_MASK', 'array(uint8)'),
               ('NL80211_ATTR_STA_CAPABILITY', 'hex'),
               ('NL80211_ATTR_STA_EXT_CAPABILITY', 'hex'),
               ('NL80211_ATTR_PROTOCOL_FEATURES', 'hex'),
               ('NL80211_ATTR_SPLIT_WIPHY_DUMP', 'hex'),
               ('NL80211_ATTR_DISABLE_VHT', 'hex'),
               ('NL80211_ATTR_VHT_CAPABILITY_MASK', 'array(uint8)'),
               ('NL80211_ATTR_MDID', 'hex'),
               ('NL80211_ATTR_IE_RIC', 'hex'),
               ('NL80211_ATTR_CRIT_PROT_ID', 'hex'),
               ('NL80211_ATTR_MAX_CRIT_PROT_DURATION', 'hex'),
               ('NL80211_ATTR_PEER_AID', 'hex'),
               ('NL80211_ATTR_COALESCE_RULE', 'hex'),
               ('NL80211_ATTR_CH_SWITCH_COUNT', 'hex'),
               ('NL80211_ATTR_CH_SWITCH_BLOCK_TX', 'hex'),
               ('NL80211_ATTR_CSA_IES', 'hex'),
               ('NL80211_ATTR_CSA_C_OFF_BEACON', 'hex'),
               ('NL80211_ATTR_CSA_C_OFF_PRESP', 'hex'),
               ('NL80211_ATTR_RXMGMT_FLAGS', 'hex'),
               ('NL80211_ATTR_STA_SUPPORTED_CHANNELS', 'hex'),
               ('NL80211_ATTR_STA_SUPPORTED_OPER_CLASSES', 'hex'),
               ('NL80211_ATTR_HANDLE_DFS', 'hex'),
               ('NL80211_ATTR_SUPPORT_5_MHZ', 'hex'),
               ('NL80211_ATTR_SUPPORT_10_MHZ', 'hex'),
               ('NL80211_ATTR_OPMODE_NOTIF', 'hex'),
               ('NL80211_ATTR_VENDOR_ID', 'hex'),
               ('NL80211_ATTR_VENDOR_SUBCMD', 'hex'),
               ('NL80211_ATTR_VENDOR_DATA', 'hex'),
               ('NL80211_ATTR_VENDOR_EVENTS', 'hex'),
               ('NL80211_ATTR_QOS_MAP', 'hex'),
               ('NL80211_ATTR_MAC_HINT', 'hex'),
               ('NL80211_ATTR_WIPHY_FREQ_HINT', 'hex'),
               ('NL80211_ATTR_MAX_AP_ASSOC_STA', 'hex'),
               ('NL80211_ATTR_TDLS_PEER_CAPABILITY', 'hex'),
               ('NL80211_ATTR_SOCKET_OWNER', 'hex'),
               ('NL80211_ATTR_CSA_C_OFFSETS_TX', 'hex'),
               ('NL80211_ATTR_MAX_CSA_COUNTERS', 'hex'),
               ('NL80211_ATTR_TDLS_INITIATOR', 'hex'),
               ('NL80211_ATTR_USE_RRM', 'hex'),
               ('NL80211_ATTR_WIPHY_DYN_ACK', 'hex'),
               ('NL80211_ATTR_TSID', 'hex'),
               ('NL80211_ATTR_USER_PRIO', 'hex'),
               ('NL80211_ATTR_ADMITTED_TIME', 'hex'),
               ('NL80211_ATTR_SMPS_MODE', 'hex'),
               ('NL80211_ATTR_OPER_CLASS', 'hex'),
               ('NL80211_ATTR_MAC_MASK', 'hex'),
               ('NL80211_ATTR_WIPHY_SELF_MANAGED_REG', 'hex'),
               ('NUM_NL80211_ATTR', 'hex'))

    class bss(nla):
        class elementsBinary(nla_base):

            def binary_rates(self, offset, length):
                init = offset
                string = ""
                while (offset - init) < length:
                    byte, = struct.unpack_from('B', self.data, offset)
                    r = byte & 0x7f
                    if r == BSS_MEMBERSHIP_SELECTOR_VHT_PHY and byte & 0x80:
                        string += "VHT"
                    elif r == BSS_MEMBERSHIP_SELECTOR_HT_PHY and byte & 0x80:
                        string += "HT"
                    else:
                        string += "%d.%d" % (r / 2, 5 * (r & 1))
                    offset += 1
                    string += "%s " % ("*" if byte & 0x80 else "")
                return string

            def binary_tim(self, offset):
                (count,
                 period,
                 bitmapc,
                 bitmap0) = struct.unpack_from('BBBB',
                                               self.data,
                                               offset)
                return ("DTIM Count {0} DTIM Period {1} Bitmap Control 0x{2} "
                        "Bitmap[0] 0x{3}".format(count,
                                                 period,
                                                 bitmapc,
                                                 bitmap0))

            def decode_nlas(self):
                return

            def decode(self):
                nla_base.decode(self)

                self.value = {}

                init = offset = self.offset + 4

                while (offset - init) < (self.length - 4):
                    (msg_type, length) = struct.unpack_from('BB',
                                                            self.data,
                                                            offset)
                    if msg_type == NL80211_BSS_ELEMENTS_SSID:
                        self.value["SSID"], = (struct
                                               .unpack_from('%is' % length,
                                                            self.data,
                                                            offset + 2))

                    if msg_type == NL80211_BSS_ELEMENTS_SUPPORTED_RATES:
                        supported_rates = self.binary_rates(offset + 2, length)
                        self.value["SUPPORTED_RATES"] = supported_rates

                    if msg_type == NL80211_BSS_ELEMENTS_CHANNEL:
                        channel, = struct.unpack_from('B',
                                                      self.data,
                                                      offset + 2)
                        self.value["CHANNEL"] = channel

                    if msg_type == NL80211_BSS_ELEMENTS_TIM:
                        self.value["TRAFFIC INDICATION MAP"] = \
                            self.binary_tim(offset + 2)

                    if msg_type == NL80211_BSS_ELEMENTS_RSN:
                        self.value["RSN"], = (struct
                                              .unpack_from('%is' % length,
                                                           self.data,
                                                           offset + 2))

                    if msg_type == NL80211_BSS_ELEMENTS_EXTENDED_RATE:
                        extended_rates = self.binary_rates(offset + 2, length)
                        self.value["EXTENDED_RATES"] = extended_rates

                    if msg_type == NL80211_BSS_ELEMENTS_VENDOR:
                        # There may be multiple vendor IEs, create a list
                        if "VENDOR" not in self.value.keys():
                            self.value["VENDOR"] = []
                        vendor_ie, = (struct.unpack_from('%is' % length,
                                                         self.data,
                                                         offset + 2))
                        self.value["VENDOR"].append(vendor_ie)

                    offset += length + 2

        class TSF(nla_base):
            """Timing Synchronization Function"""
            def decode(self):
                nla_base.decode(self)

                offset = self.offset + 4
                self.value = {}
                tsf, = struct.unpack_from('Q', self.data, offset)
                self.value["VALUE"] = tsf
                # TSF is in microseconds
                self.value["TIME"] = datetime.timedelta(microseconds=tsf)

        class SignalMBM(nla_base):
            def decode(self):
                nla_base.decode(self)
                offset = self.offset + 4
                self.value = {}
                ss, = struct.unpack_from('i', self.data, offset)
                self.value["VALUE"] = ss
                self.value["SIGNAL_STRENGTH"] = {"VALUE": ss / 100.0,
                                                 "UNITS": "dBm"}

        class capability(nla_base):
            # iw scan.c
            WLAN_CAPABILITY_ESS = (1 << 0)
            WLAN_CAPABILITY_IBSS = (1 << 1)
            WLAN_CAPABILITY_CF_POLLABLE = (1 << 2)
            WLAN_CAPABILITY_CF_POLL_REQUEST = (1 << 3)
            WLAN_CAPABILITY_PRIVACY = (1 << 4)
            WLAN_CAPABILITY_SHORT_PREAMBLE = (1 << 5)
            WLAN_CAPABILITY_PBCC = (1 << 6)
            WLAN_CAPABILITY_CHANNEL_AGILITY = (1 << 7)
            WLAN_CAPABILITY_SPECTRUM_MGMT = (1 << 8)
            WLAN_CAPABILITY_QOS = (1 << 9)
            WLAN_CAPABILITY_SHORT_SLOT_TIME = (1 << 10)
            WLAN_CAPABILITY_APSD = (1 << 11)
            WLAN_CAPABILITY_RADIO_MEASURE = (1 << 12)
            WLAN_CAPABILITY_DSSS_OFDM = (1 << 13)
            WLAN_CAPABILITY_DEL_BACK = (1 << 14)
            WLAN_CAPABILITY_IMM_BACK = (1 << 15)

#            def decode_nlas(self):
#                return

            def decode(self):
                nla_base.decode(self)

                offset = self.offset + 4
                self.value = {}
                capa, = struct.unpack_from('H', self.data, offset)
                self.value["VALUE"] = capa

                s = []
                if capa & self.WLAN_CAPABILITY_ESS:
                    s.append("ESS")
                if capa & self.WLAN_CAPABILITY_IBSS:
                    s.append("IBSS")
                if capa & self.WLAN_CAPABILITY_CF_POLLABLE:
                    s.append("CfPollable")
                if capa & self.WLAN_CAPABILITY_CF_POLL_REQUEST:
                    s.append("CfPollReq")
                if capa & self.WLAN_CAPABILITY_PRIVACY:
                    s.append("Privacy")
                if capa & self.WLAN_CAPABILITY_SHORT_PREAMBLE:
                    s.append("ShortPreamble")
                if capa & self.WLAN_CAPABILITY_PBCC:
                    s.append("PBCC")
                if capa & self.WLAN_CAPABILITY_CHANNEL_AGILITY:
                    s.append("ChannelAgility")
                if capa & self.WLAN_CAPABILITY_SPECTRUM_MGMT:
                    s.append("SpectrumMgmt")
                if capa & self.WLAN_CAPABILITY_QOS:
                    s.append("QoS")
                if capa & self.WLAN_CAPABILITY_SHORT_SLOT_TIME:
                    s.append("ShortSlotTime")
                if capa & self.WLAN_CAPABILITY_APSD:
                    s.append("APSD")
                if capa & self.WLAN_CAPABILITY_RADIO_MEASURE:
                    s.append("RadioMeasure")
                if capa & self.WLAN_CAPABILITY_DSSS_OFDM:
                    s.append("DSSS-OFDM")
                if capa & self.WLAN_CAPABILITY_DEL_BACK:
                    s.append("DelayedBACK")
                if capa & self.WLAN_CAPABILITY_IMM_BACK:
                    s.append("ImmediateBACK")

                self.value['CAPABILITIES'] = " ".join(s)

        prefix = 'NL80211_BSS_'
        nla_map = (('__NL80211_BSS_INVALID', 'hex'),
                   ('NL80211_BSS_BSSID', 'hex'),
                   ('NL80211_BSS_FREQUENCY', 'uint32'),
                   ('NL80211_BSS_TSF', 'TSF'),
                   ('NL80211_BSS_BEACON_INTERVAL', 'uint16'),
                   ('NL80211_BSS_CAPABILITY', 'capability'),
                   ('NL80211_BSS_INFORMATION_ELEMENTS', 'elementsBinary'),
                   ('NL80211_BSS_SIGNAL_MBM', 'SignalMBM'),
                   ('NL80211_BSS_SIGNAL_UNSPEC', 'uint8'),
                   ('NL80211_BSS_STATUS', 'uint32'),
                   ('NL80211_BSS_SEEN_MS_AGO', 'uint32'),
                   ('NL80211_BSS_BEACON_IES', 'elementsBinary'),
                   ('NL80211_BSS_CHAN_WIDTH', 'uint32'),
                   ('NL80211_BSS_BEACON_TSF', 'uint64'),
                   ('NL80211_BSS_PRESP_DATA', 'hex'),
                   ('NL80211_BSS_MAX', 'hex')
                   )

    class STAInfo(nla):
        class STAFlags(nla_base):
            '''
            Decode the flags that may be set.
            See nl80211.h: struct nl80211_sta_flag_update,
            NL80211_STA_INFO_STA_FLAGS
            '''

            def decode_nlas(self):
                return

            def decode(self):
                nla_base.decode(self)
                self.value = {}
                self.value["AUTHORIZED"] = False
                self.value["SHORT_PREAMBLE"] = False
                self.value["WME"] = False
                self.value["MFP"] = False
                self.value["AUTHENTICATED"] = False
                self.value["TDLS_PEER"] = False
                self.value["ASSOCIATED"] = False

                init = offset = self.offset + 4
                while (offset - init) < (self.length - 4):
                    (msg_type, length) = struct.unpack_from('BB',
                                                            self.data,
                                                            offset)
                    mask, set_ = struct.unpack_from('II',
                                                    self.data,
                                                    offset + 2)

                    if mask & NL80211_STA_FLAG_AUTHORIZED:
                        if set_ & NL80211_STA_FLAG_AUTHORIZED:
                            self.value["AUTHORIZED"] = True

                    if mask & NL80211_STA_FLAG_SHORT_PREAMBLE:
                        if set_ & NL80211_STA_FLAG_SHORT_PREAMBLE:
                            self.value["SHORT_PREAMBLE"] = True

                    if mask & NL80211_STA_FLAG_WME:
                        if set_ & NL80211_STA_FLAG_WME:
                            self.value["WME"] = True

                    if mask & NL80211_STA_FLAG_MFP:
                        if set_ & NL80211_STA_FLAG_MFP:
                            self.value["MFP"] = True

                    if mask & NL80211_STA_FLAG_AUTHENTICATED:
                        if set_ & NL80211_STA_FLAG_AUTHENTICATED:
                            self.value["AUTHENTICATED"] = True

                    if mask & NL80211_STA_FLAG_TDLS_PEER:
                        if set_ & NL80211_STA_FLAG_TDLS_PEER:
                            self.value["TDLS_PEER"] = True

                    if mask & NL80211_STA_FLAG_ASSOCIATED:
                        if set_ & NL80211_STA_FLAG_ASSOCIATED:
                            self.value["ASSOCIATED"] = True

                    offset += length + 2

        prefix = 'NL80211_STA_INFO_'
        nla_map = (('__NL80211_STA_INFO_INVALID', 'hex'),
                   ('NL80211_STA_INFO_INACTIVE_TIME', 'uint32'),
                   ('NL80211_STA_INFO_RX_BYTES', 'uint32'),
                   ('NL80211_STA_INFO_TX_BYTES', 'uint32'),
                   ('NL80211_STA_INFO_LLID', 'uint16'),
                   ('NL80211_STA_INFO_PLID', 'uint16'),
                   ('NL80211_STA_INFO_PLINK_STATE', 'uint8'),
                   ('NL80211_STA_INFO_SIGNAL', 'int8'),
                   ('NL80211_STA_INFO_TX_BITRATE', 'hex'),
                   ('NL80211_STA_INFO_RX_PACKETS', 'uint32'),
                   ('NL80211_STA_INFO_TX_PACKETS', 'uint32'),
                   ('NL80211_STA_INFO_TX_RETRIES', 'uint32'),
                   ('NL80211_STA_INFO_TX_FAILED', 'uint32'),
                   ('NL80211_STA_INFO_SIGNAL_AVG', 'int8'),
                   ('NL80211_STA_INFO_RX_BITRATE', 'hex'),
                   ('NL80211_STA_INFO_BSS_PARAM', 'hex'),
                   ('NL80211_STA_INFO_CONNECTED_TIME', 'uint32'),
                   ('NL80211_STA_INFO_STA_FLAGS', 'STAFlags'),
                   ('NL80211_STA_INFO_BEACON_LOSS', 'uint32'),
                   ('NL80211_STA_INFO_T_OFFSET', 'int64'),
                   ('NL80211_STA_INFO_LOCAL_PM', 'hex'),
                   ('NL80211_STA_INFO_PEER_PM', 'hex'),
                   ('NL80211_STA_INFO_NONPEER_PM', 'hex'),
                   ('NL80211_STA_INFO_RX_BYTES64', 'uint64'),
                   ('NL80211_STA_INFO_TX_BYTES64', 'uint64'),
                   ('NL80211_STA_INFO_CHAIN_SIGNAL', 'string'),
                   ('NL80211_STA_INFO_CHAIN_SIGNAL_AVG', 'string'),
                   ('NL80211_STA_INFO_EXPECTED_THROUGHPUT', 'uint32'),
                   ('NL80211_STA_INFO_RX_DROP_MISC', 'uint32'),
                   ('NL80211_STA_INFO_BEACON_RX', 'uint64'),
                   ('NL80211_STA_INFO_BEACON_SIGNAL_AVG', 'uint8'),
                   ('NL80211_STA_INFO_TID_STATS', 'hex'),
                   ('NL80211_STA_INFO_RX_DURATION', 'uint64'),
                   ('NL80211_STA_INFO_PAD', 'hex'),
                   ('NL80211_STA_INFO_MAX', 'hex')
                   )


class MarshalNl80211(Marshal):
    msg_map = {NL80211_CMD_UNSPEC: nl80211cmd,
               NL80211_CMD_GET_WIPHY: nl80211cmd,
               NL80211_CMD_SET_WIPHY: nl80211cmd,
               NL80211_CMD_NEW_WIPHY: nl80211cmd,
               NL80211_CMD_DEL_WIPHY: nl80211cmd,
               NL80211_CMD_GET_INTERFACE: nl80211cmd,
               NL80211_CMD_SET_INTERFACE: nl80211cmd,
               NL80211_CMD_NEW_INTERFACE: nl80211cmd,
               NL80211_CMD_DEL_INTERFACE: nl80211cmd,
               NL80211_CMD_GET_KEY: nl80211cmd,
               NL80211_CMD_SET_KEY: nl80211cmd,
               NL80211_CMD_NEW_KEY: nl80211cmd,
               NL80211_CMD_DEL_KEY: nl80211cmd,
               NL80211_CMD_GET_BEACON: nl80211cmd,
               NL80211_CMD_SET_BEACON: nl80211cmd,
               NL80211_CMD_START_AP: nl80211cmd,
               NL80211_CMD_NEW_BEACON: nl80211cmd,
               NL80211_CMD_STOP_AP: nl80211cmd,
               NL80211_CMD_DEL_BEACON: nl80211cmd,
               NL80211_CMD_GET_STATION: nl80211cmd,
               NL80211_CMD_SET_STATION: nl80211cmd,
               NL80211_CMD_NEW_STATION: nl80211cmd,
               NL80211_CMD_DEL_STATION: nl80211cmd,
               NL80211_CMD_GET_MPATH: nl80211cmd,
               NL80211_CMD_SET_MPATH: nl80211cmd,
               NL80211_CMD_NEW_MPATH: nl80211cmd,
               NL80211_CMD_DEL_MPATH: nl80211cmd,
               NL80211_CMD_SET_BSS: nl80211cmd,
               NL80211_CMD_SET_REG: nl80211cmd,
               NL80211_CMD_REQ_SET_REG: nl80211cmd,
               NL80211_CMD_GET_MESH_CONFIG: nl80211cmd,
               NL80211_CMD_SET_MESH_CONFIG: nl80211cmd,
               NL80211_CMD_SET_MGMT_EXTRA_IE: nl80211cmd,
               NL80211_CMD_GET_REG: nl80211cmd,
               NL80211_CMD_GET_SCAN: nl80211cmd,
               NL80211_CMD_TRIGGER_SCAN: nl80211cmd,
               NL80211_CMD_NEW_SCAN_RESULTS: nl80211cmd,
               NL80211_CMD_SCAN_ABORTED: nl80211cmd,
               NL80211_CMD_REG_CHANGE: nl80211cmd,
               NL80211_CMD_AUTHENTICATE: nl80211cmd,
               NL80211_CMD_ASSOCIATE: nl80211cmd,
               NL80211_CMD_DEAUTHENTICATE: nl80211cmd,
               NL80211_CMD_DISASSOCIATE: nl80211cmd,
               NL80211_CMD_MICHAEL_MIC_FAILURE: nl80211cmd,
               NL80211_CMD_REG_BEACON_HINT: nl80211cmd,
               NL80211_CMD_JOIN_IBSS: nl80211cmd,
               NL80211_CMD_LEAVE_IBSS: nl80211cmd,
               NL80211_CMD_TESTMODE: nl80211cmd,
               NL80211_CMD_CONNECT: nl80211cmd,
               NL80211_CMD_ROAM: nl80211cmd,
               NL80211_CMD_DISCONNECT: nl80211cmd,
               NL80211_CMD_SET_WIPHY_NETNS: nl80211cmd,
               NL80211_CMD_GET_SURVEY: nl80211cmd,
               NL80211_CMD_NEW_SURVEY_RESULTS: nl80211cmd,
               NL80211_CMD_SET_PMKSA: nl80211cmd,
               NL80211_CMD_DEL_PMKSA: nl80211cmd,
               NL80211_CMD_FLUSH_PMKSA: nl80211cmd,
               NL80211_CMD_REMAIN_ON_CHANNEL: nl80211cmd,
               NL80211_CMD_CANCEL_REMAIN_ON_CHANNEL: nl80211cmd,
               NL80211_CMD_SET_TX_BITRATE_MASK: nl80211cmd,
               NL80211_CMD_REGISTER_FRAME: nl80211cmd,
               NL80211_CMD_REGISTER_ACTION: nl80211cmd,
               NL80211_CMD_FRAME: nl80211cmd,
               NL80211_CMD_ACTION: nl80211cmd,
               NL80211_CMD_FRAME_TX_STATUS: nl80211cmd,
               NL80211_CMD_ACTION_TX_STATUS: nl80211cmd,
               NL80211_CMD_SET_POWER_SAVE: nl80211cmd,
               NL80211_CMD_GET_POWER_SAVE: nl80211cmd,
               NL80211_CMD_SET_CQM: nl80211cmd,
               NL80211_CMD_NOTIFY_CQM: nl80211cmd,
               NL80211_CMD_SET_CHANNEL: nl80211cmd,
               NL80211_CMD_SET_WDS_PEER: nl80211cmd,
               NL80211_CMD_FRAME_WAIT_CANCEL: nl80211cmd,
               NL80211_CMD_JOIN_MESH: nl80211cmd,
               NL80211_CMD_LEAVE_MESH: nl80211cmd,
               NL80211_CMD_UNPROT_DEAUTHENTICATE: nl80211cmd,
               NL80211_CMD_UNPROT_DISASSOCIATE: nl80211cmd,
               NL80211_CMD_NEW_PEER_CANDIDATE: nl80211cmd,
               NL80211_CMD_GET_WOWLAN: nl80211cmd,
               NL80211_CMD_SET_WOWLAN: nl80211cmd,
               NL80211_CMD_START_SCHED_SCAN: nl80211cmd,
               NL80211_CMD_STOP_SCHED_SCAN: nl80211cmd,
               NL80211_CMD_SCHED_SCAN_RESULTS: nl80211cmd,
               NL80211_CMD_SCHED_SCAN_STOPPED: nl80211cmd,
               NL80211_CMD_SET_REKEY_OFFLOAD: nl80211cmd,
               NL80211_CMD_PMKSA_CANDIDATE: nl80211cmd,
               NL80211_CMD_TDLS_OPER: nl80211cmd,
               NL80211_CMD_TDLS_MGMT: nl80211cmd,
               NL80211_CMD_UNEXPECTED_FRAME: nl80211cmd,
               NL80211_CMD_PROBE_CLIENT: nl80211cmd,
               NL80211_CMD_REGISTER_BEACONS: nl80211cmd,
               NL80211_CMD_UNEXPECTED_4ADDR_FRAME: nl80211cmd,
               NL80211_CMD_SET_NOACK_MAP: nl80211cmd,
               NL80211_CMD_CH_SWITCH_NOTIFY: nl80211cmd,
               NL80211_CMD_START_P2P_DEVICE: nl80211cmd,
               NL80211_CMD_STOP_P2P_DEVICE: nl80211cmd,
               NL80211_CMD_CONN_FAILED: nl80211cmd,
               NL80211_CMD_SET_MCAST_RATE: nl80211cmd,
               NL80211_CMD_SET_MAC_ACL: nl80211cmd,
               NL80211_CMD_RADAR_DETECT: nl80211cmd,
               NL80211_CMD_GET_PROTOCOL_FEATURES: nl80211cmd,
               NL80211_CMD_UPDATE_FT_IES: nl80211cmd,
               NL80211_CMD_FT_EVENT: nl80211cmd,
               NL80211_CMD_CRIT_PROTOCOL_START: nl80211cmd,
               NL80211_CMD_CRIT_PROTOCOL_STOP: nl80211cmd,
               NL80211_CMD_GET_COALESCE: nl80211cmd,
               NL80211_CMD_SET_COALESCE: nl80211cmd,
               NL80211_CMD_CHANNEL_SWITCH: nl80211cmd,
               NL80211_CMD_VENDOR: nl80211cmd,
               NL80211_CMD_SET_QOS_MAP: nl80211cmd,
               NL80211_CMD_ADD_TX_TS: nl80211cmd,
               NL80211_CMD_DEL_TX_TS: nl80211cmd,
               NL80211_CMD_GET_MPP: nl80211cmd,
               NL80211_CMD_JOIN_OCB: nl80211cmd,
               NL80211_CMD_LEAVE_OCB: nl80211cmd,
               NL80211_CMD_CH_SWITCH_STARTED_NOTIFY: nl80211cmd,
               NL80211_CMD_TDLS_CHANNEL_SWITCH: nl80211cmd,
               NL80211_CMD_TDLS_CANCEL_CHANNEL_SWITCH: nl80211cmd,
               NL80211_CMD_WIPHY_REG_CHANGE: nl80211cmd}

    def fix_message(self, msg):
        try:
            msg['event'] = NL80211_VALUES[msg['cmd']]
        except Exception:
            pass


class NL80211(GenericNetlinkSocket):

    def __init__(self):
        GenericNetlinkSocket.__init__(self)
        self.marshal = MarshalNl80211()

    def bind(self, groups=0, **kwarg):
        GenericNetlinkSocket.bind(self, 'nl80211', nl80211cmd,
                                  groups, None, **kwarg)
