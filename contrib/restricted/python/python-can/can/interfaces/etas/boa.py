import ctypes

from ...exceptions import CanInitializationError, CanOperationError

try:
    # try to load libraries from the system default paths
    _csi = ctypes.windll.LoadLibrary("dll-csiBind")
    _oci = ctypes.windll.LoadLibrary("dll-ocdProxy")
except FileNotFoundError:
    # try to load libraries with hardcoded paths
    if ctypes.sizeof(ctypes.c_voidp) == 4:
        # 32 bit
        path = "C:/Program Files (x86)/ETAS/BOA_V2/Bin/Win32/Dll/Framework/"
    elif ctypes.sizeof(ctypes.c_voidp) == 8:
        # 64 bit
        path = "C:/Program Files/ETAS/BOA_V2/Bin/x64/Dll/Framework/"
    _csi = ctypes.windll.LoadLibrary(path + "dll-csiBind")
    _oci = ctypes.windll.LoadLibrary(path + "dll-ocdProxy")


# define helper functions to use with errcheck


def errcheck_init(result, func, _arguments):
    # unfortunately, we can't use OCI_GetError here
    # because we don't always have a handle to use
    # text = ctypes.create_string_buffer(500)
    # OCI_GetError(self.ctrl, ec, text, 500)
    if result != 0x0:
        raise CanInitializationError(f"{func.__name__} failed with error 0x{result:X}")
    return result


def errcheck_oper(result, func, _arguments):
    if result != 0x0:
        raise CanOperationError(f"{func.__name__} failed with error 0x{result:X}")
    return result


# Common (BOA)

BOA_ResultCode = ctypes.c_uint32
BOA_Handle = ctypes.c_int32
BOA_Time = ctypes.c_int64

BOA_NO_VALUE = -1
BOA_NO_HANDLE = BOA_Handle(BOA_NO_VALUE)
BOA_NO_TIME = BOA_Time(BOA_NO_VALUE)


class BOA_UuidBin(ctypes.Structure):
    _fields_ = [("data", ctypes.c_uint8 * 16)]


class BOA_Version(ctypes.Structure):
    _fields_ = [
        ("majorVersion", ctypes.c_uint8),
        ("minorVersion", ctypes.c_uint8),
        ("bugfix", ctypes.c_uint8),
        ("build", ctypes.c_uint8),
    ]


class BOA_UuidVersion(ctypes.Structure):
    _fields_ = [("uuid", BOA_UuidBin), ("version", BOA_Version)]


class BOA_ServiceId(ctypes.Structure):
    _fields_ = [("api", BOA_UuidVersion), ("access", BOA_UuidVersion)]


class BOA_ServiceIdParam(ctypes.Structure):
    _fields_ = [
        ("id", BOA_ServiceId),
        ("count", ctypes.c_uint32),
        ("accessParam", ctypes.c_char * 128),
    ]


# Connection Service Interface (CSI)

# CSI - Search For Service (SFS)

CSI_NodeType = ctypes.c_uint32
CSI_NODE_MIN = CSI_NodeType(0)
CSI_NODE_MAX = CSI_NodeType(0x7FFF)


class CSI_NodeRange(ctypes.Structure):
    _fields_ = [("min", CSI_NodeType), ("max", CSI_NodeType)]


class CSI_SubItem(ctypes.Structure):
    _fields_ = [
        ("server", BOA_ServiceIdParam),
        ("nodeType", CSI_NodeType),
        ("uriName", ctypes.c_char * 128),
        ("visibleName", ctypes.c_char * 4),
        ("version", BOA_Version),
        ("reserved2", ctypes.c_char * 88),
        ("serverAffinity", BOA_UuidBin),
        ("requiredAffinity0", BOA_UuidBin),
        ("reserved", ctypes.c_int32 * 4),
        ("requiredAffinity1", BOA_UuidBin),
        ("count", ctypes.c_int32),
        ("requiredAPI", BOA_ServiceId * 4),
    ]


class CSI_Tree(ctypes.Structure):
    pass


CSI_Tree._fields_ = [
    ("item", CSI_SubItem),
    ("sibling", ctypes.POINTER(CSI_Tree)),
    ("child", ctypes.POINTER(CSI_Tree)),
    ("childrenProbed", ctypes.c_int),
]

CSI_CreateProtocolTree = _csi.CSI_CreateProtocolTree
CSI_CreateProtocolTree.argtypes = [
    ctypes.c_char_p,
    CSI_NodeRange,
    ctypes.POINTER(ctypes.POINTER(CSI_Tree)),
]
CSI_CreateProtocolTree.restype = BOA_ResultCode
CSI_CreateProtocolTree.errcheck = errcheck_init

CSI_DestroyProtocolTree = _csi.CSI_DestroyProtocolTree
CSI_DestroyProtocolTree.argtypes = [
    ctypes.POINTER(CSI_Tree),
]
CSI_DestroyProtocolTree.restype = BOA_ResultCode
CSI_DestroyProtocolTree.errcheck = errcheck_oper

# Open Controller Interface (OCI)

# OCI Common - Global Types

OCI_NO_VALUE = BOA_NO_VALUE
OCI_NO_HANDLE = BOA_NO_HANDLE
OCI_Handle = BOA_Handle
OCI_Time = BOA_Time

# OCI Common - Controller Handling

OCI_ControllerHandle = OCI_Handle

OCI_ControllerPropertiesMode = ctypes.c_uint32
OCI_CONTROLLER_MODE_RUNNING = OCI_ControllerPropertiesMode(0)
OCI_CONTROLLER_MODE_SUSPENDED = OCI_ControllerPropertiesMode(1)

OCI_SelfReceptionMode = ctypes.c_uint32
OCI_SELF_RECEPTION_OFF = OCI_SelfReceptionMode(0)
OCI_SELF_RECEPTION_ON = OCI_SelfReceptionMode(1)

# OCI Common - Event Handling

# OCI Common - Error Management

OCI_ErrorCode = BOA_ResultCode

OCI_InternalErrorEvent = ctypes.c_uint32
OCI_INTERNAL_GENERAL_ERROR = OCI_InternalErrorEvent(0)


class OCI_InternalErrorEventMessage(ctypes.Structure):
    _fields_ = [
        ("timeStamp", OCI_Time),
        ("tag", ctypes.c_uint32),
        ("eventCode", OCI_InternalErrorEvent),
        ("errorCode", OCI_ErrorCode),
    ]


OCI_GetError = _oci.OCI_GetError
OCI_GetError.argtypes = [
    OCI_Handle,
    OCI_ErrorCode,
    ctypes.c_char_p,
    ctypes.c_uint32,
]
OCI_GetError.restype = OCI_ErrorCode
OCI_GetError.errcheck = errcheck_oper

# OCI Common - Queue Handling

OCI_QueueHandle = OCI_Handle

OCI_QueueEvent = ctypes.c_uint32
OCI_QUEUE_UNDERRUN = OCI_QueueEvent(0)
OCI_QUEUE_EMPTY = OCI_QueueEvent(1)
OCI_QUEUE_NOT_EMPTY = OCI_QueueEvent(2)
OCI_QUEUE_LOW_WATERMARK = OCI_QueueEvent(3)
OCI_QUEUE_HIGH_WATERMARK = OCI_QueueEvent(4)
OCI_QUEUE_FULL = OCI_QueueEvent(5)
OCI_QUEUE_OVERFLOW = OCI_QueueEvent(6)


class OCI_QueueEventMessage(ctypes.Structure):
    _fields_ = [
        ("timeStamp", OCI_Time),
        ("tag", ctypes.c_uint32),
        ("eventCode", OCI_QueueEvent),
        ("destination", ctypes.c_uint32),
    ]


OCI_ResetQueue = _oci.OCI_ResetQueue
OCI_ResetQueue.argtypes = [OCI_QueueHandle]
OCI_ResetQueue.restype = OCI_ErrorCode
OCI_ResetQueue.errcheck = errcheck_oper

# OCI Common - Timer Handling

OCI_NO_TIME = BOA_NO_TIME

OCI_TimeReferenceScale = ctypes.c_uint32
OCI_TimeReferenceScaleUnknown = OCI_TimeReferenceScale(0)
OCI_TimeReferenceScaleTAI = OCI_TimeReferenceScale(1)
OCI_TimeReferenceScaleUTC = OCI_TimeReferenceScale(2)

OCI_TimerEvent = ctypes.c_uint32
OCI_TIMER_EVENT_SYNC_LOCK = OCI_TimerEvent(0)
OCI_TIMER_EVENT_SYNC_LOSS = OCI_TimerEvent(1)
OCI_TIMER_EVENT_LEAP_SECOND = OCI_TimerEvent(2)


class OCI_TimerCapabilities(ctypes.Structure):
    _fields_ = [
        ("localClockID", ctypes.c_char * 40),
        ("format", ctypes.c_uint32),
        ("tickFrequency", ctypes.c_uint32),
        ("ticksPerIncrement", ctypes.c_uint32),
        ("localStratumLevel", ctypes.c_uint32),
        ("localReferenceScale", OCI_TimeReferenceScale),
        ("localTimeOriginIso8601", ctypes.c_char * 40),
        ("syncSlave", ctypes.c_uint32),
        ("syncMaster", ctypes.c_uint32),
    ]


class OCI_TimerEventMessage(ctypes.Structure):
    _fields_ = [
        ("timeStamp", OCI_Time),
        ("tag", ctypes.c_uint32),
        ("eventCode", OCI_TimerEvent),
        ("destination", ctypes.c_uint32),
    ]


OCI_GetTimerCapabilities = _oci.OCI_GetTimerCapabilities
OCI_GetTimerCapabilities.argtypes = [
    OCI_ControllerHandle,
    ctypes.POINTER(OCI_TimerCapabilities),
]
OCI_GetTimerCapabilities.restype = OCI_ErrorCode
OCI_GetTimerCapabilities.errcheck = errcheck_init

OCI_GetTimerValue = _oci.OCI_GetTimerValue
OCI_GetTimerValue.argtypes = [
    OCI_ControllerHandle,
    ctypes.POINTER(OCI_Time),
]
OCI_GetTimerValue.restype = OCI_ErrorCode
OCI_GetTimerValue.errcheck = errcheck_oper

# OCI CAN

# OCI CAN - CAN-FD

OCI_CANFDRxMode = ctypes.c_uint32
OCI_CANFDRXMODE_CANFD_FRAMES_IGNORED = OCI_CANFDRxMode(1)
OCI_CANFDRXMODE_CANFD_FRAMES_USING_CAN_MESSAGE = OCI_CANFDRxMode(2)
OCI_CANFDRXMODE_CANFD_FRAMES_USING_CANFD_MESSAGE = OCI_CANFDRxMode(4)
OCI_CANFDRXMODE_CANFD_FRAMES_USING_CANFD_MESSAGE_PADDING = OCI_CANFDRxMode(8)

OCI_CANRxMode = ctypes.c_uint32
OCI_CAN_RXMODE_CAN_FRAMES_IGNORED = OCI_CANRxMode(1)
OCI_CAN_RXMODE_CAN_FRAMES_USING_CAN_MESSAGE = OCI_CANRxMode(2)

OCI_CANFDTxConfig = ctypes.c_uint32
OCI_CANFDTX_USE_CAN_FRAMES_ONLY = OCI_CANFDTxConfig(1)
OCI_CANFDTX_USE_CANFD_FRAMES_ONLY = OCI_CANFDTxConfig(2)
OCI_CANFDTX_USE_CAN_AND_CANFD_FRAMES = OCI_CANFDTxConfig(4)


class OCI_CANFDRxConfig(ctypes.Structure):
    _fields_ = [
        ("canRxMode", OCI_CANRxMode),
        ("canFdRxMode", OCI_CANFDRxMode),
    ]


class OCI_CANFDConfiguration(ctypes.Structure):
    _fields_ = [
        ("dataBitRate", ctypes.c_uint32),
        ("dataSamplePoint", ctypes.c_uint32),
        ("dataBTL_Cycles", ctypes.c_uint32),
        ("dataSJW", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("txSecondarySamplePointOffset", ctypes.c_uint32),
        ("canFdRxConfig", OCI_CANFDRxConfig),
        ("canFdTxConfig", OCI_CANFDTxConfig),
        ("txSecondarySamplePointFilterWindow", ctypes.c_uint16),
        ("reserved", ctypes.c_uint16),
    ]


class OCI_CANFDRxMessage(ctypes.Structure):
    _fields_ = [
        ("timeStamp", OCI_Time),
        ("tag", ctypes.c_uint32),
        ("frameID", ctypes.c_uint32),
        ("flags", ctypes.c_uint16),
        ("res", ctypes.c_uint8),
        ("size", ctypes.c_uint8),
        ("res1", ctypes.c_uint8 * 4),
        ("data", ctypes.c_uint8 * 64),
    ]


class OCI_CANFDTxMessage(ctypes.Structure):
    _fields_ = [
        ("frameID", ctypes.c_uint32),
        ("flags", ctypes.c_uint16),
        ("res", ctypes.c_uint8),
        ("size", ctypes.c_uint8),
        ("data", ctypes.c_uint8 * 64),
    ]


# OCI CAN - Initialization

OCI_CAN_THREE_SAMPLES_PER_BIT = 2
OCI_CAN_SINGLE_SYNC_EDGE = 1
OCI_CAN_MEDIA_HIGH_SPEED = 1

OCI_CAN_STATE_ACTIVE = 0x00000001
OCI_CAN_STATE_PASSIVE = 0x00000002
OCI_CAN_STATE_ERRLIMIT = 0x00000004
OCI_CAN_STATE_BUSOFF = 0x00000008

OCI_CANBusParticipationMode = ctypes.c_uint32
OCI_BUSMODE_PASSIVE = OCI_CANBusParticipationMode(1)
OCI_BUSMODE_ACTIVE = OCI_CANBusParticipationMode(2)

OCI_CANBusTransmissionPolicies = ctypes.c_uint32
OCI_CANTX_UNDEFINED = OCI_CANBusTransmissionPolicies(0)
OCI_CANTX_DONTCARE = OCI_CANBusTransmissionPolicies(0)
OCI_CANTX_FIFO = OCI_CANBusTransmissionPolicies(1)
OCI_CANTX_BESTEFFORT = OCI_CANBusTransmissionPolicies(2)


class OCI_CANConfiguration(ctypes.Structure):
    _fields_ = [
        ("baudrate", ctypes.c_uint32),
        ("samplePoint", ctypes.c_uint32),
        ("samplesPerBit", ctypes.c_uint32),
        ("BTL_Cycles", ctypes.c_uint32),
        ("SJW", ctypes.c_uint32),
        ("syncEdge", ctypes.c_uint32),
        ("physicalMedia", ctypes.c_uint32),
        ("selfReceptionMode", OCI_SelfReceptionMode),
        ("busParticipationMode", OCI_CANBusParticipationMode),
        ("canFDEnabled", ctypes.c_uint32),
        ("canFDConfig", OCI_CANFDConfiguration),
        ("canTxPolicy", OCI_CANBusTransmissionPolicies),
    ]


class OCI_CANControllerProperties(ctypes.Structure):
    _fields_ = [
        ("mode", OCI_ControllerPropertiesMode),
    ]


class OCI_CANControllerCapabilities(ctypes.Structure):
    _fields_ = [
        ("samplesPerBit", ctypes.c_uint32),
        ("syncEdge", ctypes.c_uint32),
        ("physicalMedia", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
        ("busEvents", ctypes.c_uint32),
        ("errorFrames", ctypes.c_uint32),
        ("messageFlags", ctypes.c_uint32),
        ("canFDSupport", ctypes.c_uint32),
        ("canFDMaxDataSize", ctypes.c_uint32),
        ("canFDMaxQualifiedDataRate", ctypes.c_uint32),
        ("canFDMaxDataRate", ctypes.c_uint32),
        ("canFDRxConfig_CANMode", ctypes.c_uint32),
        ("canFDRxConfig_CANFDMode", ctypes.c_uint32),
        ("canFDTxConfig_Mode", ctypes.c_uint32),
        ("canBusParticipationMode", ctypes.c_uint32),
        ("canTxPolicies", ctypes.c_uint32),
    ]


class OCI_CANControllerStatus(ctypes.Structure):
    _fields_ = [
        ("reserved", ctypes.c_uint32),
        ("stateCode", ctypes.c_uint32),
    ]


OCI_CreateCANControllerNoSearch = _oci.OCI_CreateCANControllerNoSearch
OCI_CreateCANControllerNoSearch.argtypes = [
    ctypes.c_char_p,
    ctypes.POINTER(BOA_Version),
    ctypes.POINTER(CSI_Tree),
    ctypes.POINTER(OCI_ControllerHandle),
]
OCI_CreateCANControllerNoSearch.restype = OCI_ErrorCode
OCI_CreateCANControllerNoSearch.errcheck = errcheck_init

OCI_OpenCANController = _oci.OCI_OpenCANController
OCI_OpenCANController.argtypes = [
    OCI_ControllerHandle,
    ctypes.POINTER(OCI_CANConfiguration),
    ctypes.POINTER(OCI_CANControllerProperties),
]
OCI_OpenCANController.restype = OCI_ErrorCode
# no .errcheck, since we tolerate OCI_WARN_PARAM_ADAPTED warning
# OCI_OpenCANController.errcheck = errcheck_init

OCI_CloseCANController = _oci.OCI_CloseCANController
OCI_CloseCANController.argtypes = [OCI_ControllerHandle]
OCI_CloseCANController.restype = OCI_ErrorCode
OCI_CloseCANController.errcheck = errcheck_oper

OCI_DestroyCANController = _oci.OCI_DestroyCANController
OCI_DestroyCANController.argtypes = [OCI_ControllerHandle]
OCI_DestroyCANController.restype = OCI_ErrorCode
OCI_DestroyCANController.errcheck = errcheck_oper

OCI_AdaptCANConfiguration = _oci.OCI_AdaptCANConfiguration
OCI_AdaptCANConfiguration.argtypes = [
    OCI_ControllerHandle,
    ctypes.POINTER(OCI_CANConfiguration),
]
OCI_AdaptCANConfiguration.restype = OCI_ErrorCode
OCI_AdaptCANConfiguration.errcheck = errcheck_oper

OCI_GetCANControllerCapabilities = _oci.OCI_GetCANControllerCapabilities
OCI_GetCANControllerCapabilities.argtypes = [
    OCI_ControllerHandle,
    ctypes.POINTER(OCI_CANControllerCapabilities),
]
OCI_GetCANControllerCapabilities.restype = OCI_ErrorCode
OCI_GetCANControllerCapabilities.errcheck = errcheck_init

OCI_GetCANControllerStatus = _oci.OCI_GetCANControllerStatus
OCI_GetCANControllerStatus.argtypes = [
    OCI_ControllerHandle,
    ctypes.POINTER(OCI_CANControllerStatus),
]
OCI_GetCANControllerStatus.restype = OCI_ErrorCode
OCI_GetCANControllerStatus.errcheck = errcheck_oper


# OCI CAN - Filter


class OCI_CANRxFilter(ctypes.Structure):
    _fields_ = [
        ("frameIDValue", ctypes.c_uint32),
        ("frameIDMask", ctypes.c_uint32),
        ("tag", ctypes.c_uint32),
    ]


class OCI_CANRxFilterEx(ctypes.Structure):
    _fields_ = [
        ("frameIDValue", ctypes.c_uint32),
        ("frameIDMask", ctypes.c_uint32),
        ("tag", ctypes.c_uint32),
        ("flagsValue", ctypes.c_uint16),
        ("flagsMask", ctypes.c_uint16),
    ]


OCI_AddCANFrameFilterEx = _oci.OCI_AddCANFrameFilterEx
OCI_AddCANFrameFilterEx.argtypes = [
    OCI_QueueHandle,
    ctypes.POINTER(ctypes.POINTER(OCI_CANRxFilterEx)),
    ctypes.c_uint32,
]
OCI_AddCANFrameFilterEx.restype = OCI_ErrorCode
OCI_AddCANFrameFilterEx.errcheck = errcheck_oper

OCI_RemoveCANFrameFilterEx = _oci.OCI_RemoveCANFrameFilterEx
OCI_RemoveCANFrameFilterEx.argtypes = [
    OCI_QueueHandle,
    ctypes.POINTER(ctypes.POINTER(OCI_CANRxFilterEx)),
    ctypes.c_uint32,
]
OCI_RemoveCANFrameFilterEx.restype = OCI_ErrorCode
OCI_RemoveCANFrameFilterEx.errcheck = errcheck_oper

# OCI CAN - Messages

OCI_CAN_MSG_FLAG_EXTENDED = 0x1
OCI_CAN_MSG_FLAG_REMOTE_FRAME = 0x2
OCI_CAN_MSG_FLAG_SELFRECEPTION = 0x4
OCI_CAN_MSG_FLAG_FD_DATA_BIT_RATE = 0x8
OCI_CAN_MSG_FLAG_FD_TRUNC_AND_PAD = 0x10
OCI_CAN_MSG_FLAG_FD_ERROR_PASSIVE = 0x20
OCI_CAN_MSG_FLAG_FD_DATA = 0x40

OCI_CANMessageDataType = ctypes.c_uint32
OCI_CAN_RX_MESSAGE = OCI_CANMessageDataType(1)
OCI_CAN_TX_MESSAGE = OCI_CANMessageDataType(2)
OCI_CAN_ERROR_FRAME = OCI_CANMessageDataType(3)
OCI_CAN_BUS_EVENT = OCI_CANMessageDataType(4)
OCI_CAN_INTERNAL_ERROR_EVENT = OCI_CANMessageDataType(5)
OCI_CAN_QUEUE_EVENT = OCI_CANMessageDataType(6)
OCI_CAN_TIMER_EVENT = OCI_CANMessageDataType(7)
OCI_CANFDRX_MESSAGE = OCI_CANMessageDataType(8)
OCI_CANFDTX_MESSAGE = OCI_CANMessageDataType(9)


class OCI_CANTxMessage(ctypes.Structure):
    _fields_ = [
        ("frameID", ctypes.c_uint32),
        ("flags", ctypes.c_uint16),
        ("res", ctypes.c_uint8),
        ("dlc", ctypes.c_uint8),
        ("data", ctypes.c_uint8 * 8),
    ]


class OCI_CANRxMessage(ctypes.Structure):
    _fields_ = [
        ("timeStamp", OCI_Time),
        ("tag", ctypes.c_uint32),
        ("frameID", ctypes.c_uint32),
        ("flags", ctypes.c_uint16),
        ("res", ctypes.c_uint8),
        ("dlc", ctypes.c_uint8),
        ("res1", ctypes.c_uint8 * 4),
        ("data", ctypes.c_uint8 * 8),
    ]


class OCI_CANErrorFrameMessage(ctypes.Structure):
    _fields_ = [
        ("timeStamp", OCI_Time),
        ("tag", ctypes.c_uint32),
        ("frameID", ctypes.c_uint32),
        ("flags", ctypes.c_uint16),
        ("res", ctypes.c_uint8),
        ("dlc", ctypes.c_uint8),
        ("type", ctypes.c_uint32),
        ("destination", ctypes.c_uint32),
    ]


class OCI_CANEventMessage(ctypes.Structure):
    _fields_ = [
        ("timeStamp", OCI_Time),
        ("tag", ctypes.c_uint32),
        ("eventCode", ctypes.c_uint32),
        ("destination", ctypes.c_uint32),
    ]


class OCI_CANMessageData(ctypes.Union):
    _fields_ = [
        ("rxMessage", OCI_CANRxMessage),
        ("txMessage", OCI_CANTxMessage),
        ("errorFrameMessage", OCI_CANErrorFrameMessage),
        ("canEventMessage", OCI_CANEventMessage),
        ("internalErrorEventMessage", OCI_InternalErrorEventMessage),
        ("timerEventMessage", OCI_TimerEventMessage),
        ("queueEventMessage", OCI_QueueEventMessage),
    ]


class OCI_CANMessageDataEx(ctypes.Union):
    _fields_ = [
        ("rxMessage", OCI_CANRxMessage),
        ("txMessage", OCI_CANTxMessage),
        ("errorFrameMessage", OCI_CANErrorFrameMessage),
        ("canEventMessage", OCI_CANEventMessage),
        ("internalErrorEventMessage", OCI_InternalErrorEventMessage),
        ("timerEventMessage", OCI_TimerEventMessage),
        ("queueEventMessage", OCI_QueueEventMessage),
        ("canFDRxMessage", OCI_CANFDRxMessage),
        ("canFDTxMessage", OCI_CANFDTxMessage),
    ]


class OCI_CANMessage(ctypes.Structure):
    _fields_ = [
        ("type", OCI_CANMessageDataType),
        ("reserved", ctypes.c_uint32),
        ("data", OCI_CANMessageData),
    ]


class OCI_CANMessageEx(ctypes.Structure):
    _fields_ = [
        ("type", OCI_CANMessageDataType),
        ("reserved", ctypes.c_uint32),
        ("data", OCI_CANMessageDataEx),
    ]


# OCI CAN - Queues

OCI_CANRxCallbackFunctionSingleMsg = ctypes.CFUNCTYPE(
    None, ctypes.c_void_p, ctypes.POINTER(OCI_CANMessage)
)

OCI_CANRxCallbackFunctionSingleMsgEx = ctypes.CFUNCTYPE(
    None, ctypes.c_void_p, ctypes.POINTER(OCI_CANMessageEx)
)


class OCI_CANRxCallbackSingleMsg(ctypes.Structure):
    class _U(ctypes.Union):
        _fields_ = [
            ("function", OCI_CANRxCallbackFunctionSingleMsg),
            ("functionEx", OCI_CANRxCallbackFunctionSingleMsgEx),
        ]

    _anonymous_ = ("u",)
    _fields_ = [
        ("u", _U),
        ("userData", ctypes.c_void_p),
    ]


class OCI_CANRxQueueConfiguration(ctypes.Structure):
    _fields_ = [
        ("onFrame", OCI_CANRxCallbackSingleMsg),
        ("onEvent", OCI_CANRxCallbackSingleMsg),
        ("selfReceptionMode", OCI_SelfReceptionMode),
    ]


class OCI_CANTxQueueConfiguration(ctypes.Structure):
    _fields_ = [
        ("reserved", ctypes.c_uint32),
    ]


OCI_CreateCANRxQueue = _oci.OCI_CreateCANRxQueue
OCI_CreateCANRxQueue.argtypes = [
    OCI_ControllerHandle,
    ctypes.POINTER(OCI_CANRxQueueConfiguration),
    ctypes.POINTER(OCI_QueueHandle),
]
OCI_CreateCANRxQueue.restype = OCI_ErrorCode
OCI_CreateCANRxQueue.errcheck = errcheck_init

OCI_DestroyCANRxQueue = _oci.OCI_DestroyCANRxQueue
OCI_DestroyCANRxQueue.argtypes = [OCI_QueueHandle]
OCI_DestroyCANRxQueue.restype = OCI_ErrorCode
OCI_DestroyCANRxQueue.errcheck = errcheck_oper

OCI_CreateCANTxQueue = _oci.OCI_CreateCANTxQueue
OCI_CreateCANTxQueue.argtypes = [
    OCI_ControllerHandle,
    ctypes.POINTER(OCI_CANTxQueueConfiguration),
    ctypes.POINTER(OCI_QueueHandle),
]
OCI_CreateCANTxQueue.restype = OCI_ErrorCode
OCI_CreateCANTxQueue.errcheck = errcheck_init

OCI_DestroyCANTxQueue = _oci.OCI_DestroyCANTxQueue
OCI_DestroyCANTxQueue.argtypes = [OCI_QueueHandle]
OCI_DestroyCANTxQueue.restype = OCI_ErrorCode
OCI_DestroyCANTxQueue.errcheck = errcheck_oper

OCI_WriteCANDataEx = _oci.OCI_WriteCANDataEx
OCI_WriteCANDataEx.argtypes = [
    OCI_QueueHandle,
    OCI_Time,
    ctypes.POINTER(ctypes.POINTER(OCI_CANMessageEx)),
    ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_uint32),
]
OCI_WriteCANDataEx.restype = OCI_ErrorCode
OCI_WriteCANDataEx.errcheck = errcheck_oper

OCI_ReadCANDataEx = _oci.OCI_ReadCANDataEx
OCI_ReadCANDataEx.argtypes = [
    OCI_QueueHandle,
    OCI_Time,
    ctypes.POINTER(ctypes.POINTER(OCI_CANMessageEx)),
    ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_uint32),
    ctypes.POINTER(ctypes.c_uint32),
]
OCI_ReadCANDataEx.restype = OCI_ErrorCode
OCI_ReadCANDataEx.errcheck = errcheck_oper
