import time
from typing import Optional

import can
from can.exceptions import CanInitializationError

from .boa import *


class EtasBus(can.BusABC):
    def __init__(
        self,
        channel: str,
        can_filters: Optional[can.typechecking.CanFilters] = None,
        receive_own_messages: bool = False,
        bitrate: int = 1000000,
        fd: bool = True,
        data_bitrate: int = 2000000,
        **kwargs: dict[str, any],
    ):
        self.receive_own_messages = receive_own_messages
        self._can_protocol = can.CanProtocol.CAN_FD if fd else can.CanProtocol.CAN_20

        nodeRange = CSI_NodeRange(CSI_NODE_MIN, CSI_NODE_MAX)
        self.tree = ctypes.POINTER(CSI_Tree)()
        CSI_CreateProtocolTree(ctypes.c_char_p(b""), nodeRange, ctypes.byref(self.tree))

        oci_can_v = BOA_Version(1, 4, 0, 0)

        self.ctrl = OCI_ControllerHandle()
        OCI_CreateCANControllerNoSearch(
            channel.encode(),
            ctypes.byref(oci_can_v),
            self.tree,
            ctypes.byref(self.ctrl),
        )

        ctrlConf = OCI_CANConfiguration()
        ctrlConf.baudrate = bitrate
        ctrlConf.samplePoint = 80
        ctrlConf.samplesPerBit = OCI_CAN_THREE_SAMPLES_PER_BIT
        ctrlConf.BTL_Cycles = 10
        ctrlConf.SJW = 1
        ctrlConf.syncEdge = OCI_CAN_SINGLE_SYNC_EDGE
        ctrlConf.physicalMedia = OCI_CAN_MEDIA_HIGH_SPEED
        if receive_own_messages:
            ctrlConf.selfReceptionMode = OCI_SELF_RECEPTION_ON
        else:
            ctrlConf.selfReceptionMode = OCI_SELF_RECEPTION_OFF
        ctrlConf.busParticipationMode = OCI_BUSMODE_ACTIVE

        if fd:
            ctrlConf.canFDEnabled = True
            ctrlConf.canFDConfig.dataBitRate = data_bitrate
            ctrlConf.canFDConfig.dataBTL_Cycles = 10
            ctrlConf.canFDConfig.dataSamplePoint = 80
            ctrlConf.canFDConfig.dataSJW = 1
            ctrlConf.canFDConfig.flags = 0
            ctrlConf.canFDConfig.canFdTxConfig = OCI_CANFDTX_USE_CAN_AND_CANFD_FRAMES
            ctrlConf.canFDConfig.canFdRxConfig.canRxMode = (
                OCI_CAN_RXMODE_CAN_FRAMES_USING_CAN_MESSAGE
            )
            ctrlConf.canFDConfig.canFdRxConfig.canFdRxMode = (
                OCI_CANFDRXMODE_CANFD_FRAMES_USING_CANFD_MESSAGE
            )

        ctrlProp = OCI_CANControllerProperties()
        ctrlProp.mode = OCI_CONTROLLER_MODE_RUNNING

        ec = OCI_OpenCANController(
            self.ctrl, ctypes.byref(ctrlConf), ctypes.byref(ctrlProp)
        )
        if ec != 0x0 and ec != 0x40004000:  # accept BOA_WARN_PARAM_ADAPTED
            raise CanInitializationError(
                f"OCI_OpenCANController failed with error 0x{ec:X}"
            )

        # RX

        rxQConf = OCI_CANRxQueueConfiguration()
        rxQConf.onFrame.function = ctypes.cast(None, OCI_CANRxCallbackFunctionSingleMsg)
        rxQConf.onFrame.userData = None
        rxQConf.onEvent.function = ctypes.cast(None, OCI_CANRxCallbackFunctionSingleMsg)
        rxQConf.onEvent.userData = None
        if receive_own_messages:
            rxQConf.selfReceptionMode = OCI_SELF_RECEPTION_ON
        else:
            rxQConf.selfReceptionMode = OCI_SELF_RECEPTION_OFF
        self.rxQueue = OCI_QueueHandle()
        OCI_CreateCANRxQueue(
            self.ctrl, ctypes.byref(rxQConf), ctypes.byref(self.rxQueue)
        )

        self._oci_filters = None
        self.filters = can_filters

        # TX

        txQConf = OCI_CANTxQueueConfiguration()
        txQConf.reserved = 0
        self.txQueue = OCI_QueueHandle()
        OCI_CreateCANTxQueue(
            self.ctrl, ctypes.byref(txQConf), ctypes.byref(self.txQueue)
        )

        # Common

        timerCapabilities = OCI_TimerCapabilities()
        OCI_GetTimerCapabilities(self.ctrl, ctypes.byref(timerCapabilities))
        self.tickFrequency = timerCapabilities.tickFrequency  # clock ticks per second

        # all timestamps are hardware timestamps relative to the CAN device powerup
        # calculate an offset to make them relative to epoch
        now = OCI_Time()
        OCI_GetTimerValue(self.ctrl, ctypes.byref(now))
        self.timeOffset = time.time() - (float(now.value) / self.tickFrequency)

        self.channel_info = channel

        # Super call must be after child init since super calls set_filters
        super().__init__(channel=channel, **kwargs)

    def _recv_internal(
        self, timeout: Optional[float]
    ) -> tuple[Optional[can.Message], bool]:
        ociMsgs = (ctypes.POINTER(OCI_CANMessageEx) * 1)()
        ociMsg = OCI_CANMessageEx()
        ociMsgs[0] = ctypes.pointer(ociMsg)

        count = ctypes.c_uint32()
        if timeout is not None:  # wait for specified time
            t = OCI_Time(round(timeout * self.tickFrequency))
        else:  # wait indefinitely
            t = OCI_NO_TIME
        OCI_ReadCANDataEx(
            self.rxQueue,
            t,
            ociMsgs,
            1,
            ctypes.byref(count),
            None,
        )

        msg = None

        if count.value != 0:
            if ociMsg.type == OCI_CANFDRX_MESSAGE.value:
                ociRxMsg = ociMsg.data.canFDRxMessage
                msg = can.Message(
                    timestamp=float(ociRxMsg.timeStamp) / self.tickFrequency
                    + self.timeOffset,
                    arbitration_id=ociRxMsg.frameID,
                    is_extended_id=bool(ociRxMsg.flags & OCI_CAN_MSG_FLAG_EXTENDED),
                    is_remote_frame=bool(
                        ociRxMsg.flags & OCI_CAN_MSG_FLAG_REMOTE_FRAME
                    ),
                    # is_error_frame=False,
                    # channel=None,
                    dlc=ociRxMsg.size,
                    data=ociRxMsg.data[0 : ociRxMsg.size],
                    is_fd=True,
                    is_rx=not bool(ociRxMsg.flags & OCI_CAN_MSG_FLAG_SELFRECEPTION),
                    bitrate_switch=bool(
                        ociRxMsg.flags & OCI_CAN_MSG_FLAG_FD_DATA_BIT_RATE
                    ),
                    # error_state_indicator=False,
                    # check=False,
                )
            elif ociMsg.type == OCI_CAN_RX_MESSAGE.value:
                ociRxMsg = ociMsg.data.rxMessage
                msg = can.Message(
                    timestamp=float(ociRxMsg.timeStamp) / self.tickFrequency
                    + self.timeOffset,
                    arbitration_id=ociRxMsg.frameID,
                    is_extended_id=bool(ociRxMsg.flags & OCI_CAN_MSG_FLAG_EXTENDED),
                    is_remote_frame=bool(
                        ociRxMsg.flags & OCI_CAN_MSG_FLAG_REMOTE_FRAME
                    ),
                    # is_error_frame=False,
                    # channel=None,
                    dlc=ociRxMsg.dlc,
                    data=ociRxMsg.data[0 : ociRxMsg.dlc],
                    # is_fd=False,
                    is_rx=not bool(ociRxMsg.flags & OCI_CAN_MSG_FLAG_SELFRECEPTION),
                    # bitrate_switch=False,
                    # error_state_indicator=False,
                    # check=False,
                )

        return (msg, True)

    def send(self, msg: can.Message, timeout: Optional[float] = None) -> None:
        ociMsgs = (ctypes.POINTER(OCI_CANMessageEx) * 1)()
        ociMsg = OCI_CANMessageEx()
        ociMsgs[0] = ctypes.pointer(ociMsg)

        if msg.is_fd:
            ociMsg.type = OCI_CANFDTX_MESSAGE
            ociTxMsg = ociMsg.data.canFDTxMessage
            ociTxMsg.size = msg.dlc
        else:
            ociMsg.type = OCI_CAN_TX_MESSAGE
            ociTxMsg = ociMsg.data.txMessage
            ociTxMsg.dlc = msg.dlc

        # set fields common to CAN / CAN-FD
        ociTxMsg.frameID = msg.arbitration_id
        ociTxMsg.flags = 0
        if msg.is_extended_id:
            ociTxMsg.flags |= OCI_CAN_MSG_FLAG_EXTENDED
        if msg.is_remote_frame:
            ociTxMsg.flags |= OCI_CAN_MSG_FLAG_REMOTE_FRAME
        ociTxMsg.data = tuple(msg.data)

        if msg.is_fd:
            ociTxMsg.flags |= OCI_CAN_MSG_FLAG_FD_DATA
            if msg.bitrate_switch:
                ociTxMsg.flags |= OCI_CAN_MSG_FLAG_FD_DATA_BIT_RATE

        OCI_WriteCANDataEx(self.txQueue, OCI_NO_TIME, ociMsgs, 1, None)

    def _apply_filters(self, filters: Optional[can.typechecking.CanFilters]) -> None:
        if self._oci_filters:
            OCI_RemoveCANFrameFilterEx(self.rxQueue, self._oci_filters, 1)

        # "accept all" filter
        if filters is None:
            filters = [{"can_id": 0x0, "can_mask": 0x0}]

        self._oci_filters = (ctypes.POINTER(OCI_CANRxFilterEx) * len(filters))()

        for i, filter_ in enumerate(filters):
            f = OCI_CANRxFilterEx()
            f.frameIDValue = filter_["can_id"]
            f.frameIDMask = filter_["can_mask"]
            f.tag = 0
            f.flagsValue = 0
            if self.receive_own_messages:
                # mask out the SR bit, i.e. ignore the bit -> receive all
                f.flagsMask = 0
            else:
                # enable the SR bit in the mask. since the bit is 0 in flagsValue -> do not self-receive
                f.flagsMask = OCI_CAN_MSG_FLAG_SELFRECEPTION
            if filter_.get("extended"):
                f.flagsValue |= OCI_CAN_MSG_FLAG_EXTENDED
                f.flagsMask |= OCI_CAN_MSG_FLAG_EXTENDED
            self._oci_filters[i].contents = f

        OCI_AddCANFrameFilterEx(self.rxQueue, self._oci_filters, len(self._oci_filters))

    def flush_tx_buffer(self) -> None:
        OCI_ResetQueue(self.txQueue)

    def shutdown(self) -> None:
        super().shutdown()
        # Cleanup TX
        if self.txQueue:
            OCI_DestroyCANTxQueue(self.txQueue)
            self.txQueue = None

        # Cleanup RX
        if self.rxQueue:
            OCI_DestroyCANRxQueue(self.rxQueue)
            self.rxQueue = None

        # Cleanup common
        if self.ctrl:
            OCI_CloseCANController(self.ctrl)
            OCI_DestroyCANController(self.ctrl)
            self.ctrl = None

        if self.tree:
            CSI_DestroyProtocolTree(self.tree)
            self.tree = None

    @property
    def state(self) -> can.BusState:
        status = OCI_CANControllerStatus()
        OCI_GetCANControllerStatus(self.ctrl, ctypes.byref(status))
        if status.stateCode & OCI_CAN_STATE_ACTIVE:
            return can.BusState.ACTIVE
        elif status.stateCode & OCI_CAN_STATE_PASSIVE:
            return can.BusState.PASSIVE

    @state.setter
    def state(self, new_state: can.BusState) -> None:
        # disabled, OCI_AdaptCANConfiguration does not allow changing the bus mode
        # if new_state == can.BusState.ACTIVE:
        #     self.ctrlConf.busParticipationMode = OCI_BUSMODE_ACTIVE
        # else:
        #     self.ctrlConf.busParticipationMode = OCI_BUSMODE_PASSIVE
        # ec = OCI_AdaptCANConfiguration(self.ctrl, ctypes.byref(self.ctrlConf))
        # if ec != 0x0:
        #     raise CanOperationError(f"OCI_AdaptCANConfiguration failed with error 0x{ec:X}")
        raise NotImplementedError("Setting state is not implemented.")

    @staticmethod
    def _detect_available_configs() -> list[can.typechecking.AutoDetectedConfig]:
        nodeRange = CSI_NodeRange(CSI_NODE_MIN, CSI_NODE_MAX)
        tree = ctypes.POINTER(CSI_Tree)()
        CSI_CreateProtocolTree(ctypes.c_char_p(b""), nodeRange, ctypes.byref(tree))

        nodes: list[dict[str, str]] = []

        def _findNodes(tree, prefix):
            uri = f"{prefix}/{tree.contents.item.uriName.decode()}"
            if "CAN:" in uri:
                nodes.append({"interface": "etas", "channel": uri})
            elif tree.contents.child:
                _findNodes(
                    tree.contents.child,
                    f"{prefix}/{tree.contents.item.uriName.decode()}",
                )

            if tree.contents.sibling:
                _findNodes(tree.contents.sibling, prefix)

        _findNodes(tree, "ETAS:/")

        CSI_DestroyProtocolTree(tree)

        return nodes
