from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .end_to_end_properties import AutosarEnd2EndProperties
    from .secoc_properties import AutosarSecOCProperties


class AutosarMessageSpecifics:
    """This class collects all AUTOSAR specific information of a CAN message

    This means useful information about CAN messages which is provided
    by ARXML files, but is specific to AUTOSAR.
    """

    def __init__(self) -> None:
        self._pdu_paths: list[str] = []
        self._is_nm = False
        self._is_general_purpose = False
        self._secoc: AutosarSecOCProperties | None = None
        self._e2e: AutosarEnd2EndProperties | None = None
        self._signal_group = None

    @property
    def pdu_paths(self):
        """The ARXML paths of all PDUs featured by this message.

        For the vast majority of messages, this list only has a single
        entry. Messages with multiplexers and container frames are
        different, though.
        """
        return self._pdu_paths

    @property
    def is_nm(self):
        """True iff the message is used for network management
        """
        return self._is_nm

    @property
    def is_general_purpose(self):
        """True iff the message is not used for signal-based communication

        This comprises messages used for diagnostic and calibration
        purpuses, e.g. messages used for the ISO-TP or XCP protocols.

        """
        return self._is_general_purpose

    @property
    def is_secured(self):
        """True iff the message integrity is secured using SecOC
        """
        return self._secoc is not None

    @property
    def secoc(self):
        """The properties required to implement secured on-board communication
        """
        return self._secoc

    @property
    def e2e(self) -> Optional['AutosarEnd2EndProperties']:
        """Returns the end-to-end protection properties for the message"""
        return self._e2e

    @e2e.setter
    def e2e(self, value: Optional['AutosarEnd2EndProperties']) -> None:
        self._e2e = value
