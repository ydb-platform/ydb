__all__ = ['AddressingMode', 'TargetAddressType', 'Address']

from enum import Enum
from isotp import CanMessage
import abc

from typing import Optional, Any, List, Callable, Dict, Tuple, Union


class AddressingMode(Enum):
    Normal_11bits = 0
    Normal_29bits = 1
    NormalFixed_29bits = 2
    Extended_11bits = 3
    Extended_29bits = 4
    Mixed_11bits = 5
    Mixed_29bits = 6

    @classmethod
    def get_name(cls, num: Union[int, "AddressingMode"]) -> str:
        return cls(num).name


class TargetAddressType(Enum):
    Physical = 0        # 1 to 1 communication
    Functional = 1      # 1 to n communication


class AbstractAddress(abc.ABC):

    @abc.abstractmethod
    def get_tx_arbitration_id(self, address_type: TargetAddressType = TargetAddressType.Physical) -> int:
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def get_rx_arbitration_id(self, address_type: TargetAddressType = TargetAddressType.Physical) -> int:
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def requires_tx_extension_byte(self) -> bool:
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def requires_rx_extension_byte(self) -> bool:
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def get_tx_extension_byte(self) -> Optional[int]:
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def get_rx_extension_byte(self) -> Optional[int]:
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def is_tx_29bits(self) -> bool:
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def is_rx_29bits(self) -> bool:
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def is_for_me(self, msg: CanMessage) -> bool:
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def get_rx_prefix_size(self) -> int:
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def get_tx_payload_prefix(self) -> bytes:
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def is_partial_address(self) -> bool:
        raise NotImplementedError("Abstract method")

    @abc.abstractmethod
    def get_content_str(self) -> str:
        raise NotImplementedError("Abstract method")


class Address(AbstractAddress):
    """
    Represents the addressing information (N_AI) of the IsoTP layer. Will define what messages will be received and how to craft transmitted message to reach a specific party.

    Parameters must be given according to the addressing mode. When not needed, a parameter may be left unset or set to ``None``.

    Both the :class:`TransportLayer<isotp.TransportLayer>` and the :class:`isotp.socket<isotp.socket>` expects this address object

    :param addressing_mode: The addressing mode. Valid values are defined by the :class:`AddressingMode<isotp.AddressingMode>` class
    :type addressing_mode: int

    :param txid: The CAN ID for transmission. Used for these addressing mode: ``Normal_11bits``, ``Normal_29bits``, ``Extended_11bits``, ``Extended_29bits``, ``Mixed_11bits``
    :type txid: int | None

    :param rxid: The CAN ID for reception. Used for these addressing mode: ``Normal_11bits``, ``Normal_29bits``, ``Extended_11bits``, ``Extended_29bits``, ``Mixed_11bits``
    :type rxid: int | None

    :param target_address: Target address (N_TA) used in ``NormalFixed_29bits`` and ``Mixed_29bits`` addressing mode.
    :type target_address: int | None

    :param source_address: Source address (N_SA) used in ``NormalFixed_29bits`` and ``Mixed_29bits`` addressing mode.
    :type source_address: int | None

    :param physical_id: The CAN ID for physical (unicast) messages. Only bits 28-16 are used. Used for these addressing modes: ``NormalFixed_29bits``, ``Mixed_29bits``. Set to standard mandated value if None.
    :type physical_id: int | None

    :param functional_id: The CAN ID for functional (multicast) messages. Only bits 28-16 are used. Used for these addressing modes: ``NormalFixed_29bits``, ``Mixed_29bits``. Set to standard mandated value if None.
    :type functional_id: int | None

    :param address_extension: Address extension (N_AE) used in ``Mixed_11bits``, ``Mixed_29bits`` addressing mode
    :type address_extension: int | None

    :param rx_only: When using :class:`AsymmetricAddress<isotp.address.AsymmetricAddress>`, indicates that this address is the RX part, disabling validation of TX part 
    :type rx_only: bool

    :param tx_only: When using :class:`AsymmetricAddress<isotp.address.AsymmetricAddress>`, indicates that this address is the TX part, disabling validation of RX part
    :type tx_only: bool

    """

    _addressing_mode: AddressingMode
    _target_address: Optional[int]
    _source_address: Optional[int]
    _address_extension: Optional[int]
    _txid: Optional[int]
    _rxid: Optional[int]
    _is_29bits: bool
    _tx_arbitration_id_physical: int
    _tx_arbitration_id_functional: int
    _rx_arbitration_id_physical: int
    _rx_arbitration_id_functional: int
    _tx_payload_prefix: bytes
    _rx_prefix_size: int
    _rx_only: bool
    _tx_only: bool

    def __init__(self,
                 addressing_mode: AddressingMode = AddressingMode.Normal_11bits,
                 txid: Optional[int] = None,
                 rxid: Optional[int] = None,
                 target_address: Optional[int] = None,
                 source_address: Optional[int] = None,
                 physical_id: Optional[int] = None,
                 functional_id: Optional[int] = None,
                 address_extension: Optional[int] = None,
                 rx_only: bool = False,
                 tx_only: bool = False
                 ):

        self._rx_only = rx_only
        self._tx_only = tx_only
        self._addressing_mode = addressing_mode
        self._target_address = target_address
        self._source_address = source_address
        self._address_extension = address_extension
        self._txid = txid
        self._rxid = rxid
        self._is_29bits = True if self._addressing_mode in [
            AddressingMode.Normal_29bits, AddressingMode.NormalFixed_29bits, AddressingMode.Extended_29bits, AddressingMode.Mixed_29bits] else False

        if self._addressing_mode == AddressingMode.NormalFixed_29bits:
            self.physical_id = 0x18DA0000 if physical_id is None else physical_id & 0x1FFF0000
            self.functional_id = 0x18DB0000 if functional_id is None else functional_id & 0x1FFF0000

        if self._addressing_mode == AddressingMode.Mixed_29bits:
            self.physical_id = 0x18CE0000 if physical_id is None else physical_id & 0x1FFF0000
            self.functional_id = 0x18CD0000 if functional_id is None else functional_id & 0x1FFF0000

        self.validate()

        # From here, input is good. Do some precomputing for speed optimization without bothering about types or values
        self._tx_payload_prefix = bytes()
        self._rx_prefix_size = 0

        if not self._tx_only:   # Rx supported
            self._rx_arbitration_id_physical = self._get_rx_arbitration_id(TargetAddressType.Physical)
            self._rx_arbitration_id_functional = self._get_rx_arbitration_id(TargetAddressType.Functional)

            if self._addressing_mode in [AddressingMode.Extended_11bits, AddressingMode.Extended_29bits, AddressingMode.Mixed_11bits, AddressingMode.Mixed_29bits]:
                self._rx_prefix_size = 1

        if not self._rx_only:   # Tx supported
            self._tx_arbitration_id_physical = self._get_tx_arbitration_id(TargetAddressType.Physical)
            self._tx_arbitration_id_functional = self._get_tx_arbitration_id(TargetAddressType.Functional)

            if self._addressing_mode in [AddressingMode.Extended_11bits, AddressingMode.Extended_29bits]:
                assert self._target_address is not None
                self._tx_payload_prefix = bytes([self._target_address])
            elif self._addressing_mode in [AddressingMode.Mixed_11bits, AddressingMode.Mixed_29bits]:
                assert self._address_extension is not None
                self._tx_payload_prefix = bytes([self._address_extension])

        if not self._tx_only:
            if self._addressing_mode in [AddressingMode.Normal_11bits, AddressingMode.Normal_29bits]:
                setattr(self, 'is_for_me', self._is_for_me_normal)
            elif self._addressing_mode in [AddressingMode.Extended_11bits, AddressingMode.Extended_29bits]:
                setattr(self, 'is_for_me', self._is_for_me_extended)
            elif self._addressing_mode == AddressingMode.NormalFixed_29bits:
                setattr(self, 'is_for_me', self._is_for_me_normal_fixed)
            elif self._addressing_mode == AddressingMode.Mixed_11bits:
                setattr(self, 'is_for_me', self._is_for_me_mixed_11bits)
            elif self._addressing_mode == AddressingMode.Mixed_29bits:
                setattr(self, 'is_for_me', self._is_for_me_mixed_29bits)
            else:
                raise RuntimeError('This exception should never be raised.')

        def not_implemented_func_with_partial(*args: Any, **kwargs: Any) -> None:
            raise NotImplementedError("Not possible with partial address")

        # Remove unavailable functions to be strict
        if self._tx_only:
            setattr(self, 'get_rx_arbitration_id', not_implemented_func_with_partial)
            setattr(self, 'requires_rx_extension_byte', not_implemented_func_with_partial)
            setattr(self, 'get_rx_extension_byte', not_implemented_func_with_partial)
            setattr(self, 'is_rx_29bits', not_implemented_func_with_partial)
            setattr(self, 'is_for_me', not_implemented_func_with_partial)
            setattr(self, 'get_rx_prefix_size', not_implemented_func_with_partial)

        if self._rx_only:
            setattr(self, 'get_tx_arbitration_id', not_implemented_func_with_partial)
            setattr(self, 'requires_tx_extension_byte', not_implemented_func_with_partial)
            setattr(self, 'get_tx_extension_byte', not_implemented_func_with_partial)
            setattr(self, 'is_tx_29bits', not_implemented_func_with_partial)
            setattr(self, 'get_tx_payload_prefix', not_implemented_func_with_partial)

    def validate(self) -> None:
        if self._rx_only and self._tx_only:
            raise ValueError("Address cannot be tx only and rx only")

        if self._addressing_mode not in [AddressingMode.Normal_11bits, AddressingMode.Normal_29bits, AddressingMode.NormalFixed_29bits, AddressingMode.Extended_11bits, AddressingMode.Extended_29bits, AddressingMode.Mixed_11bits, AddressingMode.Mixed_29bits]:
            raise ValueError('Addressing mode is not valid')

        if self._addressing_mode in [AddressingMode.Normal_11bits, AddressingMode.Normal_29bits]:
            if self._rxid is None and not self._tx_only:
                raise ValueError('rxid must be specified for Normal addressing mode (11 or 29 bits ID)')
            if self._txid is None and not self._rx_only:
                raise ValueError('txid must be specified for Normal addressing mode (11 or 29 bits ID)')
            if self._rxid == self._txid:
                raise ValueError('txid and rxid must be different for Normal addressing mode')

        elif self._addressing_mode == AddressingMode.NormalFixed_29bits:
            if self._target_address is None or self._source_address is None:
                raise ValueError('target_address and source_address must be specified for Normal Fixed addressing (29 bits ID)')

        elif self._addressing_mode in [AddressingMode.Extended_11bits, AddressingMode.Extended_29bits]:
            if not self._rx_only:
                if self._target_address is None or self._txid is None:
                    raise ValueError('target_address and txid must be specified for Extended addressing mode (11 or 29 bits ID)')

            if not self._tx_only:
                if self._source_address is None or self._rxid is None:
                    raise ValueError('source_address and rxid must be specified for Extended addressing mode (11 or 29 bits ID)')

            if self._rxid == self._txid:
                raise ValueError('txid and rxid must be different')

        elif self._addressing_mode == AddressingMode.Mixed_11bits:
            if self._address_extension is None:
                raise ValueError('address_extension must be specified for Mixed addressing mode (11 bits ID)')

            if self._rxid is None and not self._tx_only:
                raise ValueError('rxid must be specified for Mixed addressing mode (11 bits ID)')
            if self._txid is None and not self._rx_only:
                raise ValueError('txid must be specified for Mixed addressing mode (11 bits ID)')
            if self._rxid == self._txid:
                raise ValueError('txid and rxid must be different for Mixed addressing mode (11 bits ID)')

        elif self._addressing_mode == AddressingMode.Mixed_29bits:
            # partial or full address requires all 3 params.
            if self._target_address is None or self._source_address is None or self._address_extension is None:
                raise ValueError('target_address, source_address and address_extension must be specified for Mixed addressing mode (29 bits ID)')

        if self._target_address is not None:
            if not isinstance(self._target_address, int):
                raise ValueError('target_address must be an integer')
            if self._target_address < 0 or self._target_address > 0xFF:
                raise ValueError('target_address must be an integer between 0x00 and 0xFF')

        if self._source_address is not None:
            if not isinstance(self._source_address, int):
                raise ValueError('source_address must be an integer')
            if self._source_address < 0 or self._source_address > 0xFF:
                raise ValueError('source_address must be an integer between 0x00 and 0xFF')

        if self._address_extension is not None:
            if not isinstance(self._address_extension, int):
                raise ValueError('source_address must be an integer')
            if self._address_extension < 0 or self._address_extension > 0xFF:
                raise ValueError('address_extension must be an integer between 0x00 and 0xFF')

        if self._txid is not None:
            if not isinstance(self._txid, int):
                raise ValueError('txid must be an integer')
            if self._txid < 0:
                raise ValueError('txid must be greater than 0')
            if not self._is_29bits:
                if self._txid > 0x7FF:
                    raise ValueError('txid must be smaller than 0x7FF for 11 bits identifier')

        if self._rxid is not None:
            if not isinstance(self._rxid, int):
                raise ValueError('rxid must be an integer')
            if self._rxid < 0:
                raise ValueError('rxid must be greater than 0')
            if not self._is_29bits:
                if self._rxid > 0x7FF:
                    raise ValueError('rxid must be smaller than 0x7FF for 11 bits identifier')

    def is_partial_address(self) -> bool:
        return self._tx_only or self._rx_only

    def is_tx_only(self) -> bool:
        return self._tx_only

    def is_rx_only(self) -> bool:
        return self._rx_only

    def get_rx_prefix_size(self) -> int:
        return self._rx_prefix_size

    def get_tx_payload_prefix(self) -> bytes:
        return self._tx_payload_prefix

    def is_for_me(self, msg: CanMessage) -> bool:
        raise NotImplementedError("is_for_me should be overriden in constructor")

    def get_tx_arbitration_id(self, address_type: TargetAddressType = TargetAddressType.Physical) -> int:
        if address_type == TargetAddressType.Physical:
            return self._tx_arbitration_id_physical
        else:
            return self._tx_arbitration_id_functional

    def get_rx_arbitration_id(self, address_type: TargetAddressType = TargetAddressType.Physical) -> int:
        if address_type == TargetAddressType.Physical:
            return self._rx_arbitration_id_physical
        else:
            return self._rx_arbitration_id_functional

    def _get_tx_arbitration_id(self, address_type: TargetAddressType) -> int:
        if self._addressing_mode in (AddressingMode.Normal_11bits,
                                     AddressingMode.Normal_29bits,
                                     AddressingMode.Extended_11bits,
                                     AddressingMode.Extended_29bits,
                                     AddressingMode.Mixed_11bits):
            assert self._txid is not None
            return self._txid
        elif self._addressing_mode in [AddressingMode.Mixed_29bits, AddressingMode.NormalFixed_29bits]:
            assert self._target_address is not None
            assert self._source_address is not None
            bits28_16 = self.physical_id if address_type == TargetAddressType.Physical else self.functional_id
            return bits28_16 | (self._target_address << 8) | self._source_address
        raise ValueError("Unsupported addressing mode")

    def _get_rx_arbitration_id(self, address_type: TargetAddressType = TargetAddressType.Physical) -> int:
        if self._addressing_mode in (AddressingMode.Normal_11bits,
                                     AddressingMode.Normal_29bits,
                                     AddressingMode.Extended_11bits,
                                     AddressingMode.Extended_29bits,
                                     AddressingMode.Mixed_11bits):
            assert self._rxid is not None
            return self._rxid
        elif self._addressing_mode in [AddressingMode.Mixed_29bits, AddressingMode.NormalFixed_29bits]:
            assert self._target_address is not None
            assert self._source_address is not None
            bits28_16 = self.physical_id if address_type == TargetAddressType.Physical else self.functional_id
            return bits28_16 | (self._source_address << 8) | self._target_address
        raise ValueError("Unsupported addressing mode")

    def _is_for_me_normal(self, msg: CanMessage) -> bool:
        if self._is_29bits == msg.is_extended_id:
            return msg.arbitration_id == self._rxid
        return False

    def _is_for_me_extended(self, msg: CanMessage) -> bool:
        if self._is_29bits == msg.is_extended_id:
            if msg.data is not None and len(msg.data) > 0:
                return msg.arbitration_id == self._rxid and int(msg.data[0]) == self._source_address
        return False

    def _is_for_me_normal_fixed(self, msg: CanMessage) -> bool:
        if self._is_29bits == msg.is_extended_id:
            return (msg.arbitration_id & 0x1FFF0000 in [self.physical_id, self.functional_id]) and (msg.arbitration_id & 0xFF00) >> 8 == self._source_address and msg.arbitration_id & 0xFF == self._target_address
        return False

    def _is_for_me_mixed_11bits(self, msg: CanMessage) -> bool:
        if self._is_29bits == msg.is_extended_id:
            if msg.data is not None and len(msg.data) > 0:
                return msg.arbitration_id == self._rxid and int(msg.data[0]) == self._address_extension
        return False

    def _is_for_me_mixed_29bits(self, msg: CanMessage) -> bool:
        if self._is_29bits == msg.is_extended_id:
            if msg.data is not None and len(msg.data) > 0:
                return (msg.arbitration_id & 0x1FFF0000) in [self.physical_id, self.functional_id] and (msg.arbitration_id & 0xFF00) >> 8 == self._source_address and msg.arbitration_id & 0xFF == self._target_address and int(msg.data[0]) == self._address_extension
        return False

    def _requires_extension_byte(self) -> bool:
        return True if self._addressing_mode in [AddressingMode.Extended_11bits, AddressingMode.Extended_29bits, AddressingMode.Mixed_11bits, AddressingMode.Mixed_29bits] else False

    def requires_rx_extension_byte(self) -> bool:
        return self._requires_extension_byte()

    def requires_tx_extension_byte(self) -> bool:
        return self._requires_extension_byte()

    def get_tx_extension_byte(self) -> Optional[int]:
        if self._addressing_mode in [AddressingMode.Extended_11bits, AddressingMode.Extended_29bits]:
            return self._target_address
        if self._addressing_mode in [AddressingMode.Mixed_11bits, AddressingMode.Mixed_29bits]:
            return self._address_extension
        return None

    def get_rx_extension_byte(self) -> Optional[int]:
        if self._addressing_mode in [AddressingMode.Extended_11bits, AddressingMode.Extended_29bits]:
            return self._source_address
        if self._addressing_mode in [AddressingMode.Mixed_11bits, AddressingMode.Mixed_29bits]:
            return self._address_extension
        return None

    def is_tx_29bits(self) -> bool:
        return self._is_29bits

    def is_rx_29bits(self) -> bool:
        return self._is_29bits

    def get_content_str(self) -> str:
        val_dict = {}
        keys = ['_target_address', '_source_address', '_address_extension', '_txid', '_rxid']
        for key in keys:
            val = getattr(self, key)
            if key.startswith('_'):
                key = key[1:]
            if val is not None:
                val_dict[key] = val
        vals_str = ', '.join(['%s:0x%02x' % (k, val_dict[k]) for k in val_dict])
        return '[%s - %s]' % (AddressingMode.get_name(self._addressing_mode), vals_str)

    def __repr__(self) -> str:
        return '<IsoTP Address %s at 0x%08x>' % (self.get_content_str(), id(self))


class AsymmetricAddress(AbstractAddress):
    """ 
    Address that uses independent addressing modes for transmission and reception.

    :param tx_addr: The Address object used for transmission
    :type tx_addr: :class:`Address<isotp.Address>` with ``tx_only=True``

    :param rx_addr: The Address object used for reception
    :type rx_addr: :class:`Address<isotp.Address>` with ``rx_only=True``

    """
    tx_addr: Address
    rx_addr: Address

    def __init__(self, tx_addr: Address, rx_addr: Address):
        if not isinstance(rx_addr, Address):
            raise ValueError("rx_addr must be an isotp.Address instance")

        if not isinstance(tx_addr, Address):
            raise ValueError("tx_addr must be an isotp.Address instance")

        if not tx_addr.is_tx_only():
            raise ValueError("tx_addr must be configured with tx_only=True")

        if not rx_addr.is_rx_only():
            raise ValueError("rx_addr must be configured with rx_only=True")

        self.tx_addr = tx_addr
        self.rx_addr = rx_addr

    def get_tx_extension_byte(self) -> Optional[int]:
        return self.tx_addr.get_tx_extension_byte()

    def get_rx_extension_byte(self) -> Optional[int]:
        return self.rx_addr.get_rx_extension_byte()

    def is_for_me(self, msg: CanMessage) -> bool:
        return self.rx_addr.is_for_me(msg)

    def get_tx_arbitration_id(self, address_type: TargetAddressType = TargetAddressType.Physical) -> int:
        return self.tx_addr.get_tx_arbitration_id(address_type)

    def get_rx_arbitration_id(self, address_type: TargetAddressType = TargetAddressType.Physical) -> int:
        return self.rx_addr.get_rx_arbitration_id(address_type)

    def is_tx_29bits(self) -> bool:
        return self.tx_addr.is_tx_29bits()

    def is_rx_29bits(self) -> bool:
        return self.rx_addr.is_rx_29bits()

    def requires_tx_extension_byte(self) -> bool:
        return self.tx_addr.requires_tx_extension_byte()

    def requires_rx_extension_byte(self) -> bool:
        return self.rx_addr.requires_rx_extension_byte()

    def get_rx_prefix_size(self) -> int:
        return self.rx_addr.get_rx_prefix_size()

    def get_tx_payload_prefix(self) -> bytes:
        return self.tx_addr.get_tx_payload_prefix()

    def is_partial_address(self) -> bool:
        return False

    def get_content_str(self) -> str:
        return f"RxAddr: {self.rx_addr.__repr__()} - TxAddr: {self.tx_addr.__repr__()}"

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} - {self.get_content_str()}>'
