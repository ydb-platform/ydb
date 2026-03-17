# A CAN signal.
from typing import TYPE_CHECKING, Optional

from ...typechecking import ByteOrder, Choices, Comments, SignalValueType
from ..conversion import BaseConversion, IdentityConversion
from ..namedsignalvalue import NamedSignalValue

if TYPE_CHECKING:
    from ...database.can.formats.dbc import DbcSpecifics

class Signal:
    """A CAN signal with position, size, unit and other information. A
    signal is part of a message.

    Signal bit numbering in a message:

    .. code:: text

       Byte:       0        1        2        3        4        5        6        7
              +--------+--------+--------+--------+--------+--------+--------+--------+--- - -
              |        |        |        |        |        |        |        |        |
              +--------+--------+--------+--------+--------+--------+--------+--------+--- - -
       Bit:    7      0 15     8 23    16 31    24 39    32 47    40 55    48 63    56

    Big endian signal with start bit 2 and length 5 (0=LSB, 4=MSB):

    .. code:: text

       Byte:       0        1        2        3
              +--------+--------+--------+--- - -
              |    |432|10|     |        |
              +--------+--------+--------+--- - -
       Bit:    7      0 15     8 23    16 31

    Little endian signal with start bit 2 and length 9 (0=LSB, 8=MSB):

    .. code:: text

       Byte:       0        1        2        3
              +--------+--------+--------+--- - -
              |543210| |    |876|        |
              +--------+--------+--------+--- - -
       Bit:    7      0 15     8 23    16 31

    """

    def __init__(
        self,
        name: str,
        start: int,
        length: int,
        byte_order: ByteOrder = "little_endian",
        is_signed: bool = False,
        raw_initial: int | float | None = None,
        raw_invalid: int | float | None = None,
        conversion: BaseConversion | None = None,
        minimum: float | None = None,
        maximum: float | None = None,
        unit: str | None = None,
        dbc_specifics: Optional["DbcSpecifics"] = None,
        comment: str | Comments | None = None,
        receivers: list[str] | None = None,
        is_multiplexer: bool = False,
        multiplexer_ids: list[int] | None = None,
        multiplexer_signal: str | None = None,
        spn: int | None = None,
    ) -> None:
        # avoid using properties to improve encoding/decoding performance

        #: The signal name as a string.
        self.name: str = name

        #: The conversion instance, which is used to convert
        #: between raw and scaled/physical values.
        self.conversion: BaseConversion = conversion or IdentityConversion(is_float=False)

        #: The scaled minimum value of the signal, or ``None`` if unavailable.
        self.minimum: float | None = minimum

        #: The scaled maximum value of the signal, or ``None`` if unavailable.
        self.maximum: float | None = maximum

        #: The start bit position of the signal within its message.
        self.start: int = start

        #: The length of the signal in bits.
        self.length: int = length

        #: Signal byte order as ``'little_endian'`` or ``'big_endian'``.
        self.byte_order: ByteOrder = byte_order

        #: ``True`` if the signal is signed, ``False`` otherwise. Ignore this
        #: attribute if :attr:`is_float` is ``True``.
        self.is_signed: bool = is_signed

        #: The internal representation of the initial value of the signal,
        #: or ``None`` if unavailable.
        self.raw_initial: int | float | None = raw_initial

        #: The initial value of the signal in units of the physical world,
        #: or ``None`` if unavailable.
        self.initial: SignalValueType | None = (
            self.conversion.raw_to_scaled(raw_initial) if raw_initial is not None else None
        )

        #: The raw value representing that the signal is invalid,
        #: or ``None`` if unavailable.
        self.raw_invalid: int | float | None = raw_invalid

        #: The scaled value representing that the signal is invalid,
        #: or ``None`` if unavailable.
        self.invalid: SignalValueType | None = (
            self.conversion.raw_to_scaled(raw_invalid) if raw_invalid is not None else None
        )

        #: The unit of the signal as a string, or ``None`` if unavailable.
        self.unit: str | None = unit

        #: An object containing dbc specific properties like e.g. attributes.
        self.dbc: DbcSpecifics | None = dbc_specifics

        #: A list of all receiver nodes of this signal.
        self.receivers: list[str] = receivers or []

        #: ``True`` if this is the multiplexer signal in a message, ``False``
        #: otherwise.
        self.is_multiplexer: bool = is_multiplexer

        #: The multiplexer ids list if the signal is part of a multiplexed
        #: message, ``None`` otherwise.
        self.multiplexer_ids: list[int] | None = multiplexer_ids

        #: The multiplexer signal if the signal is part of a multiplexed
        #: message, ``None`` otherwise.
        self.multiplexer_signal: str | None = multiplexer_signal

        #: The J1939 Suspect Parameter Number (SPN) value if the signal
        #: has this attribute, ``None`` otherwise.
        self.spn: int | None = spn

        #: The dictionary with the descriptions of the signal in multiple
        #: languages. ``None`` if unavailable.
        self.comments: Comments | None

        # if the 'comment' argument is a string, we assume that is an
        # english comment. this is slightly hacky because the
        # function's behavior depends on the type of the passed
        # argument, but it is quite convenient...
        if isinstance(comment, str):
            # use the first comment in the dictionary as "The" comment
            self.comments = {None: comment}
        else:
            # assume that we have either no comment at all or a
            # multilingual dictionary
            self.comments = comment

    def raw_to_scaled(
        self, raw_value: int | float, decode_choices: bool = True
    ) -> SignalValueType:
        """Convert an internal raw value according to the defined scaling or value table.

        :param raw_value:
            The raw value
        :param decode_choices:
            If `decode_choices` is ``False`` scaled values are not
            converted to choice strings (if available).
        :return:
            The calculated scaled value
        """
        return self.conversion.raw_to_scaled(raw_value, decode_choices)

    def scaled_to_raw(self, scaled_value: SignalValueType) -> int | float:
        """Convert a scaled value to the internal raw value.

        :param scaled_value:
            The scaled value.
        :return:
            The internal raw value.
        """
        return self.conversion.scaled_to_raw(scaled_value)

    @property
    def scale(self) -> int | float:
        """The scale factor of the signal value."""
        return self.conversion.scale

    @scale.setter
    def scale(self, value: int | float) -> None:
        self.conversion = self.conversion.factory(
            scale=value,
            offset=self.conversion.offset,
            choices=self.conversion.choices,
            is_float=self.conversion.is_float,
        )

    @property
    def offset(self) -> int | float:
        """The offset of the signal value."""
        return self.conversion.offset

    @offset.setter
    def offset(self, value: int | float) -> None:
        self.conversion = self.conversion.factory(
            scale=self.conversion.scale,
            offset=value,
            choices=self.conversion.choices,
            is_float=self.conversion.is_float,
        )

    @property
    def choices(self) -> Choices | None:
        """A dictionary mapping signal values to enumerated choices, or
        ``None`` if unavailable."""
        return self.conversion.choices

    @choices.setter
    def choices(self, choices: Choices | None) -> None:
        self.conversion = self.conversion.factory(
            scale=self.conversion.scale,
            offset=self.conversion.offset,
            choices=choices,
            is_float=self.conversion.is_float,
        )

    @property
    def is_float(self) -> bool:
        """``True`` if the raw signal value is a float, ``False`` otherwise."""
        return self.conversion.is_float

    @is_float.setter
    def is_float(self, is_float: bool) -> None:
        self.conversion = self.conversion.factory(
            scale=self.conversion.scale,
            offset=self.conversion.offset,
            choices=self.conversion.choices,
            is_float=is_float,
        )

    @property
    def comment(self) -> str | None:
        """The signal comment, or ``None`` if unavailable.

        Note that we implicitly try to return the English comment if
        multiple languages were specified.

        """
        if self.comments is None:
            return None
        elif self.comments.get(None) is not None:
            return self.comments.get(None)
        elif self.comments.get("FOR-ALL") is not None:
            return self.comments.get("FOR-ALL")

        return self.comments.get("EN")

    @comment.setter
    def comment(self, value: str | None) -> None:
        if value is None:
            self.comments = None
        else:
            self.comments = {None: value}

    def choice_to_number(self, choice: str | NamedSignalValue) -> int:
        try:
            return self.conversion.choice_to_number(choice)
        except KeyError as exc:
            err_msg = f"Choice {choice} not found in Signal {self.name}."
            raise KeyError(err_msg) from exc

    def __repr__(self) -> str:
        if self.choices is None:
            choices = None
        else:
            list_of_choices = ", ".join(
                [f"{value}: '{text}'" for value, text in self.choices.items()]
            )
            choices = f"{{{list_of_choices}}}"

        return (
            f"signal("
            f"'{self.name}', "
            f"{self.start}, "
            f"{self.length}, "
            f"'{self.byte_order}', "
            f"{self.is_signed}, "
            f"{self.raw_initial}, "
            f"{self.conversion.scale}, "
            f"{self.conversion.offset}, "
            f"{self.minimum}, "
            f"{self.maximum}, "
            f"'{self.unit}', "
            f"{self.is_multiplexer}, "
            f"{self.multiplexer_ids}, "
            f"{choices}, "
            f"{self.spn}, "
            f"{self.comments})"
        )
