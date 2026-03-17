# DID data.

from ...typechecking import ByteOrder, Choices, SignalValueType
from ..can.signal import NamedSignalValue
from ..conversion import BaseConversion, IdentityConversion


class Data:
    """A data data with position, size, unit and other information. A data
    is part of a DID.

    """

    def __init__(self,
                 name: str,
                 start: int,
                 length: int,
                 byte_order: ByteOrder = 'little_endian',
                 conversion: BaseConversion | None = None,
                 minimum: float | None = None,
                 maximum: float | None = None,
                 unit: str | None = None,
                 ) -> None:
        #: The data name as a string.
        self.name: str = name

        #: The conversion instance, which is used to convert
        #: between raw and scaled/physical values.
        self.conversion = conversion or IdentityConversion(is_float=False)

        #: The start bit position of the data within its DID.
        self.start: int = start

        #: The length of the data in bits.
        self.length = length

        #: Data byte order as ``'little_endian'`` or ``'big_endian'``.
        self.byte_order: ByteOrder = byte_order

        #: The minimum value of the data, or ``None`` if unavailable.
        self.minimum: float | None = minimum

        #: The maximum value of the data, or ``None`` if unavailable.
        self.maximum: float | None = maximum

        #: The unit of the data as a string, or ``None`` if unavailable.
        self.unit = unit

        # ToDo: Remove once types are handled properly.
        self.is_signed: bool = False

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

    def choice_to_number(self, string: str | NamedSignalValue) -> int:
        try:
            return self.conversion.choice_to_number(string)
        except KeyError as exc:
            err_msg = f"Choice {string} not found in Data {self.name}."
            raise KeyError(err_msg) from exc

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

    def __repr__(self) -> str:
        if self.choices is None:
            choices = None
        else:
            choices = '{{{}}}'.format(', '.join(
                [f"{value}: '{text}'"
                 for value, text in self.choices.items()]))

        return f"data('{self.name}', {self.start}, {self.length}, '{self.byte_order}', {self.conversion.scale}, {self.conversion.offset}, {self.minimum}, {self.maximum}, '{self.unit}', {choices})"
