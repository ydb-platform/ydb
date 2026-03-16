from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Union

from ..typechecking import Choices, SignalValueType

if TYPE_CHECKING:
    from .namedsignalvalue import NamedSignalValue


class BaseConversion(ABC):
    """The BaseConversion class defines the interface for all signal conversion classes."""

    #: the scaling factor of the conversion
    scale: float

    #: the offset value of the conversion
    offset: float

    #: ``True`` if the raw/internal value is a floating datatype
    #: ``False`` if it is an integer datatype
    is_float: bool

    #: an optional mapping of raw values to their corresponding text value
    choices: Choices | None

    @staticmethod
    def factory(
        scale: float = 1,
        offset: float = 0,
        choices: Choices | None = None,
        is_float: bool = False,
    ) -> "BaseConversion":
        """Factory method that returns an instance of a conversion subclass based on the given parameters.

        :param scale:
            The scale factor to use for the conversion.
        :param offset:
            The offset to use for the conversion.
        :param choices:
            A dictionary of named signal choices, mapping raw values to string labels.
        :param is_float:
            A boolean flag indicating whether the raw value is a float or an integer.

        :returns:
            An instance of a conversion subclass, either an `IdentityConversion`, a `LinearIntegerConversion`,
            a `LinearConversion`or a `NamedSignalConversion`.

        :raises TypeError: If the given parameters are of the wrong type.
        """
        if choices is None:
            if scale == 1 and offset == 0:
                return IdentityConversion(is_float=is_float)

            if _is_integer(scale) and _is_integer(offset) and not is_float:
                return LinearIntegerConversion(scale=int(scale), offset=int(offset))

            return LinearConversion(
                scale=scale,
                offset=offset,
                is_float=is_float,
            )

        return NamedSignalConversion(
            scale=scale, offset=offset, choices=choices, is_float=is_float
        )

    @abstractmethod
    def raw_to_scaled(
        self,
        raw_value: int | float,
        decode_choices: bool = True,
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
        raise NotImplementedError

    @abstractmethod
    def scaled_to_raw(self, scaled_value: SignalValueType) -> int | float:
        """Convert a scaled value to the internal raw value.

        :param scaled_value:
            The scaled value.
        :return:
            The internal raw value.
        """
        raise NotImplementedError

    @abstractmethod
    def numeric_scaled_to_raw(
        self, scaled_value: int | float
    ) -> int | float:
        """Convert a numeric scaled value to the internal raw value.

        :param scaled_value:
            The numeric scaled value.
        :return:
            The internal raw value.
        """
        raise NotImplementedError

    def choice_to_number(self, choice: Union[str, "NamedSignalValue"]) -> int:
        raise KeyError

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError


class IdentityConversion(BaseConversion):
    scale = 1
    offset = 0
    choices = None

    def __init__(self, is_float: bool) -> None:
        self.is_float = is_float

    def raw_to_scaled(
        self,
        raw_value: int | float,
        decode_choices: bool = True,
    ) -> int | float:
        return raw_value

    def scaled_to_raw(self, scaled_value: SignalValueType) -> int | float:
        if not isinstance(scaled_value, (int, float)):
            raise TypeError(
                f"'scaled_value' must have type 'int' or 'float' (is {type(scaled_value)})"
            )
        return self.numeric_scaled_to_raw(scaled_value)

    def numeric_scaled_to_raw(
        self, scaled_value: int | float
    ) -> int | float:
        return scaled_value if self.is_float else round(scaled_value)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(is_float={self.is_float})"


class LinearIntegerConversion(BaseConversion):
    is_float = False
    choices = None

    def __init__(self, scale: int, offset: int) -> None:
        self.scale: int = scale
        self.offset: int = offset

    def raw_to_scaled(
        self,
        raw_value: int | float,
        decode_choices: bool = True,
    ) -> SignalValueType:
        return raw_value * self.scale + self.offset

    def scaled_to_raw(self, scaled_value: SignalValueType) -> int | float:
        if not isinstance(scaled_value, (int, float)):
            raise TypeError(
                f"'scaled_value' must have type 'int' or 'float' (is {type(scaled_value)})"
            )
        return self.numeric_scaled_to_raw(scaled_value)

    def numeric_scaled_to_raw(
        self, scaled_value: int | float
    ) -> int | float:
        # try to avoid a loss of precision whenever possible
        _raw = scaled_value - self.offset
        quotient, remainder = divmod(_raw, self.scale)
        if remainder == 0:
            _raw = quotient
        else:
            _raw /= self.scale
        return round(_raw)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(scale={self.scale}, offset={self.offset})"


class LinearConversion(BaseConversion):
    choices = None

    def __init__(self, scale: float, offset: float, is_float: bool) -> None:
        self.scale = scale
        self.offset = offset
        self.is_float = is_float

    def raw_to_scaled(
        self,
        raw_value: int | float,
        decode_choices: bool = True,
    ) -> SignalValueType:
        return raw_value * self.scale + self.offset

    def scaled_to_raw(self, scaled_value: SignalValueType) -> int | float:
        if not isinstance(scaled_value, (int, float)):
            raise TypeError(
                f"'scaled_value' must have type 'int' or 'float' (is {type(scaled_value)})"
            )
        return self.numeric_scaled_to_raw(scaled_value)

    def numeric_scaled_to_raw(
        self, scaled_value: int | float
    ) -> int | float:
        _raw = (scaled_value - self.offset) / self.scale
        return _raw if self.is_float else round(_raw)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"scale={self.scale}, "
            f"offset={self.offset}, "
            f"is_float={self.is_float})"
        )


class NamedSignalConversion(BaseConversion):
    def __init__(
        self, scale: float, offset: float, choices: Choices, is_float: bool
    ) -> None:
        self.scale = scale
        self.offset = offset
        self.is_float = is_float
        self._inverse_choices: dict[str, int] = {}
        self.choices: Choices = choices
        self._update_choices()

        self._conversion = BaseConversion.factory(
            scale=self.scale,
            offset=self.offset,
            choices=None,
            is_float=is_float,
        )
        # monkeypatch method to avoid unnecessary function call
        self.numeric_scaled_to_raw = self._conversion.numeric_scaled_to_raw  # type: ignore[method-assign]

    def raw_to_scaled(
        self,
        raw_value: int | float,
        decode_choices: bool = True,
    ) -> SignalValueType:
        if decode_choices and (choice := self.choices.get(raw_value)) is not None:  # type: ignore[arg-type]
            return choice
        return self._conversion.raw_to_scaled(raw_value, False)

    def scaled_to_raw(self, scaled_value: SignalValueType) -> int | float:
        if isinstance(scaled_value, (int, float)):
            return self._conversion.scaled_to_raw(scaled_value)

        if hasattr(scaled_value, "value"):
            # scaled_value is NamedSignalValue
            return scaled_value.value

        if isinstance(scaled_value, str):
            return self.choice_to_number(scaled_value)

        raise TypeError

    def numeric_scaled_to_raw(
        self, scaled_value: int | float
    ) -> int | float:
        return self._conversion.scaled_to_raw(scaled_value)

    def set_choices(self, choices: Choices) -> None:
        self.choices = choices
        self._update_choices()

    def _update_choices(self) -> None:
        # we simply assume that the choices are invertible
        self._inverse_choices = {str(x[1]): x[0] for x in self.choices.items()}

    def choice_to_number(self, choice: Union[str, "NamedSignalValue"]) -> int:
        return self._inverse_choices[str(choice)]

    def __repr__(self) -> str:
        list_of_choices = ", ".join(
            [f"{value}: '{text}'" for value, text in self.choices.items()]
        )
        choices = f"{{{list_of_choices}}}"
        return (
            f"{self.__class__.__name__}("
            f"scale={self.scale}, "
            f"offset={self.offset}, "
            f"choices={choices}, "
            f"is_float={self.is_float})"
        )


def _is_integer(value: int | float) -> bool:
    if isinstance(value, int) or (hasattr(value, "is_integer") and value.is_integer()):
        return True
    elif isinstance(value, float):
        return False

    err_msg = f"`value` must be of type `int` or `float`, is {type(value)}"
    raise TypeError(err_msg)
