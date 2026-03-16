# pylint: disable=too-many-lines
import math
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from can.typechecking import BitTimingDict, BitTimingFdDict


class BitTiming(Mapping[str, int]):
    """Representation of a bit timing configuration for a CAN 2.0 bus.

    The class can be constructed in multiple ways, depending on the information
    available. The preferred way is using CAN clock frequency, prescaler, tseg1, tseg2 and sjw::

        can.BitTiming(f_clock=8_000_000, brp=1, tseg1=5, tseg2=1, sjw=1)

    Alternatively you can set the bitrate instead of the bit rate prescaler::

        can.BitTiming.from_bitrate_and_segments(
            f_clock=8_000_000, bitrate=1_000_000, tseg1=5, tseg2=1, sjw=1
        )

    It is also possible to specify BTR registers::

        can.BitTiming.from_registers(f_clock=8_000_000, btr0=0x00, btr1=0x14)

    or to calculate the timings for a given sample point::

        can.BitTiming.from_sample_point(f_clock=8_000_000, bitrate=1_000_000, sample_point=75.0)
    """

    def __init__(
        self,
        f_clock: int,
        brp: int,
        tseg1: int,
        tseg2: int,
        sjw: int,
        nof_samples: int = 1,
        strict: bool = False,
    ) -> None:
        """
        :param int f_clock:
            The CAN system clock frequency in Hz.
        :param int brp:
            Bit rate prescaler.
        :param int tseg1:
            Time segment 1, that is, the number of quanta from (but not including)
            the Sync Segment to the sampling point.
        :param int tseg2:
            Time segment 2, that is, the number of quanta from the sampling
            point to the end of the bit.
        :param int sjw:
            The Synchronization Jump Width. Decides the maximum number of time quanta
            that the controller can resynchronize every bit.
        :param int nof_samples:
            Either 1 or 3. Some CAN controllers can also sample each bit three times.
            In this case, the bit will be sampled three quanta in a row,
            with the last sample being taken in the edge between TSEG1 and TSEG2.
            Three samples should only be used for relatively slow baudrates.
        :param bool strict:
            If True, restrict bit timings to the minimum required range as defined in
            ISO 11898. This can be used to ensure compatibility across a wide variety
            of CAN hardware.
        :raises ValueError:
            if the arguments are invalid.
        """
        self._data: BitTimingDict = {
            "f_clock": f_clock,
            "brp": brp,
            "tseg1": tseg1,
            "tseg2": tseg2,
            "sjw": sjw,
            "nof_samples": nof_samples,
        }
        self._validate()
        if strict:
            self._restrict_to_minimum_range()

    def _validate(self) -> None:
        if not 1 <= self.brp <= 64:
            raise ValueError(f"bitrate prescaler (={self.brp}) must be in [1...64].")

        if not 1 <= self.tseg1 <= 16:
            raise ValueError(f"tseg1 (={self.tseg1}) must be in [1...16].")

        if not 1 <= self.tseg2 <= 8:
            raise ValueError(f"tseg2 (={self.tseg2}) must be in [1...8].")

        if not 1 <= self.sjw <= 4:
            raise ValueError(f"sjw (={self.sjw}) must be in [1...4].")

        if self.sjw > self.tseg2:
            raise ValueError(
                f"sjw (={self.sjw}) must not be greater than tseg2 (={self.tseg2})."
            )

        if self.sample_point < 50.0:
            raise ValueError(
                f"The sample point must be greater than or equal to 50% "
                f"(sample_point={self.sample_point:.2f}%)."
            )

        if self.nof_samples not in (1, 3):
            raise ValueError("nof_samples must be 1 or 3")

    def _restrict_to_minimum_range(self) -> None:
        if not 8 <= self.nbt <= 25:
            raise ValueError(f"nominal bit time (={self.nbt}) must be in [8...25].")

        if not 1 <= self.brp <= 32:
            raise ValueError(f"bitrate prescaler (={self.brp}) must be in [1...32].")

        if not 5_000 <= self.bitrate <= 1_000_000:
            raise ValueError(
                f"bitrate (={self.bitrate}) must be in [5,000...1,000,000]."
            )

    @classmethod
    def from_bitrate_and_segments(
        cls,
        f_clock: int,
        bitrate: int,
        tseg1: int,
        tseg2: int,
        sjw: int,
        nof_samples: int = 1,
        strict: bool = False,
    ) -> "BitTiming":
        """Create a :class:`~can.BitTiming` instance from bitrate and segment lengths.

        :param int f_clock:
            The CAN system clock frequency in Hz.
        :param int bitrate:
            Bitrate in bit/s.
        :param int tseg1:
            Time segment 1, that is, the number of quanta from (but not including)
            the Sync Segment to the sampling point.
        :param int tseg2:
            Time segment 2, that is, the number of quanta from the sampling
            point to the end of the bit.
        :param int sjw:
            The Synchronization Jump Width. Decides the maximum number of time quanta
            that the controller can resynchronize every bit.
        :param int nof_samples:
            Either 1 or 3. Some CAN controllers can also sample each bit three times.
            In this case, the bit will be sampled three quanta in a row,
            with the last sample being taken in the edge between TSEG1 and TSEG2.
            Three samples should only be used for relatively slow baudrates.
        :param bool strict:
            If True, restrict bit timings to the minimum required range as defined in
            ISO 11898. This can be used to ensure compatibility across a wide variety
            of CAN hardware.
        :raises ValueError:
            if the arguments are invalid.
        """
        try:
            brp = round(f_clock / (bitrate * (1 + tseg1 + tseg2)))
        except ZeroDivisionError:
            raise ValueError("Invalid inputs") from None

        bt = cls(
            f_clock=f_clock,
            brp=brp,
            tseg1=tseg1,
            tseg2=tseg2,
            sjw=sjw,
            nof_samples=nof_samples,
            strict=strict,
        )
        if abs(bt.bitrate - bitrate) > bitrate / 256:
            raise ValueError(
                f"the effective bitrate (={bt.bitrate}) diverges "
                f"from the requested bitrate (={bitrate})"
            )
        return bt

    @classmethod
    def from_registers(
        cls,
        f_clock: int,
        btr0: int,
        btr1: int,
    ) -> "BitTiming":
        """Create a :class:`~can.BitTiming` instance from registers btr0 and btr1.

        :param int f_clock:
            The CAN system clock frequency in Hz.
        :param int btr0:
            The BTR0 register value used by many CAN controllers.
        :param int btr1:
            The BTR1 register value used by many CAN controllers.
        :raises ValueError:
            if the arguments are invalid.
        """
        if not 0 <= btr0 < 2**16:
            raise ValueError(f"Invalid btr0 value. ({btr0})")
        if not 0 <= btr1 < 2**16:
            raise ValueError(f"Invalid btr1 value. ({btr1})")

        brp = (btr0 & 0x3F) + 1
        sjw = (btr0 >> 6) + 1
        tseg1 = (btr1 & 0xF) + 1
        tseg2 = ((btr1 >> 4) & 0x7) + 1
        nof_samples = 3 if btr1 & 0x80 else 1
        return cls(
            brp=brp,
            f_clock=f_clock,
            tseg1=tseg1,
            tseg2=tseg2,
            sjw=sjw,
            nof_samples=nof_samples,
        )

    @classmethod
    def iterate_from_sample_point(
        cls, f_clock: int, bitrate: int, sample_point: float = 69.0
    ) -> Iterator["BitTiming"]:
        """Create a :class:`~can.BitTiming` iterator with all the solutions for a sample point.

        :param int f_clock:
            The CAN system clock frequency in Hz.
        :param int bitrate:
            Bitrate in bit/s.
        :param int sample_point:
            The sample point value in percent.
        :raises ValueError:
            if the arguments are invalid.
        """

        if sample_point < 50.0:
            raise ValueError(f"sample_point (={sample_point}) must not be below 50%.")

        for brp in range(1, 65):
            nbt = int(f_clock / (bitrate * brp))
            if nbt < 8:
                break

            effective_bitrate = f_clock / (nbt * brp)
            if abs(effective_bitrate - bitrate) > bitrate / 256:
                continue

            tseg1 = round(sample_point / 100 * nbt) - 1
            # limit tseg1, so tseg2 is at least 1 TQ
            tseg1 = min(tseg1, nbt - 2)

            tseg2 = nbt - tseg1 - 1
            sjw = min(tseg2, 4)

            try:
                bt = BitTiming(
                    f_clock=f_clock,
                    brp=brp,
                    tseg1=tseg1,
                    tseg2=tseg2,
                    sjw=sjw,
                    strict=True,
                )
                yield bt
            except ValueError:
                continue

    @classmethod
    def from_sample_point(
        cls, f_clock: int, bitrate: int, sample_point: float = 69.0
    ) -> "BitTiming":
        """Create a :class:`~can.BitTiming` instance for a sample point.

        This function tries to find bit timings, which are close to the requested
        sample point. It does not take physical bus properties into account, so the
        calculated bus timings might not work properly for you.

        The :func:`oscillator_tolerance` function might be helpful to evaluate the
        bus timings.

        :param int f_clock:
            The CAN system clock frequency in Hz.
        :param int bitrate:
            Bitrate in bit/s.
        :param int sample_point:
            The sample point value in percent.
        :raises ValueError:
            if the arguments are invalid.
        """

        if sample_point < 50.0:
            raise ValueError(f"sample_point (={sample_point}) must not be below 50%.")

        possible_solutions: list[BitTiming] = list(
            cls.iterate_from_sample_point(f_clock, bitrate, sample_point)
        )

        if not possible_solutions:
            raise ValueError("No suitable bit timings found.")

        # sort solutions
        for key, reverse in (
            # prefer low prescaler
            (lambda x: x.brp, False),
            # prefer low sample point deviation from requested values
            (lambda x: abs(x.sample_point - sample_point), False),
        ):
            possible_solutions.sort(key=key, reverse=reverse)

        return possible_solutions[0]

    @property
    def f_clock(self) -> int:
        """The CAN system clock frequency in Hz."""
        return self._data["f_clock"]

    @property
    def bitrate(self) -> int:
        """Bitrate in bits/s."""
        return round(self.f_clock / (self.nbt * self.brp))

    @property
    def brp(self) -> int:
        """Bit Rate Prescaler."""
        return self._data["brp"]

    @property
    def tq(self) -> int:
        """Time quantum in nanoseconds"""
        return round(self.brp / self.f_clock * 1e9)

    @property
    def nbt(self) -> int:
        """Nominal Bit Time."""
        return 1 + self.tseg1 + self.tseg2

    @property
    def tseg1(self) -> int:
        """Time segment 1.

        The number of quanta from (but not including) the Sync Segment to the sampling point.
        """
        return self._data["tseg1"]

    @property
    def tseg2(self) -> int:
        """Time segment 2.

        The number of quanta from the sampling point to the end of the bit.
        """
        return self._data["tseg2"]

    @property
    def sjw(self) -> int:
        """Synchronization Jump Width."""
        return self._data["sjw"]

    @property
    def nof_samples(self) -> int:
        """Number of samples (1 or 3)."""
        return self._data["nof_samples"]

    @property
    def sample_point(self) -> float:
        """Sample point in percent."""
        return 100.0 * (1 + self.tseg1) / (1 + self.tseg1 + self.tseg2)

    @property
    def btr0(self) -> int:
        """Bit timing register 0 for SJA1000."""
        return (self.sjw - 1) << 6 | self.brp - 1

    @property
    def btr1(self) -> int:
        """Bit timing register 1 for SJA1000."""
        sam = 1 if self.nof_samples == 3 else 0
        return sam << 7 | (self.tseg2 - 1) << 4 | self.tseg1 - 1

    def oscillator_tolerance(
        self,
        node_loop_delay_ns: float = 250.0,
        bus_length_m: float = 10.0,
    ) -> float:
        """Oscillator tolerance in percent according to ISO 11898-1.

        :param float node_loop_delay_ns:
            Transceiver loop delay in nanoseconds.
        :param float bus_length_m:
            Bus length in meters.
        """
        delay_per_meter = 5
        bidirectional_propagation_delay_ns = 2 * (
            node_loop_delay_ns + delay_per_meter * bus_length_m
        )

        prop_seg = math.ceil(bidirectional_propagation_delay_ns / self.tq)
        nom_phase_seg1 = self.tseg1 - prop_seg
        nom_phase_seg2 = self.tseg2
        df_clock_list = [
            _oscillator_tolerance_condition_1(nom_sjw=self.sjw, nbt=self.nbt),
            _oscillator_tolerance_condition_2(
                nbt=self.nbt,
                nom_phase_seg1=nom_phase_seg1,
                nom_phase_seg2=nom_phase_seg2,
            ),
        ]
        return max(0.0, min(df_clock_list) * 100)

    def recreate_with_f_clock(self, f_clock: int) -> "BitTiming":
        """Return a new :class:`~can.BitTiming` instance with the given *f_clock* but the same
        bit rate and sample point.

        :param int f_clock:
            The CAN system clock frequency in Hz.
        :raises ValueError:
            if no suitable bit timings were found.
        """
        # try the most simple solution first: another bitrate prescaler
        try:
            return BitTiming.from_bitrate_and_segments(
                f_clock=f_clock,
                bitrate=self.bitrate,
                tseg1=self.tseg1,
                tseg2=self.tseg2,
                sjw=self.sjw,
                nof_samples=self.nof_samples,
                strict=True,
            )
        except ValueError:
            pass

        # create a new timing instance with the same sample point
        bt = BitTiming.from_sample_point(
            f_clock=f_clock, bitrate=self.bitrate, sample_point=self.sample_point
        )
        if abs(bt.sample_point - self.sample_point) > 1.0:
            raise ValueError(
                "f_clock change failed because of sample point discrepancy."
            )
        # adapt synchronization jump width, so it has the same size relative to bit time as self
        sjw = round(self.sjw / self.nbt * bt.nbt)
        sjw = max(1, min(4, bt.tseg2, sjw))
        bt._data["sjw"] = sjw  # pylint: disable=protected-access
        bt._data["nof_samples"] = self.nof_samples  # pylint: disable=protected-access
        bt._validate()  # pylint: disable=protected-access
        return bt

    def __str__(self) -> str:
        segments = [
            f"BR: {self.bitrate:_} bit/s",
            f"SP: {self.sample_point:.2f}%",
            f"BRP: {self.brp}",
            f"TSEG1: {self.tseg1}",
            f"TSEG2: {self.tseg2}",
            f"SJW: {self.sjw}",
            f"BTR: {self.btr0:02X}{self.btr1:02X}h",
            f"CLK: {self.f_clock / 1e6:.0f}MHz",
        ]
        return ", ".join(segments)

    def __repr__(self) -> str:
        args = ", ".join(f"{key}={value}" for key, value in self.items())
        return f"can.{self.__class__.__name__}({args})"

    def __getitem__(self, key: str) -> int:
        return cast("int", self._data.__getitem__(key))

    def __len__(self) -> int:
        return self._data.__len__()

    def __iter__(self) -> Iterator[str]:
        return self._data.__iter__()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BitTiming):
            return False

        return self._data == other._data

    def __hash__(self) -> int:
        return tuple(self._data.values()).__hash__()


class BitTimingFd(Mapping[str, int]):
    """Representation of a bit timing configuration for a CAN FD bus.

    The class can be constructed in multiple ways, depending on the information
    available. The preferred way is using CAN clock frequency, bit rate prescaler, tseg1,
    tseg2 and sjw for both the arbitration (nominal) and data phase::

        can.BitTimingFd(
            f_clock=80_000_000,
            nom_brp=1,
            nom_tseg1=59,
            nom_tseg2=20,
            nom_sjw=10,
            data_brp=1,
            data_tseg1=6,
            data_tseg2=3,
            data_sjw=2,
        )

    Alternatively you can set the bit rates instead of the bit rate prescalers::

        can.BitTimingFd.from_bitrate_and_segments(
            f_clock=80_000_000,
            nom_bitrate=1_000_000,
            nom_tseg1=59,
            nom_tseg2=20,
            nom_sjw=10,
            data_bitrate=8_000_000,
            data_tseg1=6,
            data_tseg2=3,
            data_sjw=2,
        )

    It is also possible to calculate the timings for a given
    pair of arbitration and data sample points::

        can.BitTimingFd.from_sample_point(
            f_clock=80_000_000,
            nom_bitrate=1_000_000,
            nom_sample_point=75.0,
            data_bitrate=8_000_000,
            data_sample_point=70.0,
        )
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        f_clock: int,
        nom_brp: int,
        nom_tseg1: int,
        nom_tseg2: int,
        nom_sjw: int,
        data_brp: int,
        data_tseg1: int,
        data_tseg2: int,
        data_sjw: int,
        strict: bool = False,
    ) -> None:
        """
        Initialize a BitTimingFd instance with the specified parameters.

        :param int f_clock:
            The CAN system clock frequency in Hz.
        :param int nom_brp:
            Nominal (arbitration) phase bitrate prescaler.
        :param int nom_tseg1:
            Nominal phase Time segment 1, that is, the number of quanta from (but not including)
            the Sync Segment to the sampling point.
        :param int nom_tseg2:
            Nominal phase Time segment 2, that is, the number of quanta from the sampling
            point to the end of the bit.
        :param int nom_sjw:
            The Synchronization Jump Width for the nominal phase. This value determines
            the maximum number of time quanta that the controller can resynchronize every bit.
        :param int data_brp:
            Data phase bitrate prescaler.
        :param int data_tseg1:
            Data phase Time segment 1, that is, the number of quanta from (but not including)
            the Sync Segment to the sampling point.
        :param int data_tseg2:
            Data phase Time segment 2, that is, the number of quanta from the sampling
            point to the end of the bit.
        :param int data_sjw:
            The Synchronization Jump Width for the data phase. This value determines
            the maximum number of time quanta that the controller can resynchronize every bit.
        :param bool strict:
            If True, restrict bit timings to the minimum required range as defined in
            ISO 11898. This can be used to ensure compatibility across a wide variety
            of CAN hardware.
        :raises ValueError:
            if the arguments are invalid.
        """
        self._data: BitTimingFdDict = {
            "f_clock": f_clock,
            "nom_brp": nom_brp,
            "nom_tseg1": nom_tseg1,
            "nom_tseg2": nom_tseg2,
            "nom_sjw": nom_sjw,
            "data_brp": data_brp,
            "data_tseg1": data_tseg1,
            "data_tseg2": data_tseg2,
            "data_sjw": data_sjw,
        }
        self._validate()
        if strict:
            self._restrict_to_minimum_range()

    def _validate(self) -> None:
        for param, value in self._data.items():
            if value < 0:  # type: ignore[operator]
                err_msg = f"'{param}' (={value}) must not be negative."
                raise ValueError(err_msg)

        if self.nom_brp < 1:
            raise ValueError(
                f"nominal bitrate prescaler (={self.nom_brp}) must be at least 1."
            )

        if self.data_brp < 1:
            raise ValueError(
                f"data bitrate prescaler (={self.data_brp}) must be at least 1."
            )

        if self.data_bitrate < self.nom_bitrate:
            raise ValueError(
                f"data_bitrate (={self.data_bitrate}) must be greater than or "
                f"equal to nom_bitrate (={self.nom_bitrate})"
            )

        if self.nom_sjw > self.nom_tseg2:
            raise ValueError(
                f"nom_sjw (={self.nom_sjw}) must not be "
                f"greater than nom_tseg2 (={self.nom_tseg2})."
            )

        if self.data_sjw > self.data_tseg2:
            raise ValueError(
                f"data_sjw (={self.data_sjw}) must not be "
                f"greater than data_tseg2 (={self.data_tseg2})."
            )

        if self.nom_sample_point < 50.0:
            raise ValueError(
                f"The arbitration sample point must be greater than or equal to 50% "
                f"(nom_sample_point={self.nom_sample_point:.2f}%)."
            )

        if self.data_sample_point < 50.0:
            raise ValueError(
                f"The data sample point must be greater than or equal to 50% "
                f"(data_sample_point={self.data_sample_point:.2f}%)."
            )

    def _restrict_to_minimum_range(self) -> None:
        # restrict to minimum required range as defined in ISO 11898
        if not 8 <= self.nbt <= 80:
            raise ValueError(f"Nominal bit time (={self.nbt}) must be in [8...80]")

        if not 5 <= self.dbt <= 25:
            raise ValueError(f"Nominal bit time (={self.dbt}) must be in [5...25]")

        if not 1 <= self.data_tseg1 <= 16:
            raise ValueError(f"data_tseg1 (={self.data_tseg1}) must be in [1...16].")

        if not 2 <= self.data_tseg2 <= 8:
            raise ValueError(f"data_tseg2 (={self.data_tseg2}) must be in [2...8].")

        if not 1 <= self.data_sjw <= 8:
            raise ValueError(f"data_sjw (={self.data_sjw}) must be in [1...8].")

        if self.nom_brp == self.data_brp:
            # shared prescaler
            if not 2 <= self.nom_tseg1 <= 128:
                raise ValueError(f"nom_tseg1 (={self.nom_tseg1}) must be in [2...128].")

            if not 2 <= self.nom_tseg2 <= 32:
                raise ValueError(f"nom_tseg2 (={self.nom_tseg2}) must be in [2...32].")

            if not 1 <= self.nom_sjw <= 32:
                raise ValueError(f"nom_sjw (={self.nom_sjw}) must be in [1...32].")
        else:
            # separate prescaler
            if not 2 <= self.nom_tseg1 <= 64:
                raise ValueError(f"nom_tseg1 (={self.nom_tseg1}) must be in [2...64].")

            if not 2 <= self.nom_tseg2 <= 16:
                raise ValueError(f"nom_tseg2 (={self.nom_tseg2}) must be in [2...16].")

            if not 1 <= self.nom_sjw <= 16:
                raise ValueError(f"nom_sjw (={self.nom_sjw}) must be in [1...16].")

    @classmethod
    def from_bitrate_and_segments(  # pylint: disable=too-many-arguments
        cls,
        f_clock: int,
        nom_bitrate: int,
        nom_tseg1: int,
        nom_tseg2: int,
        nom_sjw: int,
        data_bitrate: int,
        data_tseg1: int,
        data_tseg2: int,
        data_sjw: int,
        strict: bool = False,
    ) -> "BitTimingFd":
        """
        Create a :class:`~can.BitTimingFd` instance with the bitrates and segments lengths.

        :param int f_clock:
            The CAN system clock frequency in Hz.
        :param int nom_bitrate:
            Nominal (arbitration) phase bitrate in bit/s.
        :param int nom_tseg1:
            Nominal phase Time segment 1, that is, the number of quanta from (but not including)
            the Sync Segment to the sampling point.
        :param int nom_tseg2:
            Nominal phase Time segment 2, that is, the number of quanta from the sampling
            point to the end of the bit.
        :param int nom_sjw:
            The Synchronization Jump Width for the nominal phase. This value determines
            the maximum number of time quanta that the controller can resynchronize every bit.
        :param int data_bitrate:
            Data phase bitrate in bit/s.
        :param int data_tseg1:
            Data phase Time segment 1, that is, the number of quanta from (but not including)
            the Sync Segment to the sampling point.
        :param int data_tseg2:
            Data phase Time segment 2, that is, the number of quanta from the sampling
            point to the end of the bit.
        :param int data_sjw:
            The Synchronization Jump Width for the data phase. This value determines
            the maximum number of time quanta that the controller can resynchronize every bit.
        :param bool strict:
            If True, restrict bit timings to the minimum required range as defined in
            ISO 11898. This can be used to ensure compatibility across a wide variety
            of CAN hardware.
        :raises ValueError:
            if the arguments are invalid.
        """
        try:
            nom_brp = round(f_clock / (nom_bitrate * (1 + nom_tseg1 + nom_tseg2)))
            data_brp = round(f_clock / (data_bitrate * (1 + data_tseg1 + data_tseg2)))
        except ZeroDivisionError:
            raise ValueError("Invalid inputs.") from None

        bt = cls(
            f_clock=f_clock,
            nom_brp=nom_brp,
            nom_tseg1=nom_tseg1,
            nom_tseg2=nom_tseg2,
            nom_sjw=nom_sjw,
            data_brp=data_brp,
            data_tseg1=data_tseg1,
            data_tseg2=data_tseg2,
            data_sjw=data_sjw,
            strict=strict,
        )

        if abs(bt.nom_bitrate - nom_bitrate) > nom_bitrate / 256:
            raise ValueError(
                f"the effective nom. bitrate (={bt.nom_bitrate}) diverges "
                f"from the requested nom. bitrate (={nom_bitrate})"
            )

        if abs(bt.data_bitrate - data_bitrate) > data_bitrate / 256:
            raise ValueError(
                f"the effective data bitrate (={bt.data_bitrate}) diverges "
                f"from the requested data bitrate (={data_bitrate})"
            )

        return bt

    @classmethod
    def iterate_from_sample_point(
        cls,
        f_clock: int,
        nom_bitrate: int,
        nom_sample_point: float,
        data_bitrate: int,
        data_sample_point: float,
    ) -> Iterator["BitTimingFd"]:
        """Create an :class:`~can.BitTimingFd` iterator with all the solutions for a sample point.

        :param int f_clock:
            The CAN system clock frequency in Hz.
        :param int nom_bitrate:
            Nominal bitrate in bit/s.
        :param int nom_sample_point:
            The sample point value of the arbitration phase in percent.
        :param int data_bitrate:
            Data bitrate in bit/s.
        :param int data_sample_point:
            The sample point value of the data phase in percent.
        :raises ValueError:
            if the arguments are invalid.
        """
        if nom_sample_point < 50.0:
            raise ValueError(
                f"nom_sample_point (={nom_sample_point}) must not be below 50%."
            )

        if data_sample_point < 50.0:
            raise ValueError(
                f"data_sample_point (={data_sample_point}) must not be below 50%."
            )

        sync_seg = 1

        for nom_brp in range(1, 257):
            nbt = int(f_clock / (nom_bitrate * nom_brp))
            if nbt < 1:
                break

            effective_nom_bitrate = f_clock / (nbt * nom_brp)
            if abs(effective_nom_bitrate - nom_bitrate) > nom_bitrate / 256:
                continue

            nom_tseg1 = round(nom_sample_point / 100 * nbt) - 1
            # limit tseg1, so tseg2 is at least 2 TQ
            nom_tseg1 = min(nom_tseg1, nbt - sync_seg - 2)
            nom_tseg2 = nbt - nom_tseg1 - 1

            nom_sjw = min(nom_tseg2, 128)

            for data_brp in range(1, 257):
                dbt = round(int(f_clock / (data_bitrate * data_brp)))
                if dbt < 1:
                    break

                effective_data_bitrate = f_clock / (dbt * data_brp)
                if abs(effective_data_bitrate - data_bitrate) > data_bitrate / 256:
                    continue

                data_tseg1 = round(data_sample_point / 100 * dbt) - 1
                # limit tseg1, so tseg2 is at least 2 TQ
                data_tseg1 = min(data_tseg1, dbt - sync_seg - 2)
                data_tseg2 = dbt - data_tseg1 - 1

                data_sjw = min(data_tseg2, 16)

                try:
                    bt = BitTimingFd(
                        f_clock=f_clock,
                        nom_brp=nom_brp,
                        nom_tseg1=nom_tseg1,
                        nom_tseg2=nom_tseg2,
                        nom_sjw=nom_sjw,
                        data_brp=data_brp,
                        data_tseg1=data_tseg1,
                        data_tseg2=data_tseg2,
                        data_sjw=data_sjw,
                        strict=True,
                    )
                    yield bt
                except ValueError:
                    continue

    @classmethod
    def from_sample_point(
        cls,
        f_clock: int,
        nom_bitrate: int,
        nom_sample_point: float,
        data_bitrate: int,
        data_sample_point: float,
    ) -> "BitTimingFd":
        """Create a :class:`~can.BitTimingFd` instance for a sample point.

        This function tries to find bit timings, which are close to the requested
        sample points. It does not take physical bus properties into account, so the
        calculated bus timings might not work properly for you.

        The :func:`oscillator_tolerance` function might be helpful to evaluate the
        bus timings.

        :param int f_clock:
            The CAN system clock frequency in Hz.
        :param int nom_bitrate:
            Nominal bitrate in bit/s.
        :param int nom_sample_point:
            The sample point value of the arbitration phase in percent.
        :param int data_bitrate:
            Data bitrate in bit/s.
        :param int data_sample_point:
            The sample point value of the data phase in percent.
        :raises ValueError:
            if the arguments are invalid.
        """
        if nom_sample_point < 50.0:
            raise ValueError(
                f"nom_sample_point (={nom_sample_point}) must not be below 50%."
            )

        if data_sample_point < 50.0:
            raise ValueError(
                f"data_sample_point (={data_sample_point}) must not be below 50%."
            )

        possible_solutions: list[BitTimingFd] = list(
            cls.iterate_from_sample_point(
                f_clock,
                nom_bitrate,
                nom_sample_point,
                data_bitrate,
                data_sample_point,
            )
        )

        if not possible_solutions:
            raise ValueError("No suitable bit timings found.")

        # prefer using the same prescaler for arbitration and data phase
        same_prescaler = list(
            filter(lambda x: x.nom_brp == x.data_brp, possible_solutions)
        )
        if same_prescaler:
            possible_solutions = same_prescaler

        # sort solutions
        for key, reverse in (
            # prefer low prescaler
            (lambda x: x.nom_brp + x.data_brp, False),
            # prefer same prescaler for arbitration and data
            (lambda x: abs(x.nom_brp - x.data_brp), False),
            # prefer low sample point deviation from requested values
            (
                lambda x: (
                    abs(x.nom_sample_point - nom_sample_point)
                    + abs(x.data_sample_point - data_sample_point)
                ),
                False,
            ),
        ):
            possible_solutions.sort(key=key, reverse=reverse)

        return possible_solutions[0]

    @property
    def f_clock(self) -> int:
        """The CAN system clock frequency in Hz."""
        return self._data["f_clock"]

    @property
    def nom_bitrate(self) -> int:
        """Nominal (arbitration phase) bitrate."""
        return round(self.f_clock / (self.nbt * self.nom_brp))

    @property
    def nom_brp(self) -> int:
        """Prescaler value for the arbitration phase."""
        return self._data["nom_brp"]

    @property
    def nom_tq(self) -> int:
        """Nominal time quantum in nanoseconds"""
        return round(self.nom_brp / self.f_clock * 1e9)

    @property
    def nbt(self) -> int:
        """Number of time quanta in a bit of the arbitration phase."""
        return 1 + self.nom_tseg1 + self.nom_tseg2

    @property
    def nom_tseg1(self) -> int:
        """Time segment 1 value of the arbitration phase.

        This is the sum of the propagation time segment and the phase buffer segment 1.
        """
        return self._data["nom_tseg1"]

    @property
    def nom_tseg2(self) -> int:
        """Time segment 2 value of the arbitration phase. Also known as phase buffer segment 2."""
        return self._data["nom_tseg2"]

    @property
    def nom_sjw(self) -> int:
        """Synchronization jump width of the arbitration phase.

        The phase buffer segments may be shortened or lengthened by this value.
        """
        return self._data["nom_sjw"]

    @property
    def nom_sample_point(self) -> float:
        """Sample point of the arbitration phase in percent."""
        return 100.0 * (1 + self.nom_tseg1) / (1 + self.nom_tseg1 + self.nom_tseg2)

    @property
    def data_bitrate(self) -> int:
        """Bitrate of the data phase in bit/s."""
        return round(self.f_clock / (self.dbt * self.data_brp))

    @property
    def data_brp(self) -> int:
        """Prescaler value for the data phase."""
        return self._data["data_brp"]

    @property
    def data_tq(self) -> int:
        """Data time quantum in nanoseconds"""
        return round(self.data_brp / self.f_clock * 1e9)

    @property
    def dbt(self) -> int:
        """Number of time quanta in a bit of the data phase."""
        return 1 + self.data_tseg1 + self.data_tseg2

    @property
    def data_tseg1(self) -> int:
        """TSEG1 value of the data phase.

        This is the sum of the propagation time segment and the phase buffer segment 1.
        """
        return self._data["data_tseg1"]

    @property
    def data_tseg2(self) -> int:
        """TSEG2 value of the data phase. Also known as phase buffer segment 2."""
        return self._data["data_tseg2"]

    @property
    def data_sjw(self) -> int:
        """Synchronization jump width of the data phase.

        The phase buffer segments may be shortened or lengthened by this value.
        """
        return self._data["data_sjw"]

    @property
    def data_sample_point(self) -> float:
        """Sample point of the data phase in percent."""
        return 100.0 * (1 + self.data_tseg1) / (1 + self.data_tseg1 + self.data_tseg2)

    def oscillator_tolerance(
        self,
        node_loop_delay_ns: float = 250.0,
        bus_length_m: float = 10.0,
    ) -> float:
        """Oscillator tolerance in percent according to ISO 11898-1.

        :param float node_loop_delay_ns:
            Transceiver loop delay in nanoseconds.
        :param float bus_length_m:
            Bus length in meters.
        """
        delay_per_meter = 5
        bidirectional_propagation_delay_ns = 2 * (
            node_loop_delay_ns + delay_per_meter * bus_length_m
        )

        prop_seg = math.ceil(bidirectional_propagation_delay_ns / self.nom_tq)
        nom_phase_seg1 = self.nom_tseg1 - prop_seg
        nom_phase_seg2 = self.nom_tseg2

        data_phase_seg2 = self.data_tseg2

        df_clock_list = [
            _oscillator_tolerance_condition_1(nom_sjw=self.nom_sjw, nbt=self.nbt),
            _oscillator_tolerance_condition_2(
                nbt=self.nbt,
                nom_phase_seg1=nom_phase_seg1,
                nom_phase_seg2=nom_phase_seg2,
            ),
            _oscillator_tolerance_condition_3(data_sjw=self.data_sjw, dbt=self.dbt),
            _oscillator_tolerance_condition_4(
                nom_phase_seg1=nom_phase_seg1,
                nom_phase_seg2=nom_phase_seg2,
                data_phase_seg2=data_phase_seg2,
                nbt=self.nbt,
                dbt=self.dbt,
                data_brp=self.data_brp,
                nom_brp=self.nom_brp,
            ),
            _oscillator_tolerance_condition_5(
                data_sjw=self.data_sjw,
                data_brp=self.data_brp,
                nom_brp=self.nom_brp,
                data_phase_seg2=data_phase_seg2,
                nom_phase_seg2=nom_phase_seg2,
                nbt=self.nbt,
                dbt=self.dbt,
            ),
        ]
        return max(0.0, min(df_clock_list) * 100)

    def recreate_with_f_clock(self, f_clock: int) -> "BitTimingFd":
        """Return a new :class:`~can.BitTimingFd` instance with the given *f_clock* but the same
        bit rates and sample points.

        :param int f_clock:
            The CAN system clock frequency in Hz.
        :raises ValueError:
            if no suitable bit timings were found.
        """
        # try the most simple solution first: another bitrate prescaler
        try:
            return BitTimingFd.from_bitrate_and_segments(
                f_clock=f_clock,
                nom_bitrate=self.nom_bitrate,
                nom_tseg1=self.nom_tseg1,
                nom_tseg2=self.nom_tseg2,
                nom_sjw=self.nom_sjw,
                data_bitrate=self.data_bitrate,
                data_tseg1=self.data_tseg1,
                data_tseg2=self.data_tseg2,
                data_sjw=self.data_sjw,
                strict=True,
            )
        except ValueError:
            pass

        # create a new timing instance with the same sample points
        bt = BitTimingFd.from_sample_point(
            f_clock=f_clock,
            nom_bitrate=self.nom_bitrate,
            nom_sample_point=self.nom_sample_point,
            data_bitrate=self.data_bitrate,
            data_sample_point=self.data_sample_point,
        )
        if (
            abs(bt.nom_sample_point - self.nom_sample_point) > 1.0
            or abs(bt.data_sample_point - self.data_sample_point) > 1.0
        ):
            raise ValueError(
                "f_clock change failed because of sample point discrepancy."
            )
        # adapt synchronization jump width, so it has the same size relative to bit time as self
        nom_sjw = round(self.nom_sjw / self.nbt * bt.nbt)
        nom_sjw = max(1, min(bt.nom_tseg2, nom_sjw))
        bt._data["nom_sjw"] = nom_sjw  # pylint: disable=protected-access
        data_sjw = round(self.data_sjw / self.dbt * bt.dbt)
        data_sjw = max(1, min(bt.data_tseg2, data_sjw))
        bt._data["data_sjw"] = data_sjw  # pylint: disable=protected-access
        bt._validate()  # pylint: disable=protected-access
        return bt

    def __str__(self) -> str:
        segments = [
            f"NBR: {self.nom_bitrate:_} bit/s",
            f"NSP: {self.nom_sample_point:.2f}%",
            f"NBRP: {self.nom_brp}",
            f"NTSEG1: {self.nom_tseg1}",
            f"NTSEG2: {self.nom_tseg2}",
            f"NSJW: {self.nom_sjw}",
            f"DBR: {self.data_bitrate:_} bit/s",
            f"DSP: {self.data_sample_point:.2f}%",
            f"DBRP: {self.data_brp}",
            f"DTSEG1: {self.data_tseg1}",
            f"DTSEG2: {self.data_tseg2}",
            f"DSJW: {self.data_sjw}",
            f"CLK: {self.f_clock / 1e6:.0f}MHz",
        ]
        return ", ".join(segments)

    def __repr__(self) -> str:
        args = ", ".join(f"{key}={value}" for key, value in self.items())
        return f"can.{self.__class__.__name__}({args})"

    def __getitem__(self, key: str) -> int:
        return cast("int", self._data.__getitem__(key))

    def __len__(self) -> int:
        return self._data.__len__()

    def __iter__(self) -> Iterator[str]:
        return self._data.__iter__()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BitTimingFd):
            return False

        return self._data == other._data

    def __hash__(self) -> int:
        return tuple(self._data.values()).__hash__()


def _oscillator_tolerance_condition_1(nom_sjw: int, nbt: int) -> float:
    """Arbitration phase - resynchronization"""
    return nom_sjw / (2 * 10 * nbt)


def _oscillator_tolerance_condition_2(
    nbt: int, nom_phase_seg1: int, nom_phase_seg2: int
) -> float:
    """Arbitration phase - sampling of bit after error flag"""
    return min(nom_phase_seg1, nom_phase_seg2) / (2 * (13 * nbt - nom_phase_seg2))


def _oscillator_tolerance_condition_3(data_sjw: int, dbt: int) -> float:
    """Data phase - resynchronization"""
    return data_sjw / (2 * 10 * dbt)


def _oscillator_tolerance_condition_4(
    nom_phase_seg1: int,
    nom_phase_seg2: int,
    data_phase_seg2: int,
    nbt: int,
    dbt: int,
    data_brp: int,
    nom_brp: int,
) -> float:
    """Data phase - sampling of bit after error flag"""
    return min(nom_phase_seg1, nom_phase_seg2) / (
        2 * ((6 * dbt - data_phase_seg2) * data_brp / nom_brp + 7 * nbt)
    )


def _oscillator_tolerance_condition_5(
    data_sjw: int,
    data_brp: int,
    nom_brp: int,
    nom_phase_seg2: int,
    data_phase_seg2: int,
    nbt: int,
    dbt: int,
) -> float:
    """Data phase - bit rate switch"""
    max_correctable_phase_shift = data_sjw - max(0.0, nom_brp / data_brp - 1)
    time_between_resync = 2 * (
        (2 * nbt - nom_phase_seg2) * nom_brp / data_brp + data_phase_seg2 + 4 * dbt
    )
    return max_correctable_phase_shift / time_between_resync
