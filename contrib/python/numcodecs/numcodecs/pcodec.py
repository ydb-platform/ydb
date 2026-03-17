from typing import Literal

from numcodecs.abc import Codec
from numcodecs.compat import ensure_bytes, ensure_contiguous_ndarray
from pcodec import ChunkConfig, DeltaSpec, ModeSpec, PagingSpec, standalone

DEFAULT_MAX_PAGE_N = 262144


class PCodec(Codec):
    """
    PCodec (or pco, pronounced "pico") losslessly compresses and decompresses
    numerical sequences with high compression ratio and fast speed.

    See `PCodec Repo <https://github.com/mwlon/pcodec>`_ for more information.

    PCodec supports only the following numerical dtypes: uint16, uint32, uint64,
    int16, int32, int64, float16, float32, and float64.

    Parameters
    ----------
    level : int
        A compression level from 0-12, where 12 take the longest and compresses
        the most.
    mode_spec : {"auto", "classic"}
        Configures whether Pcodec should try to infer the best "mode" or
        structure of the data (e.g. approximate multiples of 0.1) to improve
        compression ratio, or skip this step and just use the numbers as-is
        (Classic mode). Note that the "try*" specs are not currently supported.
    delta_spec : {"auto", "none", "try_consecutive", "try_lookback"}
        Configures the delta encoding strategy. By default, uses "auto" which
        will try to infer the best encoding order.
    paging_spec : {"equal_pages_up_to"}
        Configures the paging strategy. Only "equal_pages_up_to" is currently
        supported.
    delta_encoding_order : int or None
        Explicit delta encoding level from 0-7. Only valid if delta_spec is
        "try_consecutive" or "auto" (to support backwards compatibility with
        older versions of this codec).
    equal_pages_up_to : int
        Divide the chunk into equal pages of up to this many numbers.
    """

    codec_id = "pcodec"

    def __init__(
        self,
        level: int = 8,
        *,
        mode_spec: Literal["auto", "classic"] = "auto",
        delta_spec: Literal["auto", "none", "try_consecutive", "try_lookback"] = "auto",
        paging_spec: Literal["equal_pages_up_to"] = "equal_pages_up_to",
        delta_encoding_order: int | None = None,
        equal_pages_up_to: int = DEFAULT_MAX_PAGE_N,
    ):
        # note that we use `level` instead of `compression_level` to
        # match other codecs
        self.level = level
        self.mode_spec = mode_spec
        self.delta_spec = delta_spec
        self.paging_spec = paging_spec
        self.delta_encoding_order = delta_encoding_order
        self.equal_pages_up_to = equal_pages_up_to

    def _get_chunk_config(self):
        match self.mode_spec:
            case "auto":
                mode_spec = ModeSpec.auto()
            case "classic":
                mode_spec = ModeSpec.classic()
            case _:
                raise ValueError(f"mode_spec {self.mode_spec} is not supported")

        if self.delta_encoding_order is not None and self.delta_spec == "auto":
            # backwards compat for before delta_spec was introduced
            delta_spec = DeltaSpec.try_consecutive(self.delta_encoding_order)
        elif self.delta_encoding_order is not None and self.delta_spec != "try_consecutive":
            raise ValueError(
                "delta_encoding_order can only be set for delta_spec='try_consecutive'"
            )
        else:
            match self.delta_spec:
                case "auto":
                    delta_spec = DeltaSpec.auto()
                case "none":
                    delta_spec = DeltaSpec.none()
                case "try_consecutive":
                    delta_spec = DeltaSpec.try_consecutive(self.delta_encoding_order)
                case "try_lookback":
                    delta_spec = DeltaSpec.try_lookback()
                case _:
                    raise ValueError(f"delta_spec {self.delta_spec} is not supported")

        match self.paging_spec:
            case "equal_pages_up_to":
                paging_spec = PagingSpec.equal_pages_up_to(self.equal_pages_up_to)
            case _:
                raise ValueError(f"paging_spec {self.paging_spec} is not supported")

        return ChunkConfig(
            compression_level=self.level,
            delta_spec=delta_spec,
            mode_spec=mode_spec,
            paging_spec=paging_spec,
        )

    def encode(self, buf):
        buf = ensure_contiguous_ndarray(buf)
        config = self._get_chunk_config()
        return standalone.simple_compress(buf, config)

    def decode(self, buf, out=None):
        buf = ensure_bytes(buf)
        if out is not None:
            out = ensure_contiguous_ndarray(out)
            standalone.simple_decompress_into(buf, out)
            return out
        else:
            return standalone.simple_decompress(buf)
