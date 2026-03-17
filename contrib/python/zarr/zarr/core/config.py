"""
The config module is responsible for managing the configuration of zarr and is based on the Donfig python library.
For selecting custom implementations of codecs, pipelines, buffers and ndbuffers, first register the implementations
in the registry and then select them in the config.

Example:
    An implementation of the bytes codec in a class ``your.module.NewBytesCodec`` requires the value of ``codecs.bytes``
    to be ``your.module.NewBytesCodec``. Donfig can be configured programmatically, by environment variables, or from
    YAML files in standard locations.

    ```python
    from your.module import NewBytesCodec
    from zarr.core.config import register_codec, config

    register_codec("bytes", NewBytesCodec)
    config.set({"codecs.bytes": "your.module.NewBytesCodec"})
    ```

    Instead of setting the value programmatically with ``config.set``, you can also set the value with an environment
    variable. The environment variable ``ZARR_CODECS__BYTES`` can be set to ``your.module.NewBytesCodec``. The double
    underscore ``__`` is used to indicate nested access.

    ```bash
    export ZARR_CODECS__BYTES="your.module.NewBytesCodec"
    ```

For more information, see the Donfig documentation at https://github.com/pytroll/donfig.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

from donfig import Config as DConfig

if TYPE_CHECKING:
    from donfig.config_obj import ConfigSet


class BadConfigError(ValueError):
    _msg = "bad Config: %r"


class Config(DConfig):  # type: ignore[misc]
    """The Config will collect configuration from config files and environment variables

    Example environment variables:
    Grabs environment variables of the form "ZARR_FOO__BAR_BAZ=123" and
    turns these into config variables of the form ``{"foo": {"bar-baz": 123}}``
    It transforms the key and value in the following way:

    -  Lower-cases the key text
    -  Treats ``__`` (double-underscore) as nested access
    -  Calls ``ast.literal_eval`` on the value

    """

    def reset(self) -> None:
        self.clear()
        self.refresh()

    def enable_gpu(self) -> ConfigSet:
        """
        Configure Zarr to use GPUs where possible.
        """
        return self.set(
            {"buffer": "zarr.buffer.gpu.Buffer", "ndbuffer": "zarr.buffer.gpu.NDBuffer"}
        )


# these keys were removed from the config as part of the 3.1.0 release.
# these deprecations should be removed in 3.1.1 or thereabouts.
deprecations = {
    "array.v2_default_compressor.numeric": None,
    "array.v2_default_compressor.string": None,
    "array.v2_default_compressor.bytes": None,
    "array.v2_default_filters.string": None,
    "array.v2_default_filters.bytes": None,
    "array.v3_default_filters.numeric": None,
    "array.v3_default_filters.raw": None,
    "array.v3_default_filters.bytes": None,
    "array.v3_default_serializer.numeric": None,
    "array.v3_default_serializer.string": None,
    "array.v3_default_serializer.bytes": None,
    "array.v3_default_compressors.string": None,
    "array.v3_default_compressors.bytes": None,
    "array.v3_default_compressors": None,
}

# The default configuration for zarr
config = Config(
    "zarr",
    defaults=[
        {
            "default_zarr_format": 3,
            "array": {
                "order": "C",
                "write_empty_chunks": False,
                "target_shard_size_bytes": None,
            },
            "async": {"concurrency": 10, "timeout": None},
            "threading": {"max_workers": None},
            "json_indent": 2,
            "codec_pipeline": {
                "path": "zarr.core.codec_pipeline.BatchedCodecPipeline",
                "batch_size": 1,
            },
            "codecs": {
                "blosc": "zarr.codecs.blosc.BloscCodec",
                "gzip": "zarr.codecs.gzip.GzipCodec",
                "zstd": "zarr.codecs.zstd.ZstdCodec",
                "bytes": "zarr.codecs.bytes.BytesCodec",
                "endian": "zarr.codecs.bytes.BytesCodec",  # compatibility with earlier versions of ZEP1
                "crc32c": "zarr.codecs.crc32c_.Crc32cCodec",
                "sharding_indexed": "zarr.codecs.sharding.ShardingCodec",
                "transpose": "zarr.codecs.transpose.TransposeCodec",
                "vlen-utf8": "zarr.codecs.vlen_utf8.VLenUTF8Codec",
                "vlen-bytes": "zarr.codecs.vlen_utf8.VLenBytesCodec",
                "numcodecs.bz2": "zarr.codecs.numcodecs.BZ2",
                "numcodecs.crc32": "zarr.codecs.numcodecs.CRC32",
                "numcodecs.crc32c": "zarr.codecs.numcodecs.CRC32C",
                "numcodecs.lz4": "zarr.codecs.numcodecs.LZ4",
                "numcodecs.lzma": "zarr.codecs.numcodecs.LZMA",
                "numcodecs.zfpy": "zarr.codecs.numcodecs.ZFPY",
                "numcodecs.adler32": "zarr.codecs.numcodecs.Adler32",
                "numcodecs.astype": "zarr.codecs.numcodecs.AsType",
                "numcodecs.bitround": "zarr.codecs.numcodecs.BitRound",
                "numcodecs.blosc": "zarr.codecs.numcodecs.Blosc",
                "numcodecs.delta": "zarr.codecs.numcodecs.Delta",
                "numcodecs.fixedscaleoffset": "zarr.codecs.numcodecs.FixedScaleOffset",
                "numcodecs.fletcher32": "zarr.codecs.numcodecs.Fletcher32",
                "numcodecs.gzip": "zarr.codecs.numcodecs.GZip",
                "numcodecs.jenkins_lookup3": "zarr.codecs.numcodecs.JenkinsLookup3",
                "numcodecs.pcodec": "zarr.codecs.numcodecs.PCodec",
                "numcodecs.packbits": "zarr.codecs.numcodecs.PackBits",
                "numcodecs.shuffle": "zarr.codecs.numcodecs.Shuffle",
                "numcodecs.quantize": "zarr.codecs.numcodecs.Quantize",
                "numcodecs.zlib": "zarr.codecs.numcodecs.Zlib",
                "numcodecs.zstd": "zarr.codecs.numcodecs.Zstd",
            },
            "buffer": "zarr.buffer.cpu.Buffer",
            "ndbuffer": "zarr.buffer.cpu.NDBuffer",
        }
    ],
    deprecations=deprecations,
)


def parse_indexing_order(data: Any) -> Literal["C", "F"]:
    if data in ("C", "F"):
        return cast("Literal['C', 'F']", data)
    msg = f"Expected one of ('C', 'F'), got {data} instead."
    raise ValueError(msg)
