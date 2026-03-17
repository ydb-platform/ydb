from udsoncan.common.DidCodec import DidCodec
from typing import Dict, Optional, Any, Callable, Union, Type
import sys

if sys.version_info < (3, 8):
    class TypedDict(dict):
        def __init_subclass__(cls, *args, **kwargs):
            pass
else:
    from typing import TypedDict

SecurityAlgoType = Callable[[int, bytes, Any], bytes]
Nrc78CallbackType = Callable[[], None]


CodecDefinition = Union[str, DidCodec, Type[DidCodec]]
DIDConfig = Dict[Union[int, str], CodecDefinition]


class IOConfigEntry(TypedDict, total=False):
    codec: CodecDefinition
    mask: Dict[str, int]
    mask_size: int


IOConfig = Dict[Union[int, str], Union[IOConfigEntry, CodecDefinition]]


class ClientConfig(TypedDict, total=False):
    exception_on_negative_response: bool
    exception_on_invalid_response: bool
    exception_on_unexpected_response: bool
    security_algo: Optional[SecurityAlgoType]
    security_algo_params: Optional[Any]
    tolerate_zero_padding: bool
    ignore_all_zero_dtc: bool
    dtc_snapshot_did_size: int
    server_address_format: Optional[int]
    server_memorysize_format: Optional[int]
    data_identifiers: DIDConfig
    input_output: IOConfig
    request_timeout: float
    p2_timeout: float
    p2_star_timeout: float
    standard_version: int
    use_server_timing: bool
    logger_name: str
    extended_data_size: Optional[Union[int, Dict[int, int]]]
    nrc78_callback:Optional[Nrc78CallbackType]
