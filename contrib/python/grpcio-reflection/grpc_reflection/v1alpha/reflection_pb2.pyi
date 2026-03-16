from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ServerReflectionRequest(_message.Message):
    __slots__ = ("host", "file_by_filename", "file_containing_symbol", "file_containing_extension", "all_extension_numbers_of_type", "list_services")
    HOST_FIELD_NUMBER: _ClassVar[int]
    FILE_BY_FILENAME_FIELD_NUMBER: _ClassVar[int]
    FILE_CONTAINING_SYMBOL_FIELD_NUMBER: _ClassVar[int]
    FILE_CONTAINING_EXTENSION_FIELD_NUMBER: _ClassVar[int]
    ALL_EXTENSION_NUMBERS_OF_TYPE_FIELD_NUMBER: _ClassVar[int]
    LIST_SERVICES_FIELD_NUMBER: _ClassVar[int]
    host: str
    file_by_filename: str
    file_containing_symbol: str
    file_containing_extension: ExtensionRequest
    all_extension_numbers_of_type: str
    list_services: str
    def __init__(self, host: _Optional[str] = ..., file_by_filename: _Optional[str] = ..., file_containing_symbol: _Optional[str] = ..., file_containing_extension: _Optional[_Union[ExtensionRequest, _Mapping]] = ..., all_extension_numbers_of_type: _Optional[str] = ..., list_services: _Optional[str] = ...) -> None: ...

class ExtensionRequest(_message.Message):
    __slots__ = ("containing_type", "extension_number")
    CONTAINING_TYPE_FIELD_NUMBER: _ClassVar[int]
    EXTENSION_NUMBER_FIELD_NUMBER: _ClassVar[int]
    containing_type: str
    extension_number: int
    def __init__(self, containing_type: _Optional[str] = ..., extension_number: _Optional[int] = ...) -> None: ...

class ServerReflectionResponse(_message.Message):
    __slots__ = ("valid_host", "original_request", "file_descriptor_response", "all_extension_numbers_response", "list_services_response", "error_response")
    VALID_HOST_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_REQUEST_FIELD_NUMBER: _ClassVar[int]
    FILE_DESCRIPTOR_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    ALL_EXTENSION_NUMBERS_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    LIST_SERVICES_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    ERROR_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    valid_host: str
    original_request: ServerReflectionRequest
    file_descriptor_response: FileDescriptorResponse
    all_extension_numbers_response: ExtensionNumberResponse
    list_services_response: ListServiceResponse
    error_response: ErrorResponse
    def __init__(self, valid_host: _Optional[str] = ..., original_request: _Optional[_Union[ServerReflectionRequest, _Mapping]] = ..., file_descriptor_response: _Optional[_Union[FileDescriptorResponse, _Mapping]] = ..., all_extension_numbers_response: _Optional[_Union[ExtensionNumberResponse, _Mapping]] = ..., list_services_response: _Optional[_Union[ListServiceResponse, _Mapping]] = ..., error_response: _Optional[_Union[ErrorResponse, _Mapping]] = ...) -> None: ...

class FileDescriptorResponse(_message.Message):
    __slots__ = ("file_descriptor_proto",)
    FILE_DESCRIPTOR_PROTO_FIELD_NUMBER: _ClassVar[int]
    file_descriptor_proto: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, file_descriptor_proto: _Optional[_Iterable[bytes]] = ...) -> None: ...

class ExtensionNumberResponse(_message.Message):
    __slots__ = ("base_type_name", "extension_number")
    BASE_TYPE_NAME_FIELD_NUMBER: _ClassVar[int]
    EXTENSION_NUMBER_FIELD_NUMBER: _ClassVar[int]
    base_type_name: str
    extension_number: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, base_type_name: _Optional[str] = ..., extension_number: _Optional[_Iterable[int]] = ...) -> None: ...

class ListServiceResponse(_message.Message):
    __slots__ = ("service",)
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    service: _containers.RepeatedCompositeFieldContainer[ServiceResponse]
    def __init__(self, service: _Optional[_Iterable[_Union[ServiceResponse, _Mapping]]] = ...) -> None: ...

class ServiceResponse(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ErrorResponse(_message.Message):
    __slots__ = ("error_code", "error_message")
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    error_code: int
    error_message: str
    def __init__(self, error_code: _Optional[int] = ..., error_message: _Optional[str] = ...) -> None: ...
