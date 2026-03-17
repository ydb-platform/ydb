"""OpenAPI core unmarshalling schemas enums module"""
from enum import Enum


class UnmarshalContext(Enum):
    REQUEST = 'request'
    RESPONSE = 'response'
