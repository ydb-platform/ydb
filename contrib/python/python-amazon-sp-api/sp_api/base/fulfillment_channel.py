from enum import Enum


class FulfillmentChannel(str, Enum):
    AFN = 'AFN'
    MFN = 'MFN'
