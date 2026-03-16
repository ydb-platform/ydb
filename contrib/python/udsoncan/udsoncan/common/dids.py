__all__ = [
    'DataIdentifier',
    'CodecDefinition',
    'check_did_config',
    'fetch_codec_definition_from_config',
    'make_did_codec_from_definition'
]

import inspect
from copy import deepcopy
from udsoncan.exceptions import ConfigError
from udsoncan.typing import CodecDefinition, DIDConfig, IOConfigEntry
from udsoncan.common.DidCodec import DidCodec

from typing import Dict, List, Union, Optional, cast


class DataIdentifier:
    """
    Defines a list of constants that are data identifiers defined by the UDS standard.
    This class provides no functionality apart from defining these constants
    """
    BootSoftwareIdentification = 0xF180
    ApplicationSoftwareIdentification = 0xF181
    ApplicationDataIdentification = 0xF182
    BootSoftwareFingerprint = 0xF183
    ApplicationSoftwareFingerprint = 0xF184
    ApplicationDataFingerprint = 0xF185
    ActiveDiagnosticSession = 0xF186
    VehicleManufacturerSparePartNumber = 0xF187
    VehicleManufacturerECUSoftwareNumber = 0xF188
    VehicleManufacturerECUSoftwareVersionNumber = 0xF189
    SystemSupplierIdentifier = 0xF18A
    ECUManufacturingDate = 0xF18B
    ECUSerialNumber = 0xF18C
    SupportedFunctionalUnits = 0xF18D
    VehicleManufacturerKitAssemblyPartNumber = 0xF18E
    ISOSAEReservedStandardized = 0xF18F
    VIN = 0xF190
    VehicleManufacturerECUHardwareNumber = 0xF191
    SystemSupplierECUHardwareNumber = 0xF192
    SystemSupplierECUHardwareVersionNumber = 0xF193
    SystemSupplierECUSoftwareNumber = 0xF194
    SystemSupplierECUSoftwareVersionNumber = 0xF195
    ExhaustRegulationOrTypeApprovalNumber = 0xF196
    SystemNameOrEngineType = 0xF197
    RepairShopCodeOrTesterSerialNumber = 0xF198
    ProgrammingDate = 0xF199
    CalibrationRepairShopCodeOrCalibrationEquipmentSerialNumber = 0xF19A
    CalibrationDate = 0xF19B
    CalibrationEquipmentSoftwareNumber = 0xF19C
    ECUInstallationDate = 0xF19D
    ODXFile = 0xF19E
    Entity = 0xF19F

    @classmethod
    def name_from_id(cls, did: int) -> Optional[str]:
        # As defined by ISO-14229:2006, Annex F
        if not isinstance(did, int) or did < 0 or did > 0xFFFF:
            raise ValueError('Data IDentifier must be a valid integer between 0 and 0xFFFF')

        if did >= 0x0000 and did <= 0x00FF:
            return 'ISOSAEReserved'
        if did >= 0x0100 and did <= 0xEFFF:
            return 'VehicleManufacturerSpecific'
        if did >= 0xF000 and did <= 0xF00F:
            return 'NetworkConfigurationDataForTractorTrailerApplicationDataIdentifier'
        if did >= 0xF010 and did <= 0xF0FF:
            return 'VehicleManufacturerSpecific'
        if did >= 0xF100 and did <= 0xF17F:
            return 'IdentificationOptionVehicleManufacturerSpecificDataIdentifier'

        if did == 0xF180:
            return 'BootSoftwareIdentificationDataIdentifier'
        if did == 0xF181:
            return 'ApplicationSoftwareIdentificationDataIdentifier'
        if did == 0xF182:
            return 'ApplicationDataIdentificationDataIdentifier'
        if did == 0xF183:
            return 'BootSoftwareFingerprintDataIdentifier'
        if did == 0xF184:
            return 'ApplicationSoftwareFingerprintDataIdentifier'
        if did == 0xF185:
            return 'ApplicationDataFingerprintDataIdentifier'
        if did == 0xF186:
            return 'ActiveDiagnosticSessionDataIdentifier'
        if did == 0xF187:
            return 'VehicleManufacturerSparePartNumberDataIdentifier'
        if did == 0xF188:
            return 'VehicleManufacturerECUSoftwareNumberDataIdentifier'
        if did == 0xF189:
            return 'VehicleManufacturerECUSoftwareVersionNumberDataIdentifier'
        if did == 0xF18A:
            return 'SystemSupplierIdentifierDataIdentifier'
        if did == 0xF18B:
            return 'ECUManufacturingDateDataIdentifier'
        if did == 0xF18C:
            return 'ECUSerialNumberDataIdentifier'
        if did == 0xF18D:
            return 'SupportedFunctionalUnitsDataIdentifier'
        if did == 0xF18E:
            return 'VehicleManufacturerKitAssemblyPartNumberDataIdentifier'
        if did == 0xF18F:
            return 'ISOSAEReservedStandardized'
        if did == 0xF190:
            return 'VINDataIdentifier'
        if did == 0xF191:
            return 'VehicleManufacturerECUHardwareNumberDataIdentifier'
        if did == 0xF192:
            return 'SystemSupplierECUHardwareNumberDataIdentifier'
        if did == 0xF193:
            return 'SystemSupplierECUHardwareVersionNumberDataIdentifier'
        if did == 0xF194:
            return 'SystemSupplierECUSoftwareNumberDataIdentifier'
        if did == 0xF195:
            return 'SystemSupplierECUSoftwareVersionNumberDataIdentifier'
        if did == 0xF196:
            return 'ExhaustRegulationOrTypeApprovalNumberDataIdentifier'
        if did == 0xF197:
            return 'SystemNameOrEngineTypeDataIdentifier'
        if did == 0xF198:
            return 'RepairShopCodeOrTesterSerialNumberDataIdentifier'
        if did == 0xF199:
            return 'ProgrammingDateDataIdentifier'
        if did == 0xF19A:
            return 'CalibrationRepairShopCodeOrCalibrationEquipmentSerialNumberDataIdentifier'
        if did == 0xF19B:
            return 'CalibrationDateDataIdentifier'
        if did == 0xF19C:
            return 'CalibrationEquipmentSoftwareNumberDataIdentifier'
        if did == 0xF19D:
            return 'ECUInstallationDateDataIdentifier'
        if did == 0xF19E:
            return 'ODXFileDataIdentifier'
        if did == 0xF19F:
            return 'EntityDataIdentifier'

        if did >= 0xF1A0 and did <= 0xF1EF:
            return 'IdentificationOptionVehicleManufacturerSpecific'
        if did >= 0xF1F0 and did <= 0xF1FF:
            return 'IdentificationOptionSystemSupplierSpecific'
        if did >= 0xF200 and did <= 0xF2FF:
            return 'PeriodicDataIdentifier'
        if did >= 0xF300 and did <= 0xF3FF:
            return 'DynamicallyDefinedDataIdentifier'
        if did >= 0xF400 and did <= 0xF4FF:
            return 'OBDDataIdentifier'
        if did >= 0xF500 and did <= 0xF5FF:
            return 'OBDDataIdentifier'
        if did >= 0xF600 and did <= 0xF6FF:
            return 'OBDMonitorDataIdentifier'
        if did >= 0xF700 and did <= 0xF7FF:
            return 'OBDMonitorDataIdentifier'
        if did >= 0xF800 and did <= 0xF8FF:
            return 'OBDInfoTypeDataIdentifier'
        if did >= 0xF900 and did <= 0xF9FF:
            return 'TachographDataIdentifier'
        if did >= 0xFA00 and did <= 0xFA0F:
            return 'AirbagDeploymentDataIdentifier'
        if did >= 0xFA10 and did <= 0xFAFF:
            return 'SafetySystemDataIdentifier'
        if did >= 0xFB00 and did <= 0xFCFF:
            return 'ReservedForLegislativeUse'
        if did >= 0xFD00 and did <= 0xFEFF:
            return 'SystemSupplierSpecific'
        if did >= 0xFF00 and did <= 0xFFFF:
            return 'ISOSAEReserved'

        return None


def check_did_config(didlist: Union[int, List[int]], didconfig: Optional[Dict]) -> DIDConfig:
    """Make sure that the actual client configuration contains valid definitions for given Data Identifiers"""
    if didconfig is None:
        raise ConfigError("didconfig is not set")
    didlist = [didlist] if not isinstance(didlist, list) else didlist

    if 'data_identifiers' in didconfig:
        didconfig = didconfig['data_identifiers']

    assert didconfig is not None
    for did in didlist:
        if did not in didconfig:
            if 'default' not in didconfig:
                raise ConfigError(did, msg='Actual data identifier configuration contains no definition for data identifier 0x%04x' % did)

    return cast(DIDConfig, didconfig)


def fetch_codec_definition_from_config(did: int, didconfig: DIDConfig) -> CodecDefinition:
    """Fetch a DID codec definition from a user configuration supporting default codec for unknown DID"""
    if did not in didconfig:
        if 'default' in didconfig:
            return didconfig['default']
        else:
            raise ConfigError(did, msg='Actual data identifier configuration contains no definition for data identifier 0x%04x' % did)
    return didconfig[did]


def make_did_codec_from_definition(didconfig: Union[CodecDefinition, IOConfigEntry]) -> DidCodec:
    if isinstance(didconfig, DidCodec):  # the given object already is a DidCodec instance
        return didconfig

    # The definition of the codec is a class. Returns an instance of this codec.
    if inspect.isclass(didconfig) and issubclass(didconfig, DidCodec):
        return didconfig()

    # It could be that the codec is in a dict. (for io_control)
    if isinstance(didconfig, dict) and 'codec' in didconfig:
        return make_did_codec_from_definition(didconfig['codec'])

    # The codec can be defined by a struct pack/unpack string
    if isinstance(didconfig, str):
        if len(didconfig) == 0:
            raise ValueError("pack/unpack string given for Codec config should not be empty.")
        return DidCodec(packstr=didconfig)

    raise ValueError('Given codec of type %s is not a valid DidCodec' % (type(didconfig)))
