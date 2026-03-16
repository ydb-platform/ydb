import struct
from udsoncan import Dtc, check_did_config, make_did_codec_from_definition, fetch_codec_definition_from_config, latest_standard, valid_standards, DIDConfig
from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.exceptions import *
from udsoncan.BaseService import BaseService, BaseSubfunction, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
import udsoncan.tools as tools

from typing import Dict, Union, Optional, cast, List


class ReadDTCInformation(BaseService):
    _sid = 0x19

    supported_negative_response = [ResponseCode.SubFunctionNotSupported,
                                   ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.RequestOutOfRange
                                   ]

    class ResponseData(BaseResponseData):
        """
        .. data:: subfunction_echo

                Subfunction echoed back by the server

        .. data:: dtcs

                :ref:`DTC<DTC>` instances and their status read from the server.

        .. data:: dtc_count

                Number of DTC read or available

        .. data:: dtc_format

                Integer indicating the format of the DTC. See :ref:`DTC.Format<DTC_Format>`

        .. data:: status_availability

                :ref:`Dtc.Status<DTC_Status>` indicating which status the server supports

        .. data:: extended_data

                List of bytes containing the DTC extended data

        .. data:: memory_selection_echo

                Echo of the memory selection byte                               

        """

        subfunction_echo: int
        dtcs: List[Dtc]
        dtc_count: Optional[int]
        dtc_format: Optional[int]
        status_availability: Optional[Dtc.Status]
        extended_data: Optional[List[bytes]]
        memory_selection_echo: Optional[int]
        functional_group_id:Optional[int]
        severity_availability: Optional[Dtc.Severity]

        def __init__(self, subfunction_echo: int,
                     dtcs: Optional[List[Dtc]] = None,
                     dtc_count: Optional[int] = None,
                     dtc_format: Optional[int] = None,
                     status_availability: Optional[Dtc.Status] = None,
                     extended_data: Optional[List[bytes]] = [],
                     memory_selection_echo: Optional[int] = None,
                     functional_group_id:Optional[int] = None,
                     severity_availability: Optional[Dtc.Severity] = None
                     ):
            super().__init__(ReadDTCInformation)
            self.subfunction_echo = subfunction_echo
            self.dtcs = dtcs if dtcs is not None else []
            self.dtc_count = dtc_count
            self.dtc_format = dtc_format
            self.status_availability = status_availability
            self.extended_data = extended_data if extended_data is not None else []
            self.memory_selection_echo = memory_selection_echo
            self.functional_group_id = functional_group_id
            self.severity_availability=severity_availability

    class InterpretedResponse(Response):
        service_data: "ReadDTCInformation.ResponseData"

    class Subfunction(BaseSubfunction):
        __pretty_name__ = 'subfunction'

        reportNumberOfDTCByStatusMask = 1
        reportDTCByStatusMask = 2
        reportDTCSnapshotIdentification = 3
        reportDTCSnapshotRecordByDTCNumber = 4
        reportDTCSnapshotRecordByRecordNumber = 5
        reportDTCExtendedDataRecordByDTCNumber = 6
        reportNumberOfDTCBySeverityMaskRecord = 7
        reportDTCBySeverityMaskRecord = 8
        reportSeverityInformationOfDTC = 9
        reportSupportedDTCs = 0xA
        reportFirstTestFailedDTC = 0xB
        reportFirstConfirmedDTC = 0xC
        reportMostRecentTestFailedDTC = 0xD
        reportMostRecentConfirmedDTC = 0xE
        reportMirrorMemoryDTCByStatusMask = 0xF
        reportMirrorMemoryDTCExtendedDataRecordByDTCNumber = 0x10
        reportNumberOfMirrorMemoryDTCByStatusMask = 0x11
        reportNumberOfEmissionsRelatedOBDDTCByStatusMask = 0x12
        reportEmissionsRelatedOBDDTCByStatusMask = 0x13
        reportDTCFaultDetectionCounter = 0x14
        reportDTCWithPermanentStatus = 0x15
        reportDTCExtDataRecordByRecordNumber = 0x16
        reportUserDefMemoryDTCByStatusMask = 0x17
        reportUserDefMemoryDTCSnapshotRecordByDTCNumber = 0x18
        reportUserDefMemoryDTCExtDataRecordByDTCNumber = 0x19
        reportWWHOBDDTCByMaskRecord = 0x42
        reportWWHOBDDTCWithPermanentStatus = 0x55

        # todo
        reportSupportedDTCExtDataRecord = 0x1A
        reportDTCInformationByDTCReadinessGroupIdentifier = 0x56

    @classmethod
    def assert_severity_mask(cls, severity_mask: Optional[int], subfunction: int) -> None:
        if severity_mask is None:
            raise ValueError('severity_mask must be provided for subfunction 0x%02x' % subfunction)
        tools.validate_int(severity_mask, min=0, max=0xFF, name='Severity mask')

    @classmethod
    def assert_status_mask(cls, status_mask: Optional[int], subfunction: int) -> None:
        if status_mask is None:
            raise ValueError('status_mask must be provided for subfunction 0x%02x' % subfunction)
        tools.validate_int(status_mask, min=0, max=0xFF, name='Status mask')

    @classmethod
    def assert_memory_selection(cls, memory_selection: Optional[int], subfunction: int) -> None:
        if memory_selection is None:
            raise ValueError('memory_selection must be provided for subfunction 0x%02x' % subfunction)
        tools.validate_int(memory_selection, min=0, max=0xFF, name='Memory Selection')

    @classmethod
    def assert_dtc(cls, dtc: Optional[int], subfunction: int) -> None:
        if dtc is None:
            raise ValueError('A dtc value must be provided for subfunction 0x%02x' % subfunction)
        tools.validate_int(dtc, min=0, max=0xFFFFFF, name='DTC')

    @classmethod
    def assert_snapshot_record_number(cls, snapshot_record_number: Optional[int], subfunction: int) -> None:
        if snapshot_record_number is None:
            raise ValueError('snapshot_record_number must be provided for subfunction 0x%02x' % subfunction)
        tools.validate_int(snapshot_record_number, min=0, max=0xFF, name='Snapshot record number')

    @classmethod
    def assert_extended_data_record_number(cls, extended_data_record_number: Optional[int], subfunction: int, maxval: int = 0xFF) -> None:
        if extended_data_record_number is None:
            raise ValueError('extended_data_record_number must be provided for subfunction 0x%02x' % subfunction)
        tools.validate_int(extended_data_record_number, min=0, max=maxval, name='Extended data record number')

    @classmethod
    def assert_extended_data_size_int_or_dict(cls,
                                              extended_data_size: Optional[Union[int, Dict[int, int]]],
                                              subfunction: int) -> None:
        if extended_data_size is None:
            raise ValueError('extended_data_size must be provided as length of data is not given by the server for subfunction %d.' % subfunction)
        if isinstance(extended_data_size, int):
            tools.validate_int(extended_data_size, min=0, max=0xFFF, name='Extended data size')
        if isinstance(extended_data_size, dict):
            for dtcid in extended_data_size:
                tools.validate_int(extended_data_size[dtcid], min=0, max=0xFFF, name='Extended data size for DTC=0x%06x' % dtcid)

    @classmethod
    def assert_functional_group_id(cls, functional_group_id: Optional[int], subfunction:int) -> None:
        if functional_group_id is None:
            raise ValueError('functional_group_id must be provided for subfunction 0x%02x' % subfunction)
        # ISO-14229:2020 Disallow a value of 0xFF. But ISO-27145-3 Defines it as "All functional system groups".
        # Allowing 0xFF because of the ambiguity
        tools.validate_int(functional_group_id, min=0, max=0xFF, name='Functional Group ID')    

    @classmethod
    def pack_dtc(cls, dtcid: int) -> bytes:
        return struct.pack('BBB', (dtcid >> 16) & 0xFF, (dtcid >> 8) & 0xFF, (dtcid >> 0) & 0xFF)

    @classmethod
    def check_subfunction_valid(cls, subfunction: int, standard_version: int = latest_standard) -> None:
        tools.validate_int(subfunction, min=1, max=0xFF, name='Subfunction')
        if standard_version not in valid_standards:
            raise ValueError(f"Standard version {standard_version} is not valid")

        vlist = vars(cls)
        ok = True
        for v in vlist:
            if isinstance(v, int) and v == subfunction:
                ok = False
                break
        if not ok:
            raise ValueError('Unknown subfunction : 0x%02x', subfunction)

        # These subfunction have been added in the 2020 version of the standard
        subfunction_disallow_map:Dict[int, List[int]] = {
            2006: [
                cls.Subfunction.reportDTCExtDataRecordByRecordNumber,
                cls.Subfunction.reportUserDefMemoryDTCByStatusMask,
                cls.Subfunction.reportUserDefMemoryDTCSnapshotRecordByDTCNumber,
                cls.Subfunction.reportUserDefMemoryDTCExtDataRecordByDTCNumber,
                #cls.Subfunction.reportDTCExtendedDataRecordIdentification, # Not implemented
                cls.Subfunction.reportWWHOBDDTCByMaskRecord,
                cls.Subfunction.reportWWHOBDDTCWithPermanentStatus,
                cls.Subfunction.reportDTCInformationByDTCReadinessGroupIdentifier,
            ],
            2013: [
                #cls.Subfunction.reportDTCExtendedDataRecordIdentification, # Not implemented
                cls.Subfunction.reportDTCInformationByDTCReadinessGroupIdentifier,
            ],
            2020: [
                cls.Subfunction.reportMirrorMemoryDTCByStatusMask,
                #cls.Subfunction.reportMirrorMemoryDTCExtDataRecordByDTCNumber, # Not implemented
                cls.Subfunction.reportNumberOfMirrorMemoryDTCByStatusMask,
                #cls.Subfunction.reportNumberOfEmissionsOBDDTCByStatusMask, # Not implemented
                #cls.Subfunction.reportEmissionsOBDDTCByStatusMask, # Not implemented
            ]
        }

        if standard_version in subfunction_disallow_map:
            if subfunction in subfunction_disallow_map[standard_version]:
                raise NotImplementedError(f"Subfunction 0x{subfunction:02x} is not allowed by ISO-14229:{standard_version}. Check for a different version of the standard")

    @classmethod
    def make_request(cls,
                     subfunction: int,
                     status_mask: Optional[Union[int, Dtc.Status]] = None,
                     severity_mask: Optional[Union[int, Dtc.Severity]] = None,
                     dtc_class: Optional[Union[int, Dtc.DtcClass]] = None,
                     dtc: Optional[Union[int, Dtc]] = None,
                     snapshot_record_number: Optional[int] = None,
                     extended_data_record_number: Optional[int] = None,
                     memory_selection: Optional[int] = None,
                     standard_version: int = latest_standard,
                     functional_group_id:Optional[int] = None,
                     ) -> Request:
        """
        Generates a request for ReadDTCInformation. 
        Each subfunction uses a subset of parameters. 

        :param subfunction: The service subfunction. Values are defined in :class:`ReadDTCInformation.Subfunction<ReadDTCInformation.Subfunction>`
        :type subfunction: int

        :param status_mask: A DTC status mask used to filter DTC
        :type status_mask: int or :ref:`Dtc.Status <DTC_Status>`

        :param severity_mask: A severity mask used to filter DTC 
        :type severity_mask: int or :ref:`Dtc.Severity <DTC_Severity>`

        :param dtc_class: A DTC class to be packed in the same byte as the severity mask
        :type dtc_class: int or :ref:`Dtc.DtcClass <DTC_DtcClass>`

        :param dtc: A DTC mask used to filter DTC
        :type dtc: int or :ref:`Dtc <DTC>`

        :param snapshot_record_number: Snapshot record number
        :type snapshot_record_number: int

        :param extended_data_record_number: Extended data record number
        :type extended_data_record_number: int

        :param memory_selection: Memory selection for user defined memory DTC
        :type memory_selection: int


        :raises ValueError: If parameters are out of range, missing or wrong type
        :raises NotImplementedError: If the requested subfunction is not supported by the active ISO-14229 standard version

        """

        # Request grouping for subfunctions that have the same request format
        request_subfn_no_param = [
            ReadDTCInformation.Subfunction.reportSupportedDTCs,
            ReadDTCInformation.Subfunction.reportFirstTestFailedDTC,
            ReadDTCInformation.Subfunction.reportFirstConfirmedDTC,
            ReadDTCInformation.Subfunction.reportMostRecentTestFailedDTC,
            ReadDTCInformation.Subfunction.reportMostRecentConfirmedDTC,
            ReadDTCInformation.Subfunction.reportDTCFaultDetectionCounter,
            ReadDTCInformation.Subfunction.reportDTCWithPermanentStatus,

            # Documentation is confusing about reportDTCSnapshotIdentification subfunction.
            # It is presented with reportDTCSnapshotRecordByDTCNumber (2 params) but a footnote says that these 2 parameters
            # are not to be provided for reportDTCSnapshotIdentification. Therefore, it is the same as other no-params subfn
            ReadDTCInformation.Subfunction.reportDTCSnapshotIdentification

        ]

        request_subfn_status_mask = [
            ReadDTCInformation.Subfunction.reportNumberOfDTCByStatusMask,
            ReadDTCInformation.Subfunction.reportDTCByStatusMask,
            ReadDTCInformation.Subfunction.reportMirrorMemoryDTCByStatusMask,
            ReadDTCInformation.Subfunction.reportNumberOfMirrorMemoryDTCByStatusMask,
            ReadDTCInformation.Subfunction.reportNumberOfEmissionsRelatedOBDDTCByStatusMask,
            ReadDTCInformation.Subfunction.reportEmissionsRelatedOBDDTCByStatusMask
        ]

        request_subfn_mask_record_plus_snapshot_record_number = [
            ReadDTCInformation.Subfunction.reportDTCSnapshotRecordByDTCNumber
        ]

        request_subfn_mask_record_plus_snapshot_record_number_plus_memory_selection = [
            ReadDTCInformation.Subfunction.reportUserDefMemoryDTCSnapshotRecordByDTCNumber
        ]

        request_subfn_snapshot_record_number = [
            ReadDTCInformation.Subfunction.reportDTCSnapshotRecordByRecordNumber
        ]

        request_subfn_mask_record_plus_extdata_record_number = [
            ReadDTCInformation.Subfunction.reportDTCExtendedDataRecordByDTCNumber,
            ReadDTCInformation.Subfunction.reportMirrorMemoryDTCExtendedDataRecordByDTCNumber
        ]

        request_subfn_mask_record_plus_extdata_record_number_plus_memory_selection = [
            ReadDTCInformation.Subfunction.reportUserDefMemoryDTCExtDataRecordByDTCNumber,
        ]

        request_subfn_severity_plus_status_mask = [
            ReadDTCInformation.Subfunction.reportNumberOfDTCBySeverityMaskRecord,
            ReadDTCInformation.Subfunction.reportDTCBySeverityMaskRecord
        ]

        request_subfn_mask_record = [
            ReadDTCInformation.Subfunction.reportSeverityInformationOfDTC
        ]

        request_subfn_status_mask_plus_memory_selection = [
            ReadDTCInformation.Subfunction.reportUserDefMemoryDTCByStatusMask
        ]

        cls.check_subfunction_valid(subfunction, standard_version)

        if status_mask is not None and isinstance(status_mask, Dtc.Status):
            status_mask = status_mask.get_byte_as_int()

        if severity_mask is not None and isinstance(severity_mask, Dtc.Severity):
            severity_mask = severity_mask.get_byte_as_int()
            severity_mask = severity_mask & 0xE0

        if dtc_class is not None :
            if severity_mask is None:
                raise ValueError("A severity mask must be specified in order to specify a dataclass")
            
            if isinstance(dtc_class, Dtc.DtcClass):
                dtc_class = dtc_class.get_byte_as_int()
            severity_mask |= (dtc_class & 0x1F)

        if dtc is not None and isinstance(dtc, Dtc):
            dtc = dtc.id

        req = Request(service=cls, subfunction=subfunction)

        if subfunction in request_subfn_no_param:		# Service ID + Subfunction
            pass

        elif subfunction in request_subfn_status_mask:
            cls.assert_status_mask(status_mask, subfunction)
            req.data = struct.pack('B', status_mask)

        elif subfunction in request_subfn_mask_record_plus_snapshot_record_number:
            cls.assert_dtc(dtc, subfunction)
            assert dtc is not None  # mypy nitpick
            cls.assert_snapshot_record_number(snapshot_record_number, subfunction)
            req.data = cls.pack_dtc(dtc) + struct.pack('B', snapshot_record_number)

        elif subfunction in request_subfn_mask_record_plus_snapshot_record_number_plus_memory_selection:
            cls.assert_dtc(dtc, subfunction)
            cls.assert_snapshot_record_number(snapshot_record_number, subfunction)
            cls.assert_memory_selection(memory_selection, subfunction)
            assert dtc is not None  # mypy nitpick
            req.data = cls.pack_dtc(dtc) + struct.pack('BB', snapshot_record_number, memory_selection)

        elif subfunction in request_subfn_snapshot_record_number:
            cls.assert_snapshot_record_number(snapshot_record_number, subfunction)
            req.data = struct.pack('B', snapshot_record_number)

        elif subfunction in request_subfn_mask_record_plus_extdata_record_number:
            cls.assert_dtc(dtc, subfunction)
            assert dtc is not None  # mypy nitpick
            cls.assert_extended_data_record_number(extended_data_record_number, subfunction)
            req.data = cls.pack_dtc(dtc) + struct.pack('B', extended_data_record_number)

        elif subfunction in request_subfn_mask_record_plus_extdata_record_number_plus_memory_selection:
            cls.assert_dtc(dtc, subfunction)
            assert dtc is not None  # mypy nitpick
            cls.assert_memory_selection(memory_selection, subfunction)
            cls.assert_extended_data_record_number(extended_data_record_number, subfunction)
            req.data = cls.pack_dtc(dtc) + struct.pack('BB', extended_data_record_number, memory_selection)

        elif subfunction in request_subfn_severity_plus_status_mask:
            cls.assert_status_mask(status_mask, subfunction)
            cls.assert_severity_mask(severity_mask, subfunction)
            req.data = struct.pack('BB', severity_mask, status_mask)

        elif subfunction in request_subfn_mask_record:
            cls.assert_dtc(dtc, subfunction)
            assert dtc is not None
            req.data = cls.pack_dtc(dtc)

        elif subfunction in request_subfn_status_mask_plus_memory_selection:
            cls.assert_memory_selection(memory_selection, subfunction)
            cls.assert_status_mask(status_mask, subfunction)
            req.data = struct.pack('BB', status_mask, memory_selection)

        elif subfunction == ReadDTCInformation.Subfunction.reportDTCExtDataRecordByRecordNumber:
            cls.assert_extended_data_record_number(extended_data_record_number, subfunction, maxval=0xEF)  # Maximum specified by ISO-14229:2020
            req.data = struct.pack('B', extended_data_record_number)

        elif subfunction == ReadDTCInformation.Subfunction.reportWWHOBDDTCByMaskRecord:
            cls.assert_status_mask(status_mask, subfunction)
            cls.assert_severity_mask(severity_mask, subfunction)
            cls.assert_functional_group_id(functional_group_id, subfunction)
            req.data = struct.pack('BBB', functional_group_id, status_mask, severity_mask)
       
        elif subfunction == ReadDTCInformation.Subfunction.reportWWHOBDDTCWithPermanentStatus:
            cls.assert_functional_group_id(functional_group_id, subfunction)
            req.data = struct.pack('B', functional_group_id)

        return req

    @classmethod
    def interpret_response(cls,
                           response: Response,
                           subfunction: int,
                           extended_data_size: Optional[Union[int, Dict[int, int]]] = None,
                           tolerate_zero_padding: bool = True,
                           ignore_all_zero_dtc: bool = True,
                           dtc_snapshot_did_size: int = 2,
                           didconfig: Optional[DIDConfig] = None,
                           standard_version: int = latest_standard) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`ReadDTCInformation.ResponseData<udsoncan.services.ReadDTCInformation.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :param subfunction: The service subfunction. Values are defined in :class:`ReadDTCInformation.Subfunction<udsoncan.services.ReadDTCInformation.Subfunction>`
        :type subfunction: int

        :param extended_data_size: Extended data size to expect. Extended data is implementation specific, therefore, size is not standardized
        :type extended_data_size: int or dict[int, int] to specify a size per extended data ID

        :param tolerate_zero_padding: Ignore trailing zeros in the response data avoiding raising false :class:`InvalidResponseException<udsoncan.exceptions.InvalidResponseException>`.
        :type tolerate_zero_padding:  bool

        :param ignore_all_zero_dtc: Discard any DTC entries that have an ID of 0. Avoid reading extra DTCs when using a transport protocol using zero padding.
        :type ignore_all_zero_dtc: bool

        :param dtc_snapshot_did_size: Number of bytes to encode the data identifier number. Other services such as :ref:`ReadDataByIdentifier<ReadDataByIdentifier>` encode DID over 2 bytes.
                UDS standard does not define the size of the snapshot DID, therefore, it must be supplied.
        :type dtc_snapshot_did_size: int

        :param didconfig: Definition of DID codecs. Dictionary mapping a DID (int) to a valid :ref:`DidCodec<DidCodec>` class or pack/unpack string 
        :type didconfig: dict[int] = :ref:`DidCodec<DidCodec>`

        :raises InvalidResponseException: If response length is wrong or does not match DID configuration
        :raises ValueError: If parameters are out of range, missing or wrong types
        :raises ConfigError: If the server returns a snapshot DID not defined in ``didconfig``
        """

        cls.check_subfunction_valid(subfunction, standard_version)

        # Response grouping for responses that are encoded the same way
        response_subfn_dtc_availability_mask_plus_dtc_record = [
            ReadDTCInformation.Subfunction.reportDTCByStatusMask,
            ReadDTCInformation.Subfunction.reportUserDefMemoryDTCByStatusMask,
            ReadDTCInformation.Subfunction.reportSupportedDTCs,
            ReadDTCInformation.Subfunction.reportFirstTestFailedDTC,
            ReadDTCInformation.Subfunction.reportFirstConfirmedDTC,
            ReadDTCInformation.Subfunction.reportMostRecentTestFailedDTC,
            ReadDTCInformation.Subfunction.reportMostRecentConfirmedDTC,
            ReadDTCInformation.Subfunction.reportMirrorMemoryDTCByStatusMask,
            ReadDTCInformation.Subfunction.reportEmissionsRelatedOBDDTCByStatusMask,
            ReadDTCInformation.Subfunction.reportDTCWithPermanentStatus
        ]

        response_subfn_number_of_dtc = [
            ReadDTCInformation.Subfunction.reportNumberOfDTCByStatusMask,
            ReadDTCInformation.Subfunction.reportNumberOfDTCBySeverityMaskRecord,
            ReadDTCInformation.Subfunction.reportNumberOfMirrorMemoryDTCByStatusMask,
            ReadDTCInformation.Subfunction.reportNumberOfEmissionsRelatedOBDDTCByStatusMask,
        ]

        response_subfn_dtc_availability_mask_plus_dtc_record_with_severity = [
            ReadDTCInformation.Subfunction.reportDTCBySeverityMaskRecord,
            ReadDTCInformation.Subfunction.reportSeverityInformationOfDTC
        ]

        response_subfn_dtc_plus_fault_counter = [
            ReadDTCInformation.Subfunction.reportDTCFaultDetectionCounter
        ]

        response_subfn_dtc_plus_sapshot_record = [
            ReadDTCInformation.Subfunction.reportDTCSnapshotIdentification
        ]

        response_sbfn_dtc_status_snapshots_records = [
            ReadDTCInformation.Subfunction.reportDTCSnapshotRecordByDTCNumber,
            ReadDTCInformation.Subfunction.reportUserDefMemoryDTCSnapshotRecordByDTCNumber
        ]

        response_sbfn_dtc_status_snapshots_records_record_first = [
            ReadDTCInformation.Subfunction.reportDTCSnapshotRecordByRecordNumber
        ]

        response_subfn_mask_record_plus_extdata = [
            ReadDTCInformation.Subfunction.reportDTCExtendedDataRecordByDTCNumber,
            ReadDTCInformation.Subfunction.reportMirrorMemoryDTCExtendedDataRecordByDTCNumber,
            ReadDTCInformation.Subfunction.reportUserDefMemoryDTCExtDataRecordByDTCNumber
        ]

        response_subfn_record_number_plus_dtc_mask_plus_extdata = [
            ReadDTCInformation.Subfunction.reportDTCExtDataRecordByRecordNumber
        ]

        subfunctions_with_memory_selection = [
            ReadDTCInformation.Subfunction.reportUserDefMemoryDTCByStatusMask,
            ReadDTCInformation.Subfunction.reportUserDefMemoryDTCSnapshotRecordByDTCNumber,
            ReadDTCInformation.Subfunction.reportUserDefMemoryDTCExtDataRecordByDTCNumber
        ]

        firstbyte_is_memory_selection_echo = (subfunction in subfunctions_with_memory_selection)

        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 1:
            raise InvalidResponseException(response, 'Response must be at least 1 byte long (echo of subfunction)')

        response.service_data = cls.ResponseData(
            subfunction_echo=response.data[0],  # First byte is subfunction
        )

        # Now for each response group, we have a different decoding algorithm
        if subfunction in response_subfn_dtc_availability_mask_plus_dtc_record + response_subfn_dtc_availability_mask_plus_dtc_record_with_severity:

            if subfunction in response_subfn_dtc_availability_mask_plus_dtc_record:
                dtc_size = 4  # DTC ID (3) + Status (1)
            elif subfunction in response_subfn_dtc_availability_mask_plus_dtc_record_with_severity:
                dtc_size = 6  # DTC ID (3) + Status (1) + Severity (1) + FunctionalUnit (1)

            if firstbyte_is_memory_selection_echo:
                if len(response.data) < 3:
                    raise InvalidResponseException(
                        response, 'Response must be at least 3 bytes long (echo of subfunction, echo of MemorySelection and DTCStatusAvailabilityMask)')
                response.service_data.memory_selection_echo = int(response.data[1])
                actual_byte = 2
            else:
                if len(response.data) < 2:
                    raise InvalidResponseException(
                        response, 'Response must be at least 2 byte long (echo of subfunction and DTCStatusAvailabilityMask)')
                actual_byte = 1

            response.service_data.status_availability = Dtc.Status.from_byte(response.data[actual_byte])
            actual_byte += 1

            while True:  # Loop until we have read all dtcs
                if len(response.data) <= actual_byte:
                    break  # done

                elif len(response.data) < actual_byte + dtc_size:
                    partial_dtc_length = len(response.data) - actual_byte
                    if tolerate_zero_padding and response.data[actual_byte:] == b'\x00' * partial_dtc_length:
                        break
                    else:
                        # We purposely ignore extra byte for subfunction reportSeverityInformationOfDTC as it is supposed to return 0 or 1 DTC.
                        if subfunction != ReadDTCInformation.Subfunction.reportSeverityInformationOfDTC or actual_byte == 2:
                            raise InvalidResponseException(
                                response, 'Incomplete DTC record. Missing %d bytes to response to complete the record' % (dtc_size - partial_dtc_length))

                else:
                    dtc_bytes = response.data[actual_byte:actual_byte + dtc_size]
                    if dtc_bytes == b'\x00' * dtc_size and ignore_all_zero_dtc:
                        pass  # ignore
                    else:
                        if subfunction in response_subfn_dtc_availability_mask_plus_dtc_record:
                            dtc = Dtc(struct.unpack('>L', b'\x00' + dtc_bytes[0:3])[0])
                            dtc.status.set_byte(dtc_bytes[3])
                        elif subfunction in response_subfn_dtc_availability_mask_plus_dtc_record_with_severity:
                            dtc = Dtc(struct.unpack('>L', b'\x00' + dtc_bytes[2:5])[0])
                            dtc.severity.set_byte(dtc_bytes[0])
                            dtc.functional_unit = dtc_bytes[1]
                            dtc.status.set_byte(dtc_bytes[5])

                        response.service_data.dtcs.append(dtc)
                actual_byte += dtc_size
            response.service_data.dtc_count = len(response.service_data.dtcs)

        # The 2 following subfunction responses have different purposes but their constructions are very similar.
        elif subfunction in response_subfn_dtc_plus_fault_counter + response_subfn_dtc_plus_sapshot_record:
            dtc_size = 4
            if len(response.data) < 1:
                raise InvalidResponseException(response, 'Response must be at least 1 byte long (echo of subfunction)')

            actual_byte = 1 	# Increasing index
            dtc_map: Dict[int, Dtc] = dict()  # This map is used to append snapshot to existing DTC.

            while True: 	# Loop until we have read all dtcs
                if len(response.data) <= actual_byte:
                    break 	# done

                elif len(response.data) < actual_byte + dtc_size:
                    partial_dtc_length = len(response.data) - actual_byte
                    if tolerate_zero_padding and response.data[actual_byte:] == b'\x00' * partial_dtc_length:
                        break
                    else:
                        raise InvalidResponseException(
                            response, 'Incomplete DTC record. Missing %d bytes to response to complete the record' % (dtc_size - partial_dtc_length))
                else:
                    dtc_bytes = response.data[actual_byte:actual_byte + dtc_size]
                    if dtc_bytes == b'\x00' * dtc_size and ignore_all_zero_dtc:
                        pass  # ignore
                    else:
                        dtcid = struct.unpack('>L', b'\x00' + dtc_bytes[0:3])[0]
                        # We create the DTC or get its reference if already created.
                        dtc_created = False
                        if dtcid in dtc_map and subfunction in response_subfn_dtc_plus_sapshot_record:
                            dtc = dtc_map[dtcid]
                        else:
                            dtc = Dtc(dtcid)
                            dtc_map[dtcid] = dtc
                            dtc_created = True

                        # We either read the DTC fault counter or Snapshot record number.
                        if subfunction in response_subfn_dtc_plus_fault_counter:
                            dtc.fault_counter = dtc_bytes[3]

                        elif subfunction in response_subfn_dtc_plus_sapshot_record:
                            record_number = dtc_bytes[3]

                            if dtc.snapshots is None:
                                dtc.snapshots = []

                            dtc.snapshots.append(record_number)

                        # Adds the DTC to the list.
                        if dtc_created:
                            response.service_data.dtcs.append(dtc)

                actual_byte += dtc_size

            response.service_data.dtc_count = len(response.service_data.dtcs)

        # This group of responses returns a number of DTCs available
        elif subfunction in response_subfn_number_of_dtc:
            if len(response.data) < 5:
                raise InvalidResponseException(response, 'Response must be exactly 5 bytes long ')

            response.service_data.status_availability = Dtc.Status.from_byte(response.data[1])
            response.service_data.dtc_format = response.data[2]
            response.service_data.dtc_count = struct.unpack('>H', response.data[3:5])[0]

        # This group of responses returns DTC snapshots
        # Responses include a DTC, many snapshot records. For each record, we find many Data Identifiers.
        # We create one Dtc.Snapshot for each DID. That'll be easier to work with.
        # <DTC,RecordNumber1,NumberOfDid_X,DID1,DID2,...DIDX, RecordNumber2,NumberOfDid_Y,DID1,DID2,...DIDY, etc>
        elif subfunction in response_sbfn_dtc_status_snapshots_records:

            minlength = 5 if not firstbyte_is_memory_selection_echo else 6
            if len(response.data) < minlength:
                raise InvalidResponseException(response, 'Response must be at least %d bytes long' % minlength)

            if firstbyte_is_memory_selection_echo:
                response.service_data.memory_selection_echo = response.data[1]
                actual_byte = 2
            else:
                actual_byte = 1

            dtc = Dtc(struct.unpack('>L', b'\x00' + response.data[actual_byte:(actual_byte + 3)])[0])
            dtc.status.set_byte(response.data[actual_byte + 3])
            actual_byte += 4		# Increasing index

            tools.validate_int(dtc_snapshot_did_size, min=1, max=8, name='dtc_snapshot_did_size')

            while True:		# Loop until we have read all dtcs
                if len(response.data) <= actual_byte:
                    break  # done

                remaining_data = response.data[actual_byte:]
                if tolerate_zero_padding and remaining_data == b'\x00' * len(remaining_data):
                    break

                if len(remaining_data) < 2:
                    raise InvalidResponseException(response, 'Incomplete response from server. Missing "number of identifier" and following data')

                record_number = remaining_data[0]
                number_of_did = remaining_data[1]
                # Validate record number and number of DID before continuing
                if number_of_did == 0:
                    raise InvalidResponseException(response, 'Server returned snapshot record #%d with no data identifier included' % (record_number))

                if len(remaining_data) < 2 + dtc_snapshot_did_size:
                    raise InvalidResponseException(response, 'Incomplete response from server. Missing DID number and associated data.')

                actual_byte += 2
                for i in range(number_of_did):
                    remaining_data = response.data[actual_byte:]
                    snapshot = Dtc.Snapshot()  # One snapshot per DID for convenience.
                    snapshot.record_number = record_number

                    # As standard does not specify the length of the DID, we craft it based on a config
                    did = 0
                    for j in range(dtc_snapshot_did_size):
                        offset = dtc_snapshot_did_size - 1 - j
                        did |= (remaining_data[offset] << (8 * j))

                    # Decode the data based on DID number.
                    snapshot.did = did
                    didconfig = check_did_config(did, didconfig)
                    codec_definition = fetch_codec_definition_from_config(did, didconfig)
                    codec = make_did_codec_from_definition(codec_definition)

                    data_offset = dtc_snapshot_did_size
                    if len(remaining_data[data_offset:]) < len(codec):
                        raise InvalidResponseException(response, 'Incomplete response. Data for DID 0x%04x is only %d bytes while %d bytes is expected' % (
                            did, len(remaining_data[data_offset:]), len(codec)))

                    snapshot.raw_data = remaining_data[data_offset:data_offset + len(codec)]
                    snapshot.data = codec.decode(snapshot.raw_data)

                    dtc.snapshots.append(snapshot)
                    actual_byte += dtc_snapshot_did_size + len(codec)

            response.service_data.dtcs.append(dtc)
            response.service_data.dtc_count = 1

        # This group of responses returns DTC snapshots
        # Responses include a DTC, many snapshots records. For each record, we find many Data Identifiers.
        # We create one Dtc.Snapshot for each DID. That'll be easier to work with.
        # Similar to previous subfunction group, but order of information is changed.

        # <RecordNumber1, DTC1,NumberOfDid_X,DID1,DID2,...DIDX, RecordNumber2,DTC2, NumberOfDid_Y,DID1,DID2,...DIDY, etc>
        elif subfunction in response_sbfn_dtc_status_snapshots_records_record_first:
            tools.validate_int(dtc_snapshot_did_size, min=1, max=8, name='dtc_snapshot_did_size')

            if len(response.data) < 2:
                raise InvalidResponseException(response, 'Response must be at least 2 bytes long. Subfunction echo + RecordNumber ')

            actual_byte = 1	 # Increasing index
            while True:  # Loop through response data
                if len(response.data) <= actual_byte:
                    break  # done

                remaining_data = response.data[actual_byte:]
                record_number = remaining_data[0]

                # If empty response but filled with 0, it is considered ok
                if remaining_data == b'\x00' * len(remaining_data) and tolerate_zero_padding:
                    break

                # If record number received but no DTC provided (allowed according to standard), we exit.
                if len(remaining_data) == 1 or tolerate_zero_padding and remaining_data[1:] == b'\x00' * len(remaining_data[1:]):
                    break

                if len(remaining_data) < 5: 	# Partial DTC (No DTC at all is checked above)
                    raise InvalidResponseException(response, 'Incomplete response from server. Missing "DTCAndStatusRecord" and following data')

                if len(remaining_data) < 6:
                    raise InvalidResponseException(response, 'Incomplete response from server. Missing number of data identifier')

                # DTC decoding
                dtc = Dtc(struct.unpack('>L', b'\x00' + remaining_data[1:4])[0])
                dtc.status.set_byte(remaining_data[4])
                number_of_did = remaining_data[5]

                actual_byte += 6
                remaining_data = response.data[actual_byte:]

                if number_of_did == 0:
                    raise InvalidResponseException(response, 'Server returned snapshot record #%d with no data identifier included' % (record_number))

                if len(remaining_data) < dtc_snapshot_did_size:
                    raise InvalidResponseException(response, 'Incomplete response from server. Missing DID and associated data')

                # We have a DTC and 0 DID, next loop
                if tolerate_zero_padding and remaining_data == b'\x00' * len(remaining_data):
                    break

                # For each DID
                for i in range(number_of_did):
                    remaining_data = response.data[actual_byte:]
                    snapshot = Dtc.Snapshot()  # One snapshot epr DID for convenience
                    snapshot.record_number = record_number

                    # As standard does not specify the length of the DID, we craft it based on a config
                    did = 0
                    for j in range(dtc_snapshot_did_size):
                        offset = dtc_snapshot_did_size - 1 - j
                        did |= (remaining_data[offset] << (8 * j))

                    # Decode the data based on DID number.
                    snapshot.did = did
                    didconfig = check_did_config(did, didconfig)
                    codec_definition = fetch_codec_definition_from_config(did, didconfig)
                    codec = make_did_codec_from_definition(codec_definition)

                    data_offset = dtc_snapshot_did_size
                    if len(remaining_data[data_offset:]) < len(codec):
                        raise InvalidResponseException(response, 'Incomplete response. Data for DID 0x%04x is only %d bytes while %d bytes is expected' % (
                            did, len(remaining_data[data_offset:]), len(codec)))

                    snapshot.raw_data = remaining_data[data_offset:data_offset + len(codec)]
                    snapshot.data = codec.decode(snapshot.raw_data)

                    dtc.snapshots.append(snapshot)

                    actual_byte += dtc_snapshot_did_size + len(codec)

                response.service_data.dtcs.append(dtc)
            response.service_data.dtc_count = len(response.service_data.dtcs)

        # These subfunctions include DTC ExtraData. We give it raw to user.
        elif subfunction in response_subfn_mask_record_plus_extdata:
            cls.assert_extended_data_size_int_or_dict(extended_data_size, subfunction)
            assert extended_data_size is not None

            minlength = 5 if not firstbyte_is_memory_selection_echo else 6

            if len(response.data) < minlength:
                missing_data = 'MemorySelection and DTCAndStatusRecord' if firstbyte_is_memory_selection_echo else 'DTCAndStatusRecord'
                raise InvalidResponseException(response, 'Incomplete response from server. Missing %s' % missing_data)

            if firstbyte_is_memory_selection_echo:
                response.service_data.memory_selection_echo = response.data[1]
                actual_byte = 2
            else:
                actual_byte = 1

            # DTC decoding
            dtc = Dtc(struct.unpack('>L', b'\x00' + response.data[actual_byte:(actual_byte + 3)])[0])
            dtc.status.set_byte(response.data[actual_byte + 3])

            size = cls.get_extended_data_size(dtc, extended_data_size)

            actual_byte = actual_byte + 4  # Increasing index
            while actual_byte < len(response.data):  # Loop through data
                remaining_data = response.data[actual_byte:]
                record_number = remaining_data[0]

                if record_number == 0:
                    if remaining_data == b'\x00' * len(remaining_data) and tolerate_zero_padding:
                        break
                    else:
                        raise InvalidResponseException(
                            response, 'Extended data record number given by the server is 0 but this value is a reserved value.')

                actual_byte += 1
                remaining_data = response.data[actual_byte:]

                if len(remaining_data) < size:
                    raise InvalidResponseException(response, 'Incomplete response from server. Length of extended data for DTC 0x%06x with record number 0x%02x is %d bytes but smaller than given data_size of %d bytes' % (
                        dtc.id, record_number, len(remaining_data), size))

                exdata = Dtc.ExtendedData()
                exdata.record_number = record_number
                exdata.raw_data = remaining_data[0:size]

                dtc.extended_data.append(exdata)

                actual_byte += size

            response.service_data.dtcs.append(dtc)
            response.service_data.dtc_count = len(response.service_data.dtcs)

        elif subfunction in response_subfn_record_number_plus_dtc_mask_plus_extdata:
            cls.assert_extended_data_size_int_or_dict(extended_data_size, subfunction)
            assert extended_data_size is not None

            if len(response.data) < 2:
                raise InvalidResponseException(response, 'Incomplete response from server. Missing DTCExtDataRecordNumber')

            record_number = int(response.data[1])

            actual_byte = 2
            received_dtcs = set()
            while True:
                if actual_byte == len(response.data):
                    break

                remaining_data = response.data[actual_byte:]
                all_zero_ending = True if remaining_data == b'\x00' * len(remaining_data) else False

                if all_zero_ending:
                    try:
                        zero_dtc_size = cls.get_extended_data_size(Dtc(0), extended_data_size)
                    except:
                        zero_dtc_size = None

                    all_zero_dtc_to_read = (zero_dtc_size is not None and len(remaining_data) >= zero_dtc_size + 4) and ~ignore_all_zero_dtc

                    if ~all_zero_dtc_to_read:
                        if tolerate_zero_padding:
                            break
                        else:
                            raise InvalidResponseException(response, 'Server response ends with a sequence of zero and zero adding is not tolerated')

                # DTC decoding
                if len(response.data) - actual_byte < 4:
                    raise InvalidResponseException(response, 'Incomplete DTCAndStatusRecord at position %d' % actual_byte)

                dtc = Dtc(struct.unpack('>L', b'\x00' + response.data[actual_byte:(actual_byte + 3)])[0])
                if dtc.id in received_dtcs:
                    raise InvalidResponseException(
                        response, 'The server returned two set of data for the same DTC (0x%06X) and same record number (%d)', (dtc.id, record_number))

                received_dtcs.add(dtc.id)

                dtc.status.set_byte(response.data[actual_byte + 3])
                actual_byte += 4

                size = cls.get_extended_data_size(dtc, extended_data_size)

                if len(response.data) - actual_byte < size:
                    raise InvalidResponseException(response, 'Incomplete ExtendedData for DTC 0x%06X. Configuration says that there should be %d bytes of data but server only gave %d bytes.' % (
                        dtc.id, size, len(response.data) - actual_byte))

                exdata = Dtc.ExtendedData()
                exdata.record_number = record_number
                exdata.raw_data = response.data[actual_byte:actual_byte + size]
                dtc.extended_data.append(exdata)
                response.service_data.dtcs.append(dtc)

                actual_byte += size

            response.service_data.dtc_count = len(response.service_data.dtcs)

        elif subfunction in [ReadDTCInformation.Subfunction.reportWWHOBDDTCByMaskRecord, ReadDTCInformation.Subfunction.reportWWHOBDDTCWithPermanentStatus]:

            if subfunction == ReadDTCInformation.Subfunction.reportWWHOBDDTCByMaskRecord:
                if len(response.data) < 5:
                    raise InvalidResponseException(response, 'Incomplete response from server.')
                
                response.service_data.functional_group_id = response.data[1]
                response.service_data.status_availability = Dtc.Status.from_byte(response.data[2])
                response.service_data.severity_availability = Dtc.Severity.from_byte(response.data[3])
                response.service_data.dtc_format = response.data[4]
                remaining_bytes = response.data[5:]
            elif subfunction == ReadDTCInformation.Subfunction.reportWWHOBDDTCWithPermanentStatus:
                if len(response.data) < 4:
                    raise InvalidResponseException(response, 'Incomplete response from server.')
                
                response.service_data.functional_group_id = response.data[1]
                response.service_data.status_availability = Dtc.Status.from_byte(response.data[2])
                response.service_data.dtc_format = response.data[3]
                remaining_bytes = response.data[4:]
            else:
                raise NotImplementedError("Unreachable code")

            # Don't check functional_group_id on purpose. range 0 to FF is accepted

            if response.service_data.dtc_format not in [Dtc.Format.SAE_J2012_DA_DTCFormat_04, Dtc.Format.SAE_J1939_73]:
                raise InvalidResponseException(response, "DTCFormatIdentifier returned by the server is not one of the following: SAE_J2012-DA_DTCFormat_04 (4), SAE_J1939-73_DTCFormat(2). Got 0x%02x" % response.service_data.dtc_format)

            if len(remaining_bytes) % 5 != 0:
                raise InvalidResponseException(response, 'Incomplete response from server. Remaining bytes must be a multiple of 5')

            while remaining_bytes:
                severity = remaining_bytes[0]
                dtc = Dtc(struct.unpack('>L', b'\x00' + remaining_bytes[1:4])[0])
                status_of_dtc = Dtc.Status.from_byte(remaining_bytes[4])
                dtc.severity.set_byte(severity)
                dtc.status = status_of_dtc
                remaining_bytes = remaining_bytes[5:]
                response.service_data.dtcs.append(dtc)

            response.service_data.dtc_count = len(response.service_data.dtcs)

        return cast(ReadDTCInformation.InterpretedResponse, response)

    @classmethod
    def get_extended_data_size(cls, dtc: Dtc, given_size: Union[int, Dict[int, int]]) -> int:
        size = None
        if isinstance(given_size, int):
            size = given_size
        elif isinstance(given_size, dict):
            if dtc.id not in given_size:
                raise ConfigError(key=dtc.id, msg='Server returned extended data for DTC 0x%06X but no data size was specified for this DTC' % (dtc.id))
            size = given_size[dtc.id]
        else:
            raise ValueError('Unsupported extended data size type.')

        return size
