from udsoncan import Request, Response, services
from udsoncan.common.Routine import Routine
from udsoncan.common.dtc import Dtc
from udsoncan.common.dids import DataIdentifier
from udsoncan.common.MemoryLocation import MemoryLocation
from udsoncan.common.DynamicDidDefinition import DynamicDidDefinition
from udsoncan.common.CommunicationType import CommunicationType
from udsoncan.common.DataFormatIdentifier import DataFormatIdentifier
from udsoncan.common.Baudrate import Baudrate
from udsoncan.common.IOControls import IOValues, IOMasks
from udsoncan.common.Filesize import Filesize
from udsoncan.connections import BaseConnection
from udsoncan.BaseService import BaseService

from udsoncan.exceptions import *
from udsoncan.configs import default_client_config
from udsoncan.typing import ClientConfig
from udsoncan import valid_standards
import logging
import binascii
import functools
import time

from typing import Callable, Optional, Union, Dict, List, Any, cast, Type


class SessionTiming:
    """Container for server provided P2 & P2* timeouts."""

    p2_server_max: Optional[float]
    """P2 server max provided by the server. ``None`` if not provided yet """
    p2_star_server_max: Optional[float]
    """P2* server max provided by the server. ``None`` if not provided yet """

    def __init__(self, p2_server_max:Optional[float]=None, p2_star_server_max:Optional[float]=None) -> None:
        self.p2_server_max = p2_server_max
        self.p2_star_server_max = p2_star_server_max


class Client:
    """
    __init__(self, conn, config=default_client_config, request_timeout = None)

    Object that interacts with a UDS server. 
    It builds a service request, sends it to the server, receives and parses its response, detects communication anomalies and logs what it is doing for further debugging.

    :param conn: The underlying protocol interface.
    :type conn: :ref:`Connection<Connection>`

    :param config: The :ref:`client configuration<client_config>`
    :type config: dict

    :param request_timeout: Maximum amount of time to wait for a response. This parameter exists for backward compatibility only. For detailed timeout handling, see :ref:`Client configuration<config_timeouts>`
    :type request_timeout: int
    """

    class SuppressPositiveResponse:
        enabled: bool
        wait_nrc: bool

        def __init__(self):
            self.enabled = False
            self.wait_nrc = False

        def __call__(self, wait_nrc: bool = False):
            self.wait_nrc = wait_nrc
            return self

        def __enter__(self):
            self.enabled = True
            return self

        def __exit__(self, type, value, traceback):
            self.enabled = False
            self.wait_nrc = None

    class PayloadOverrider:
        modifier: Optional[Union[Callable, bytes]]
        enabled: bool

        def __init__(self):
            self.modifier = None
            self.enabled = False

        def __enter__(self):
            self.enabled = True
            return self

        def __exit__(self, type, value, traceback):
            self.modifier = None
            self.enabled = False

        def __call__(self, modifier):
            self.modifier = modifier
            return self

        def get_overrided_payload(self, original_payload: bytes) -> bytes:
            assert self.modifier is not None
            if callable(self.modifier):
                return self.modifier(original_payload)
            else:
                return self.modifier

    conn: BaseConnection
    config: ClientConfig
    suppress_positive_response: "Client.SuppressPositiveResponse"
    payload_override: "Client.PayloadOverrider"
    last_response: Optional[Response]
    session_timing: SessionTiming
    logger: logging.Logger

    def __init__(self, conn: BaseConnection, config: ClientConfig = default_client_config, request_timeout: Optional[float] = None):
        self.conn = conn
        self.config = cast(ClientConfig, dict(config))  # Makes a copy of given configuration

        # For backward compatibility
        if request_timeout is not None:
            self.config['request_timeout'] = request_timeout
        self.suppress_positive_response = Client.SuppressPositiveResponse()
        self.payload_override = Client.PayloadOverrider()
        self.last_response = None

        self.session_timing = SessionTiming(p2_server_max=None, p2_star_server_max=None)

        self.refresh_config()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def open(self) -> None:
        if not self.conn.is_open():
            self.conn.open()

    def close(self) -> None:
        self.conn.close()

    def configure_logger(self) -> None:
        logger_name = 'UdsClient'
        if 'logger_name' in self.config:
            logger_name = "UdsClient[%s]" % self.config['logger_name']

        self.logger = logging.getLogger(logger_name)

    def set_config(self, key: str, value: Any) -> None:
        self.config[key] = value    # type:ignore
        self.refresh_config()

    def set_configs(self, dic: ClientConfig) -> None:
        self.config.update(dic)
        self.refresh_config()

    def refresh_config(self) -> None:
        self.configure_logger()
        for k in default_client_config:
            if k not in self.config:
                self.config[k] = default_client_config[k]  # type:ignore
        self.validate_config()

    def validate_config(self) -> None:
        if self.config['standard_version'] not in valid_standards:
            raise ConfigError('Valid standard versions are 2006, 2013, 2020. %s is not supported' % self.config['standard_version'])

    # Decorator to apply on functions that the user will call.
    # Each function raises exceptions. This decorator handles these exceptions, logs them,
    # then suppresses them or not depending on the client configuration.
    # if func1 and func2 are decorated and func2 calls func1, it should be done this way : self.func1._func_no_error_management(self, ...)

    def standard_error_management(func: Callable):  # type: ignore
        @functools.wraps(func)
        def decorated(self: "Client", *args, **kwargs):
            try:
                return func(self, *args, **kwargs)

            except NegativeResponseException as e:
                e.response.positive = False
                if self.config['exception_on_negative_response']:
                    logline = '[%s] : %s' % (e.__class__.__name__, str(e))
                    self.logger.warning(logline)
                    raise
                else:
                    self.logger.warning(str(e))
                    return e.response

            except InvalidResponseException as e:
                e.response.valid = False
                if self.config['exception_on_invalid_response']:
                    self.logger.error('[%s] : %s' % (e.__class__.__name__, str(e)))
                    raise
                else:
                    self.logger.error(str(e))
                    return e.response

            except UnexpectedResponseException as e:
                e.response.unexpected = True
                if self.config['exception_on_unexpected_response']:
                    self.logger.error('[%s] : %s' % (e.__class__.__name__, str(e)))
                    raise
                else:
                    self.logger.error(str(e))
                    return e.response

            except Exception as e:
                self.logger.error('[%s] : %s' % (e.__class__.__name__, str(e)))
                raise

        decorated._func_no_error_management = func  # type:ignore
        return decorated

    def service_log_prefix(self, service: Type[BaseService]):
        return "%s<0x%02x>" % (service.get_name(), service.request_id())

    def get_session_timing(self) -> SessionTiming:
        """Return the session timing provided by the server, including P2 & P2* timeouts.
        If the timeout values are ``None``, it means that no timing has been given by the server yet and the timings form the client configuration 
        (:ref:`p2_timeout<config_p2_timeout>`, :ref:`p2_star_timeout<config_p2_star_timeout>`)

        :return: The session timings
        """
        return self.session_timing

    @standard_error_management
    def change_session(self, newsession: int) -> Optional[services.DiagnosticSessionControl.InterpretedResponse]:
        """ 
        Requests the server to change the diagnostic session with a :ref:`DiagnosticSessionControl<DiagnosticSessionControl>` service request

        :Effective configuration: ``exception_on_<type>_response``

        :param newsession: The session to try to switch. Values from :class:`DiagnosticSessionControl.Session <udsoncan.services.DiagnosticSessionControl.Session>` can be used.
        :type newsession: int 

        :return: The server response parsed by :meth:`DiagnosticSessionControl.interpret_response<udsoncan.services.DiagnosticSessionControl.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        req = services.DiagnosticSessionControl.make_request(newsession)

        named_newsession = '%s (0x%02x)' % (services.DiagnosticSessionControl.Session.get_name(newsession), newsession)
        self.logger.info('%s - Switching session to %s' % (self.service_log_prefix(services.DiagnosticSessionControl), named_newsession))

        response = self.send_request(req)
        if response is None:
            return None

        response = services.DiagnosticSessionControl.interpret_response(response, standard_version=self.config['standard_version'])

        if newsession != response.service_data.session_echo:
            raise UnexpectedResponseException(response, "Response subfunction received from server (0x%02x) does not match the requested subfunction (0x%02x)" % (
                response.service_data.session_echo, newsession))

        if self.config['standard_version'] > 2006:
            assert response.service_data.p2_server_max is not None
            assert response.service_data.p2_star_server_max is not None
            if self.config['use_server_timing']:
                self.logger.info('%s - Received new timing parameters. P2=%.3fs and P2*=%.3fs.  Using these value from now on.' %
                                 (self.service_log_prefix(services.DiagnosticSessionControl), response.service_data.p2_server_max, response.service_data.p2_star_server_max))
                self.session_timing.p2_server_max = response.service_data.p2_server_max
                self.session_timing.p2_star_server_max = response.service_data.p2_star_server_max

        return response

    @standard_error_management
    def request_seed(self, level: int, data=bytes()) -> Optional[services.SecurityAccess.InterpretedResponse]:
        """ 
        Requests a seed to unlock a security level with the :ref:`SecurityAccess<SecurityAccess>` service 

        :Effective configuration: ``exception_on_<type>_response``

        :param level: The security level to unlock. If value is even, it will be converted to the corresponding odd value
        :type level: int 

        :param data: The data to send to the server (securityAccessDataRecord)
        :type data: bytes 

        :return: The server response parsed by :meth:`SecurityAccess.interpret_response<udsoncan.services.SecurityAccess.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        req = services.SecurityAccess.make_request(level, mode=services.SecurityAccess.Mode.RequestSeed, data=data)
        assert req.subfunction is not None
        # level may be corrected by service.
        self.logger.info('%s - Requesting seed to unlock security access level 0x%02x' %
                         (self.service_log_prefix(services.SecurityAccess), req.subfunction))

        response = self.send_request(req)
        if response is None:
            return None

        response = services.SecurityAccess.interpret_response(response, mode=services.SecurityAccess.Mode.RequestSeed)
        assert response.service_data.seed is not None

        expected_level = services.SecurityAccess.normalize_level(mode=services.SecurityAccess.Mode.RequestSeed, level=level)
        received_level = response.service_data.security_level_echo
        if expected_level != received_level:
            raise UnexpectedResponseException(
                response, "Response subfunction received from server (0x%02x) does not match the requested subfunction (0x%02x)" % (received_level, expected_level))

        self.logger.debug('Received seed [%s]' % (binascii.hexlify(response.service_data.seed).decode('ascii')))
        return response

    # Performs a SecurityAccess service request. Send key
    @standard_error_management
    def send_key(self, level: int, key: bytes) -> Optional[services.SecurityAccess.InterpretedResponse]:
        """ 
        Sends a key to unlock a security level with the :ref:`SecurityAccess<SecurityAccess>` service 

        :Effective configuration: ``exception_on_<type>_response``

        :param level: The security level to unlock. If value is odd, it will be converted to the corresponding even value
        :type level: int 

        :param key: The key to send to the server
        :type key: bytes 

        :return: The server response parsed by :meth:`SecurityAccess.interpret_response<udsoncan.services.SecurityAccess.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        req = services.SecurityAccess.make_request(level, mode=services.SecurityAccess.Mode.SendKey, data=key)
        assert req.subfunction is not None

        self.logger.info('%s - Sending key to unlock security access level 0x%02x' %
                         (self.service_log_prefix(services.SecurityAccess), req.subfunction))
        self.logger.debug('\tKey to send [%s]' % (binascii.hexlify(key).decode('ascii')))

        response = self.send_request(req)
        if response is None:
            return None

        response = services.SecurityAccess.interpret_response(response, mode=services.SecurityAccess.Mode.SendKey)

        expected_level = services.SecurityAccess.normalize_level(mode=services.SecurityAccess.Mode.SendKey, level=level)
        received_level = response.service_data.security_level_echo
        if expected_level != received_level:
            raise UnexpectedResponseException(
                response, "Response subfunction received from server (0x%02x) does not match the requested subfunction (0x%02x)" % (received_level, expected_level))

        return response

    @standard_error_management
    def unlock_security_access(self, level, seed_params=bytes()) -> Optional[services.SecurityAccess.InterpretedResponse]:
        """
        Successively calls request_seed and send_key to unlock a security level with the :ref:`SecurityAccess<SecurityAccess>` service.
        The key computation is done by calling config['security_algo']

        :Effective configuration: ``exception_on_<type>_response`` ``security_algo`` ``security_algo_params``

        :param level: The level to unlock. Can be the odd or even variant of it.
        :type level: int

        :param seed_params: Optional data to attach to the RequestSeed request (securityAccessDataRecord).
        :type seed_params: bytes

        :return: The server response parsed by :meth:`SecurityAccess.interpret_response<udsoncan.services.SecurityAccess.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """

        if 'security_algo' not in self.config or not callable(self.config['security_algo']):
            raise NotImplementedError("Client configuration does not provide a security algorithm")

        response = self.request_seed._func_no_error_management(self, level, data=seed_params)
        seed = response.service_data.seed
        if len(seed) > 0 and seed == b'\x00' * len(seed):
            self.logger.info('%s - Security access level 0x%02x is already unlocked, no key will be sent.' %
                             (self.service_log_prefix(services.SecurityAccess), level))
            return response

        params = self.config['security_algo_params'] if 'security_algo_params' in self.config else None

        # Starting from V1.12, level is now passed to the algorithm.
        # We now use named parameters for backward compatibility
        algo_params = {}
        try:
            algo_args = self.config['security_algo'].__code__.co_varnames[:self.config['security_algo'].__code__.co_argcount]

            if 'seed' in algo_args:
                algo_params['seed'] = seed
            if 'level' in algo_args:
                algo_params['level'] = level
            if 'params' in algo_args:
                algo_params['params'] = params
        except:
            algo_params = {'seed': seed, 'params': params, 'level': level}

        key = self.config['security_algo'].__call__(**algo_params)  # type: ignore
        return self.send_key._func_no_error_management(self, level, key)

    @standard_error_management
    def tester_present(self) -> Optional[services.TesterPresent.InterpretedResponse]:
        """
        Sends a TesterPresent request to keep the session active.

        :Effective configuration: ``exception_on_<type>_response``

        :return: The server response parsed by :meth:`TesterPresent.interpret_response<udsoncan.services.TesterPresent.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        req = services.TesterPresent.make_request()
        assert req.subfunction is not None

        self.logger.info('%s - Sending TesterPresent request' % (self.service_log_prefix(services.TesterPresent)))
        response = self.send_request(req)
        if response is None:
            return None

        response = services.TesterPresent.interpret_response(response)

        if req.subfunction != response.service_data.subfunction_echo:
            raise UnexpectedResponseException(response, "Response subfunction received from server (0x%02x) does not match the requested subfunction (0x%02x)" % (
                response.service_data.subfunction_echo, req.subfunction))

        return response

    @standard_error_management
    def read_data_by_identifier_first(self, didlist: Union[int, List[int]]) -> Optional[Any]:
        """
        Shortcut to extract a single DID. 
        Calls read_data_by_identifier then returns the first DID asked for. 

        :Effective configuration: ``exception_on_<type>_response`` ``data_identifiers`` ``tolerate_zero_padding``

        :param didlist: The list of DID to be read
        :type didlist: list[int]

        :return: The server response parsed by :meth:`ReadDataByIdentifier.interpret_response<udsoncan.services.ReadDataByIdentifier.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        didlist = services.ReadDataByIdentifier.validate_didlist_input(didlist)
        response = self.read_data_by_identifier(didlist)
        values = response.service_data.values
        if len(values) > 0 and len(didlist) > 0:
            return values[didlist[0]]
        return None

    @standard_error_management
    def test_data_identifier(self, didlist: Union[int, List[int]]) -> Optional[Response]:
        """
            Sends a request for the ReadDataByIdentifier and returns blindly the received response without parsing.
            The requested DIDs do not have to be inside the client list of supported DID.
            This method can be useful for testing if a DID exists on an ECU

            :Effective configuration: ``exception_on_<type>_response``

            :param didlist: The DIDs to peek
            :type didlist: list[int] 

            :return: The raw server response. The response will not be parsed by any service, causing ``service_data`` to always be ``None``
            :rtype: :ref:`Response<Response>`

        """
        # Do the validation. No need to read return value as we enforced a single DID already
        didlist = services.ReadDataByIdentifier.validate_didlist_input(didlist)
        req = services.ReadDataByIdentifier.make_request(didlist=didlist, didconfig=None)  # No config
        return self.send_request(req)

    @standard_error_management
    def read_data_by_identifier(self, didlist: Union[int, List[int]]) -> Optional[services.ReadDataByIdentifier.InterpretedResponse]:
        """
        Requests a value associated with a data identifier (DID) through the :ref:`ReadDataByIdentifier<ReadDataByIdentifier>` service.

        :Effective configuration: ``exception_on_<type>_response`` ``data_identifiers`` ``tolerate_zero_padding``

        See :ref:`an example<reading_a_did>` about how to read a DID

        :param didlist: The list of DID to be read
        :type didlist: list[int]

        :return: The server response parsed by :meth:`ReadDataByIdentifier.interpret_response<udsoncan.services.ReadDataByIdentifier.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        didlist = services.ReadDataByIdentifier.validate_didlist_input(didlist)
        req = services.ReadDataByIdentifier.make_request(didlist=didlist, didconfig=self.config['data_identifiers'])

        if len(didlist) == 1:
            self.logger.info("%s - Reading data identifier : 0x%04x (%s)" %
                             (self.service_log_prefix(services.ReadDataByIdentifier), didlist[0], DataIdentifier.name_from_id(didlist[0])))
        else:
            self.logger.info("%s - Reading %d data identifier : %s" %
                             (self.service_log_prefix(services.ReadDataByIdentifier), len(didlist), list(map(hex, didlist))))

        if 'data_identifiers' not in self.config or not isinstance(self.config['data_identifiers'], dict):
            raise ConfigError('Configuration does not contains a valid data identifier description.')

        response = self.send_request(req)
        if response is None:
            return None

        try:
            response = services.ReadDataByIdentifier.interpret_response(response,
                                                                        didlist=didlist,
                                                                        didconfig=self.config['data_identifiers'],
                                                                        tolerate_zero_padding=self.config['tolerate_zero_padding']
                                                                        )
        except ConfigError as e:
            if e.key in didlist:
                raise
            else:
                raise UnexpectedResponseException(
                    response, "Server returned values for data identifier 0x%04x that was not requested and no Codec was defined for it. Parsing must be stopped." % (e.key))

        set_request_didlist = set(didlist)
        set_response_didlist = set(response.service_data.values.keys())
        extra_did = set_response_didlist - set_request_didlist
        missing_did = set_request_didlist - set_response_didlist

        if len(extra_did) > 0:
            raise UnexpectedResponseException(
                response, "Server returned values for %d data identifier that were not requested. Dids are : %s" % (len(extra_did), extra_did))

        if len(missing_did) > 0:
            raise UnexpectedResponseException(
                response, "%d data identifier values are missing from server response. Dids are : %s" % (len(missing_did), missing_did))

        return response

    # Performs a WriteDataByIdentifier request.

    @standard_error_management
    def write_data_by_identifier(self, did: int, value: Any) -> Optional[services.WriteDataByIdentifier.InterpretedResponse]:
        """
        Requests to write a value associated with a data identifier (DID) through the :ref:`WriteDataByIdentifier<WriteDataByIdentifier>` service.

        :Effective configuration:  ``exception_on_<type>_response`` ``data_identifiers``

        :param did: The DID to write its value
        :type did: int

        :param value: Value given to the :ref:`DidCodec <DidCodec>`.encode method. The payload returned by the codec will be sent to the server.
        :type value: int

        :return: The server response parsed by :meth:`WriteDataByIdentifier.interpret_response<udsoncan.services.WriteDataByIdentifier.interpret_response>`
        :rtype: :ref:`Response<Response>`

        """
        req = services.WriteDataByIdentifier.make_request(did, value, didconfig=self.config['data_identifiers'])
        self.logger.info("%s - Writing data identifier 0x%04x (%s)" %
                         (self.service_log_prefix(services.WriteDataByIdentifier), did, DataIdentifier.name_from_id(did)))

        response = self.send_request(req)
        if response is None:
            return None
        response = services.WriteDataByIdentifier.interpret_response(response)

        if response.service_data.did_echo != did:
            raise UnexpectedResponseException(
                response, "Server returned a response for data identifier 0x%04x while client requested for did 0x%04x" % (response.service_data.did_echo, did))

        return response

    @standard_error_management
    def ecu_reset(self, reset_type: int) -> Optional[services.ECUReset.InterpretedResponse]:
        """
        Requests the server to execute a reset sequence through the :ref:`ECUReset<ECUReset>` service.

        :Effective configuration: ``exception_on_<type>_response``

        :param reset_type: The type of reset to perform.  :class:`ECUReset.ResetType<udsoncan.services.ECUReset.ResetType>`
        :type reset_type: int

        :return: The server response parsed by :meth:`ECUReset.interpret_response<udsoncan.services.ECUReset.interpret_response>`
        :rtype: :ref:`Response<Response>`

        """
        req = services.ECUReset.make_request(reset_type)
        self.logger.info("%s - Requesting reset of type 0x%02x (%s)" %
                         (self.service_log_prefix(services.ECUReset), reset_type, services.ECUReset.ResetType.get_name(reset_type)))

        response = self.send_request(req)
        if response is None:
            return None
        response = services.ECUReset.interpret_response(response)

        if response.service_data.reset_type_echo != reset_type:
            raise UnexpectedResponseException(response, "Response subfunction received from server (0x%02x) does not match the requested subfunction (0x%02x)" % (
                response.service_data.reset_type_echo, reset_type))

        if response.service_data.reset_type_echo == services.ECUReset.ResetType.enableRapidPowerShutDown and response.service_data.powerdown_time != 0xFF:
            assert response.service_data.powerdown_time is not None
            self.logger.info('Server will shutdown in %d seconds.' % (response.service_data.powerdown_time))

        return response

    @standard_error_management
    def clear_dtc(self, group: int = 0xFFFFFF, memory_selection: Optional[int] = None) -> Optional[services.ClearDiagnosticInformation.InterpretedResponse]:
        """
        Requests the server to clear its active Diagnostic Trouble Codes with the :ref:`ClearDiagnosticInformation<ClearDiagnosticInformation>` service.

        :Effective configuration: ``exception_on_<type>_response``. ``standard_version``

        :param group: The group of DTCs to clear. It may refer to Powertrain DTCs, Chassis DTCs, etc. Values are defined by the ECU manufacturer except for two specific values

                - ``0x000000`` : Emissions-related systems
                - ``0xFFFFFF`` : All DTCs
        :type group: int

        :param memory_selection: MemorySelection byte (0-0xFF). This value is user defined and introduced in 2020 version of ISO-14229-1. 
            Only added to the request payload when different from None. Default : None
        :type memory_selection: int

        :return: The server response parsed by :meth:`ClearDiagnosticInformation.interpret_response<udsoncan.services.ClearDiagnosticInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`

        """

        request = services.ClearDiagnosticInformation.make_request(
            group, memory_selection=memory_selection, standard_version=self.config['standard_version'])
        memys_str = ''
        if memory_selection is not None:
            memys_str = ' , MemorySelection : %d' % memory_selection
        if group == 0xFFFFFF:
            self.logger.info('%s - Clearing all DTCs (group mask : 0xFFFFFF%s)' %
                             (self.service_log_prefix(services.ClearDiagnosticInformation), memys_str))
        else:
            self.logger.info('%s - Clearing DTCs matching group mask : 0x%06x%s' %
                             (self.service_log_prefix(services.ClearDiagnosticInformation), group, memys_str))

        response = self.send_request(request)
        if response is None:
            return None

        response = services.ClearDiagnosticInformation.interpret_response(response)

        return response

    # Performs a RoutineControl Service request
    def start_routine(self, routine_id: int, data: Optional[bytes] = None) -> Optional[services.RoutineControl.InterpretedResponse]:
        """
        Requests the server to start a routine through the :ref:`RoutineControl<RoutineControl>` service (subfunction = 0x01).

        :Effective configuration: ``exception_on_<type>_response``

        :param routine_id: The 16-bit numerical ID of the routine to start
        :type group: int

        :param data: Optional additional data to give to the server
        :type data: bytes

        :return: The server response parsed by :meth:`RoutineControl.interpret_response<udsoncan.services.RoutineControl.interpret_response>`
        :rtype: :ref:`Response<Response>`

        """
        return self.routine_control(routine_id, services.RoutineControl.ControlType.startRoutine, data)

    # Performs a RoutineControl Service request
    def stop_routine(self, routine_id: int, data: Optional[bytes] = None) -> Optional[services.RoutineControl.InterpretedResponse]:
        """
        Requests the server to stop a routine through the :ref:`RoutineControl<RoutineControl>` service (subfunction = 0x02).

        :Effective configuration: ``exception_on_<type>_response``

        :param routine_id: The 16-bit numerical ID of the routine to stop
        :type group: int

        :param data: Optional additional data to give to the server
        :type data: bytes

        :return: The server response parsed by :meth:`RoutineControl.interpret_response<udsoncan.services.RoutineControl.interpret_response>`
        :rtype: :ref:`Response<Response>`

        """
        return self.routine_control(routine_id, services.RoutineControl.ControlType.stopRoutine, data)

    # Performs a RoutineControl Service request
    def get_routine_result(self, routine_id: int, data: Optional[bytes] = None) -> Optional[services.RoutineControl.InterpretedResponse]:
        """
        Requests the server to send back the execution result of the specified routine through the :ref:`RoutineControl<RoutineControl>` service (subfunction = 0x03).

        :Effective configuration: ``exception_on_<type>_response``

        :param routine_id: The 16-bit numerical ID of the routine
        :type group: int

        :param data: Optional additional data to give to the server
        :type data: bytes

        :return: The server response parsed by :meth:`RoutineControl.interpret_response<udsoncan.services.RoutineControl.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.routine_control(routine_id, services.RoutineControl.ControlType.requestRoutineResults, data)

    # Performs a RoutineControl Service request
    @standard_error_management
    def routine_control(self, routine_id: int, control_type: int, data: Optional[bytes] = None) -> Optional[services.RoutineControl.InterpretedResponse]:
        """
        Sends a generic request for the :ref:`RoutineControl<RoutineControl>` service with custom subfunction (control_type).

        :Effective configuration: ``exception_on_<type>_response``

        :param control_type: The service subfunction. See :class:`RoutineControl.ControlType<udsoncan.services.RoutineControl.ControlType>`
        :type group: int

        :param routine_id: The 16-bit numerical ID of the routine
        :type group: int

        :param data: Optional additional data to give to the server
        :type data: bytes

        :return: The server response parsed by :meth:`RoutineControl.interpret_response<udsoncan.services.RoutineControl.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        request = services.RoutineControl.make_request(routine_id, control_type, data=data)
        payload_length = 0 if data is None else len(data)
        action = "ISOSAEReserved action for routine ID"
        if control_type == services.RoutineControl.ControlType.startRoutine:
            action = "Starting routine ID"
        elif control_type == services.RoutineControl.ControlType.stopRoutine:
            action = "Stoping routine ID"
        elif control_type == services.RoutineControl.ControlType.requestRoutineResults:
            action = "Requesting result for routine ID"

        self.logger.info("%s - ControlType=0x%02x - %s 0x%04x (%s) with a payload of %d bytes" %
                         (self.service_log_prefix(services.RoutineControl), control_type, action, routine_id, Routine.name_from_id(routine_id), payload_length))
        if data is not None:
            self.logger.debug("\tPayload data : %s" % binascii.hexlify(data).decode('ascii'))

        response = self.send_request(request)
        if response is None:
            return None
        response = services.RoutineControl.interpret_response(response)

        if control_type != response.service_data.control_type_echo:
            raise UnexpectedResponseException(response, "Control type of response (0x%02x) does not match request control type (0x%02x)" % (
                response.service_data.control_type_echo, control_type))

        if routine_id != response.service_data.routine_id_echo:
            raise UnexpectedResponseException(response, "Response received from server (ID = 0x%04x) does not match the requested routine ID (0x%04x)" % (
                response.service_data.routine_id_echo, routine_id))

        return response

    def read_extended_timing_parameters(self) -> Optional[services.AccessTimingParameter.InterpretedResponse]:
        """
        Reads the timing parameters from the server with :ref:`AccessTimingParameter<AccessTimingParameter>` service with subfunction ``readExtendedTimingParameterSet`` (0x01).

        :Effective configuration: ``exception_on_<type>_response``

        :return: The server response parsed by :meth:`AccessTimingParameter.interpret_response<udsoncan.services.AccessTimingParameter.interpret_response>`
        :rtype: :ref:`Response<Response>`

        """
        return self.access_timing_parameter(access_type=services.AccessTimingParameter.AccessType.readExtendedTimingParameterSet)

    def reset_default_timing_parameters(self) -> Optional[services.AccessTimingParameter.InterpretedResponse]:
        """
        Resets the server timing parameters to their default value with :ref:`AccessTimingParameter<AccessTimingParameter>` service with subfunction ``setTimingParametersToDefaultValues`` (0x02).

        :Effective configuration: ``exception_on_<type>_response``

        :return: The server response parsed by :meth:`AccessTimingParameter.interpret_response<udsoncan.services.AccessTimingParameter.interpret_response>`
        :rtype: :ref:`Response<Response>`

        """
        return self.access_timing_parameter(access_type=services.AccessTimingParameter.AccessType.setTimingParametersToDefaultValues)

    def read_active_timing_parameters(self) -> Optional[services.AccessTimingParameter.InterpretedResponse]:
        """
        Reads the currently active timing parameters from the server with :ref:`AccessTimingParameter<AccessTimingParameter>` service with subfunction ``readCurrentlyActiveTimingParameters`` (0x03).

        :Effective configuration: ``exception_on_<type>_response``

        :return: The server response parsed by :meth:`AccessTimingParameter.interpret_response<udsoncan.services.AccessTimingParameter.interpret_response>`
        :rtype: :ref:`Response<Response>`

        """
        return self.access_timing_parameter(access_type=services.AccessTimingParameter.AccessType.readCurrentlyActiveTimingParameters)

    def set_timing_parameters(self, params: bytes) -> Optional[services.AccessTimingParameter.InterpretedResponse]:
        """
        Sets the timing parameters into the server with :ref:`AccessTimingParameter<AccessTimingParameter>` service with subfunction ``setTimingParametersToGivenValues`` (0x04).

        :Effective configuration: ``exception_on_<type>_response``

        :param params: The parameters data. Specific to each ECU.
        :type params: bytes

        :return: The server response parsed by :meth:`AccessTimingParameter.interpret_response<udsoncan.services.AccessTimingParameter.interpret_response>`
        :rtype: :ref:`Response<Response>`

        """
        return self.access_timing_parameter(access_type=services.AccessTimingParameter.AccessType.setTimingParametersToGivenValues, timing_param_record=params)

    @standard_error_management
    def access_timing_parameter(self,
                                access_type: int,
                                timing_param_record: Optional[bytes] = None
                                ) -> Optional[services.AccessTimingParameter.InterpretedResponse]:
        """
        Sends a generic request for :ref:`AccessTimingParameter<AccessTimingParameter>` service with configurable subfunction (access_type).

        :Effective configuration: ``exception_on_<type>_response``

        :param access_type: The service subfunction. See :class:`AccessTimingParameter.AccessType<udsoncan.services.AccessTimingParameter.AccessType>`
        :type access_type: int

        :param params: The parameters data. Specific to each ECU.
        :type params: bytes

        :return: The server response parsed by :meth:`AccessTimingParameter.interpret_response<udsoncan.services.AccessTimingParameter.interpret_response>`
        :rtype: :ref:`Response<Response>`

        """

        request = services.AccessTimingParameter.make_request(access_type, timing_param_record)
        payload_length = 0 if timing_param_record is None else len(timing_param_record)

        self.logger.info('%s - AccessType=0x%02x (%s) - Sending request with record payload of %d bytes' % (self.service_log_prefix(
            services.AccessTimingParameter), access_type, services.AccessTimingParameter.AccessType.get_name(access_type), payload_length))
        if timing_param_record is not None:
            self.logger.debug("Payload data : %s" % binascii.hexlify(timing_param_record).decode('ascii'))

        response = self.send_request(request)
        if response is None:
            return None

        response = services.AccessTimingParameter.interpret_response(response)

        if access_type != response.service_data.access_type_echo:
            raise UnexpectedResponseException(response, "Access type of response (0x%02x) does not match request access type (0x%02x)" % (
                response.service_data.access_type_echo, access_type))

        allowed_response_record_access_type = [
            services.AccessTimingParameter.AccessType.readExtendedTimingParameterSet,
            services.AccessTimingParameter.AccessType.readCurrentlyActiveTimingParameters
        ]

        if len(response.service_data.timing_param_record) > 0 and access_type not in allowed_response_record_access_type:
            self.logger.warning("Server returned data in the AccessTimingParameter response although none was asked")

        return response

    @standard_error_management
    def communication_control(self,
                              control_type: int,
                              communication_type: Union[int, bytes, CommunicationType],
                              node_id: Optional[int] = None,
                              ) -> Optional[services.CommunicationControl.InterpretedResponse]:
        """
        Switches the transmission or reception of certain messages on/off with :ref:`CommunicationControl<CommunicationControl>` service.

        :Effective configuration: ``exception_on_<type>_response``

        :param control_type: The action to request such as enabling or disabling some messages. See :class:`CommunicationControl.ControlType<udsoncan.services.CommunicationControl.ControlType>`. This value can also be ECU manufacturer-specific
        :type control_type: int

        :param communication_type: Indicates what section of the network and the type of message that should be affected by the command. Refer to :ref:`CommunicationType<CommunicationType>` for more details. If an `integer` or a `bytes` is given, the value will be decoded to create the required :ref:`CommunicationType<CommunicationType>` object
        :type communication_type: :ref:`CommunicationType<CommunicationType>`, bytes, int

        :param node_id: DTC memory identifier (nodeIdentificationNumber). This value is user defined and introduced in 2013 version of ISO-14229-1. 
            Possible only when control type is ``enableRxAndDisableTxWithEnhancedAddressInformation`` or ``enableRxAndTxWithEnhancedAddressInformation``
            Only added to the request payload when different from None. Default : None
        :type node_id: int

        :return: The server response parsed by :meth:`CommunicationControl.interpret_response<udsoncan.services.CommunicationControl.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """

        communication_type = services.CommunicationControl.normalize_communication_type(communication_type)

        request = services.CommunicationControl.make_request(
            control_type, communication_type, node_id, standard_version=self.config['standard_version'])

        self.logger.info('%s - ControlType=0x%02x (%s) - Sending request with a CommunicationType byte of 0x%02x (%s). nodeIdentificationNumber=%s ' % (
            self.service_log_prefix(services.CommunicationControl),
            control_type,
            services.CommunicationControl.ControlType.get_name(control_type),
            communication_type.get_byte_as_int(),
            str(communication_type),
            str(node_id)
        )
        )

        response = self.send_request(request)
        if response is None:
            return None

        response = services.CommunicationControl.interpret_response(response)

        if control_type != response.service_data.control_type_echo:
            raise UnexpectedResponseException(response, "Control type of response (0x%02x) does not match request control type (0x%02x)" % (
                response.service_data.control_type_echo, control_type))

        return response

    def request_download(self,
                         memory_location: MemoryLocation,
                         dfi: Optional[DataFormatIdentifier] = None
                         ) -> Optional[services.RequestDownload.InterpretedResponse]:
        """
        Informs the server that the client wants to initiate a download from the client to the server by sending a :ref:`RequestDownload<RequestDownload>` service request.

        :Effective configuration: ``exception_on_<type>_response`` ``server_address_format`` ``server_memorysize_format``

        :param memory_location: The address and size of the memory block to be written.
        :type memory_location: :ref:`MemoryLocation <MemoryLocation>`

        :param dfi: Optional :ref:`DataFormatIdentifier <DataFormatIdentifier>` defining the compression and encryption scheme of the data. 
                If not specified, the default value of 00 will be used, specifying no encryption and no compression
        :type dfi: :ref:`DataFormatIdentifier <DataFormatIdentifier>`

        :return: The server response parsed by :meth:`RequestDownload.interpret_response<udsoncan.services.RequestDownload.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        response = self.request_upload_download(services.RequestDownload, memory_location, dfi)
        return cast(Optional[services.RequestDownload.InterpretedResponse], response)

    def request_upload(self,
                       memory_location: MemoryLocation,
                       dfi: Optional[DataFormatIdentifier] = None
                       ) -> Optional[services.RequestUpload.InterpretedResponse]:
        """
        Informs the server that the client wants to initiate an upload from the server to the client by sending a :ref:`RequestUpload<RequestUpload>` service request.

        :Effective configuration: ``exception_on_<type>_response`` ``server_address_format`` ``server_memorysize_format``

        :param memory_location: The address and size of the memory block to be written.
        :type memory_location: :ref:`MemoryLocation <MemoryLocation>`

        :param dfi: Optional :ref:`DataFormatIdentifier <DataFormatIdentifier>` defining the compression and encryption scheme of the data. 
                If not specified, the default value of 00 will be used, specifying no encryption and no compression
        :type dfi: :ref:`DataFormatIdentifier <DataFormatIdentifier>`

        :return: The server response parsed by :meth:`RequestUpload.interpret_response<udsoncan.services.RequestUpload.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        response = self.request_upload_download(services.RequestUpload, memory_location, dfi)
        return cast(Optional[services.RequestUpload.InterpretedResponse], response)

    # Common code for both RequestDownload and RequestUpload services
    @standard_error_management
    def request_upload_download(self, service_cls, memory_location, dfi=None):
        dfi = service_cls.normalize_data_format_identifier(dfi)

        if service_cls not in [services.RequestDownload, services.RequestUpload]:
            raise ValueError('Service must either be RequestDownload or RequestUpload')

        if not isinstance(memory_location, MemoryLocation):
            raise ValueError('memory_location must be an instance of MemoryLocation')

        # If user does not specify a byte format, we apply the one in client configuration.
        if 'server_address_format' in self.config:
            memory_location.set_format_if_none(address_format=self.config['server_address_format'])

        if 'server_memorysize_format' in self.config:
            memory_location.set_format_if_none(memorysize_format=self.config['server_memorysize_format'])

        request = service_cls.make_request(memory_location=memory_location, dfi=dfi)

        action = ""
        if service_cls == services.RequestDownload:
            action = "Requesting a download (client to server)"
        elif service_cls == services.RequestUpload:
            action = "Requesting an upload (server to client)"
        else:
            raise ValueError("Bad service")

        self.logger.info('%s - %s for memory location [%s] and DataFormatIdentifier 0x%02x (%s)' %
                         (self.service_log_prefix(service_cls), action, str(memory_location), dfi.get_byte_as_int(), str(dfi)))

        response = self.send_request(request)
        if response is None:
            return None
        service_cls.interpret_response(response)

        return response

    @standard_error_management
    def transfer_data(self,
                      sequence_number: int,
                      data: Optional[bytes] = None
                      ) -> Optional[services.TransferData.InterpretedResponse]:
        """
        Transfer a block of data to/from the client to/from the server by sending a :ref:`TransferData<TransferData>` service request and returning the server response.

        :Effective configuration: ``exception_on_<type>_response``

        :param sequence_number: Corresponds to an 8bit counter that should increment for each new block transferred.
                Allowed values are from 0 to 0xFF
        :type sequence_number: int

        :param data: Optional additional data to send to the server
        :type data: bytes

        :return: The server response parsed by :meth:`TransferData.interpret_response<udsoncan.services.TransferData.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        request = services.TransferData.make_request(sequence_number, data)

        data_len = 0 if data is None else len(data)
        self.logger.info('%s - Sending a block of data with SequenceNumber=%d that is %d bytes long .' %
                         (self.service_log_prefix(services.TransferData), sequence_number, data_len))
        if data is not None:
            self.logger.debug('Data to transfer : %s' % binascii.hexlify(data).decode('ascii'))

        response = self.send_request(request)
        if response is None:
            return None
        response = services.TransferData.interpret_response(response)

        if sequence_number != response.service_data.sequence_number_echo:
            raise UnexpectedResponseException(response, "Block sequence number of response (0x%02x) does not match request block sequence number (0x%02x)" % (
                response.service_data.sequence_number_echo, sequence_number))

        return response

    @standard_error_management
    def request_transfer_exit(self, data: Optional[bytes] = None) -> Optional[services.RequestTransferExit.InterpretedResponse]:
        """
        Informs the server that the client wants to stop the data transfer by sending a :ref:`RequestTransferExit<RequestTransferExit>` service request.

        :Effective configuration: ``exception_on_<type>_response``

        :param data: Optional additional data to send to the server
        :type data: bytes

        :return: The server response parsed by :meth:`RequestTransferExit.interpret_response<udsoncan.services.RequestTransferExit.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        request = services.RequestTransferExit.make_request(data)
        self.logger.info('%s - Sending exit request' % (self.service_log_prefix(services.RequestTransferExit)))

        response = self.send_request(request)
        if response is None:
            return None
        response = services.RequestTransferExit.interpret_response(response)

        return response

    @standard_error_management
    def link_control(self, control_type: int, baudrate: Optional[Baudrate] = None) -> Optional[services.LinkControl.InterpretedResponse]:
        """
        Controls the communication baudrate by sending a :ref:`LinkControl<LinkControl>` service request.

        :Effective configuration: ``exception_on_<type>_response``

        :param control_type: Allowed values are from 0 to 0xFF. See :class:`LinkControl.ControlType<udsoncan.services.LinkControl.ControlType>`
        :type control_type: int

        :param baudrate: Required baudrate value when ``control_type`` is either ``verifyBaudrateTransitionWithFixedBaudrate`` (1) or ``verifyBaudrateTransitionWithSpecificBaudrate`` (2)
        :type baudrate: :ref:`Baudrate <Baudrate>`

        :return: The server response parsed by :meth:`LinkControl.interpret_response<udsoncan.services.LinkControl.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        request = services.LinkControl.make_request(control_type, baudrate)
        baudrate_str = 'No baudrate specified' if baudrate is None else 'Baudrate : ' + str(baudrate)

        action = "Performing LinkControl request"
        if control_type in [services.LinkControl.ControlType.verifyBaudrateTransitionWithFixedBaudrate, services.LinkControl.ControlType.verifyBaudrateTransitionWithSpecificBaudrate]:
            action = "Verifiying support"
        elif control_type == services.LinkControl.ControlType.transitionBaudrate:
            action = "Switching"

        self.logger.info('%s - ControlType=0x%02x (%s) - %s for baudrate %s ' % (self.service_log_prefix(services.LinkControl),
                         control_type, services.LinkControl.ControlType.get_name(control_type), action, baudrate_str))

        response = self.send_request(request)
        if response is None:
            return None
        response = services.LinkControl.interpret_response(response)

        if control_type != response.service_data.control_type_echo:
            raise UnexpectedResponseException(response, "Control type of response (0x%02x) does not match request control type (0x%02x)" % (
                response.service_data.control_type_echo, control_type))

        return response

    @standard_error_management
    def io_control(self,
                   did: int,
                   control_param: Optional[int] = None,
                   values: Optional[Union[List[Any], Dict[str, Any], IOValues]] = None,
                   masks: Optional[Union[List[str], Dict[str, bool], IOMasks, bool]] = None
                   ) -> Optional[services.InputOutputControlByIdentifier.InterpretedResponse]:
        """
        Substitutes the value of an input signal or overrides the state of an output by sending a :ref:`InputOutputControlByIdentifier<InputOutputControlByIdentifier>` service request.

        :Effective configuration: ``exception_on_<type>_response`` ``input_output`` ``tolerate_zero_padding``

        :param did: Data identifier to represent the IO
        :type did: int

        :param control_param: Optional parameter that can be a value from :class:`InputOutputControlByIdentifier.ControlParam<udsoncan.services.InputOutputControlByIdentifier.ControlParam>`
        :type control_param: int

        :param values: Optional values to send to the server. This parameter will be given to :ref:`DidCodec<DidCodec>`.encode() method. 
                It can be:

                        - A list for positional arguments
                        - A dict for named arguments
                        - An instance of :ref:`IOValues<IOValues>` for mixed arguments

        :type values: list, dict, :ref:`IOValues<IOValues>`

        :param masks: Optional mask record for composite values. The mask definition must be included in ``config['input_output']``
                It can be:

                        - A list naming the bit mask to set
                        - A dict with the mask name as a key and a boolean setting or clearing the mask as the value
                        - An instance of :ref:`IOMask<IOMask>`
                        - A boolean value to set all masks to the same value.
        :type masks: list, dict, :ref:`IOMask<IOMask>`, bool

        :return: The server response parsed by :meth:`InputOutputControlByIdentifier.interpret_response<udsoncan.services.InputOutputControlByIdentifier.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        if 'input_output' not in self.config:
            raise ConfigError('input_output', msg='input_output must be defined in client configuration in order to use InputOutputControlByIdentifier service')

        request = services.InputOutputControlByIdentifier.make_request(
            did,
            control_param=control_param,
            values=values,
            masks=masks,
            ioconfig=self.config['input_output']
        )

        control_param_str = 'no control parameter' if control_param is None else 'control parameter 0x%02x (%s)' % (
            control_param, services.InputOutputControlByIdentifier.ControlParam.get_name(control_param))
        self.logger.info('%s - Sending request for DID=0x%04x, %s.' %
                         (self.service_log_prefix(services.InputOutputControlByIdentifier), did, control_param_str))

        response = self.send_request(request)
        if response is None:
            return None
        response = services.InputOutputControlByIdentifier.interpret_response(
            response,
            control_param=control_param,
            tolerate_zero_padding=self.config['tolerate_zero_padding'],
            ioconfig=self.config['input_output']
        )

        if response.service_data.did_echo != did:
            raise UnexpectedResponseException(
                response, "Echo of the DID number (0x%04x) does not match the value in the request (0x%04x)" % (response.service_data.did_echo, did))

        if control_param != response.service_data.control_param_echo:
            control_param_str = '0x%02x' % control_param if control_param is not None else '<None>'
            received_control_param_str = '0x%02x' % response.service_data.control_param_echo if response.service_data.control_param_echo is not None else '<None>'
            raise UnexpectedResponseException(response, 'Echo of the InputOutputControlParameter (%s) does not match the value in the request (%s)' % (
                received_control_param_str, control_param_str))

        return response

    @standard_error_management
    def control_dtc_setting(self, setting_type: int, data: Optional[bytes] = None) -> Optional[services.ControlDTCSetting.InterpretedResponse]:
        """
        Controls some settings related to the Diagnostic Trouble Codes by sending a :ref:`ControlDTCSetting<ControlDTCSetting>` service request. 
        It can enable/disable some DTCs or perform some ECU specific configuration.

        :Effective configuration: ``exception_on_<type>_response``

        :param setting_type: Allowed values are from 0 to 0x7F. See :class:`ControlDTCSetting.SettingType<udsoncan.services.ControlDTCSetting.SettingType>`
        :type setting_type: int

        :param data: Optional additional data sent with the request called `DTCSettingControlOptionRecord`
        :type data: bytes

        :return: The server response parsed by :meth:`ControlDTCSetting.interpret_response<udsoncan.services.ControlDTCSetting.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        request = services.ControlDTCSetting.make_request(setting_type, data)
        data_len = 0 if data is None else len(data)
        action = "Performing ControlDTCSetting request"
        if setting_type == services.ControlDTCSetting.SettingType.on:
            action = "Turning DTC On"
        elif setting_type == services.ControlDTCSetting.SettingType.off:
            action = "Turning DTC Off"

        self.logger.info('%s - SettingType=0x%02x (%s) - %s with a payload of %d bytes' % (self.service_log_prefix(services.ControlDTCSetting),
                         setting_type, services.ControlDTCSetting.SettingType.get_name(setting_type), action, data_len))
        if data is not None:
            self.logger.debug("Payload of data : %s" % binascii.hexlify(data).decode('ascii'))

        response = self.send_request(request)
        if response is None:
            return None

        response = services.ControlDTCSetting.interpret_response(response)

        if response.service_data.setting_type_echo != setting_type:
            raise UnexpectedResponseException(response, "Setting type of response (0x%02x) does not match request control type (0x%02x)" % (
                response.service_data.setting_type_echo, setting_type))

        return response

    @standard_error_management
    def read_memory_by_address(self, memory_location: MemoryLocation) -> Optional[services.ReadMemoryByAddress.InterpretedResponse]:
        """
        Reads a block of memory from the server by sending a :ref:`ReadMemoryByAddress<ReadMemoryByAddress>` service request. 

        :Effective configuration: ``exception_on_<type>_response`` ``server_address_format`` ``server_memorysize_format``

        :param memory_location: The address and the size of the memory block to read.
        :type memory_location: :ref:`MemoryLocation <MemoryLocation>`

        :return: The server response parsed by :meth:`ReadMemoryByAddress.interpret_response<udsoncan.services.ReadMemoryByAddress.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        if not isinstance(memory_location, MemoryLocation):
            raise ValueError('memory_location must be an instance of MemoryLocation')

        if 'server_address_format' in self.config:
            memory_location.set_format_if_none(address_format=self.config['server_address_format'])

        if 'server_memorysize_format' in self.config:
            memory_location.set_format_if_none(memorysize_format=self.config['server_memorysize_format'])

        request = services.ReadMemoryByAddress.make_request(memory_location)
        self.logger.info('%s - Reading memory address at %s' % (self.service_log_prefix(services.ReadMemoryByAddress), str(memory_location)))

        response = self.send_request(request)
        if response is None:
            return None
        response = services.ReadMemoryByAddress.interpret_response(response)
        memdata = response.service_data.memory_block

        if len(memdata) < memory_location.memorysize:
            raise UnexpectedResponseException(response, 'Data block given by the server is too short. Client requested for %d bytes but only received %s bytes' % (
                memory_location.memorysize, str(len(response.data)) if response.data is not None else '<None>'))

        if len(memdata) > memory_location.memorysize:
            extra_bytes = len(memdata) - memory_location.memorysize
            if memdata[memory_location.memorysize:] == b'\x00' * extra_bytes and self.config['tolerate_zero_padding']:
                response.service_data.memory_block = memdata[0:memory_location.memorysize]  # trim exceeding zeros
            else:
                raise UnexpectedResponseException(response, 'Data block given by the server is too long. Client requested for %d bytes but received %s bytes' % (
                    memory_location.memorysize, str(len(response.data)) if response.data is not None else '<None>'))

        return response

    @standard_error_management
    def write_memory_by_address(self, memory_location: MemoryLocation, data: bytes) -> Optional[services.WriteMemoryByAddress.InterpretedResponse]:
        """
        Writes a block of memory in the server by sending a :ref:`WriteMemoryByAddress<WriteMemoryByAddress>` service request. 

        :Effective configuration: ``exception_on_<type>_response`` ``server_address_format`` ``server_memorysize_format``

        :param memory_location: The address and the size of the memory block to read. 
        :type memory_location: :ref:`MemoryLocation <MemoryLocation>`

        :param data: The data to write into memory.
        :type data: bytes

        :return: The server response parsed by :meth:`WriteMemoryByAddress.interpret_response<udsoncan.services.WriteMemoryByAddress.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """

        if not isinstance(memory_location, MemoryLocation):
            raise ValueError('memory_location must be an instance of MemoryLocation')

        if 'server_address_format' in self.config:
            memory_location.set_format_if_none(address_format=self.config['server_address_format'])

        if 'server_memorysize_format' in self.config:
            memory_location.set_format_if_none(memorysize_format=self.config['server_memorysize_format'])

        request = services.WriteMemoryByAddress.make_request(memory_location, data)
        self.logger.info('%s - Writing %d bytes to memory address at %s' %
                         (self.service_log_prefix(services.WriteMemoryByAddress), len(data), str(memory_location)))

        if len(data) != memory_location.memorysize:
            self.logger.warning('%s: Given data block length (%d bytes) does not match MemoryLocation size (%d bytes)' %
                                (self.service_log_prefix(services.WriteMemoryByAddress), len(data), memory_location.memorysize))

        response = self.send_request(request)
        if response is None:
            return None
        response = services.WriteMemoryByAddress.interpret_response(response, memory_location)

        alfid_byte = memory_location.alfid.get_byte_as_int()   # AddressAndLengthFormatIdentifier

        # We make sure that the echo from the server matches the request we sent.
        if response.service_data.alfid_echo != alfid_byte:
            raise UnexpectedResponseException(response, 'AddressAndLengthFormatIdentifier echoed back by the server (0x%02X) does not match the one requested by the client (0x%02X)' % (
                response.service_data.alfid_echo, int(alfid_byte)))

        if response.service_data.memory_location_echo.address != memory_location.address:
            raise UnexpectedResponseException(response, 'Address echoed back by the server (0x%X) does not match the one requested by the client (0x%X)' % (
                response.service_data.memory_location_echo.address, memory_location.address))

        if response.service_data.memory_location_echo.memorysize != memory_location.memorysize:
            raise UnexpectedResponseException(response, 'Memory size echoed back by the server (0x%X) does not match the one requested by the client (0x%X)' % (
                response.service_data.memory_location_echo.memorysize, memory_location.memorysize))

        return response

# ====  ReadDTCInformation
    def get_dtc_by_status_mask(self, status_mask: int) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportDTCByStatusMask``

        Reads all the Diagnostic Trouble Codes that have a status matching the given mask. 
        The server will check all of its DTCs and if (Dtc.status & status_mask) != 0, then the DTCs match the filter and are sent back to the client.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :param status_mask: The status mask against which the DTCs are tested. 
        :type status_mask: int or :ref:`Dtc.Status<DTC_Status>`

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportDTCByStatusMask, status_mask=status_mask)

    def get_user_defined_memory_dtc_by_status_mask(self, status_mask: int, memory_selection: int) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportUserDefMemoryDTCByStatusMask``

        Reads  Diagnostic Trouble Codes that have a status matching the given mask in a user defined memory . 
        The server will check all of its DTCs inside the user defined memory region and if (Dtc.status & status_mask) != 0, 
        then the DTCs match the filter and are sent back to the client.

        Introduced in 2020 version of ISO-14229

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc`` ``standard_version``

        :param status_mask: The status mask against which the DTCs are tested. 
        :type status_mask: int or :ref:`Dtc.Status<DTC_Status>`

        :param memory_selection: A 1 byte wide identifier for the memory region. Defined by ECU manufacturer.
        :type memory_selection: int

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportUserDefMemoryDTCByStatusMask, status_mask=status_mask, memory_selection=memory_selection)

    def get_emission_dtc_by_status_mask(self, status_mask: int) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportEmissionsRelatedOBDDTCByStatusMask``

        Reads the emission-related Diagnostic Trouble Codes that have a status matching the given mask.
        The server will check its emission-related DTCs and if (Dtc.status & status_mask) != 0, then the DTCs match the filter and are sent back to the client.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :param status_mask: The status mask against which the DTCs are tested. 
        :type status_mask: int or :ref:`Dtc.Status<DTC_Status>`

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportEmissionsRelatedOBDDTCByStatusMask, status_mask=status_mask)

    def get_mirrormemory_dtc_by_status_mask(self, status_mask: int) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportMirrorMemoryDTCByStatusMask``

        Reads all the Diagnostic Trouble Codes stored in mirror memory that have a status matching the given mask. 
        The server will check all of its DTCs and if (Dtc.status & status_mask) != 0, then the DTCs match the filter and are sent back to the client.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :param status_mask: The status mask against which the DTCs are tested. 
        :type status_mask: int or :ref:`Dtc.Status<DTC_Status>`

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportMirrorMemoryDTCByStatusMask, status_mask=status_mask)

    def get_dtc_by_status_severity_mask(self, status_mask: int, severity_mask: int) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportDTCBySeverityMaskRecord``

        Reads all the Diagnostic Trouble Codes that have a status and a severity matching the given masks. 
        The server will check all of its DTCs and if ( (Dtc.status & status_mask) != 0 && (Dtc.severity & severity) !=0), then the DTCs match the filter and are sent back to the client.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :param status_mask: The status mask against which the DTCs are tested. 
        :type status_mask: int or :ref:`Dtc.Status<DTC_Status>`

        :param severity_mask: The severity mask against which the DTCs are tested. 
        :type severity_mask: int or :ref:`Dtc.Severity<DTC_Severity>`

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportDTCBySeverityMaskRecord, status_mask=status_mask, severity_mask=severity_mask)

    def get_wwh_obd_dtc_by_status_mask(self, 
                                       functional_group_id: int, 
                                       status_mask: int, 
                                       severity_mask: Union[int,Dtc.Severity], 
                                       dtc_class: Union[int, Dtc.DtcClass]
                                       ) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportWWHOBDDTCByMaskRecord``

        Reads all the WWH OBD Diagnostic Trouble Codes that have a functional_group, class, status and a severity matching the given masks. 
        The server will check all of its DTCs and if ( (Dtc.status & status_mask) != 0 && (Dtc.severity & severity) !=0), then the DTCs match the filter and are sent back to the client.
        Note: severity_mask and dtc_class are combined into a single byte to populate DTCSeverityMask- see Table D.11.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :param functional_group_id: Functional Group ID to search for (FGID) (0x00 to 0xFE) :ref:`Dtc.FunctionalGroupIdentifiers<DTC_FunctionalGroupIdentifiers>` 
        :type functional_group_id: int

        :param status_mask: The status mask against which the DTCs are tested. 
        :type status_mask: int or :ref:`Dtc.Status<DTC_Status>`

        :param severity_mask: The severity mask against which the DTCs are tested. (Bit mask of: 0x20, 0x40, or 0x80)
        :type severity_mask: int or :ref:`Dtc.Severity<DTC_Severity>`

        :param dtc_class: The GTR DTC class mask against which the DTCs are tested. (Bit mask of: 0x01, 0x02, 0x04, 0x08, 0x10)
        :type dtc_class: int or :ref:`Dtc.DtcClass<DTC_DtcClass>`

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """

        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportWWHOBDDTCByMaskRecord, status_mask=status_mask, severity_mask=severity_mask, dtc_class=dtc_class, functional_group_id=functional_group_id)
    
    def get_wwh_obd_dtc_with_permanent_status(self, functional_group_id: int) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportWWHOBDDTCWithPermanentStatus,``

        Reads all the WWH OBD Diagnostic Trouble Codes that have the specified functional_group and a permanent status. 

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :param functional_group_id: Functional Group ID to search for (FGID) (0x00 to 0xFE) :ref:`Dtc.FunctionalGroupIdentifiers<DTC_FunctionalGroupIdentifiers>` 
        :type functional_group_id: int


        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """

        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportWWHOBDDTCWithPermanentStatus, functional_group_id=functional_group_id)

    def get_number_of_dtc_by_status_mask(self, status_mask: Union[int, Dtc.Status]) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportNumberOfDTCByStatusMask``

        Gets the number of DTCs that match the specified status mask.

        :Effective configuration: ``exception_on_<type>_response``

        :param status_mask: The status mask against which the DTCs are tested. 
        :type status_mask: int or :ref:`Dtc.Status<DTC_Status>`

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportNumberOfDTCByStatusMask, status_mask=status_mask)

    def get_mirrormemory_number_of_dtc_by_status_mask(self, status_mask: Union[int, Dtc.Status]) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportNumberOfMirrorMemoryDTCByStatusMask``

        Gets the number of DTCs that match the specified status mask in mirror memory.

        :Effective configuration: ``exception_on_<type>_response``

        :param status_mask: The status mask against which the DTCs are tested. 
        :type status_mask: int or :ref:`Dtc.Status<DTC_Status>`

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportNumberOfMirrorMemoryDTCByStatusMask, status_mask=status_mask)

    def get_number_of_emission_dtc_by_status_mask(self, status_mask: Union[int, Dtc.Status]) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportNumberOfEmissionsRelatedOBDDTCByStatusMask``

        Gets the number of emission-related DTCs that match the specified status mask.

        :Effective configuration: ``exception_on_<type>_response``

        :param status_mask: The status mask against which the DTCs are tested. 
        :type status_mask: int or :ref:`Dtc.Status<DTC_Status>`

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportNumberOfEmissionsRelatedOBDDTCByStatusMask, status_mask=status_mask)

    def get_number_of_dtc_by_status_severity_mask(self, status_mask: Union[int, Dtc.Status], severity_mask: Union[int, Dtc.Severity]) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportNumberOfDTCBySeverityMaskRecord``

        Gets the number of DTCs that match the specified status mask and severity mask.

        :Effective configuration: ``exception_on_<type>_response``

        :param status_mask: The status mask against which the DTCs are tested. 
        :type status_mask: int or :ref:`Dtc.Status<DTC_Status>`

        :param severity_mask: The severity mask against which the DTCs are tested. 
        :type severity_mask: int or :ref:`Dtc.Severity<DTC_Severity>`

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportNumberOfDTCBySeverityMaskRecord, status_mask=status_mask, severity_mask=severity_mask)

    def get_dtc_severity(self, dtc: Union[int, Dtc]) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportSeverityInformationOfDTC``

        Requests the server for a specific DTC severity level.

        :Effective configuration: ``exception_on_<type>_response``

        :param dtc: The DTC ID for which we request the severity. It can be a 3-byte integer or a DTC instance with an ID set.
        :type dtc: int or :ref:`Dtc<DTC>`

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportSeverityInformationOfDTC, dtc=dtc)

    def get_supported_dtc(self) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportSupportedDTCs``

        Requests the list of supported DTCs by the server.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportSupportedDTCs)

    def get_first_test_failed_dtc(self) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportFirstTestFailedDTC``

        Reads a single DTC. Requests the server for the first DTC that set its ``Dtc.Status.test_failed`` bit.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportFirstTestFailedDTC)

    def get_first_confirmed_dtc(self) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportFirstConfirmedDTC``

        Reads a single DTC. Requests the server for the first DTC that set its ``Dtc.Status.confirmed`` bit.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportFirstConfirmedDTC)

    def get_most_recent_test_failed_dtc(self) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportMostRecentTestFailedDTC``

        Reads a single DTC. Requests the server for the last DTC that set its ``Dtc.Status.test_failed`` bit.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportMostRecentTestFailedDTC)

    def get_most_recent_confirmed_dtc(self) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportMostRecentConfirmedDTC``

        Reads a single DTC. Requests the server for the last DTC that set its ``Dtc.Status.confirmed`` bit.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportMostRecentConfirmedDTC)

    def get_dtc_with_permanent_status(self) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportDTCWithPermanentStatus``

        Returns all DTCs that the server marked as `permanent`. 

        A permanent DTC is a DTC stored in Non-Volatile memory and that cannot be erased by test equipment or by power-cycling the ECU.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportDTCWithPermanentStatus)

    def get_dtc_fault_counter(self) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportDTCFaultDetectionCounter``

        Requests the server for all DTCs that are `prefailed` along with their fault detection counter. 

        A prefailed DTC is a DTC for which the detection condition is met, but has not been identified as `pending` or `confirmed` yet. 

        If the ECU follows the UDS guidelines, it will wait to detect a fault many times before setting a status bit for this fault DTC. Each time the fault is detected, a fault counter is incremented, when it is not detected, the counter is decremented.
        Once the fault counter reaches a threshold, a status bit is set and the DTC is not `prefailed` anymore. A `prefailed` DTC is any DTC that has fault detection counter greater than 0, but less than the detection threshold.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportDTCFaultDetectionCounter)

    def get_dtc_snapshot_identification(self) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportDTCSnapshotIdentification``

        Requests the server to return an index of all the DTC snapshots available. The server will respond with a list of DTCs and a list of snapshot record numbers for each DTC.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc``

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportDTCSnapshotIdentification)

    def get_dtc_snapshot_by_dtc_number(self, dtc, record_number=0xFF) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportDTCSnapshotRecordByDTCNumber``

        Requests the server for one or many specific DTC snapshots associated with a single DTC.
        Each snapshot has a data identifier associated with it. The data will be decoded using the associated :ref:`DidCodec<DidCodec>` defined in ``config['data_identifiers']``.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc`` ``dtc_snapshot_did_size``

        :param dtc: The DTC ID for which we request the snapshot data. It can be a 3-byte integer or a DTC instance with an ID set.
        :type dtc: int or :ref:`Dtc<DTC>`

        :param record_number: The record number of the snapshot data to read. If 0xFF is given, then all snapshots will be read, otherwise, a single snapshot will be read.
        :type record_number: int

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportDTCSnapshotRecordByDTCNumber, dtc=dtc, snapshot_record_number=record_number)

    def get_user_defined_dtc_snapshot_by_dtc_number(self, dtc: Union[Dtc, int], memory_selection: int, record_number: int = 0xFF) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportUserDefMemoryDTCSnapshotRecordByDTCNumber``

        Requests the server for one or many specific DTC snapshots associated with a single DTC in a user defined memory.
        Each snapshot has a data identifier associated with it. The data will be decoded using the associated :ref:`DidCodec<DidCodec>` defined in ``config['data_identifiers']``.

        Introduced in 2020 version of ISO-14229

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc`` ``dtc_snapshot_did_size`` ``standard_version`` 

        :param dtc: The DTC ID for which we request the snapshot data. It can be a 3-byte integer or a DTC instance with an ID set.
        :type dtc: int or :ref:`Dtc<DTC>`

        :param record_number: The record number of the snapshot data to read. If 0xFF is given, then all snapshots will be read, otherwise, a single snapshot will be read.
        :type record_number: int

        :param memory_selection: A 1 byte wide identifier for the memory region. Defined by ECU manufacturer.
        :type memory_selection: int      

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportUserDefMemoryDTCSnapshotRecordByDTCNumber, dtc=dtc, snapshot_record_number=record_number, memory_selection=memory_selection)

    def get_dtc_snapshot_by_record_number(self, record_number: int = 0xFF) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportDTCSnapshotRecordByRecordNumber``

        Requests the server for one or many DTC snapshots by specifying a record number. This functionality can exist only if the server assigns globally unique record_numbers to DTC snapshots, regardless of the DTC ID.

        Each snapshot has a data identifier associated with it. The data will be decoded using the associated :ref:`DidCodec<DidCodec>` defined in ``config['data_identifiers']``.

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding``  ``dtc_snapshot_did_size``

        :param record_number: The record number of the snapshot data to read. If 0xFF is given, then all snapshots will be read, otherwise, a single snapshot will be read.
        :type record_number: int

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportDTCSnapshotRecordByRecordNumber, snapshot_record_number=record_number)

    def get_dtc_extended_data_by_dtc_number(self, dtc: Union[int, Dtc], record_number: int = 0xFF, data_size: Optional[int] = None) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportDTCExtendedDataRecordByDTCNumber``

        Requests the server for one or many DTC **extended data** by specifying a DTC and an record number. This mehtod may return a single DTC containing multiple records of extended data

        The DTC extended data is an ECU specific set of data that is not associated with a data identifier. Given as ``bytes``

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``extended_data_size``

        :param dtc: The DTC ID for which we request the extended data. It can be a 3-byte integer or a DTC instance with an ID set.
        :type dtc: int or :ref:`Dtc<DTC>`

        :param record_number: The record number of the extended data to read. If 0xFF is given, then all extended data entries will be read, otherwise, a single entry will be read.
        :type record_number: int

        :param data_size: The number of bytes of an extended data record. If not specified ``config['extended_data_size'][dtc]`` will be used.
        :type data_size: int or None

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportDTCExtendedDataRecordByDTCNumber, dtc=dtc, extended_data_record_number=record_number, extended_data_size=data_size)

    def get_dtc_extended_data_by_record_number(self, record_number: int, data_size: Optional[int] = None) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportDTCExtDataRecordByRecordNumber``

        Requests the server for one or many DTC **extended data** by specifying a record number only. This method may return multiple DTC containing each a single record of extended data.

        The DTC extended data is an ECU specific set of data that is not associated with a data identifier. Given as ``bytes``

        Introduced in 2020 version of ISO-14229

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``ignore_all_zero_dtc`` ``extended_data_size`` ``standard_version``

        :param record_number: The record number of the extended data to read. Value must range between 0x00 and 0xEF. 0xFF (all) cannot be used.
        :type record_number: int

        :param data_size: The number of bytes of each extended data record. If not specified ``config['extended_data_size']`` will be used. 
            Since this method can return data for multiple DTCs and data size might be different for each DTC, it is possible to pass a dictionary 
            with a size for each DTC id (just like ``extended_data_size`` configuration). Example : size = {0x123456 : 5, 0x112233 : 10}
        :type data_size: int, dict or None

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """

        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportDTCExtDataRecordByRecordNumber, extended_data_record_number=record_number, extended_data_size=data_size)

    def get_user_defined_dtc_extended_data_by_dtc_number(self, dtc: Union[int, Dtc], memory_selection: int, record_number: int = 0xFF, data_size: Optional[int] = None) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportUserDefMemoryDTCExtDataRecordByDTCNumber``

        Requests the server for one or many DTC **extended data** by specifying a DTC and an optional record number in a user defined memory.
        This mehtod may return a single DTC containing multiple records of extended data
        The DTC extended data is an ECU specific set of data that is not associated with a data identifier. Given as ``bytes``

        Introduced in 2020 version of ISO-14229

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``extended_data_size`` ``standard_version`` 

        :param dtc: The DTC ID for which we request the extended data. It can be a 3-byte integer or a DTC instance with an ID set.
        :type dtc: int or :ref:`Dtc<DTC>`

        :param memory_selection: A 1 byte wide identifier for the memory region. Defined by ECU manufacturer.
        :type memory_selection: int           

        :param record_number: The record number of the extended data to read. If 0xFF is given, then all extended data entries will be read, otherwise, a single entry will be read.
        :type record_number: int

        :param data_size: The number of bytes of an extended data record. If not specified ``config['extended_data_size'][dtc]`` will be used.
        :type data_size: int or None

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportUserDefMemoryDTCExtDataRecordByDTCNumber, dtc=dtc, memory_selection=memory_selection, extended_data_record_number=record_number, extended_data_size=data_size)

    def get_mirrormemory_dtc_extended_data_by_dtc_number(self, dtc: Union[int, Dtc], record_number: int = 0xFF, data_size: Optional[int] = None) -> Optional[services.ReadDTCInformation.InterpretedResponse]:
        """
        Performs a ``ReadDTCInformation`` service request with subfunction ``reportMirrorMemoryDTCExtendedDataRecordByDTCNumber``

        Requests the server for one or many DTC **extended data** stored in mirror memory by specifying a record number.

        The DTC extended data is an ECU specific set of data that is not associated with a data identifier. Given as ``bytes``

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding`` ``extended_data_size``

        :param dtc: The DTC ID for which we request the extended data. It can be a 3-byte integer or a DTC instance with an ID set.
        :type dtc: int or :ref:`Dtc<DTC>`

        :param record_number: The record number of the extended data to read. If 0xFF is given, then all extended data entries will be read, otherwise, a single entry will be read.
        :type record_number: int

        :param data_size: The number of bytes of an extended data record. If not specified ``config['extended_data_size'][dtc]`` wil be used.
        :type data_size: int or None

        :return: The server response parsed by :meth:`ReadDTCInformation.interpret_response<udsoncan.services.ReadDTCInformation.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.read_dtc_information(services.ReadDTCInformation.Subfunction.reportMirrorMemoryDTCExtendedDataRecordByDTCNumber, dtc=dtc, extended_data_record_number=record_number, extended_data_size=data_size)

    # Performs a ReadDiagnsticInformation service request.
    # Many requests are encoded the same way and many responses are encoded the same way. Request grouping and response grouping are independent.

    @standard_error_management
    def read_dtc_information(self,
                             subfunction: int,
                             status_mask: Optional[Union[Dtc.Status, int]] = None,
                             severity_mask: Optional[Union[Dtc.Severity, int]] = None,
                             dtc: Optional[Union[int, Dtc]] = None,
                             snapshot_record_number: Optional[int] = None,
                             extended_data_record_number: Optional[int] = None,
                             extended_data_size: Optional[int] = None,
                             memory_selection: Optional[int] = None,
                             functional_group_id: Optional[int] = None,
                             dtc_class: Optional[Union[int, Dtc.DtcClass]] = None
                             ):
        if dtc is not None and isinstance(dtc, Dtc):
            dtc = dtc.id

        request = services.ReadDTCInformation.make_request(subfunction=subfunction,
                                                           status_mask=status_mask,
                                                           severity_mask=severity_mask,
                                                           dtc=dtc,
                                                           snapshot_record_number=snapshot_record_number,
                                                           extended_data_record_number=extended_data_record_number,
                                                           memory_selection=memory_selection,
                                                           standard_version=self.config['standard_version'],
                                                           functional_group_id=functional_group_id,
                                                           dtc_class=dtc_class)

        self.logger.info('%s - Sending request with subfunction "%s" (0x%02X).' % (self.service_log_prefix(services.ReadDTCInformation),
                         services.ReadDTCInformation.Subfunction.get_name(subfunction), subfunction))
        response = self.send_request(request)
        if response is None:
            return None

        extended_data_size2: Optional[Union[int, Dict[int, int]]] = None
        if extended_data_size is None:
            if 'extended_data_size' in self.config:
                extended_data_size2 = self.config['extended_data_size']
        else:
            extended_data_size2 = extended_data_size

        # So, if subfunction is a mismatch, chances are that the response won't be decoded properly because we use the
        # request subfunction to select the decoding algorithm, not the received one.
        # We want to report the subfunction mismatch as a primary cause.
        error = None
        try:
            response = services.ReadDTCInformation.interpret_response(response, subfunction=subfunction,
                                                                      tolerate_zero_padding=self.config['tolerate_zero_padding'],
                                                                      ignore_all_zero_dtc=self.config['ignore_all_zero_dtc'],
                                                                      dtc_snapshot_did_size=self.config['dtc_snapshot_did_size'],
                                                                      didconfig=self.config['data_identifiers'] if 'data_identifiers' in self.config else None,
                                                                      extended_data_size=extended_data_size2,
                                                                      standard_version=self.config['standard_version'])
        except Exception as e:
            error = e

        # If nothing can be checked, raise the error right away
        if isinstance(error, InvalidResponseException):
            if response.service_data is None:
                raise error
            response = cast(services.ReadDTCInformation.InterpretedResponse, response)
            if cast(services.ReadDTCInformation.InterpretedResponse, response).service_data.subfunction_echo is None:
                raise error

        response = cast(services.ReadDTCInformation.InterpretedResponse, response)
        # We can report a subfunction mismatch before decoding error.
        if response.service_data.subfunction_echo != subfunction:
            received_subfn_echo = 'None' if response.service_data.subfunction_echo is None else '%02x' % response.service_data.subfunction_echo
            raise UnexpectedResponseException(
                response, 'Echo of ReadDTCInformation subfunction gotten from server (%s) does not match the value in the request subfunction (0x%02x)' % (received_subfn_echo, subfunction))

        # Nothing else to check, report the real error.
        if error:
            raise error

        if subfunction in [services.ReadDTCInformation.Subfunction.reportDTCSnapshotRecordByDTCNumber, services.ReadDTCInformation.Subfunction.reportUserDefMemoryDTCSnapshotRecordByDTCNumber]:
            if len(response.service_data.dtcs) == 1:
                assert dtc is not None
                if dtc != response.service_data.dtcs[0].id:
                    raise UnexpectedResponseException(
                        response, 'Server returned snapshot with DTC ID 0x%06x while client requested for 0x%06x' % (response.service_data.dtcs[0].id, dtc))

        if subfunction in [services.ReadDTCInformation.Subfunction.reportDTCSnapshotRecordByRecordNumber, services.ReadDTCInformation.Subfunction.reportDTCSnapshotRecordByDTCNumber, services.ReadDTCInformation.Subfunction.reportUserDefMemoryDTCSnapshotRecordByDTCNumber]:
            assert snapshot_record_number is not None
            if len(response.service_data.dtcs) == 1 and snapshot_record_number != 0xFF:
                for snapshot in response.service_data.dtcs[0].snapshots:
                    gotten_record_number = snapshot if isinstance(snapshot, int) else snapshot.record_number
                    if gotten_record_number != snapshot_record_number:
                        raise UnexpectedResponseException(response, 'Server returned snapshot with record number %s while client requested for 0x%02x' % (
                            '0x%02x' % gotten_record_number if gotten_record_number is not None else '<None>', snapshot_record_number))

        if subfunction in [services.ReadDTCInformation.Subfunction.reportDTCExtendedDataRecordByDTCNumber, services.ReadDTCInformation.Subfunction.reportMirrorMemoryDTCExtendedDataRecordByDTCNumber, services.ReadDTCInformation.Subfunction.reportUserDefMemoryDTCExtDataRecordByDTCNumber]:
            # Standard specifies that values between 0xF0 and 0xFF are for reporting groups (more than one record)
            assert extended_data_record_number is not None
            if len(response.service_data.dtcs) == 1 and extended_data_record_number < 0xF0:
                for extended_data in response.service_data.dtcs[0].extended_data:
                    assert extended_data.record_number is not None
                    if extended_data.record_number != extended_data_record_number:
                        raise UnexpectedResponseException(response, 'Extended data record number given by the server (0x%02x) does not match the record number requested by the client (0x%02x)' % (
                            extended_data.record_number, extended_data_record_number))

        if subfunction in [services.ReadDTCInformation.Subfunction.reportUserDefMemoryDTCByStatusMask, services.ReadDTCInformation.Subfunction.reportUserDefMemoryDTCSnapshotRecordByDTCNumber, services.ReadDTCInformation.Subfunction.reportUserDefMemoryDTCExtDataRecordByDTCNumber]:
            if memory_selection is not None and memory_selection != response.service_data.memory_selection_echo:
                received_ms_echo = 'None' if response.service_data.memory_selection_echo is None else '%02x' % response.service_data.memory_selection_echo
                raise UnexpectedResponseException(
                    response, 'Echo of ReadDTCInformation MemorySelection gotten from server (%s) does not match the value in the request (0x%02x)' % (received_ms_echo, memory_selection))

        if subfunction == services.ReadDTCInformation.Subfunction.reportDTCExtDataRecordByRecordNumber:
            if extended_data_record_number is not None:
                for dtc_obj in response.service_data.dtcs:
                    for extended_data in dtc_obj.extended_data:
                        if extended_data.record_number != extended_data_record_number:
                            raise UnexpectedResponseException(response, 'Extended data record number given by the server for DTC 0x%06X has a value of %d but requested record number was %d', (
                                dtc_obj.id, extended_data.record_number, extended_data_record_number))

        if subfunction in [services.ReadDTCInformation.Subfunction.reportWWHOBDDTCWithPermanentStatus, services.ReadDTCInformation.Subfunction.reportWWHOBDDTCByMaskRecord]:
            assert response.service_data.functional_group_id is not None
            assert functional_group_id is not None
            if functional_group_id != response.service_data.functional_group_id:
                raise UnexpectedResponseException(response, "FunctionalGroupIdentifier in the server response does not match the value in the request. Requested 0x%02x, got 0x%02x" % (functional_group_id, response.service_data.functional_group_id))

        if Dtc.Format.get_name(response.service_data.dtc_format) is None:
            self.logger.warning('Unknown DTC Format Identifier %s. Value should be between 0 and 4' %
                                ('0x%02x' % response.service_data.dtc_format if response.service_data.dtc_format is not None else '<None>')
                                )

        return response

    def add_file(self,
                 filename: str,
                 dfi: Optional[DataFormatIdentifier] = None,
                 filesize: Optional[Union[Filesize, int]] = None
                 ) -> Optional[services.RequestFileTransfer.InterpretedResponse]:
        """
        Sends a RequestFileTransfer request with ModeOfOperation=AddFile(1).

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding``

        :param filename: The name of the file to create, limited to ASCII characters.
        :type filename: str

        :param dfi: DataFormatIdentifier defining the compression and encryption scheme of the data. 
                If not specified, the default value of 00 will be used, specifying no encryption and no compression. 
        :type dfi: :ref:`DataFormatIdentifier<DataFormatIdentifier>`

        :param filesize: The filesize of the file to write. 
            If filesize is an object of type :ref:`Filesize<Filesize>`, the uncompressed size and compressed size will be encoded on
            the minimum amount of bytes necessary, unless a ``width`` is explicitly defined. If no compressed size is given or filesize is an ``int``,
            then the compressed size will be set equal to the uncompressed size or the integer value given as specified by ISO-14229
        :type filesize: :ref:`Filesize<Filesize>` or int

        :return: The server response parsed by :meth:`RequestFileTransfer.interpret_response<udsoncan.services.RequestFileTransfer.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.request_file_transfer(moop=services.RequestFileTransfer.ModeOfOperation.AddFile, path=filename, dfi=dfi, filesize=filesize)

    def resume_file(self,
                    filename: str,
                    dfi: Optional[DataFormatIdentifier] = None,
                    filesize: Optional[Union[Filesize, int]] = None
                    ) -> Optional[services.RequestFileTransfer.InterpretedResponse]:
        """
        Sends a RequestFileTransfer request with ModeOfOperation=ResumeFile(6).

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding``

        :param filename: The name of the file to create, limited to ASCII characters.
        :type filename: str

        :param dfi: DataFormatIdentifier defining the compression and encryption scheme of the data.
                If not specified, the default value of 00 will be used, specifying no encryption and no compression.
        :type dfi: :ref:`DataFormatIdentifier<DataFormatIdentifier>`

        :param filesize: The filesize of the file to write.
            If filesize is an object of type :ref:`Filesize<Filesize>`, the uncompressed size and compressed size will be encoded on
            the minimum amount of bytes necessary, unless a ``width`` is explicitly defined. If no compressed size is given or filesize is an ``int``,
            then the compressed size will be set equal to the uncompressed size or the integer value given as specified by ISO-14229
        :type filesize: :ref:`Filesize<Filesize>` or int

        :return: The server response parsed by :meth:`RequestFileTransfer.interpret_response<udsoncan.services.RequestFileTransfer.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.request_file_transfer(moop=services.RequestFileTransfer.ModeOfOperation.ResumeFile, path=filename, dfi=dfi, filesize=filesize)

    def delete_file(self, filename: str,) -> Optional[services.RequestFileTransfer.InterpretedResponse]:
        """
        Sends a RequestFileTransfer request with ModeOfOperation=DeleteFile(2).

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding``

        :param filename: The name of the file to delete, limited to ASCII characters.
        :type filename: str

        :return: The server response parsed by :meth:`RequestFileTransfer.interpret_response<udsoncan.services.RequestFileTransfer.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.request_file_transfer(moop=services.RequestFileTransfer.ModeOfOperation.DeleteFile, path=filename)

    def replace_file(self,
                     filename: str,
                     dfi: Optional[DataFormatIdentifier] = None,
                     filesize: Optional[Union[Filesize, int]] = None
                     ) -> Optional[services.RequestFileTransfer.InterpretedResponse]:
        """
        Sends a RequestFileTransfer request with ModeOfOperation=ReplaceFile(3).

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding``

        :param filename: The name of the file to replace, limited to ASCII characters.
        :type filename: str

        :param dfi: DataFormatIdentifier defining the compression and encryption scheme of the data. 
                If not specified, the default value of 00 will be used, specifying no encryption and no compression. 
        :type dfi: :ref:`DataFormatIdentifier<DataFormatIdentifier>`

        :param filesize: The filesize of the file to write. 
            If filesize is an object of type :ref:`Filesize<Filesize>`, the uncompressed size and compressed size will be encoded on
            the minimum amount of bytes necessary, unless a ``width`` is explicitly defined. If no compressed size is given or filesize is an ``int``,
            then the compressed size will be set equal to the uncompressed size or the integer value given as specified by ISO-14229
        :type filesize: :ref:`Filesize<Filesize>` or int

        :return: The server response parsed by :meth:`RequestFileTransfer.interpret_response<udsoncan.services.RequestFileTransfer.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.request_file_transfer(moop=services.RequestFileTransfer.ModeOfOperation.ReplaceFile, path=filename, dfi=dfi, filesize=filesize)

    def read_file(self,
                  filename: str,
                  dfi: Optional[DataFormatIdentifier] = None
                  ) -> Optional[services.RequestFileTransfer.InterpretedResponse]:
        """
        Sends a RequestFileTransfer request with ModeOfOperation=ReadFile(4).

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding``

        :param filename: The name of the file to read, limited to ASCII characters.
        :type filename: str

        :param dfi: DataFormatIdentifier defining the compression and encryption scheme of the data. 
                If not specified, the default value of 00 will be used, specifying no encryption and no compression. 
        :type dfi: :ref:`DataFormatIdentifier<DataFormatIdentifier>`

        :return: The server response parsed by :meth:`RequestFileTransfer.interpret_response<udsoncan.services.RequestFileTransfer.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.request_file_transfer(moop=services.RequestFileTransfer.ModeOfOperation.ReadFile, path=filename, dfi=dfi)

    def read_dir(self, path: str) -> Optional[services.RequestFileTransfer.InterpretedResponse]:
        """
        Sends a RequestFileTransfer request with ModeOfOperation=ReadDir(5).

        :Effective configuration: ``exception_on_<type>_response`` ``tolerate_zero_padding``

        :param path: The name of the directory to read, limited to ASCII characters.
        :type path: str

        :return: The server response parsed by :meth:`RequestFileTransfer.interpret_response<udsoncan.services.RequestFileTransfer.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.request_file_transfer(moop=services.RequestFileTransfer.ModeOfOperation.ReadDir, path=path)

    @standard_error_management
    def request_file_transfer(self,
                              moop: int,
                              path: str,
                              dfi: Optional[DataFormatIdentifier] = None,
                              filesize: Optional[Union[int, Filesize]] = None
                              ) -> Optional[services.RequestFileTransfer.InterpretedResponse]:

        request = services.RequestFileTransfer.make_request(moop=moop, path=path, dfi=dfi, filesize=filesize)
        self.logger.info('%s - Sending a "%s"(0x%02x) request on "%s". ' % (self.service_log_prefix(services.RequestFileTransfer),
                         services.RequestFileTransfer.ModeOfOperation.get_name(moop), moop, path))
        self.logger.debug("%s - DataFormatIdentifier = %s. Filesize = %s" % (self.service_log_prefix(services.RequestFileTransfer), dfi, filesize))

        response = self.send_request(request)
        if response is None:
            return None

        try:
            response = services.RequestFileTransfer.interpret_response(response, tolerate_zero_padding=self.config['tolerate_zero_padding'])
        except InvalidResponseException as e:
            if e.response.service_data is not None:
                service_data = cast(services.RequestFileTransfer.ResponseData, e.response.service_data)
                if service_data.moop_echo is not None and service_data.moop_echo != moop:
                    raise UnexpectedResponseException(e.response, 'ModeOfOperation echo does not match request and caused the service to failed decoding the payload correctly. Received 0x%02x, Requested=0x%02x' % (
                        service_data.moop_echo, moop))
                else:
                    raise e
            else:
                raise e

        if response.service_data.moop_echo != moop:
            raise UnexpectedResponseException(
                response, 'ModeOfOperation echo does not match request. Received 0x%02x, Requested=0x%02x' % (response.service_data.moop_echo, moop))

        if response.service_data.dfi is not None and dfi is not None:
            received = response.service_data.dfi.get_byte_as_int()
            expected = dfi.get_byte_as_int()
            if received != expected:
                raise UnexpectedResponseException(
                    response, 'DataFormatIdentifier echo does not match request. Received 0x%02x, Requested=0x%02x' % (received, expected))

        return response

    @standard_error_management
    def dynamically_define_did(self,
                               did: int,
                               did_definition: Union[DynamicDidDefinition, MemoryLocation]
                               ) -> Optional[services.DynamicallyDefineDataIdentifier.InterpretedResponse]:
        """
        Defines a dynamically defined DID.

        :Effective configuration: ``exception_on_<type>_response`` ``server_address_format`` ``server_memorysize_format``

        :param did: The data identifier to define.
        :type did: int

        :param did_definition: The definition of the DID. Can be defined by source DID or memory address. 
            If a :ref:`MemoryLocation<MemoryLocation>` object is given, definition will automatically be by memory address
        :type did_definition: :ref:`DynamicDidDefinition<DynamicDidDefinition>` or :ref:`MemoryLocation<MemoryLocation>`

        :return: The server response parsed by :meth:`DynamicallyDefineDataIdentifier.interpret_response<udsoncan.services.DynamicallyDefineDataIdentifier.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """

        self.logger.info('%s - Dynamically defining DID %s.' % (self.service_log_prefix(services.DynamicallyDefineDataIdentifier), did))

        if isinstance(did_definition, MemoryLocation):
            did_definition = DynamicDidDefinition(did_definition)

        if did_definition.is_by_source_did():
            subfunction = services.DynamicallyDefineDataIdentifier.Subfunction.defineByIdentifier
        elif did_definition.is_by_memory_address():
            subfunction = services.DynamicallyDefineDataIdentifier.Subfunction.defineByMemoryAddress
            entries = cast(List[DynamicDidDefinition.ByMemloc], did_definition.get())
            if 'server_address_format' in self.config:
                for entry in entries:
                    entry.memloc.set_format_if_none(address_format=self.config['server_address_format'])

            if 'server_memorysize_format' in self.config:
                for entry in entries:
                    entry.memloc.set_format_if_none(memorysize_format=self.config['server_memorysize_format'])
        else:
            raise ValueError('Cannot determine the subfunction from DID Definition')

        request = services.DynamicallyDefineDataIdentifier.make_request(subfunction, did, did_definition)

        response = self.send_request(request)
        if response is None:
            return None

        response = services.DynamicallyDefineDataIdentifier.interpret_response(response)

        if subfunction != response.service_data.subfunction_echo:
            raise UnexpectedResponseException(response, "Subfunction echo of response (0x%02x) does not match request subfunction (0x%02x)" % (
                response.service_data.subfunction_echo, subfunction))

        if response.service_data.did_echo is not None:
            if did != response.service_data.did_echo:
                raise UnexpectedResponseException(response, "DID echo of response (0x%02x) does not match requested DID (0x%02x)" %
                                                  (response.service_data.did_echo, did))

        return response

    @standard_error_management
    def do_clear_dynamically_defined_did(self, did: Optional[int] = None) -> Optional[services.DynamicallyDefineDataIdentifier.InterpretedResponse]:
        subfunction = services.DynamicallyDefineDataIdentifier.Subfunction.clearDynamicallyDefinedDataIdentifier

        didstr = 'all DIDs' if did is None else 'DID %s' % did
        self.logger.info('%s - Sending a "%s"(0x%02x) for %s. ' % (self.service_log_prefix(services.DynamicallyDefineDataIdentifier),
                         services.DynamicallyDefineDataIdentifier.Subfunction.get_name(subfunction), subfunction, didstr))

        request = services.DynamicallyDefineDataIdentifier.make_request(subfunction, did=did)
        response = self.send_request(request)
        if response is None:
            return None
        response = services.DynamicallyDefineDataIdentifier.interpret_response(response)

        if subfunction != response.service_data.subfunction_echo:
            raise UnexpectedResponseException(response, "Subfunction echo of response (0x%02x) does not match request subfunction (0x%02x)" % (
                response.service_data.subfunction_echo, subfunction))

        if did is not None:
            if did != response.service_data.did_echo:
                raise UnexpectedResponseException(
                    response, "DID echo of response (%s) does not match requested DID (0x%02x)" % (
                        '0x%02x' % response.service_data.did_echo if response.service_data.did_echo is not None else '<None>',
                        did))

        return response

    def clear_dynamically_defined_did(self, did: int) -> Optional[services.DynamicallyDefineDataIdentifier.InterpretedResponse]:
        """
        Clears a dynamically defined DID.

        :Effective configuration: ``exception_on_<type>_response``

        :param did: The data identifier to clear.
        :type did: int

        :return: The server response parsed by :meth:`DynamicallyDefineDataIdentifier.interpret_response<udsoncan.services.DynamicallyDefineDataIdentifier.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        if did is None:
            raise ValueError('Missing DID number')

        return self.do_clear_dynamically_defined_did(did)

    def clear_all_dynamically_defined_did(self) -> Optional[services.DynamicallyDefineDataIdentifier.InterpretedResponse]:
        """
        Clears all dynamically defined DID. Uses subfunction ``clearDynamicallyDefinedDataIdentifier`` without specifying a DID which means "all DID" according to ISO-14229

        :Effective configuration: ``exception_on_<type>_response``

        :return: The server response parsed by :meth:`DynamicallyDefineDataIdentifier.interpret_response<udsoncan.services.DynamicallyDefineDataIdentifier.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.do_clear_dynamically_defined_did()

    # Basic transmission of requests. This will need to be improved

    def send_request(self, request: Request, timeout: int = -1) -> Optional[Response]:
        if request.service is None:
            raise ValueError("Request has no service")

        if timeout < 0:
            # Timeout not provided by user: defaults to Client request_timeout value
            overall_timeout = self.config['request_timeout']
            p2 = self.config['p2_timeout'] if self.session_timing.p2_server_max is None else self.session_timing.p2_server_max
            if overall_timeout is not None:
                single_request_timeout = min(overall_timeout, p2)
            else:
                single_request_timeout = p2
        else:
            overall_timeout = timeout
            single_request_timeout = timeout
        respect_overall_timeout = True
        if overall_timeout is None:
            respect_overall_timeout = False
        using_p2_star = False  # Will switch to true when Nrc 0x78 will be received the first time.

        self.conn.empty_rxqueue()
        self.logger.debug("Sending request to server")
        override_suppress_positive_response = False
        if self.suppress_positive_response.enabled == True and request.service.use_subfunction():
            payload = request.get_payload(suppress_positive_response=True)
            override_suppress_positive_response = True
        else:
            payload = request.get_payload()

        if self.payload_override.enabled:
            payload = self.payload_override.get_overrided_payload(payload)

        if self.suppress_positive_response.enabled and not request.service.use_subfunction():
            self.logger.warning('SuppressPositiveResponse cannot be used for service %s. Ignoring' % (request.service.get_name()))

        self.conn.send(payload)

        spr_used = request.suppress_positive_response or override_suppress_positive_response
        wait_nrc = self.suppress_positive_response.enabled and self.suppress_positive_response.wait_nrc

        if spr_used and not wait_nrc:
            return None

        done_receiving = False
        if respect_overall_timeout:
            overall_timeout_time = time.monotonic() + overall_timeout

        timed_out = False
        while not done_receiving and not timed_out:
            done_receiving = True
            self.logger.debug("Waiting for server response")

            if not respect_overall_timeout or (respect_overall_timeout and time.monotonic() + single_request_timeout < overall_timeout_time):
                timeout_type_used = 'single_request'
                timeout_value = single_request_timeout
            else:
                timeout_type_used = 'overall'
                timeout_value = max(overall_timeout_time - time.monotonic(), 0)

            try:
                recv_payload = self.conn.wait_frame(timeout=timeout_value, exception=True)
            except TimeoutException:
                timed_out = True
            except Exception as e:
                raise e

            if timed_out or recv_payload is None:
                if spr_used:
                    return None
                if timeout_type_used == 'single_request':
                    timeout_name_to_report = 'P2* timeout' if using_p2_star else 'P2 timeout'
                    timeout_value_to_report = single_request_timeout
                elif timeout_type_used == 'overall':
                    timeout_name_to_report = 'Global request timeout'
                    timeout_value_to_report = overall_timeout
                else:  # Shouldn't go here.
                    timeout_name_to_report = 'Timeout'
                    timeout_value_to_report = timeout_value

                raise TimeoutException('Did not receive response in time. %s time has expired (timeout=%.3f sec)' %
                                       (timeout_name_to_report, float(timeout_value_to_report)))

            response = Response.from_payload(recv_payload)
            self.last_response = response
            self.logger.debug("Received response from server")

            if not response.valid:
                raise InvalidResponseException(response)

            assert response.service is not None
            assert response.code is not None

            if response.service.response_id() != request.service.response_id():
                msg = "Response gotten from server has a service ID different than the request service ID. Received=0x%02x, Expected=0x%02x" % (
                    response.service.response_id(), request.service.response_id())
                raise UnexpectedResponseException(response, msg)

            if not response.positive:
                try:
                    if not Response.Code.is_supported_by_standard(response.code, self.config['standard_version']):
                        self.logger.warning('Given response code "%s" (0x%02x) is not supported byt the UDS standard version that the clients s enforcing (%s)' % (
                            response.code_name,
                            response.code,
                            self.config['standard_version']))
                except ValueError:
                    self.logger.warning('Unkown response code "%s" (0x%02x)', response.code_name, response.code)

                if not request.service.is_supported_negative_response(response.code):
                    self.logger.warning('Given response code "%s" (0x%02x) is not a supported negative response code according to UDS standard.' % (
                        response.code_name, response.code))

                if response.code == Response.Code.RequestCorrectlyReceived_ResponsePending:
                    if self.config['nrc78_callback'] is not None:
                        self.config['nrc78_callback']()
                    
                    done_receiving = False
                    if not using_p2_star:
                        # Received a 0x78 NRC: timeout is now set to P2*
                        p2_star = self.config['p2_star_timeout'] if self.session_timing.p2_star_server_max is None else self.session_timing.p2_star_server_max
                        single_request_timeout = p2_star
                        using_p2_star = True
                        self.logger.debug("Server requested to wait with response code %s (0x%02x), single request timeout is now set to P2* (%.3f seconds)" %
                                          (response.code_name, response.code, single_request_timeout))
                else:
                    raise NegativeResponseException(response)

        assert response.service is not None
        self.logger.info('Received positive response for service %s (0x%02x) from server.' %
                         (response.service.get_name(), response.service.request_id()))

        response.original_request = request

        if spr_used:
            return None

        return response

    # ====  Authentication Service Client Functions
    def deauthenticate(self) -> Optional[services.Authentication.InterpretedResponse]:
        """
        Sends a deAuthenticate request (sub function of Authentication Service) introduced in 2020 version of ISO-14229-1.

        :Effective configuration: ``exception_on_<type>_response``

        :return: The server response parsed by :meth:`Authentication.interpret_response<udsoncan.services.Authentication.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.authentication(
            authentication_task=services.Authentication.AuthenticationTask.deAuthenticate
        )

    def verify_certificate_unidirectional(self,
                                          communication_configuration: int,
                                          certificate_client: bytes,
                                          challenge_client: Optional[bytes] = None) -> Optional[
            services.Authentication.InterpretedResponse]:
        """
        Sends a verifyCertificateUnidirectional request (sub function of Authentication Service)

        :Effective configuration: ``exception_on_<type>_response``

        :param communication_configuration: Configuration information about how to proceed with security in further diagnostic communication after the Authentication (vehicle manufacturer specific).
            Allowed values are from 0 to 255.
        :type communication_configuration: int

        :param certificate_client: The Certificate to verify.
        :type certificate_client: bytes

        :param challenge_client: Optional The challenge contains vehicle manufacturer specific formatted client data (likely containing randomized information) or is a random number.
        :type challenge_client: bytes

        :return: The server response parsed by :meth:`Authentication.interpret_response<udsoncan.services.Authentication.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.authentication(
            authentication_task=services.Authentication.AuthenticationTask.verifyCertificateUnidirectional,
            communication_configuration=communication_configuration,
            certificate_client=certificate_client,
            challenge_client=challenge_client
        )

    def verify_certificate_bidirectional(self,
                                         communication_configuration: int,
                                         certificate_client: bytes,
                                         challenge_client: bytes) -> Optional[
            services.Authentication.InterpretedResponse]:
        """
        Sends a verifyCertificateBidirectional request (sub function of Authentication Service)

        :Effective configuration: ``exception_on_<type>_response``

        :param communication_configuration: Configuration information about how to proceed with security in further diagnostic communication after the Authentication (vehicle manufacturer specific).
            Allowed values are from 0 to 255.
        :type communication_configuration: int

        :param certificate_client: The Certificate to verify.
        :type certificate_client: bytes

        :param challenge_client: The challenge contains vehicle manufacturer specific formatted client data (likely containing randomized information) or is a random number.
        :type challenge_client: bytes

        :return: The server response parsed by :meth:`Authentication.interpret_response<udsoncan.services.Authentication.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.authentication(
            authentication_task=services.Authentication.AuthenticationTask.verifyCertificateBidirectional,
            communication_configuration=communication_configuration,
            certificate_client=certificate_client,
            challenge_client=challenge_client
        )

    def proof_of_ownership(self,
                           proof_of_ownership_client: bytes,
                           ephemeral_public_key_client: Optional[bytes] = None) -> Optional[
            services.Authentication.InterpretedResponse]:
        """
        Sends a proofOfOwnership request (sub function of Authentication Service)

        :Effective configuration: ``exception_on_<type>_response``

        :param proof_of_ownership_client: Proof of Ownership of the previous given challenge to be verified by the server.
        :type proof_of_ownership_client: bytes

        :param ephemeral_public_key_client: Optional Ephemeral public key generated by the client for Diffie-Hellman key agreement.
        :type ephemeral_public_key_client: bytes

        :return: The server response parsed by :meth:`Authentication.interpret_response<udsoncan.services.Authentication.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """

        return self.authentication(
            authentication_task=services.Authentication.AuthenticationTask.proofOfOwnership,
            proof_of_ownership_client=proof_of_ownership_client,
            ephemeral_public_key_client=ephemeral_public_key_client
        )

    def transmit_certificate(self,
                             certificate_evaluation_id: int,
                             certificate_data: bytes) -> Optional[services.Authentication.InterpretedResponse]:
        """
        Sends a transmitCertificate request (sub function of Authentication Service)

        :Effective configuration: ``exception_on_<type>_response``

        :param certificate_evaluation_id: Unique ID to identify the evaluation type of the transmitted certificate.
            The value of this parameter is vehicle manufacturer specific.
            Subsequent diagnostic requests with the same evaluationTypeId will overwrite the certificate data of the previous requests.
            Allowed values are from 0 to 0xFFFF.
        :type certificate_evaluation_id: int

        :param certificate_data: The Certificate to verify.
        :type certificate_data: bytes

        :return: The server response parsed by :meth:`Authentication.interpret_response<udsoncan.services.Authentication.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.authentication(
            authentication_task=services.Authentication.AuthenticationTask.transmitCertificate,
            certificate_evaluation_id=certificate_evaluation_id,
            certificate_data=certificate_data
        )

    def request_challenge_for_authentication(self,
                                             communication_configuration: int,
                                             algorithm_indicator: bytes) -> Optional[
            services.Authentication.InterpretedResponse]:
        """
        Sends a requestChallengeForAuthentication request (sub function of Authentication Service)

        :Effective configuration: ``exception_on_<type>_response``

        :param communication_configuration: Configuration information about how to proceed with security in further diagnostic communication after the Authentication (vehicle manufacturer specific).
            Allowed values are from 0 to 255.
        :type communication_configuration: int

        :param algorithm_indicator: Indicates the algorithm used in the generating and verifying Proof of Ownership (POWN),
            which further determines the parameters used in the algorithm and possibly the session key creation mode.
            This field is a 16 byte value containing the BER encoded OID value of the algorithm used.
            The value is left aligned and right padded with zero up to 16 bytes.
        :type algorithm_indicator: bytes

        :return: The server response parsed by :meth:`Authentication.interpret_response<udsoncan.services.Authentication.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.authentication(
            authentication_task=services.Authentication.AuthenticationTask.requestChallengeForAuthentication,
            communication_configuration=communication_configuration,
            algorithm_indicator=algorithm_indicator
        )

    def verify_proof_of_ownership_unidirectional(self,
                                                 algorithm_indicator: bytes,
                                                 proof_of_ownership_client: bytes,
                                                 challenge_client: Optional[bytes] = None,
                                                 additional_parameter: Optional[bytes] = None) -> Optional[
            services.Authentication.InterpretedResponse]:
        """
        Sends a verifyProofOfOwnershipUnidirectional request (sub function of Authentication Service)

        :Effective configuration: ``exception_on_<type>_response``

        :param algorithm_indicator: Indicates the algorithm used in the generating and verifying Proof of Ownership (POWN),
            which further determines the parameters used in the algorithm and possibly the session key creation mode.
            This field is a 16 byte value containing the BER encoded OID value of the algorithm used.
            The value is left aligned and right padded with zero up to 16 bytes.
        :type algorithm_indicator: bytes

        :param proof_of_ownership_client: Proof of Ownership of the previous given challenge to be verified by the server.
        :type proof_of_ownership_client: bytes

        :param challenge_client: Optional The challenge contains vehicle manufacturer specific formatted client data (likely containing randomized information) or is a random number.
        :type challenge_client: bytes

        :param additional_parameter: Optional additional parameter is provided to the server if the server indicates as neededAdditionalParameter.
        :type additional_parameter: bytes

        :return: The server response parsed by :meth:`Authentication.interpret_response<udsoncan.services.Authentication.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.authentication(
            authentication_task=services.Authentication.AuthenticationTask.verifyProofOfOwnershipUnidirectional,
            algorithm_indicator=algorithm_indicator,
            proof_of_ownership_client=proof_of_ownership_client,
            challenge_client=challenge_client,
            additional_parameter=additional_parameter
        )

    def verify_proof_of_ownership_bidirectional(self,
                                                algorithm_indicator: bytes,
                                                proof_of_ownership_client: bytes,
                                                challenge_client: bytes,
                                                additional_parameter: Optional[bytes] = None) -> Optional[
            services.Authentication.InterpretedResponse]:
        """
        Sends a verifyProofOfOwnershipBidirectional request (sub function of Authentication Service)

        :Effective configuration: ``exception_on_<type>_response``

        :param algorithm_indicator: Indicates the algorithm used in the generating and verifying Proof of Ownership (POWN),
            which further determines the parameters used in the algorithm and possibly the session key creation mode.
            This field is a 16 byte value containing the BER encoded OID value of the algorithm used.
            The value is left aligned and right padded with zero up to 16 bytes.
        :type algorithm_indicator: bytes

        :param proof_of_ownership_client: Proof of Ownership of the previous given challenge to be verified by the server.
        :type proof_of_ownership_client: bytes

        :param challenge_client: The challenge contains vehicle manufacturer specific formatted client data (likely containing randomized information) or is a random number.
        :type challenge_client: bytes

        :param additional_parameter: Optional additional parameter is provided to the server if the server indicates as neededAdditionalParameter.
        :type additional_parameter: bytes

        :return: The server response parsed by :meth:`Authentication.interpret_response<udsoncan.services.Authentication.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.authentication(
            authentication_task=services.Authentication.AuthenticationTask.verifyProofOfOwnershipBidirectional,
            algorithm_indicator=algorithm_indicator,
            proof_of_ownership_client=proof_of_ownership_client,
            challenge_client=challenge_client,
            additional_parameter=additional_parameter
        )

    def authentication_configuration(self) -> Optional[services.Authentication.InterpretedResponse]:
        """
        Sends a authenticationConfiguration request (sub function of Authentication Service) introduced in 2020 version of ISO-14229-1.

        :Effective configuration: ``exception_on_<type>_response``

        :return: The server response parsed by :meth:`Authentication.interpret_response<udsoncan.services.Authentication.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """
        return self.authentication(
            authentication_task=services.Authentication.AuthenticationTask.authenticationConfiguration
        )

    @standard_error_management
    def authentication(self,
                       authentication_task: int,
                       communication_configuration: Optional[int] = None,
                       certificate_client: Optional[bytes] = None,
                       challenge_client: Optional[bytes] = None,
                       algorithm_indicator: Optional[bytes] = None,
                       certificate_evaluation_id: Optional[int] = None,
                       certificate_data: Optional[bytes] = None,
                       proof_of_ownership_client: Optional[bytes] = None,
                       ephemeral_public_key_client: Optional[bytes] = None,
                       additional_parameter: Optional[bytes] = None) -> Optional[
            services.Authentication.InterpretedResponse]:
        """
        Sends an Authentication request introduced in 2020 version of ISO-14229-1. You can also use the helper functions to send each authentication task (sub function).

        :Effective configuration: ``exception_on_<type>_response``

        :param authentication_task: The authenticationTask (subfunction) to use.
        :type authentication_task: int

        :param communication_configuration: Optional Configuration information about how to proceed with security in further diagnostic communication after the Authentication (vehicle manufacturer specific).
            Allowed values are from 0 to 255.
        :type communication_configuration: int

        :param certificate_client: Optional The Certificate to verify.
        :type certificate_client: bytes

        :param challenge_client: Optional The challenge contains vehicle manufacturer specific formatted client data (likely containing randomized information) or is a random number.
        :type challenge_client: bytes

        :param algorithm_indicator: Optional Indicates the algorithm used in the generating and verifying Proof of Ownership (POWN),
            which further determines the parameters used in the algorithm and possibly the session key creation mode.
            This field is a 16 byte value containing the BER encoded OID value of the algorithm used.
            The value is left aligned and right padded with zero up to 16 bytes.
        :type algorithm_indicator: bytes

        :param certificate_evaluation_id: Optional unique ID to identify the evaluation type of the transmitted certificate.
            The value of this parameter is vehicle manufacturer specific.
            Subsequent diagnostic requests with the same evaluationTypeId will overwrite the certificate data of the previous requests.
            Allowed values are from 0 to 0xFFFF.
        :type certificate_evaluation_id: int

        :param certificate_data: Optional The Certificate to verify.
        :type certificate_data: bytes

        :param proof_of_ownership_client: Optional Proof of Ownership of the previous given challenge to be verified by the server.
        :type proof_of_ownership_client: bytes

        :param ephemeral_public_key_client: Optional Ephemeral public key generated by the client for Diffie-Hellman key agreement.
        :type ephemeral_public_key_client: bytes

        :param additional_parameter: Optional additional parameter is provided to the server if the server indicates as neededAdditionalParameter.
        :type additional_parameter: bytes

        :return: The server response parsed by :meth:`Authentication.interpret_response<udsoncan.services.Authentication.interpret_response>`
        :rtype: :ref:`Response<Response>`
        """

        request = services.Authentication.make_request(authentication_task,
                                                       communication_configuration,
                                                       certificate_client,
                                                       challenge_client,
                                                       algorithm_indicator,
                                                       certificate_evaluation_id,
                                                       certificate_data,
                                                       proof_of_ownership_client,
                                                       ephemeral_public_key_client,
                                                       additional_parameter)

        self.logger.info(f'{self.service_log_prefix(services.Authentication)} - Sending request with authentication'
                         f' task "{services.Authentication.AuthenticationTask.get_name(authentication_task)}"'
                         f' ({authentication_task:#02x}')
        response = self.send_request(request)
        if response is None:
            return None

        response = services.Authentication.interpret_response(response)
        if authentication_task != response.service_data.authentication_task_echo:
            raise UnexpectedResponseException(response, f'Authentication Task echo of response'
                                              f' ({response.service_data.authentication_task_echo:#02x}) does not match'
                                                        f' request Authentication Task ({authentication_task:#02x})')
        return response
