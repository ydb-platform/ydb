from onvif.client import ONVIFService, ONVIFCamera, SERVICES
from onvif.exceptions import ONVIFError, ERR_ONVIF_UNKNOWN, \
        ERR_ONVIF_PROTOCOL, ERR_ONVIF_WSDL, ERR_ONVIF_BUILD
#from onvif import cli

__all__ = ( 'ONVIFService', 'ONVIFCamera', 'ONVIFError',
            'ERR_ONVIF_UNKNOWN', 'ERR_ONVIF_PROTOCOL',
            'ERR_ONVIF_WSDL', 'ERR_ONVIF_BUILD',
            'SERVICES'#, 'cli'
           )
