SERVICES = {
        # Name                              namespace                           wsdl file                      binding name
        'devicemgmt': {'ns': 'http://www.onvif.org/ver10/device/wsdl',    'wsdl': 'devicemgmt.wsdl', 'binding' : 'DeviceBinding'},
        'media'     : {'ns': 'http://www.onvif.org/ver10/media/wsdl',     'wsdl': 'media.wsdl',      'binding' : 'MediaBinding'},
        'ptz'       : {'ns': 'http://www.onvif.org/ver20/ptz/wsdl',       'wsdl': 'ptz.wsdl',        'binding' : 'PTZBinding'},
        'imaging'   : {'ns': 'http://www.onvif.org/ver20/imaging/wsdl',   'wsdl': 'imaging.wsdl',    'binding' : 'ImagingBinding'},
        'deviceio'  : {'ns': 'http://www.onvif.org/ver10/deviceIO/wsdl',  'wsdl': 'deviceio.wsdl',   'binding' : 'DeviceIOBinding'},
        'events'    : {'ns': 'http://www.onvif.org/ver10/events/wsdl',    'wsdl': 'events.wsdl',     'binding' : 'EventBinding'},
        'pullpoint' : {'ns': 'http://www.onvif.org/ver10/events/wsdl',    'wsdl': 'events.wsdl',     'binding' : 'PullPointSubscriptionBinding'},
        'analytics' : {'ns': 'http://www.onvif.org/ver20/analytics/wsdl', 'wsdl': 'analytics.wsdl',  'binding' : 'AnalyticsEngineBinding'},
        'recording' : {'ns': 'http://www.onvif.org/ver10/recording/wsdl', 'wsdl': 'recording.wsdl',  'binding' : 'RecordingBinding'},
        'search'    : {'ns': 'http://www.onvif.org/ver10/search/wsdl',    'wsdl': 'search.wsdl',     'binding' : 'SearchBinding'},
        'replay'    : {'ns': 'http://www.onvif.org/ver10/replay/wsdl',    'wsdl': 'replay.wsdl',     'binding' : 'ReplayBinding'},
        'receiver'  : {'ns': 'http://www.onvif.org/ver10/receiver/wsdl',  'wsdl': 'receiver.wsdl',   'binding' : 'ReceiverBinding'},
        }

#
#NSMAP = { }
#for name, item in SERVICES.items():
#    NSMAP[item['ns']] = name
