from django import VERSION

from netfields.managers import NetManager
from netfields.fields import (InetAddressField, CidrAddressField,
                              MACAddressField, MACAddress8Field)

# only keep it for django 3.1 and below
if VERSION[0] < 3 or VERSION[0] == 3 and VERSION[1]  < 2:
    default_app_config = 'netfields.apps.NetfieldsConfig'
