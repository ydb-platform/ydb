from ipaddress import ip_address, ip_interface, ip_network

from netaddr import EUI
from netaddr.core import AddrFormatError
from rest_framework import serializers

from netfields.mac import mac_unix_common, mac_eui64
from netfields import fields


class InetAddressField(serializers.Field):
    default_error_messages = {
        'invalid': 'Invalid IP address.'
    }

    def __init__(self, store_prefix=True, *args, **kwargs):
        self.store_prefix = store_prefix
        super(InetAddressField, self).__init__(*args, **kwargs)

    def to_representation(self, value):
        if value is None:
            return value
        return str(value)

    def to_internal_value(self, data):
        if data is None:
            return data
        try:
            if self.store_prefix:
                return ip_interface(data)
            else:
                return ip_address(data)
        except ValueError:
            self.fail('invalid')


class CidrAddressField(serializers.Field):
    default_error_messages = {
        'invalid': 'Invalid CIDR address.',
        'network': 'Must be a network address.',
    }

    def to_representation(self, value):
        if value is None:
            return value
        return str(value)

    def to_internal_value(self, data):
        if data is None:
            return data
        try:
            return ip_network(data)
        except ValueError as e:
            if 'has host bits' in e.args[0]:
                self.fail('network')
            self.fail('invalid')


class MACAddressField(serializers.Field):
    default_error_messages = {
        'invalid': 'Invalid MAC address.'
    }

    def to_representation(self, value):
        if value is None:
            return value
        return str(value)

    def to_internal_value(self, data):
        if data is None:
            return data
        try:
            return EUI(data, version=48, dialect=mac_unix_common)
        except (AddrFormatError, IndexError, TypeError):
            self.fail('invalid')


class MACAddress8Field(serializers.Field):
    default_error_messages = {
        'invalid': 'Invalid MAC address 8.'
    }

    def to_representation(self, value):
        if value is None:
            return value
        return str(value)

    def to_internal_value(self, data):
        if data is None:
            return data
        try:
            mac = EUI(data, dialect=mac_eui64)
            if mac.version == 64:
                return mac
            mac = mac.eui64()
            mac.dialect = mac_eui64
            return mac
        except (AddrFormatError, IndexError, TypeError):
            self.fail('invalid')


class NetModelSerializer(serializers.ModelSerializer):
    pass


NetModelSerializer.serializer_field_mapping[fields.InetAddressField] = InetAddressField
NetModelSerializer.serializer_field_mapping[fields.CidrAddressField] = CidrAddressField
NetModelSerializer.serializer_field_mapping[fields.MACAddressField] = MACAddressField
NetModelSerializer.serializer_field_mapping[fields.MACAddress8Field] = MACAddress8Field
