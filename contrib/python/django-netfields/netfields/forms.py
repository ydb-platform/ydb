from ipaddress import ip_address, ip_interface, ip_network, _IPAddressBase, _BaseNetwork
from netaddr import EUI, AddrFormatError

from django import forms
from django.core.exceptions import ValidationError

from netfields.mac import mac_unix_common, mac_eui64


class InetAddressFormField(forms.Field):
    widget = forms.TextInput
    default_error_messages = {
        'invalid': u'Enter a valid IP address.',
    }

    def __init__(self, *args, **kwargs):
        super(InetAddressFormField, self).__init__(*args, **kwargs)

    def to_python(self, value):
        if not value:
            return None

        if isinstance(value, _IPAddressBase):
            return value

        if isinstance(value, str):
            value = value.strip()

        try:
            return ip_interface(value)
        except ValueError:
            raise ValidationError(self.error_messages['invalid'])


class NoPrefixInetAddressFormField(forms.Field):
    widget = forms.TextInput
    default_error_messages = {
        'invalid': u'Enter a valid IP address.',
    }

    def __init__(self, *args, **kwargs):
        super(NoPrefixInetAddressFormField, self).__init__(*args, **kwargs)

    def to_python(self, value):
        if not value:
            return None

        if isinstance(value, _IPAddressBase):
            return value

        if isinstance(value, str):
            value = value.strip()

        try:
            return ip_address(value)
        except ValueError:
            raise ValidationError(self.error_messages['invalid'])


class CidrAddressFormField(forms.Field):
    widget = forms.TextInput
    default_error_messages = {
        'invalid': u'Enter a valid CIDR address.',
        'network': u'Must be a network address.',
    }

    def __init__(self, *args, **kwargs):
        super(CidrAddressFormField, self).__init__(*args, **kwargs)

    def to_python(self, value):
        if not value:
            return None

        if isinstance(value, _BaseNetwork):
            network = value

        if isinstance(value, str):
            value = value.strip()

        try:
            network = ip_network(value)
        except ValueError as e:
            if 'has host bits' in e.args[0]:
                raise ValidationError(self.error_messages['network'])
            raise ValidationError(self.error_messages['invalid'])

        return network


class MACAddressFormField(forms.Field):
    default_error_messages = {
        'invalid': u'Enter a valid MAC address.',
    }

    def __init__(self, *args, **kwargs):
        super(MACAddressFormField, self).__init__(*args, **kwargs)

    def to_python(self, value):
        if not value:
            return None

        if isinstance(value, EUI):
            return value

        if isinstance(value, str):
            value = value.strip()

        try:
            return EUI(value, version=48, dialect=mac_unix_common)
        except (AddrFormatError, IndexError, TypeError):
            raise ValidationError(self.error_messages['invalid'])

    def widget_attrs(self, widget):
        attrs = super(MACAddressFormField, self).widget_attrs(widget)
        attrs.update({'maxlength': '17'})
        return attrs


class MACAddress8FormField(forms.Field):
    default_error_messages = {
        'invalid': u'Enter a valid MAC address 8.',
    }

    def __init__(self, *args, **kwargs):
        super(MACAddress8FormField, self).__init__(*args, **kwargs)

    def to_python(self, value):
        if not value:
            return None

        if isinstance(value, EUI):
            return value

        if isinstance(value, str):
            value = value.strip()

        try:
            mac = EUI(value, dialect=mac_eui64)
            if mac.version == 64:
                return mac
            mac = mac.eui64()
            mac.dialect = mac_eui64
            return mac
        except (AddrFormatError, IndexError, TypeError):
            raise ValidationError(self.error_messages['invalid'])

    def widget_attrs(self, widget):
        attrs = super(MACAddress8FormField, self).widget_attrs(widget)
        attrs.update({'maxlength': '23'})
        return attrs
