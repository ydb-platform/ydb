"""Various Utility Functions"""
import socket
import warnings
from .compat import compat_ord
from .dns import DNS


def mac_to_str(address):
    r"""Convert a MAC address to a readable/printable string

       Args:
           address (str): a MAC address in hex form (e.g. '\x01\x02\x03\x04\x05\x06')
       Returns:
           str: Printable/readable MAC address
    """
    return ':'.join('%02x' % compat_ord(b) for b in address)


def inet_to_str(inet):
    """Convert inet object to a string

        Args:
            inet (inet struct): inet network address
        Returns:
            str: Printable/readable IP address
    """
    # First try ipv4 and then ipv6
    try:
        return socket.inet_ntop(socket.AF_INET, inet)
    except ValueError:
        return socket.inet_ntop(socket.AF_INET6, inet)


def make_dict(obj):
    """Create a dictionary out of a non-builtin object"""
    # Recursion base case
    if is_builtin(obj):
        return obj

    output_dict = {}
    for key in dir(obj):
        if not key.startswith('__') and not callable(getattr(obj, key)):
            attr = getattr(obj, key)
            if isinstance(attr, list):
                output_dict[key] = []
                for item in attr:
                    output_dict[key].append(make_dict(item))
            else:
                output_dict[key] = make_dict(attr)

    return output_dict


def is_builtin(obj):
    return obj.__class__.__module__ in ['__builtin__', 'builtins']


def deprecation_warning(*args):
    """print a deprecation warning"""
    warnings.warn(*args, stacklevel=2)


def test_utils():
    """Test the utility methods"""
    from binascii import unhexlify
    from pprint import pprint

    print(mac_to_str(b'\x01\x02\x03\x04\x05\x06'))
    assert mac_to_str(b'\x01\x02\x03\x04\x05\x06') == '01:02:03:04:05:06'
    print(inet_to_str(b'\x91\xfe\xa0\xed'))
    assert inet_to_str(b'\x91\xfe\xa0\xed') == '145.254.160.237'
    ipv6_inet = b' \x01\r\xb8\x85\xa3\x00\x00\x00\x00\x8a.\x03ps4'
    assert inet_to_str(ipv6_inet) == '2001:db8:85a3::8a2e:370:7334'

    # Test the make_dict method with a DNS response packet
    a_resp = unhexlify("059c8180000100010000000106676f6f676c6503636f6d0000010001c00c00010"
                       "0010000012b0004d83ace2e0000290200000000000000")
    my_dns = DNS(a_resp)
    pprint(make_dict(my_dns))
