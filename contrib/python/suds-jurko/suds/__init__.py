# This program is free software; you can redistribute it and/or modify
# it under the terms of the (LGPL) GNU Lesser General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Lesser General Public License for more details at
# ( http://www.gnu.org/licenses/lgpl.html ).
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# written by: Jeff Ortel ( jortel@redhat.com )

"""
Suds is a lightweight SOAP Python client providing a Web Service proxy.
"""

import sys


#
# Project properties
#

from .version import __build__, __version__


#
# Exceptions
#

class MethodNotFound(Exception):
    def __init__(self, name):
        Exception.__init__(self, "Method not found: '%s'" % name)

class PortNotFound(Exception):
    def __init__(self, name):
        Exception.__init__(self, "Port not found: '%s'" % name)

class ServiceNotFound(Exception):
    def __init__(self, name):
        Exception.__init__(self, "Service not found: '%s'" % name)

class TypeNotFound(Exception):
    def __init__(self, name):
        Exception.__init__(self, "Type not found: '%s'" % tostr(name))

class BuildError(Exception):
    msg = """
        An error occurred while building an instance of (%s). As a result the
        object you requested could not be constructed. It is recommended that
        you construct the type manually using a Suds object. Please open a
        ticket with a description of this error.
        Reason: %s
        """
    def __init__(self, name, exception):
        Exception.__init__(self, BuildError.msg % (name, exception))

class SoapHeadersNotPermitted(Exception):
    msg = """
        Method (%s) was invoked with SOAP headers. The WSDL does not define
        SOAP headers for this method. Retry without the soapheaders keyword
        argument.
        """
    def __init__(self, name):
        Exception.__init__(self, self.msg % name)

class WebFault(Exception):
    def __init__(self, fault, document):
        if hasattr(fault, 'faultstring'):
            Exception.__init__(self, "Server raised fault: '%s'" %
                fault.faultstring)
        self.fault = fault
        self.document = document


#
# Logging
#

class Repr:
    def __init__(self, x):
        self.x = x
    def __str__(self):
        return repr(self.x)


#
# Utility
#

class null:
    """
    The I{null} object.
    Used to pass NULL for optional XML nodes.
    """
    pass

def objid(obj):
    return obj.__class__.__name__ + ':' + hex(id(obj))

def tostr(object, encoding=None):
    """ get a unicode safe string representation of an object """
    if isinstance(object, str):
        if encoding is None:
            return object
        else:
            return object.encode(encoding)
    if isinstance(object, tuple):
        s = ['(']
        for item in object:
            if isinstance(item, str):
                s.append(item)
            else:
                s.append(tostr(item))
            s.append(', ')
        s.append(')')
        return ''.join(s)
    if isinstance(object, list):
        s = ['[']
        for item in object:
            if isinstance(item, str):
                s.append(item)
            else:
                s.append(tostr(item))
            s.append(', ')
        s.append(']')
        return ''.join(s)
    if isinstance(object, dict):
        s = ['{']
        for item in list(object.items()):
            if isinstance(item[0], str):
                s.append(item[0])
            else:
                s.append(tostr(item[0]))
            s.append(' = ')
            if isinstance(item[1], str):
                s.append(item[1])
            else:
                s.append(tostr(item[1]))
            s.append(', ')
        s.append('}')
        return ''.join(s)
    try:
        return str(object)
    except:
        return str(object)


#
# Python 3 compatibility
#

if sys.version_info < (3, 0):
    from io import StringIO as BytesIO
else:
    from io import BytesIO

# Idea from 'http://lucumr.pocoo.org/2011/1/22/forwards-compatible-python'.
class UnicodeMixin(object):
    if sys.version_info >= (3, 0):
        # For Python 3, __str__() and __unicode__() should be identical.
        __str__ = lambda x: x.__unicode__()
    else:
        __str__ = lambda x: str(x).encode('utf-8')

#   Used instead of byte literals because they are not supported on Python
# versions prior to 2.6.
def byte_str(s='', encoding='utf-8', input_encoding='utf-8', errors='strict'):
    """
    Returns a bytestring version of 's', encoded as specified in 'encoding'.

    Accepts str & unicode objects, interpreting non-unicode strings as byte
    strings encoded using the given input encoding.

    """
    assert isinstance(s, str)
    if isinstance(s, str):
        return s.encode(encoding, errors)
    if s and encoding != input_encoding:
        return s.decode(input_encoding, errors).encode(encoding, errors)
    return s

# Class used to represent a byte string. Useful for asserting that correct
# string types are being passed around where needed.
if sys.version_info >= (3, 0):
    byte_str_class = bytes
else:
    byte_str_class = str
