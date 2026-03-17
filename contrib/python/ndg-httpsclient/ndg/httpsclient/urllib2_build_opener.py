"""urllib2 style build opener integrates with HTTPSConnection class from this
package.
"""
__author__ = "P J Kershaw"
__date__ = "21/12/10"
__copyright__ = "(C) 2011 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__contact__ = "Philip.Kershaw@stfc.ac.uk"
__revision__ = '$Id$'
import logging
import sys

# Py 2 <=> 3 compatibility for class type checking
if sys.version_info[0] > 2:
    class_type_ = type
    from urllib.request import (ProxyHandler, UnknownHandler, 
                                HTTPDefaultErrorHandler, FTPHandler, 
                                FileHandler, HTTPErrorProcessor, 
                                HTTPHandler, OpenerDirector, 
                                HTTPRedirectHandler)
else:
    import types
    class_type_ = types.ClassType
    
    from urllib2 import (ProxyHandler, UnknownHandler, HTTPDefaultErrorHandler, 
                         FTPHandler, FileHandler, HTTPErrorProcessor, 
                         HTTPHandler, OpenerDirector, HTTPRedirectHandler)

from ndg.httpsclient.https import HTTPSContextHandler

log = logging.getLogger(__name__)


# Copied from urllib2 with modifications for ssl
def build_opener(*handlers, **kw):
    """Create an opener object from a list of handlers.

    The opener will use several default handlers, including support
    for HTTP and FTP.

    If any of the handlers passed as arguments are subclasses of the
    default handlers, the default handlers will not be used.
    """
    def isclass(obj):
        return isinstance(obj, class_type_) or hasattr(obj, "__bases__")

    opener = OpenerDirector()
    default_classes = [ProxyHandler, UnknownHandler, HTTPHandler,
                       HTTPDefaultErrorHandler, HTTPRedirectHandler,
                       FTPHandler, FileHandler, HTTPErrorProcessor]
    check_classes = list(default_classes)
    check_classes.append(HTTPSContextHandler)
    skip = []
    for klass in check_classes:
        for check in handlers:
            if isclass(check):
                if issubclass(check, klass):
                    skip.append(klass)
            elif isinstance(check, klass):
                skip.append(klass)

    for klass in default_classes:
        if klass not in skip:
            opener.add_handler(klass())
            
    # Pick up SSL context from keyword settings
    ssl_context = kw.get('ssl_context')
    
    # Add the HTTPS handler with ssl_context
    if HTTPSContextHandler not in skip:
        opener.add_handler(HTTPSContextHandler(ssl_context))

    for h in handlers:
        if isclass(h):
            h = h()
        opener.add_handler(h)

    return opener
