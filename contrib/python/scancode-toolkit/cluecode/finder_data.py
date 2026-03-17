#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from functools import partial


def set_from_text(text):
    return set(u.lower().strip('/') for u in text.split())


JUNK_EMAILS = set_from_text(u'''
    test@test.com
    exmaple.com
    example.com
    example.net
    example.org
    test.com
    localhost
''')


JUNK_HOSTS_AND_DOMAINS = set_from_text(u'''
    exmaple.com
    example.com
    example.net
    example.org
    test.com
    schemas.android.com
    1.2.3.4
    yimg.com
    a.b.c
    maps.google.com
    hostname
    localhost
''')

JUNK_IPS = set_from_text(u'''
    1.2.3.4
''')

# Check for domain to be exactly one of below mentioned
JUNK_EXACT_DOMAIN_NAMES = set_from_text(u'''
    test.com
    something.com
    some.com
    anything.com
    any.com
    trial.com
    sample.com
    other.com
    something.com
    some.com
''')

JUNK_URLS = set_from_text(u'''
    http://www.adobe.com/2006/mxml
    http://www.w3.org/1999/XSL/Transform
    http://docs.oasis-open.org/ns/xri/xrd-1.0
    http://www.w3.org/2001/XMLSchema-instance
    http://java.sun.com/xml/ns/persistence/persistence_1_0.xsd
    http://bing.com
    http://google.com
    http://msn.com
    http://maven.apache.org/maven-v4_0_0.xsd
    http://maven.apache.org/POM/4.0.0
    http://www.w3.org/MarkUp/DTD/xhtml-rdfa-1.dtd
    http://www.w3.org/1999/02/22-rdf-syntax-ns
    http://www.w3.org/1999/xhtml
    http://www.w3.org/1999/XMLSchema
    http://www.w3.org/1999/XMLSchema-instance
    http://www.w3.org/2000/svg
    http://www.w3.org/2001/XMLSchema
    http://www.w3.org/2000/10/XMLSchema
    http://www.w3.org/2000/10/XMLSchema-instance
    http://www.w3.org/2001/XMLSchema
    http://www.w3.org/2001/XMLSchema-instance
    http://www.w3.org/2002/12/soap-encoding
    http://www.w3.org/2002/12/soap-envelope
    http://www.w3.org/2005/Atom
    http://www.w3.org/2006/01/wsdl
    http://www.w3.org/2006/01/wsdl/http
    http://www.w3.org/2006/01/wsdl/soap
    http://www.w3.org/2006/vcard/ns
    http://www.w3.org/International/O-URL-and-ident.html
    http://www.w3.org/MarkUp
    http://www.w3.org/WAI/GL
    http://xml.apache.org/axis/session
    http://xml.apache.org/xml-soap
    http://docs.oasis-open.org/ns/xri/xrd-1.0
    http://cobertura.sourceforge.net/xml/coverage-01.dtd
    http://findbugs.googlecode.com/svn/trunk/findbugs/etc/docbook/docbookx.dtd
    http://hibernate.sourceforge.net/hibernate-configuration-2.0.dtd
    http://hibernate.sourceforge.net/hibernate-generic.dtd
    http://hibernate.sourceforge.net/hibernate-mapping-2.0.dtd
    http://www.opensymphony.com/xwork/xwork-1.0.dtd
    http://]hostname
    http://+
    http://www
    http://www.w3.org/1999/xhtml
    http://www.w3.org/1999/XSL/Transform
    http://www.w3.org/2001/XMLSchema
    http://www.w3.org/2001/XMLSchema-instance
    http://www.w3.org/hypertext/WWW/Protocols/HTTP/HTRESP.html
    http://www.w3.org/hypertext/WWW/Protocols/HTTP/Object_Headers.html
    http://www.w3.org/P3P
    http://www.w3.org/pub/WWW
    http://www.w3.org/TR/html4/strict.dtd
    http://www.w3.org/TR/REC-html40/loose.dtd
    http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd
    http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd
    http://www.w3.org/TR/xslt
    https:
    https://+
    http://www.example.com
    http://www.example.com/dir/file
    http://www.example.com:dir/file
    http://www.your.org.here
    http://hostname
    https://www.trustedcomputinggroup.org/XML/SCHEMA/TNCCS_1.0.xsd
    http://glade.gnome.org/glade-2.0.dtd
    http://pagesperso-orange.fr/sebastien.godard/sysstat.dtd
    http://www.freedesktop.org/standards/dbus/1.0/busconfig.dtd
    http://www.freedesktop.org/standards/dbus/1.0/introspect.dtd
    http://gcc.gnu.org/bugs.html
    http://nsis.sf.net/NSIS_Error
''')


JUNK_URL_PREFIXES = tuple(sorted(set_from_text('''
    http://www.springframework.org/dtd/
    http://www.slickedit.com/dtd/
    http://www.oexchange.org/spec/0.8/
    http://www.puppycrawl.com/dtds/
    http://adobe.com/AS3/2006/builtin
    http://careers.msn.com
    http://foo.bar.baz
    http://foo.bar.com
    http://foobar.com
    http://java.sun.com/xml/ns/
    http://java.sun.com/j2se/1.4/docs/
    http://java.sun.com/j2se/1.5.0/docs/
    http://developer.apple.com/certificationauthority/
    http://www.apple.com/appleca/
    https://www.apple.com/certificateauthority/
    http://schemas.microsoft.com/
    http://dublincore.org/schemas/
    http://www.w3.org/TR/
    http://www.apple.com/DTDs
    http://apache.org/xml/features/
    http://apache.org/xml/properties/
    http://crl.verisign.com/
    http://crl.globalsign.net/
    http://crl.microsoft.com/
    http://crl.thawte.com/
    http://CSC3-2004-crl.verisign.com
    http://csc3-2009-2-crl.verisign.com
    http://dellincca.dell.com/crl
    http://ts-crl.ws.symantec.com
    http://java.sun.com/dtd/
    http://java.sun.com/j2ee/dtds/
    http://jakarta.apache.org/commons/dtds/
    http://jakarta.apache.org/struts/dtds/
    http://www.jboss.org/j2ee/dtd/
    http://glassfish.org/dtds/
    http://docbook.org/xml/simple/
    http://www.oasis-open.org/docbook/xml/
    http://www.w3.org/XML/1998/namespace
    https://www.w3.org/XML/1998/namespace
    http://www.w3.org/2000/xmlns/
    https://www.w3.org/2000/xmlns/
    http://ts-aia.ws.symantec.com/
    https://ts-aia.ws.symantec.com/
    https://www.verisign.com/rpa
    http://csc3-2010-crl.verisign.com/
    https://www.verisign.com/rpa
    http://csc3-2010-aia.verisign.com/
    https://www.verisign.com/cps
    http://logo.verisign.com/
    http://ocsp2.globalsign.com/
    http://crl.globalsign.com/
    http://secure.globalsign.com/cacert/
    https://www.globalsign.com/repository/
    http://www.microsoft.com/pki/certs/
    http://www.microsoft.com/pkiops/crl
    http://www.microsoft.com/PKI/
''')))

JUNK_DOMAIN_SUFFIXES = tuple(sorted(set_from_text('''
   .png
   .jpg
   .gif
   .jpeg
''')))


def classify(s, data_set, suffixes=None, ignored_hosts=None):
    """
    Return True or some classification string value that evaluates to True if
    the data in string `s` is not junk. Return False if the data in string `s` is
    classified as 'junk' or uninteresting. Use `data_set` set of junk strings,
    `suffixes` optional set of junk suffixes, and `ignored_hosts` set of junk
    email host names for classification.
    """
    if not s:
        return False
    s = s.lower().strip('/')
    # Separate test for emails - need to ignore xyz@some.com, but not say, xyz@gruesome.com
    if ignored_hosts and '@' in s:
        _name, _at, host_name = s.rpartition('@')
        if host_name in ignored_hosts:
            return False
    if any(d in s for d in data_set):
        return False
    if suffixes and s.endswith(suffixes):
        return False
    return True


classify_ip = partial(classify, data_set=JUNK_IPS)

classify_host = partial(
    classify,
    data_set=JUNK_HOSTS_AND_DOMAINS,
    suffixes=JUNK_DOMAIN_SUFFIXES,
)

classify_email = partial(
    classify,
    data_set=JUNK_EMAILS,
    suffixes=JUNK_DOMAIN_SUFFIXES,
    ignored_hosts=JUNK_EXACT_DOMAIN_NAMES,
)


def classify_url(url):
    if not url:
        return False
    u = url.lower().strip('/')
    if (u in JUNK_URLS or
        u.startswith(JUNK_URL_PREFIXES)
        or u.endswith(JUNK_DOMAIN_SUFFIXES)):
        return False
    return True
