from __future__ import absolute_import

"""M2Crypto low level OpenSSL wrapper functions.

m2 is the low level wrapper for OpenSSL functions. Typically you would not
need to use these directly, since these will be called by the higher level
objects you should try to use instead.

Naming conventions: All functions wrapped by m2 are all lower case,
words separated by underscores.

Examples:

OpenSSL                   M2Crypto

X509_get_version          m2.x509_get_version
X509_get_notBefore        m2.x509_get_not_before
X509_REQ_verify           m2.x509_req_verify

Exceptions to naming rules:

XXX TDB

Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved.

Portions created by Open Source Applications Foundation (OSAF) are
Copyright (C) 2004 OSAF. All Rights Reserved.
"""

from M2Crypto.m2crypto import *
lib_init()
