#pragma once

#include "curl_config-osx.h"

#define USE_OPENSSL 1
#undef USE_TLS_SRP
#define HAVE_LDAP_SSL 1

#undef CURL_CA_FALLBACK
#undef CURL_CA_BUNDLE
