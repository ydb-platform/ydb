#pragma once 
 
#include "curl_config-osx.h" 
 
#undef USE_OPENSSL 
#undef USE_TLS_SRP 
#define USE_DARWINSSL 1 
#define HAVE_LDAP_SSL 1 
