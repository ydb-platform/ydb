#pragma once
#include "opensslconf-linux.h"

#undef OPENSSL_RAND_SEED_OS

#define OPENSSL_NO_AFALGENG
#define OPENSSL_NO_STDIO
#define OPENSSL_NO_POSIX_IO
