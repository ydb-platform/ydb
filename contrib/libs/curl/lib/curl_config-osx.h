#pragma once

#include "curl_config-linux.h"

#undef CURL_CA_BUNDLE
#undef CURL_CA_PATH
#undef HAVE_ACCEPT4
#undef HAVE_EVENTFD
#undef HAVE_FSETXATTR_5
#undef HAVE_LINUX_TCP_H
#undef HAVE_PIPE2
#undef HAVE_SENDMMSG
#undef HAVE_SYS_EVENTFD_H

#undef CURL_OS
#define CURL_OS "x86_64-apple-darwin14"

#undef HAVE_FSETXATTR_6
#define HAVE_FSETXATTR_6 1
