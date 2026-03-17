#pragma once

#if defined(__linux__) && !defined(__ANDROID__)
#include "svn_private_config-linux.h"
#endif

#if defined(__linux__) && defined(__ANDROID__)
#include "svn_private_config-android.h"
#endif

#if defined(__APPLE__) && !defined(__IOS__)
#include "svn_private_config-osx.h"
#endif

#if defined(__APPLE__) && defined(__IOS__)
#include "svn_private_config-ios.h"
#endif

#if defined(__FreeBSD__)
#include "svn_private_config-freebsd.h"
#endif

#if defined(_MSC_VER)
#include "svn_private_config-win.h"
#endif
