#pragma once

#include "pixman-config-linux.h"

// does not compile
#undef USE_X86_MMX

// fallback to default compiler-based switch in pixman-compiler.h
#undef TLS

// even though clang-cl have support for this attribute,
// it hardly works as we do not use LLD on Windows
#undef TOOLCHAIN_SUPPORTS_ATTRIBUTE_CONSTRUCTOR
