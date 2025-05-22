#pragma once

#include "Adaptor-linux.hh"

#undef HAS_PREAD
ssize_t pread(int fd, void* buf, size_t count, off_t offset);

#undef HAS_BUILTIN_OVERFLOW_CHECK
