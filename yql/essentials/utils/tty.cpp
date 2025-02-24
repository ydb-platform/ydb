#include "tty.h"
#include <util/system/platform.h>

#ifdef _win_
#include <io.h>
#include <stdio.h>
#else
#include <unistd.h>
#endif

namespace NYql {

bool IsTty(EStdStream stream) {
#ifdef _win_
    switch (stream) {
    case EStdStream::In:
        return _isatty(_fileno(stdin));
    case EStdStream::Out:
        return _isatty(_fileno(stdout));
    case EStdStream::Err:
        return _isatty(_fileno(stderr));
    }
#else
    switch (stream) {
    case EStdStream::In:
        return isatty(STDIN_FILENO);
    case EStdStream::Out:
        return isatty(STDOUT_FILENO);
    case EStdStream::Err:
        return isatty(STDERR_FILENO);
    }
#endif
}

} // namespace NYql
