#pragma once

#include <util/generic/yexception.h>

#include <exception>

namespace NTvmAuth {
    class TTvmException: public yexception {
    };
    class TContextException: public TTvmException {
    };
    class TMalformedTvmSecretException: public TContextException {
    };
    class TMalformedTvmKeysException: public TContextException {
    };
    class TEmptyTvmKeysException: public TContextException {
    };
    class TNotAllowedException: public TTvmException {
    };
}
