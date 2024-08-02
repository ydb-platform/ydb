#include "parse_double.h"

#include <util/string/ascii.h>
#include <util/string/cast.h>

namespace NYql {

namespace {
template <typename T>
bool GenericTryFloatFromString(TStringBuf buf, T& value) {
    value = 0;
    if (!buf.size()) {
        return false;
    }

    if (TryFromString(buf.data(), buf.size(), value)) {
        return true;
    }
    
    const char* ptr = buf.data();
    ui32 size = buf.size();
    char sign = '+';
    if (*ptr == '+' || *ptr == '-') {
        sign = *ptr;
        ++ptr;
        --size;
    }

    if (size != 3) {
        return false;
    }

    // NaN or Inf (ignoring case)
    if (AsciiToUpper(ptr[0]) == 'N' && AsciiToUpper(ptr[1]) == 'A' && AsciiToUpper(ptr[2]) == 'N') {
        value = std::numeric_limits<T>::quiet_NaN();
    } else if (AsciiToUpper(ptr[0]) == 'I' && AsciiToUpper(ptr[1]) == 'N' && AsciiToUpper(ptr[2]) == 'F') {
        value = std::numeric_limits<T>::infinity();
    } else {
        return false;
    }

    if (sign == '-') {
        value = -value;
    }

    return true;
}
}

float FloatFromString(TStringBuf buf) {
    float result = 0;
    if (!TryFloatFromString(buf, result)) {
        throw yexception() << "unable to parse float from '" << buf << "'";
    }

    return result;
}

double DoubleFromString(TStringBuf buf) {
    double result = 0;
    if (!TryDoubleFromString(buf, result)) {
        throw yexception() << "unable to parse double from '" << buf << "'";
    }

    return result;
}

bool TryFloatFromString(TStringBuf buf, float& value) {
    return GenericTryFloatFromString(buf, value);
}

bool TryDoubleFromString(TStringBuf buf, double& value) {
    return GenericTryFloatFromString(buf, value);
}

}
