#include "expected.h"

namespace NYql::NJson {

TUnexpected Unexpected(TString message) {
    return std::unexpected(std::move(message));
}

TUnexpected UnexpectedField(TStringBuf key, TStringBuf message) {
    return Unexpected(TString::Join('"', key, "\" ", message));
}

} // namespace NYql::NJson
