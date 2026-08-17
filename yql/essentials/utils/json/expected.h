#pragma once

#include <util/generic/string.h>
#include <util/generic/yexception.h>

#include <expected>

namespace NYql::NJson {

template <typename T>
using TExpected = std::expected<T, TString>;

using TUnexpected = std::unexpected<TString>;

TUnexpected Unexpected(TString message);

TUnexpected UnexpectedField(TStringBuf key, TStringBuf message);

} // namespace NYql::NJson
