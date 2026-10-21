#pragma once

#include <util/generic/string.h>

#include <cstddef>

namespace NLsp::NYql {

class TRadix final {
public:
    explicit TRadix(TString alphabet);

    TString Encode(size_t value) const;
    TString Encode(size_t value, size_t length) const;

    static TString SimpleAlphabet();

private:
    size_t EncodedLen(size_t value) const;

    TString Alphabet_;
};

} // namespace NLsp::NYql
