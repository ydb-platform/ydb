#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

struct TSecretPos {
    size_t From;
    size_t Len;
};

using TSecretList = TVector<TSecretPos>;

class ISecretMasker : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ISecretMasker>;

    virtual ~ISecretMasker() = default;
    virtual TSecretList Search(TStringBuf data) = 0;
    virtual TSecretList Mask(TString& data) = 0;
};

} // namespace NYql
