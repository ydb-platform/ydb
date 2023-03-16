#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NFq {
// keep in sync with token accessor logic
class TSigner : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TSigner>;

    enum Type {
        UNKNOWN = 0,
        SERVICE_ACCOUNT_ID = 1
    };

public:
    explicit TSigner(const TString& hmacSecret);
    TString SignAccountId(const TString& serviceAccountId) const {
        return Sign(SERVICE_ACCOUNT_ID, serviceAccountId);
    }

    TString Sign(Type type, const TString& value) const;
    void Verify(Type type, const TString& value, const TString& signature) const;

private:
    const TString HmacSecret;
};

TSigner::TPtr CreateSignerFromFile(const TString& secretFile);
}
