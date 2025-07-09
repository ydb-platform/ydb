#include "dummy_secret_masker.h"

namespace NYql {

class TDummySecretMasker : public ISecretMasker {
public:
    TSecretList Search(TStringBuf) override {
        // Do nothing
        return {};
    }

    TSecretList Mask(TString&) override {
        // Do nothing
        return {};
    }
};

ISecretMasker::TPtr CreateDummySecretMasker() {
    return MakeIntrusive<TDummySecretMasker>();
}

} // namespace NYql
