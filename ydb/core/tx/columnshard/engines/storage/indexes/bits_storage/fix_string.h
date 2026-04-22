#pragma once
#include "abstract.h"

#include <util/generic/bitmap.h>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NIndexes {
class TFixStringBitsStorage: public IBitsStorageViewer {
private:
    const TString Data;

    virtual bool DoGet(const ui32 idx) const override;
    virtual ui32 DoGetBitsCount() const override {
        return Data.size() * 8;
    }

public:
    TFixStringBitsStorage() = default;

    TFixStringBitsStorage(const TString& data)
        : Data(data) {
    }
};

class TFixStringBitsStorageConstructor: public IBitsStorageConstructor {
public:
    static TString GetClassNameStatic() {
        return "SIMPLE_STRING";
    }

private:
    virtual TString DoSerializeToString(TDynBitMap&& bitsVector) const override;
    virtual TString DoSerializeToString(const TArrayPower2BitsStorage& storage) const override;

    virtual TConclusion<std::shared_ptr<IBitsStorageViewer>> DoRestore(const TString& data) const override {
        return std::make_shared<TFixStringBitsStorage>(data);
    }
    static inline const auto Registrator = TFactory::TRegistrator<TFixStringBitsStorageConstructor>(GetClassNameStatic());

public:
    using TFactory = NObjectFactory::TObjectFactory<IBitsStorageConstructor, TString>;
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
