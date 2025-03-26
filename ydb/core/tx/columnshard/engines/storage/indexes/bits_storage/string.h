#pragma once
#include "abstract.h"

#include <ydb/library/accessor/accessor.h>

#include <util/generic/bitmap.h>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NIndexes {
class TFixStringBitsStorage: public IBitsStorage {
private:
    TString Data;

    virtual bool DoGet(const ui32 idx) const override;
    virtual TConclusionStatus DoDeserializeFromString(const TString& data) override;
    virtual TString DoSerializeToString() const override {
        return Data;
    }
    virtual ui32 DoGetBitsCount() const override {
        return Data.size() * 8;
    }

public:
    TFixStringBitsStorage() = default;

    static ui32 GrowBitsCountToByte(const ui32 bitsCount) {
        const ui32 bytesCount = bitsCount / 8;
        return (bytesCount + ((bitsCount % 8) ? 1 : 0)) * 8;
    }

    TFixStringBitsStorage(const TDynBitMap& bitsVector);
};

class TFixStringBitsStorageConstructor: public IBitsStorageConstructor {
public:
    static TString GetClassNameStatic() {
        return "SIMPLE_STRING";
    }

private:
    virtual std::shared_ptr<IBitsStorage> DoBuild(TDynBitMap&& bm) const override {
        return std::make_shared<TFixStringBitsStorage>(std::move(bm));
    }
    virtual TConclusion<std::shared_ptr<IBitsStorage>> DoBuild(const TString& data) const override {
        auto result = std::make_shared<TFixStringBitsStorage>();
        auto conclusion = result->DeserializeFromString(data);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        return result;
    }
    static inline const auto Registrator = TFactory::TRegistrator<TFixStringBitsStorageConstructor>(GetClassNameStatic());

public:
    using TFactory = NObjectFactory::TObjectFactory<IBitsStorageConstructor, TString>;
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
