#pragma once
#include "abstract.h"

#include <ydb/library/accessor/accessor.h>

#include <util/generic/bitmap.h>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NIndexes {
class TBitSetStorage: public IBitsStorage {
private:
    TDynBitMap Bits;

    virtual TConclusionStatus DoDeserializeFromString(const TString& data) override;
    virtual TString DoSerializeToString() const override;
    virtual bool DoGet(const ui32 idx) const override;
    virtual ui32 DoGetBitsCount() const override {
        return Bits.Size();
    }

public:
    TBitSetStorage() = default;

    TBitSetStorage(TDynBitMap&& bits)
        : Bits(std::move(bits)) {
    }
    template <class TFixedSizeBitMap>
    TBitSetStorage(TFixedSizeBitMap&& bits)
        : Bits(std::move(bits)) {
    }

    bool TestHash(const ui64 hash) const {
        return Bits.Get(hash % Bits.Size());
    }

    static ui32 GrowBitsCountToByte(const ui32 bitsCount) {
        const ui32 bytesCount = bitsCount / 8;
        return (bytesCount + ((bitsCount % 8) ? 1 : 0)) * 8;
    }

};

class TBitSetStorageConstructor: public IBitsStorageConstructor {
public:
    static TString GetClassNameStatic() {
        return "BITSET";
    }

private:
    virtual std::shared_ptr<IBitsStorage> DoBuild(TDynBitMap&& bm) const override {
        return std::make_shared<TBitSetStorage>(std::move(bm));
    }
    virtual TConclusion<std::shared_ptr<IBitsStorage>> DoBuild(const TString& data) const override {
        auto result = std::make_shared<TBitSetStorage>();
        auto conclusion = result->DeserializeFromString(data);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        return result;
    }
    static inline const auto Registrator = TFactory::TRegistrator<TBitSetStorageConstructor>(GetClassNameStatic());

public:
    using TFactory = NObjectFactory::TObjectFactory<IBitsStorageConstructor, TString>;
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
