#pragma once
#include "abstract.h"

#include <util/generic/bitmap.h>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NIndexes {
class TBitSetStorage: public IBitsStorageViewer {
private:
    const TDynBitMap Bits;

    virtual bool DoGet(const ui32 idx) const override;
    virtual ui32 DoGetBitsCount() const override {
        return Bits.Size();
    }

public:
    TBitSetStorage() = default;

    TBitSetStorage(TDynBitMap&& bits)
        : Bits(std::move(bits)) {
    }
};

class TBitSetStorageConstructor: public IBitsStorageConstructor {
public:
    static TString GetClassNameStatic() {
        return "BITSET";
    }

private:
    virtual TString DoSerializeToString(TDynBitMap&& bm) const override;
    virtual TConclusion<std::shared_ptr<IBitsStorageViewer>> DoRestore(const TString& data) const override;

    static inline const auto Registrator = TFactory::TRegistrator<TBitSetStorageConstructor>(GetClassNameStatic());

public:
    using TFactory = NObjectFactory::TObjectFactory<IBitsStorageConstructor, TString>;
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
