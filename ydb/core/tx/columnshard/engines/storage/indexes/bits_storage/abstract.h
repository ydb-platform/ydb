#pragma once

#include "array_power2.h"

#include <ydb/library/conclusion/result.h>
#include <ydb/library/conclusion/status.h>

#include <library/cpp/object_factory/object_factory.h>

#include <util/generic/bitmap.h>
#include <util/generic/string.h>

namespace NKikimrSchemeOp {
class TSkipIndexBitSetStorage;
}

namespace NKikimr::NOlap::NIndexes {
class IBitsStorageViewer {
private:
    virtual bool DoGet(const ui32 index) const = 0;
    virtual ui32 DoGetBitsCount() const = 0;

public:
    virtual ~IBitsStorageViewer() = default;

    TString DebugString() const;

    ui32 GetBitsCount() const {
        return DoGetBitsCount();
    }

    [[nodiscard]] bool Get(const ui32 index) const {
        return DoGet(index);
    }
};

class IBitsStorageConstructor {
private:
    virtual TString DoSerializeToString(TDynBitMap&& bm) const = 0;
    virtual TString DoSerializeToString(const TArrayPower2BitsStorage& storage) const = 0;
    virtual TConclusion<std::shared_ptr<IBitsStorageViewer>> DoRestore(const TString& data) const = 0;

public:
    static std::shared_ptr<IBitsStorageConstructor> GetDefault();

    virtual ~IBitsStorageConstructor() = default;

    using TFactory = NObjectFactory::TObjectFactory<IBitsStorageConstructor, TString>;
    [[nodiscard]] TString SerializeToString(TDynBitMap&& bm) const {
        return DoSerializeToString(std::move(bm));
    }
    [[nodiscard]] TString SerializeToString(const TArrayPower2BitsStorage& storage) const {
        return DoSerializeToString(storage);
    }

    [[nodiscard]] TConclusion<std::shared_ptr<IBitsStorageViewer>> Restore(const TString& data) const {
        return DoRestore(data);
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TSkipIndexBitSetStorage& proto);

    NKikimrSchemeOp::TSkipIndexBitSetStorage SerializeToProto() const;

    virtual TString GetClassName() const = 0;
};

}   // namespace NKikimr::NOlap::NIndexes
