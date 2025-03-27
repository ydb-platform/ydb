#pragma once
#include <ydb/library/conclusion/result.h>
#include <ydb/library/conclusion/status.h>

#include <library/cpp/object_factory/object_factory.h>
#include <util/generic/bitmap.h>
#include <util/generic/string.h>

namespace NKikimrSchemeOp {
class TSkipIndexBitSetStorage;
}

namespace NKikimr::NOlap::NIndexes {
class IBitsStorage {
private:
    virtual bool DoGet(const ui32 index) const = 0;
    virtual ui32 DoGetBitsCount() const = 0;
    virtual TConclusionStatus DoDeserializeFromString(const TString& data) = 0;
    virtual TString DoSerializeToString() const = 0;

public:
    virtual ~IBitsStorage() = default;

    TString DebugString() const;

    ui32 GetBitsCount() const {
        return DoGetBitsCount();
    }

    [[nodiscard]] bool Get(const ui32 index) const {
        return DoGet(index);
    }

    [[nodiscard]] TString SerializeToString() const {
        return DoSerializeToString();
    }

    [[nodiscard]] TConclusionStatus DeserializeFromString(const TString& data) noexcept {
        try {
            return DoDeserializeFromString(data);
        } catch (...) {
            return TConclusionStatus::Fail("cannot read index: " + CurrentExceptionMessage());
        }
    }
};

class IBitsStorageConstructor {
private:
    virtual std::shared_ptr<IBitsStorage> DoBuild(TDynBitMap&& bm) const = 0;
    virtual TConclusion<std::shared_ptr<IBitsStorage>> DoBuild(const TString& data) const = 0;

public:
    static std::shared_ptr<IBitsStorageConstructor> GetDefault();;

    virtual ~IBitsStorageConstructor() = default;

    using TFactory = NObjectFactory::TObjectFactory<IBitsStorageConstructor, TString>;
    [[nodiscard]] std::shared_ptr<IBitsStorage> Build(TDynBitMap&& bm) const {
        return DoBuild(std::move(bm));
    }

    [[nodiscard]] TConclusion<std::shared_ptr<IBitsStorage>> Build(const TString& data) const {
        return DoBuild(data);
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TSkipIndexBitSetStorage& proto);

    NKikimrSchemeOp::TSkipIndexBitSetStorage SerializeToProto() const;

    virtual TString GetClassName() const = 0;
};

}   // namespace NKikimr::NOlap::NIndexes
