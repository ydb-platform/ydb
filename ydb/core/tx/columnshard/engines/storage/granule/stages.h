#pragma once
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/portions/constructors.h>
#include <ydb/core/tx/columnshard/tx_reader/abstract.h>

namespace NKikimr::NOlap {
class TGranuleMeta;
}

namespace NKikimr::NOlap::NLoading {

class TPortionDataAccessors {
private:
    TColumnChunkLoadContextV2::TBuildInfo BuildInfo;
    std::vector<TIndexChunkLoadContext> Indexes;

public:
    TColumnChunkLoadContextV2::TBuildInfo DetachBuildInfo() {
        return std::move(BuildInfo);
    }

    std::vector<TIndexChunkLoadContext>& MutableIndexes() {
        return Indexes;
    }

    TPortionDataAccessors(TColumnChunkLoadContextV2::TBuildInfo&& buildInfo)
        : BuildInfo(std::move(buildInfo)) {
    }
};

class TPortionsLoadContext {
private:
    THashMap<ui64, TPortionDataAccessors> Constructors;
    TPortionDataAccessors& MutableConstructorVerified(const ui64 portionId) {
        auto it = Constructors.find(portionId);
        AFL_VERIFY(it != Constructors.end());
        return it->second;
    }

public:
    void Clear() {
        Constructors.clear();
    }

    THashMap<ui64, TPortionDataAccessors>&& ExtractConstructors() {
        return std::move(Constructors);
    }

    void Add(TIndexChunkLoadContext&& chunk) {
        auto& constructor = MutableConstructorVerified(chunk.GetPortionId());
        constructor.MutableIndexes().emplace_back(std::move(chunk));
    }
    void Add(TColumnChunkLoadContextV2&& chunk) {
        AFL_VERIFY(Constructors.emplace(chunk.GetPortionId(), chunk.CreateBuildInfo()).second);
    }
};

class TGranuleOnlyPortionsReader: public ITxReader {
private:
    using TBase = ITxReader;

    const std::shared_ptr<IBlobGroupSelector> DsGroupSelector;
    TGranuleMeta* Self = nullptr;
    const TVersionedIndex* VersionedIndex;

    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;

public:
    TGranuleOnlyPortionsReader(const TString& name, const TVersionedIndex* versionedIndex, TGranuleMeta* self,
        const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector)
        : TBase(name)
        , DsGroupSelector(dsGroupSelector)
        , Self(self)
        , VersionedIndex(versionedIndex) {
        AFL_VERIFY(!!DsGroupSelector);
        AFL_VERIFY(VersionedIndex);
        AFL_VERIFY(Self);
    }
};

class TGranuleFinishCommonLoading: public ITxReader {
private:
    using TBase = ITxReader;
    TGranuleMeta* Self = nullptr;
    bool Started = false;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        return true;
    }
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override;

public:
    TGranuleFinishCommonLoading(const TString& name, TGranuleMeta* self)
        : TBase(name)
        , Self(self) {
        AFL_VERIFY(Self);
    }
};

class IGranuleTxReader: public ITxReader {
private:
    using TBase = ITxReader;

protected:
    const std::shared_ptr<IBlobGroupSelector> DsGroupSelector;
    TGranuleMeta* Self = nullptr;
    const TVersionedIndex* VersionedIndex;
    const std::shared_ptr<TPortionsLoadContext> Context;

public:
    IGranuleTxReader(const TString& name, const TVersionedIndex* versionedIndex, TGranuleMeta* self,
        const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector, const std::shared_ptr<TPortionsLoadContext>& context)
        : TBase(name)
        , DsGroupSelector(dsGroupSelector)
        , Self(self)
        , VersionedIndex(versionedIndex)
        , Context(context) {
        AFL_VERIFY(!!DsGroupSelector);
        AFL_VERIFY(VersionedIndex);
        AFL_VERIFY(Self);
        AFL_VERIFY(Context);
    }
};

class TGranuleStartAccessorsLoading: public IGranuleTxReader {
private:
    using TBase = IGranuleTxReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        Context->Clear();
        return true;
    }
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        return true;
    }

public:
    using TBase::TBase;
};

class TGranuleColumnsReader: public IGranuleTxReader {
private:
    using TBase = IGranuleTxReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TGranuleIndexesReader: public IGranuleTxReader {
private:
    using TBase = IGranuleTxReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;

public:
    using TBase::TBase;
};

class TGranuleFinishAccessorsLoading: public IGranuleTxReader {
private:
    using TBase = IGranuleTxReader;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        return true;
    }

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NLoading
