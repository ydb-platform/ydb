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
    YDB_ACCESSOR_DEF(std::vector<TColumnChunkLoadContextV1>, Records);
    std::vector<TIndexChunkLoadContext> Indexes;

public:
    std::vector<TIndexChunkLoadContext>& MutableIndexes() {
        return Indexes;
    }

    TPortionDataAccessors() = default;
};

class TPortionsLoadContext {
private:
    THashMap<ui64, TPortionDataAccessors> Constructors;
    TPortionDataAccessors& MutableConstructor(const ui64 portionId) {
        auto it = Constructors.find(portionId);
        if (it == Constructors.end()) {
            it = Constructors.emplace(portionId, TPortionDataAccessors()).first;
        }
        return it->second;
    }

public:
    void ClearRecords() {
        for (auto&& i : Constructors) {
            i.second.MutableRecords().clear();
        }
    }

    void ClearIndexes() {
        for (auto&& i : Constructors) {
            i.second.MutableIndexes().clear();
        }
    }

    THashMap<ui64, TPortionDataAccessors>&& ExtractConstructors() {
        return std::move(Constructors);
    }

    void Add(TIndexChunkLoadContext&& chunk) {
        auto& constructor = MutableConstructor(chunk.GetPortionId());
        constructor.MutableIndexes().emplace_back(std::move(chunk));
    }
    void Add(TColumnChunkLoadContextV1&& chunk) {
        auto& constructor = MutableConstructor(chunk.GetPortionId());
        constructor.MutableRecords().emplace_back(std::move(chunk));
    }
    void Add(TColumnChunkLoadContextV2&& chunk) {
        for (auto&& i : chunk.BuildRecordsV1()) {
            Add(std::move(i));
        }
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
