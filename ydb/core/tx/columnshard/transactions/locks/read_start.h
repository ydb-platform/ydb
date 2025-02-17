#pragma once
#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>

namespace NKikimr::NOlap::NTxInteractions {

class TEvReadStartWriter: public ITxEventWriter {
private:
    YDB_READONLY(ui64, PathId, 0);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, Schema);
    YDB_READONLY_DEF(std::shared_ptr<TPKRangesFilter>, Filter);
    YDB_READONLY_DEF(THashSet<ui64>, LockIdsForCheck);

    virtual bool DoCheckInteraction(
        const ui64 selfTxId, TInteractionsContext& /*context*/, TTxConflicts& /*conflicts*/, TTxConflicts& notifications) const override {
        for (auto&& i : LockIdsForCheck) {
            notifications.Add(i, selfTxId);
        }
        return true;
    }

    virtual std::shared_ptr<ITxEvent> DoBuildEvent() override;

public:
    TEvReadStartWriter(const ui64 pathId, const std::shared_ptr<arrow::Schema>& schema, const std::shared_ptr<TPKRangesFilter>& filter,
        const THashSet<ui64>& lockIdsForCheck)
        : PathId(pathId)
        , Schema(schema)
        , Filter(filter)
        , LockIdsForCheck(lockIdsForCheck)
    {
        AFL_VERIFY(PathId);
        AFL_VERIFY(Schema);
        AFL_VERIFY(Filter);
    }
};

class TEvReadStart: public ITxEvent {
public:
    static TString GetClassNameStatic() {
        return "READ_START";
    }

private:
    YDB_READONLY(ui64, PathId, 0);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, Schema);
    YDB_READONLY_DEF(std::shared_ptr<TPKRangesFilter>, Filter);

    virtual bool DoDeserializeFromProto(const NKikimrColumnShardTxProto::TEvent& proto) override;
    virtual void DoSerializeToProto(NKikimrColumnShardTxProto::TEvent& proto) const override;
    virtual void DoAddToInteraction(const ui64 txId, TInteractionsContext& context) const override;
    virtual void DoRemoveFromInteraction(const ui64 txId, TInteractionsContext& context) const override;
    static inline const TFactory::TRegistrator<TEvReadStart> Registrator = TFactory::TRegistrator<TEvReadStart>(GetClassNameStatic());

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    TEvReadStart() = default;
    TEvReadStart(const ui64 pathId, const std::shared_ptr<arrow::Schema>& schema, const std::shared_ptr<TPKRangesFilter>& filter)
        : PathId(pathId)
        , Schema(schema)
        , Filter(filter) {
        AFL_VERIFY(PathId);
        AFL_VERIFY(Schema);
        AFL_VERIFY(Filter);
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
