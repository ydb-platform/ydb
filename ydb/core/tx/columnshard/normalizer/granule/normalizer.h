#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>


namespace NKikimr::NOlap {

class TGranulesNormalizer: public NOlap::INormalizerComponent {
    struct TPortionKey {
        ui64 Index = 0;
        ui64 GranuleId = 0;
        ui64 PlanStep = 0;
        ui64 TxId = 0;
    };

    struct TUniqueId {
        ui64 PortionId = 0;
        ui32 Chunk = 0;
        ui64 ColumnIdx = 0;

        bool operator<(const TUniqueId& other) const {
            return std::make_tuple(PortionId, Chunk, ColumnIdx) < std::make_tuple(other.PortionId, other.Chunk, other.ColumnIdx);
        }
    };

public:
    virtual bool NormalizationRequired() const override {
        return true;
    }

    virtual const TString& GetName() const override {
        const static TString name = "TGranulesNormalizer";
        return name;
    }

    virtual ENormalizerResult NormalizeData(TNormalizationContext& nCtx, NTabletFlatExecutor::TTransactionContext& txc) override {
        Y_UNUSED(nCtx);

        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        bool ready = true;
        ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
        ready = ready & Schema::Precharge<Schema::IndexGranules>(db, txc.DB.GetScheme());
        if (!ready) {
            return ENormalizerResult::Failed;
        }

        TMap<TUniqueId, TPortionKey> portion2Key;
        {
            auto rowset = db.Table<Schema::IndexColumns>().Select();
            if (!rowset.IsReady()) {
                return ENormalizerResult::Failed;
            }

            while (!rowset.EndOfSet()) {
                if (!rowset.HaveValue<Schema::IndexColumns::PathId>()) {
                    TUniqueId id;
                    TPortionKey key;
                    key.PlanStep = rowset.GetValue<Schema::IndexColumns::PlanStep>();
                    key.TxId = rowset.GetValue<Schema::IndexColumns::TxId>();
                    id.PortionId = rowset.GetValue<Schema::IndexColumns::Portion>();
                    key.GranuleId = rowset.GetValue<Schema::IndexColumns::Granule>();
                    id.Chunk = rowset.GetValue<Schema::IndexColumns::Chunk>();
                    key.Index = rowset.GetValue<Schema::IndexColumns::Index>();
                    id.ColumnIdx = rowset.GetValue<Schema::IndexColumns::ColumnIdx>();

                    portion2Key[id] = key;
                }

                if (!rowset.Next()) {
                    return ENormalizerResult::Failed;
                }
            }
        }
        ACFL_INFO("normalizer", "TGranulesNormalizer")("message", TStringBuilder() << portion2Key.size() << " portions found");

        if (portion2Key.empty()) {
            return ENormalizerResult::Skip;
        }

        THashMap<ui64, ui64> granule2Path;
        {
            auto rowset = db.Table<Schema::IndexGranules>().Select();
            if (!rowset.IsReady()) {
                return ENormalizerResult::Failed;
            }

            while (!rowset.EndOfSet()) {
                ui64 pathId = rowset.GetValue<Schema::IndexGranules::PathId>();
                ui64 granuleId = rowset.GetValue<Schema::IndexGranules::Granule>();
                Y_ABORT_UNLESS(granuleId != 0);
                granule2Path[granuleId] = pathId;
                if (!rowset.Next()) {
                    return ENormalizerResult::Failed;
                }
            }
        }

        for (auto&& [ portionId, key ] : portion2Key) {
            auto granuleIt = granule2Path.find(key.GranuleId);
            Y_ABORT_UNLESS(granuleIt != granule2Path.end());

            db.Table<Schema::IndexColumns>().Key(key.Index, key.GranuleId, portionId.ColumnIdx,
            key.PlanStep, key.TxId, portionId.PortionId, portionId.Chunk).Update(
                NIceDb::TUpdate<Schema::IndexColumns::PathId>(granuleIt->second)
            );
        }
        return ENormalizerResult::Ok;
    }
};

}
