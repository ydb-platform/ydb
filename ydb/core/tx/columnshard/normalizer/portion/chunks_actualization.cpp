#include "chunks_actualization.h"
#include "normalizer.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap::NSyncChunksWithPortions1 {

class TPatchItem {
private:
    TPortionLoadContext PortionInfo;
    YDB_READONLY_DEF(std::vector<TColumnChunkLoadContext>, Records);
    YDB_READONLY_DEF(std::vector<TIndexChunkLoadContext>, Indexes);

public:
    const TPortionLoadContext& GetPortionInfo() const {
        return PortionInfo;
    }

    TPatchItem(TPortionLoadContext&& portion, std::vector<TColumnChunkLoadContext>&& records, std::vector<TIndexChunkLoadContext>&& indexes)
        : PortionInfo(std::move(portion))
        , Records(std::move(records))
        , Indexes(std::move(indexes))
    {

    }
};

class TChanges: public INormalizerChanges {
private:
    std::vector<TPatchItem> Patches;

public:
    TChanges(std::vector<TPatchItem>&& patches)
        : Patches(std::move(patches)) {
    }
    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for (auto&& i : Patches) {
            auto meta = i.GetPortionInfo().GetMetaProto();
            ui32 recordsCount = 0;
            ui64 columnRawBytes = 0;
            ui64 indexRawBytes = 0;
            ui32 columnBlobBytes = 0;
            ui32 indexBlobBytes = 0;

            for (auto&& c : i.GetRecords()) {
                columnRawBytes += c.GetMetaProto().GetRawBytes();
                columnBlobBytes += c.GetBlobRange().GetSize();
                if (i.GetRecords().front().GetAddress().GetColumnId() == c.GetAddress().GetColumnId()) {
                    recordsCount += c.GetMetaProto().GetNumRows();
                }
            }
            for (auto&& c : i.GetIndexes()) {
                columnRawBytes += c.GetRawBytes();
                columnBlobBytes += c.GetDataSize();
            }
            meta.SetRecordsCount(recordsCount);
            meta.SetColumnRawBytes(columnRawBytes);
            meta.SetColumnBlobBytes(columnBlobBytes);
            meta.SetIndexRawBytes(indexRawBytes);
            meta.SetIndexBlobBytes(indexBlobBytes);

            db.Table<Schema::IndexPortions>()
                .Key(i.GetPortionInfo().GetPathId().GetInternalPathIdValue(), i.GetPortionInfo().GetPortionId())
                .Update(NIceDb::TUpdate<Schema::IndexPortions::Metadata>(meta.SerializeAsString()));
        }

        return true;
    }

    virtual ui64 GetSize() const override {
        return Patches.size();
    }

};

TConclusion<std::vector<INormalizerTask::TPtr>> TNormalizer::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    if (!AppDataVerified().ColumnShardConfig.GetColumnChunksV0Usage()) {
        return std::vector<INormalizerTask::TPtr>();
    }

    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexIndexes>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }

    THashMap<ui64, TPortionLoadContext> dbPortions;
    THashMap<ui64, std::vector<TColumnChunkLoadContext>> recordsByPortion;
    THashMap<ui64, std::vector<TIndexChunkLoadContext>> indexesByPortion;
    
    {
        auto rowset = db.Table<Schema::IndexPortions>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TPortionLoadContext portion(rowset);
            AFL_VERIFY(dbPortions.emplace(portion.GetPortionId(), portion).second);

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    {
        auto rowset = db.Table<Schema::IndexColumns>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TColumnChunkLoadContext chunk(rowset, &DsGroupSelector);
            const ui64 portionId = chunk.GetPortionId();
            AFL_VERIFY(dbPortions.contains(portionId));
            recordsByPortion[portionId].emplace_back(std::move(chunk));

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    {
        auto rowset = db.Table<Schema::IndexIndexes>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TIndexChunkLoadContext chunk(rowset, &DsGroupSelector);
            const ui64 portionId = chunk.GetPortionId();
            AFL_VERIFY(dbPortions.contains(portionId));
            indexesByPortion[portionId].emplace_back(std::move(chunk));

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }
    AFL_VERIFY(dbPortions.size() == recordsByPortion.size())("portions", dbPortions.size())("records", recordsByPortion.size());

    for (auto&& i : indexesByPortion) {
        AFL_VERIFY(dbPortions.contains(i.first));
    }

    std::vector<INormalizerTask::TPtr> tasks;
    if (dbPortions.empty()) {
        return tasks;
    }

    std::vector<TPatchItem> package;

    for (auto&& [_, portion] : dbPortions) {
        if (portion.GetMetaProto().GetRecordsCount()) {
            continue;
        }
        auto itRecords = recordsByPortion.find(portion.GetPortionId());
        AFL_VERIFY(itRecords != recordsByPortion.end());
        auto itIndexes = indexesByPortion.find(portion.GetPortionId());
        auto indexes = (itIndexes == indexesByPortion.end()) ? Default<std::vector<TIndexChunkLoadContext>>() : itIndexes->second;
        package.emplace_back(std::move(portion), std::move(itRecords->second), std::move(indexes));
        if (package.size() == 100) {
            std::vector<TPatchItem> local;
            local.swap(package);
            tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::move(local))));
        }
    }

    if (package.size() > 0) {
        tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::move(package))));
    }
    return tasks;
}

}   // namespace NKikimr::NOlap::NChunksActualization
