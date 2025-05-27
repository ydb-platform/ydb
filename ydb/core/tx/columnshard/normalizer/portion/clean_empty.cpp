#include "clean_empty.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <util/string/join.h>

namespace NKikimr::NOlap::NSyncChunksWithPortions {

class IDBModifier {
public:
    virtual void Apply(NIceDb::TNiceDb& db) = 0;
    virtual TString GetId() const = 0;
    virtual ~IDBModifier() = default;
};

class TRemoveV0: public IDBModifier {
private:
    const TPortionAddress PortionAddress;
    std::vector<TColumnChunkLoadContext> Chunks;
    virtual TString GetId() const override {
        return "V0";
    }

    virtual void Apply(NIceDb::TNiceDb& db) override {
        for (auto&& i : Chunks) {
            AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "remove_portion_v0")("path_id", PortionAddress.GetPathId())(
                "portion_id", PortionAddress.GetPortionId())("chunk", i.GetAddress().DebugString());
            db.Table<NColumnShard::Schema::IndexColumns>()
                .Key(0, 0, i.GetAddress().GetColumnId(), i.GetMinSnapshotDeprecated().GetPlanStep(),
                    i.GetMinSnapshotDeprecated().GetTxId(), PortionAddress.GetPortionId(), i.GetAddress().GetChunkIdx())
                .Delete();
        }
    }

public:
    TRemoveV0(const TPortionAddress& portionAddress, const std::vector<TColumnChunkLoadContext>& chunks)
        : PortionAddress(portionAddress)
        , Chunks(chunks) {
    }
};

class TRemoveV1: public IDBModifier {
private:
    const TPortionAddress PortionAddress;
    std::vector<TChunkAddress> Chunks;
    virtual TString GetId() const override {
        return "V1";
    }

    virtual void Apply(NIceDb::TNiceDb& db) override {
        for (auto&& i : Chunks) {
            AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "remove_portion_v1")("path_id", PortionAddress.GetPathId())(
                "portion_id", PortionAddress.GetPortionId())("chunk", i.DebugString());
            db.Table<NColumnShard::Schema::IndexColumnsV1>()
                .Key(PortionAddress.GetPathId().GetRawValue(), PortionAddress.GetPortionId(), i.GetColumnId(), i.GetChunkIdx())
                .Delete();
        }
    }

public:
    TRemoveV1(const TPortionAddress& portionAddress, const std::vector<TChunkAddress>& chunks)
        : PortionAddress(portionAddress)
        , Chunks(chunks) {
    }
};

class TRemoveV2: public IDBModifier {
private:
    const TPortionAddress PortionAddress;
    std::vector<TChunkAddress> Chunks;
    virtual TString GetId() const override {
        return "V2";
    }

    virtual void Apply(NIceDb::TNiceDb& db) override {
        AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "remove_portion_v2")("path_id", PortionAddress.GetPathId())(
            "portion_id", PortionAddress.GetPortionId());
        db.Table<NColumnShard::Schema::IndexColumnsV2>().Key(PortionAddress.GetPathId().GetRawValue(), PortionAddress.GetPortionId()).Delete();
    }

public:
    TRemoveV2(const TPortionAddress& portionAddress)
        : PortionAddress(portionAddress) {
    }
};

class TRemovePortion: public IDBModifier {
private:
    const TPortionAddress PortionAddress;
    const ui64 PlanStep = 0;
    std::vector<TChunkAddress> Chunks;
    virtual TString GetId() const override {
        return TString("P::") + (PlanStep ? "DEL" : "EXIST");
    }

    virtual void Apply(NIceDb::TNiceDb& db) override {
        AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "remove_portion")("path_id", PortionAddress.GetPathId())(
            "portion_id", PortionAddress.GetPortionId());
        db.Table<NColumnShard::Schema::IndexPortions>().Key(PortionAddress.GetPathId().GetRawValue(), PortionAddress.GetPortionId()).Delete();
    }

public:
    TRemovePortion(const TPortionAddress& portionAddress, const ui64 planStep)
        : PortionAddress(portionAddress)
        , PlanStep(planStep)
    {
    }
};

namespace {
bool GetColumnPortionAddresses(NTabletFlatExecutor::TTransactionContext& txc, std::map<TPortionAddress, std::shared_ptr<IDBModifier>>& resultV0,
    std::map<TPortionAddress, std::shared_ptr<IDBModifier>>& resultV1, std::map<TPortionAddress, std::shared_ptr<IDBModifier>>& resultV2,
    std::map<TPortionAddress, std::shared_ptr<IDBModifier>>& portions, NColumnShard::TBlobGroupSelector& dsGroupSelector) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    if (!Schema::Precharge<Schema::IndexColumnsV1>(db, txc.DB.GetScheme())) {
        return false;
    }
    if (!Schema::Precharge<Schema::IndexColumnsV2>(db, txc.DB.GetScheme())) {
        return false;
    }
    if (!Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme())) {
        return false;
    }
    if (!Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme())) {
        return false;
    }

    {
        std::map<TPortionAddress, std::vector<TColumnChunkLoadContext>> usedPortions;
        auto rowset = db.Table<Schema::IndexColumns>().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            TColumnChunkLoadContext chunk(rowset, &dsGroupSelector);
            usedPortions[chunk.GetPortionAddress()].emplace_back(chunk);
            if (!rowset.Next()) {
                return false;
            }
        }
        std::map<TPortionAddress, std::shared_ptr<IDBModifier>> tasks;
        for (auto&& i : usedPortions) {
            tasks.emplace(i.first, std::make_shared<TRemoveV0>(i.first, i.second));
        }
        std::swap(resultV0, tasks);
    }
    {
        std::map<TPortionAddress, std::vector<TChunkAddress>> usedPortions;
        auto rowset = db.Table<Schema::IndexColumnsV1>()
                          .Select<Schema::IndexColumnsV1::PathId, Schema::IndexColumnsV1::PortionId, Schema::IndexColumnsV1::SSColumnId,
                              Schema::IndexColumnsV1::ChunkIdx>();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            TPortionAddress address(TInternalPathId::FromRawValue(rowset.GetValue<Schema::IndexColumnsV1::PathId>()), rowset.GetValue<Schema::IndexColumnsV1::PortionId>());
            TChunkAddress cAddress(rowset.GetValue<Schema::IndexColumnsV1::SSColumnId>(), rowset.GetValue<Schema::IndexColumnsV1::ChunkIdx>());
            usedPortions[address].emplace_back(cAddress);
            if (!rowset.Next()) {
                return false;
            }
        }
        std::map<TPortionAddress, std::shared_ptr<IDBModifier>> tasks;
        for (auto&& i : usedPortions) {
            tasks.emplace(i.first, std::make_shared<TRemoveV1>(i.first, i.second));
        }
        std::swap(resultV1, tasks);
    }
    {
        std::map<TPortionAddress, std::shared_ptr<IDBModifier>> usedPortions;
        auto rowset = db.Table<Schema::IndexColumnsV2>().Select<Schema::IndexColumnsV2::PathId, Schema::IndexColumnsV2::PortionId>();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            TPortionAddress portionAddress(
                TInternalPathId::FromRawValue(rowset.GetValue<Schema::IndexColumnsV2::PathId>()), rowset.GetValue<Schema::IndexColumnsV2::PortionId>());
            usedPortions.emplace(portionAddress, std::make_shared<TRemoveV2>(portionAddress));
            if (!rowset.Next()) {
                return false;
            }
        }
        std::swap(resultV2, usedPortions);
    }
    {
        std::map<TPortionAddress, std::shared_ptr<IDBModifier>> usedPortions;
        auto rowset = db.Table<Schema::IndexPortions>()
                          .Select<Schema::IndexPortions::PathId, Schema::IndexPortions::PortionId, Schema::IndexPortions::XPlanStep>();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            TPortionAddress portionAddress(
                TInternalPathId::FromRawValue(rowset.GetValue<Schema::IndexPortions::PathId>()), rowset.GetValue<Schema::IndexPortions::PortionId>());
            usedPortions.emplace(portionAddress,
                std::make_shared<TRemovePortion>(portionAddress,
                    rowset.HaveValue<Schema::IndexPortions::XPlanStep>() ? rowset.GetValue<Schema::IndexPortions::XPlanStep>() : 0));
            if (!rowset.Next()) {
                return false;
            }
        }
        std::swap(portions, usedPortions);
    }
    return true;
}

using TBatch = std::vector<TPortionAddress>;

class TIterator {
private:
    using TIteratorImpl = std::map<TPortionAddress, std::shared_ptr<IDBModifier>>::const_iterator;
    TIteratorImpl Current;
    TIteratorImpl End;

public:
    TIterator(std::map<TPortionAddress, std::shared_ptr<IDBModifier>>& source)
        : Current(source.begin())
        , End(source.end()) {
    }
    bool Next() {
        AFL_VERIFY(IsValid());
        ++Current;
        return Current != End;
    }

    bool IsValid() const {
        return Current != End;
    }

    TPortionAddress GetPortionAddress() const {
        AFL_VERIFY(IsValid());
        return Current->first;
    }

    std::shared_ptr<IDBModifier> GetModification() const {
        AFL_VERIFY(IsValid());
        return Current->second;
    }

    bool operator<(const TIterator& item) const {
        AFL_VERIFY(IsValid());
        AFL_VERIFY(item.IsValid());
        return Current->first < item.Current->first;
    }
};

std::optional<std::vector<std::vector<std::shared_ptr<IDBModifier>>>> GetPortionsToDelete(
    NTabletFlatExecutor::TTransactionContext& txc, NColumnShard::TBlobGroupSelector& dsGroupSelector) {
    using namespace NColumnShard;
    std::map<TPortionAddress, std::shared_ptr<IDBModifier>> v0Portions;
    std::map<TPortionAddress, std::shared_ptr<IDBModifier>> v1Portions;
    std::map<TPortionAddress, std::shared_ptr<IDBModifier>> v2Portions;
    std::map<TPortionAddress, std::shared_ptr<IDBModifier>> portions;
    if (!GetColumnPortionAddresses(txc, v0Portions, v1Portions, v2Portions, portions, dsGroupSelector)) {
        return std::nullopt;
    }
    std::vector<TPortionAddress> pack;
    std::map<TPortionAddress, std::vector<TIterator>> iteration;
    const bool v0Usage = AppDataVerified().ColumnShardConfig.GetColumnChunksV0Usage();
    const bool v1Usage = AppDataVerified().ColumnShardConfig.GetColumnChunksV1Usage();
    ui32 SourcesCount = 2;
    if (v0Usage) {
        ++SourcesCount;
        if (v0Portions.size()) {
            iteration[v0Portions.begin()->first].emplace_back(v0Portions);
        }
    }
    if (v1Usage) {
        ++SourcesCount;
        if (v1Portions.size()) {
            iteration[v1Portions.begin()->first].emplace_back(v1Portions);
        }
    }
    {
        if (v2Portions.size()) {
            iteration[v2Portions.begin()->first].emplace_back(v2Portions);
        }
    }
    {
        if (portions.size()) {
            iteration[portions.begin()->first].emplace_back(portions);
        }
    }
    std::vector<std::vector<std::shared_ptr<IDBModifier>>> result;
    std::vector<std::shared_ptr<IDBModifier>> modificationsPack;
    ui32 countPortionsForRemove = 0;
    THashMap<TString, ui32> reportCount;
    while (iteration.size()) {
        auto v = iteration.begin()->second;
        const bool isCorrect = (v.size() == SourcesCount);
        iteration.erase(iteration.begin());
        std::set<TString> problemId;
        for (auto&& i : v) {
            if (!isCorrect) {
                problemId.emplace(i.GetModification()->GetId());
                modificationsPack.emplace_back(i.GetModification());
                if (modificationsPack.size() == 100) {
                    result.emplace_back(std::vector<std::shared_ptr<IDBModifier>>());
                    countPortionsForRemove += modificationsPack.size();
                    std::swap(result.back(), modificationsPack);
                }
            }
            if (i.Next()) {
                iteration[i.GetPortionAddress()].emplace_back(i);
            }
        }
        if (isCorrect) {
            ++reportCount["normal"];
        } else {
            ++reportCount[JoinSeq(",", problemId)];
        }
    }
    {
        TStringBuilder sb;
        for (auto&& i : reportCount) {
            sb << i.first << ":" << i.second << ";";
        }
        if (modificationsPack.size()) {
            countPortionsForRemove += modificationsPack.size();
            result.emplace_back(std::move(modificationsPack));
        }
        AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("tasks_for_remove", countPortionsForRemove)("distribution", sb);
    }
    return result;
}

class TChanges: public INormalizerChanges {
public:
    TChanges(std::vector<std::shared_ptr<IDBModifier>>&& modifications)
        : Modifications(std::move(modifications)) {
    }
    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& m : Modifications) {
            m->Apply(db);
        }
        ACFL_WARN("normalizer", "TCleanEmptyPortionsNormalizer")("message", TStringBuilder() << GetSize() << " portions deleted");
        return true;
    }

    ui64 GetSize() const override {
        return Modifications.size();
    }

private:
    const std::vector<std::shared_ptr<IDBModifier>> Modifications;
};

}   //namespace

TConclusion<std::vector<INormalizerTask::TPtr>> TCleanEmptyPortionsNormalizer::DoInit(
    const TNormalizationController&, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    auto batchesToDelete = GetPortionsToDelete(txc, DsGroupSelector);
    if (!batchesToDelete) {
        return TConclusionStatus::Fail("Not ready");
    }

    std::vector<INormalizerTask::TPtr> result;
    for (auto&& b : *batchesToDelete) {
        result.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::move(b))));
    }
    return result;
}

}   // namespace NKikimr::NOlap::NSyncChunksWithPortions
