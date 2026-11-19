#include "column_cache_observer.h"

#include <ydb/core/tx/columnshard/stats/scan/reader/operations.h>

namespace NKikimr::NOlap::NStats::NScan {
namespace {

template <class TPolicy, NColumnShardStats::ESourceKind CacheKind, NColumnShardStats::EByteRepresentation Representation>
class TCacheObserver final: public ::NKikimr::NGeneralCache::NPublic::IRequestObserver<TPolicy> {
private:
    std::unique_ptr<TLocalStats> Local;
    TOperationScope Reader;
    TOperationScope PKFilter;
    TOperationScope Read;
    const std::function<bool(const NColumnShardStats::TDataSource&, std::optional<ui64>)> AttributeRead;

    void Record(const typename TPolicy::TObject& object, NColumnShardStats::ESourceKind kind) {
        Reader.Resume();
        PKFilter.Resume();
        Read.Resume();
        NColumnShardStats::TDataSource source;
        source.SetStorageId(TPolicy::GetCacheName());
        source.SetKind(kind);
        source.MutableReadBytes()->SetBoundary(NColumnShardStats::LOGICAL_DELIVERY);
        source.MutableReadBytes()->SetRepresentation(Representation);
        if constexpr (CacheKind == NColumnShardStats::COLUMN_CACHE) {
            Read.RecordRead(source, object ? object->GetRawSize() : std::nullopt);
        } else {
            Read.RecordReadSelf(source, std::nullopt);
            if (AttributeRead && !AttributeRead(source, std::nullopt)) {
                Local->ObserveLoss();
            }
        }
        Read.Suspend();
        PKFilter.Suspend();
        Reader.Suspend();
    }

public:
    explicit TCacheObserver(std::unique_ptr<TLocalStats> local, TOperationNode* parent = nullptr,
        std::function<bool(const NColumnShardStats::TDataSource&, std::optional<ui64>)> attributeRead = {})
        : Local(std::move(local))
        , AttributeRead(std::move(attributeRead))
    {
        if (!parent) {
            parent = &Local->GetRoot();
        }
        if constexpr (CacheKind == NColumnShardStats::COLUMN_CACHE) {
            NColumnShardStats::TOperation pk;
            pk.SetKey("PKFilter");
            pk.SetName("PKFilter");
            pk.MutablePKFilter();
            parent = Local->AddChild(Local->GetRoot(), pk);
        }
        auto storage = MakeStorageRead();
        storage.SetKey(CacheKind == NColumnShardStats::COLUMN_CACHE ? "StorageRead.ColumnCache" : "StorageRead.MetadataCache");
        storage.MutableStorageRead()->SetStorageId(TPolicy::GetCacheName());
        if (parent) {
            Local->AddChild(*parent, storage);
        }
    }

    void OnStarted() override {
        if constexpr (CacheKind == NColumnShardStats::COLUMN_CACHE) {
            Reader = Local->Measure(&Local->GetRoot());
            PKFilter = Local->Measure(Local->GetRoot().FindChild("PKFilter"));
        }
        auto* readNode = Local->GetRoot().FindLeafIf([](const auto& definition) {
            return definition.GetKey() ==
                   (CacheKind == NColumnShardStats::COLUMN_CACHE ? "StorageRead.ColumnCache" : "StorageRead.MetadataCache");
        });
        Read = Local->Measure(readNode);
        for (auto* scope : { &Reader, &PKFilter, &Read }) {
            scope->Observe([](auto& observation) {
                observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows)
                    .SetApplicability(NColumnShardStats::NOT_APPLICABLE);
                observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows)
                    .SetApplicability(NColumnShardStats::NOT_APPLICABLE);
            });
            scope->Suspend();
        }
    }

    void OnCacheHit(const typename TPolicy::TAddress&, const typename TPolicy::TObject& object) override {
        Record(object, CacheKind);
    }

    void OnRetrieved(const typename TPolicy::TAddress&, const typename TPolicy::TObject& object) override {
        // The cache knows that a provider returned the column, but not its backing storage.
        Record(object, NColumnShardStats::SOURCE_UNKNOWN);
    }

    void OnFinished() override {
        Reader.Resume();
        PKFilter.Resume();
        Read.Resume();
        Read.Finish();
        PKFilter.Finish();
        Reader.Finish();
        Local->Close();
    }
};

}   // namespace

std::shared_ptr<::NKikimr::NGeneralCache::NPublic::IRequestObserver<NGeneralCache::TColumnDataCachePolicy>> MakeColumnCacheObserver(
    std::unique_ptr<TLocalStats> local) {
    if (!local) {
        return {};
    }
    return std::make_shared<
        TCacheObserver<NGeneralCache::TColumnDataCachePolicy, NColumnShardStats::COLUMN_CACHE, NColumnShardStats::ARROW_LOGICAL>>(
        std::move(local));
}

std::shared_ptr<::NKikimr::NGeneralCache::NPublic::IRequestObserver<NGeneralCache::TPortionsMetadataCachePolicy>> MakeMetadataCacheObserver(
    std::unique_ptr<TLocalStats> local, const TOperationNode* parent,
    std::function<bool(const NColumnShardStats::TDataSource&, std::optional<ui64>)> attributeRead) {
    if (!local) {
        return {};
    }
    auto* node = parent ? local->CloneBranch(*parent) : &local->GetRoot();
    if (!node) {
        return {};
    }
    return std::make_shared<
        TCacheObserver<NGeneralCache::TPortionsMetadataCachePolicy, NColumnShardStats::METADATA_CACHE, NColumnShardStats::METADATA_FOOTPRINT>>(
        std::move(local), node, std::move(attributeRead));
}

}   // namespace NKikimr::NOlap::NStats::NScan
