#include "read_observer.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/operations.h>

#include <map>

namespace NKikimr::NOlap::NStats::NScan {
namespace {

class TReadObserver: public NBlobOperations::NRead::IReadObserver {
    std::unique_ptr<TLocalStats> Stats;
    TOperationScope Scope;
    const TCallHandle RequestingScope;
    const std::function<bool(const NColumnShardStats::TDataSource&, std::optional<ui64>)> AttributeRead;
    std::map<TString, ui64> Pending;
    ui64 PendingCount = 0;
    std::optional<ui64> RequestedBytes;
    bool Started = false;

    void RecordRead(const NColumnShardStats::TDataSource& source, std::optional<ui64> bytes) {
        if (AttributeRead) {
            Scope.RecordReadSelf(source, bytes);
            if (!AttributeRead(source, bytes)) {
                Stats->ObserveLoss();
            }
        } else {
            Scope.RecordRead(source, bytes);
        }
    }

    void Record(const TString& storageId, std::optional<ui64> bytes, bool fromCache) {
        auto found = Pending.find(storageId);
        if (found == Pending.end() || !found->second) {
            Stats->ObserveLoss();
        } else {
            --found->second;
            --PendingCount;
        }
        NColumnShardStats::TDataSource source;
        source.SetStorageId(storageId);
        source.SetKind(fromCache                                         ? NColumnShardStats::BLOB_CACHE
                       : storageId == IStoragesManager::DefaultStorageId ? NColumnShardStats::BLOB_STORAGE
                       : storageId == IStoragesManager::MemoryStorageId  ? NColumnShardStats::MEMORY
                                                                         : NColumnShardStats::EXTERNAL_STORAGE);
        source.MutableReadBytes()->SetBoundary(NColumnShardStats::LOGICAL_DELIVERY);
        source.MutableReadBytes()->SetRepresentation(NColumnShardStats::ENCODED_PAYLOAD);
        RecordRead(source, bytes);
        if (!PendingCount) {
            Scope.Finish();
        } else {
            Scope.Suspend();
        }
    }

public:
    TReadObserver(std::unique_ptr<TLocalStats> local, TCallHandle requestingScope,
        std::function<bool(const NColumnShardStats::TDataSource&, std::optional<ui64>)> attributeRead)
        : Stats(std::move(local))
        , RequestingScope(std::move(requestingScope))
        , AttributeRead(std::move(attributeRead))
    {
    }

    ~TReadObserver() override {
        if (PendingCount || (!Started && Scope)) {
            // Missing replies do not identify which cache/backend would have served them.
            Stats->ObserveLoss();
            Scope.Suspend();
        }
    }

    void OnRequested(const TString&, ui64 bytes) noexcept override {
        RequestedBytes = RequestedBytes.value_or(0) + bytes;
    }

    void OnPreprovided(const TString& storageId, ui64 bytes) override {
        if (!Scope) {
            Scope = Stats->Measure(Stats->GetRoot().FindLeafIf([](const auto& definition) {
                return definition.GetKey() == "StorageRead";
            }));
        }
        Scope.Resume();
        NColumnShardStats::TDataSource source;
        source.SetStorageId(storageId);
        source.SetKind(NColumnShardStats::MEMORY);
        source.MutableReadBytes()->SetBoundary(NColumnShardStats::LOGICAL_DELIVERY);
        source.MutableReadBytes()->SetRepresentation(NColumnShardStats::ENCODED_PAYLOAD);
        RecordRead(source, bytes);
        if (PendingCount) {
            Scope.Suspend();
        }
    }

    void OnStarted() override {
        if (Started) {
            return;
        }
        Started = true;
        if (RequestedBytes) {
            Scope.Observe([&](auto& observation) {
                observation.Metric(&NColumnShardStats::TStorageReadDetails::MutableRequestedBytes).SetValue(*RequestedBytes);
            });
            // This callback precedes native dispatch while the read task owns the source lease.
            if (const auto* requestingNode = RequestingScope.GetNode()) {
                RequestingScope.Observe([&](auto& observation) {
                    auto& requested = requestingNode->GetDefinition().HasFetchingColumns()
                                          ? observation.Metric(&NColumnShardStats::TFetchingColumnsDetails::MutableRequestedBytes)
                                          : observation.Metric(&NColumnShardStats::TFetchingIndexesDetails::MutableRequestedBytes);
                    requested.SetValue(requested.GetValue() + *RequestedBytes);
                });
            }
        }
        if (PendingCount) {
            Scope.Suspend();
        } else {
            Scope.Finish();
        }
    }

    void OnStart(const TString& storageId, ui64 ranges) override {
        if (ranges && !Scope) {
            Scope = Stats->Measure(Stats->GetRoot().FindLeafIf([](const auto& definition) {
                return definition.GetKey() == "StorageRead";
            }));
        }
        Pending[storageId] += ranges;
        PendingCount += ranges;
        Scope.Suspend();
    }

    void OnData(const TString& storageId, ui64 bytes, bool fromCache) override {
        Scope.Resume();
        Record(storageId, bytes, fromCache);
    }

    void OnError(const TString& storageId) override {
        Scope.Resume();
        Record(storageId, std::nullopt, false);
    }
};

}   // namespace

std::function<bool(const NColumnShardStats::TDataSource&, std::optional<ui64>)> MakeReadAttributionCallback(
    TCallHandle current, std::function<bool()> canAttributeRead) {
    return [canAttributeRead = std::move(canAttributeRead), current = std::move(current)](
               const NColumnShardStats::TDataSource& dataSource, std::optional<ui64> bytes) {
        if (!canAttributeRead()) {
            return false;
        }
        bool observed = true;
        for (auto call = current; call; call = call.GetParent()) {
            if (call.GetNode()) {
                call.AttributeRead(dataSource, bytes);
            } else {
                observed = false;
            }
        }
        return observed;
    };
}

std::shared_ptr<NBlobOperations::NRead::IReadObserver> MakeReadObserver(std::unique_ptr<TLocalStats> local,
    const TOperationNode* attributionParent, TCallHandle requestingScope,
    std::function<bool(const NColumnShardStats::TDataSource&, std::optional<ui64>)> attributeRead) {
    if (!local) {
        return {};
    }
    auto* parent = attributionParent ? local->CloneBranch(*attributionParent) : &local->GetRoot();
    if (!parent || !local->AddChild(*parent, MakeStorageRead())) {
        return {};
    }
    return std::make_shared<TReadObserver>(std::move(local), std::move(requestingScope), std::move(attributeRead));
}

}   // namespace NKikimr::NOlap::NStats::NScan
