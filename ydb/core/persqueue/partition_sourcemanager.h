#pragma once

#include <util/generic/fwd.h>
#include <ydb/core/persqueue/events/internal.h>

#include "sourceid.h"
#include "utils.h"

namespace NKikimr::NPQ {

class TPartition;

class TPartitionSourceManager {
private:
    using TPartitionNode = TPartitionGraph::Node;

public:
    using TPartitionId = ui32;
    using TSourceIds = std::unordered_set<TString>;

    class TModificationBatch;

    struct TSourceInfo {
        TSourceInfo(TSourceIdInfo::EState state = TSourceIdInfo::EState::Unknown);

        TSourceIdInfo::EState State;
        ui64 SeqNo = 0;
        ui64 MinSeqNo = 0;
        ui64 Offset = 0;
        bool Explicit = false;
        TInstant WriteTimestamp;
    };

    class TSourceManager {
    public:
        TSourceManager(TModificationBatch& batch, const TString& id);

        // Checks whether a message with the specified Sourceid can be processed.
        // The message can be processed if it is not required to receive information
        // about it in the parent partitions, or this information has already been received.
        bool CanProcess() const;

        std::optional<ui64> SeqNo() const;
        bool Explicit() const;

        std::optional<ui64> CommittedSeqNo() const;
        std::optional<ui64> UpdatedSeqNo() const;

        void Update(ui64 seqNo, ui64 offset, TInstant timestamp);
        void Update(THeartbeat&& heartbeat);

    private:
        const TSourceIdMap& MemoryStorage() const;
        const TSourceIdMap& WriteStorage() const;


    private:
        TModificationBatch& Batch;
        const TString SourceId;

        TSourceInfo Info;

        TSourceIdMap::const_iterator InMemory;
        TSourceIdMap::const_iterator InWriter;
        std::unordered_map<TString, TSourceInfo>::const_iterator InSources;
    };

    // Encapsulates the logic of SourceId operation during modification
    class TModificationBatch {
        friend TSourceManager;
    public:
        TModificationBatch(TPartitionSourceManager& manager, ESourceIdFormat format);
        ~TModificationBatch();

        TMaybe<THeartbeat> CanEmitHeartbeat() const;
        TSourceManager GetSource(const TString& id);

        void Cancel();
        bool HasModifications() const;
        void FillRequest(TEvKeyValue::TEvRequest* request);

        template <typename... Args>
        void RegisterSourceId(const TString& sourceId, Args&&... args) {
            SourceIdWriter.RegisterSourceId(sourceId, std::forward<Args>(args)...);
        }
        void DeregisterSourceId(const TString& sourceId);

    private:
        TPartitionSourceManager& GetManager() const;

    private:
        TPartitionSourceManager& Manager;

        const TPartitionNode* Node;
        TSourceIdWriter SourceIdWriter;
        THeartbeatEmitter HeartbeatEmitter;
    };

    explicit TPartitionSourceManager(TPartition& partition);

    const TSourceInfo Get(const TString& sourceId) const;

    TModificationBatch CreateModificationBatch(const TActorContext& ctx);

private:
    const TPartitionNode* GetPartitionNode() const;
    TSourceIdStorage& GetSourceIdStorage() const;
    bool HasParents() const;


private:
    TPartition& Partition;
};

} // namespace NKikimr::NPQ
