#pragma once

#include <util/generic/fwd.h>
#include <ydb/core/persqueue/events/internal.h>

#include "sourceid.h"
#include "utils.h"

namespace NKikimr::NPQ {

class TPartition;

class TPartitionSourceManager {
private:
    using TPartitionNode = std::optional<const TPartitionGraph::Node *>;

public:
    using TPartitionId = ui32;
    using TSourceIds = std::unordered_set<TString>;

    class TModificationBatch;

    struct TSourceInfo {
        TSourceInfo(TSourceIdInfo::EState state = TSourceIdInfo::EState::Unknown);

        TSourceIdInfo::EState State;
        ui64 SeqNo = 0;
        ui64 Offset = 0;
        bool Explicit = false;
        TInstant WriteTimestamp;

        bool Pending = false;

        operator bool() const;
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

        operator bool() const;

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

        TPartitionNode Node;
        TSourceIdWriter SourceIdWriter;
        THeartbeatEmitter HeartbeatEmitter;
    };

    explicit TPartitionSourceManager(TPartition& partition);

    // For a partition obtained as a result of a merge or split, it requests 
    // information about the consumer's parameters from the parent partitions.
    void EnsureSourceId(const TString& sourceId);
    void EnsureSourceIds(const TVector<TString>& sourceIds);

    // Returns true if we expect a response from the parent partitions
    bool WaitSources() const;

    const TSourceInfo Get(const TString& sourceId) const;

    TModificationBatch CreateModificationBatch(const TActorContext& ctx);

    void PassAway();

public:
    void Handle(TEvPQ::TEvSourceIdResponse::TPtr& ev, const TActorContext& ctx);

private:
    void ScheduleBatch();
    void FinishBatch(const TActorContext& ctx);
    bool RequireEnqueue(const TString& sourceId);

    TPartitionNode GetPartitionNode() const;
    TSourceIdStorage& GetSourceIdStorage() const;
    bool HasParents() const;

    TActorId PartitionRequester(TPartitionId id, ui64 tabletId);
    std::unique_ptr<TEvPQ::TEvSourceIdRequest> CreateRequest(TPartitionId id) const;


private:
    TPartition& Partition;

    TSourceIds UnknownSourceIds;
    TSourceIds PendingSourceIds;

    std::unordered_map<TString, TSourceInfo> Sources;

    ui64 Cookie = 0;
    std::set<ui64> PendingCookies;
    std::vector<TEvPQ::TEvSourceIdResponse::TPtr> Responses;
    std::unordered_map<TPartitionId, TActorId> RequesterActors;
};

NKikimrPQ::TEvSourceIdResponse::EState Convert(TSourceIdInfo::EState value);
TSourceIdInfo::EState Convert(NKikimrPQ::TEvSourceIdResponse::EState value);

} // namespace NKikimr::NPQ
