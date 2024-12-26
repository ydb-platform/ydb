#include "memory_state_migration.h"
#include "datashard_impl.h"

#include <ydb/core/protos/datashard_config.pb.h>

namespace NKikimr::NDataShard {

static constexpr size_t MAX_DATASHARD_STATE_CHUNK_SIZE = 8_MB;

class TDataShardInMemoryRestoreActor
    : public IDataShardInMemoryRestoreActor
    , public TActor<TDataShardInMemoryRestoreActor>
{
public:
    TDataShardInMemoryRestoreActor(TDataShard* owner, const TActorId& prevStateActorId)
        : TActor(&TThis::StateWork)
        , Owner(owner)
        , PrevStateActorId(prevStateActorId)
    {}

    ~TDataShardInMemoryRestoreActor() {
        Detach();
    }

    void Start() {
        Send(PrevStateActorId,
            new TEvDataShard::TEvInMemoryStateRequest(Owner->Generation()),
            IEventHandle::FlagSubscribeOnSession | IEventHandle::FlagTrackDelivery);

        TDuration timeout = TDuration::MilliSeconds(AppData()->DataShardConfig.GetInMemoryStateMigrationTimeoutMs());
        Schedule(timeout, new TEvents::TEvWakeup);
    }

private:
    void PassAway() override {
        const ui32 nodeId = PrevStateActorId.NodeId();
        if (nodeId != SelfId().NodeId()) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe);
        }
        TActor::PassAway();
    }

    void Detach() {
        if (Owner) {
            Owner->InMemoryRestoreActor = nullptr;
            Owner = nullptr;
        }
    }

    void Restored() {
        auto* owner = Owner;
        Y_ABORT_UNLESS(owner);

        if (Locks) {
            owner->SysLocks.RestoreInMemoryLocks(std::move(Locks));
        }

        if (Vars) {
            owner->SnapshotManager.RestoreImmediateWriteEdge(Vars->ImmediateWriteEdge, Vars->ImmediateWriteEdgeReplied);
            owner->SnapshotManager.RestoreUnprotectedReadEdge(Vars->UnprotectedReadEdge);
            owner->InMemoryVarsRestored = true;
        }

        Detach();
        PassAway();

        owner->OnInMemoryStateRestored();
    }

    void Failed() {
        // TODO: cleanup partial state
        Locks.clear();
        Restored();
    }

    void OnTabletDestroyed() override {
        Detach();
    }

    void OnTabletDead() override {
        Detach();
        PassAway();
    }

    void Handle(TEvDataShard::TEvInMemoryStateResponse::TPtr& ev) {
        auto* msg = ev->Get();

        size_t prevSize = Buffer.size();
        if (msg->Record.HasSerializedStatePayloadIndex()) {
            TRope payload = msg->GetPayload(msg->Record.GetSerializedStatePayloadIndex());
            Buffer.Insert(Buffer.End(), std::move(payload));
        }

        size_t lastOffset = 0;
        for (size_t offset : msg->Record.GetSerializedStateCheckpoints()) {
            // These offsets are relative to the start of the new chunk, we make
            // them relative to the start of our accumulated buffer and then
            // transform into parseable chunk sizes.
            offset += prevSize;
            // Try to fail gracefully instead of crashing on unexpected data
            if (offset < lastOffset) {
                LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                    "Received TEvInMemoryStateResponse with checkpoints that go backwards");
                Failed();
                return;
            }
            if (Buffer.size() < offset) {
                LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                    "Received TEvInMemoryStateResponse with checkpoints that overflow current buffer");
                Failed();
                return;
            }

            if (offset != lastOffset) {
                Checkpoints.push_back(offset - lastOffset);
            }
            lastOffset = offset;
        }

        if (!msg->Record.HasContinuationToken() && lastOffset < Buffer.size()) {
            // Make sure we process the last chunk in the same code below
            Checkpoints.push_back(Buffer.size() - lastOffset);
        }

        for (size_t chunkSize : Checkpoints) {
            Y_ABORT_UNLESS(chunkSize <= Buffer.size(), "Unexpected end of buffer");

            NKikimrTxDataShard::TInMemoryState* state = google::protobuf::Arena::CreateMessage<NKikimrTxDataShard::TInMemoryState>(&Arena);
            {
                TRopeStream stream(Buffer.Begin(), chunkSize);
                bool ok = state->ParseFromZeroCopyStream(&stream);
                if (!ok) {
                    LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                        "Received TEvInMemoryStateResponse has a chunk that cannot be parsed");
                    Failed();
                    return;
                }
            }
            Buffer.EraseFront(chunkSize);

            if (state->HasVars()) {
                const auto& protoVars = state->GetVars();
                if (!Vars) {
                    Vars.emplace();
                }
                if (protoVars.HasImmediateWriteEdge()) {
                    Vars->ImmediateWriteEdge = TRowVersion::FromProto(protoVars.GetImmediateWriteEdge());
                }
                if (protoVars.HasImmediateWriteEdgeReplied()) {
                    Vars->ImmediateWriteEdgeReplied = TRowVersion::FromProto(protoVars.GetImmediateWriteEdgeReplied());
                }
                if (protoVars.HasUnprotectedReadEdge()) {
                    Vars->UnprotectedReadEdge = TRowVersion::FromProto(protoVars.GetUnprotectedReadEdge());
                }
            }
            for (const auto& protoLock : state->GetLocks()) {
                auto& row = Locks[protoLock.GetLockId()];
                row.LockId = protoLock.GetLockId();
                row.LockNodeId = protoLock.GetLockNodeId();
                row.Generation = protoLock.GetGeneration();
                row.Counter = protoLock.GetCounter();
                row.CreateTs = protoLock.GetCreateTs();
                row.Flags = protoLock.GetFlags();
                if (protoLock.HasBreakVersion()) {
                    row.BreakVersion = TRowVersion::FromProto(protoLock.GetBreakVersion());
                }
                for (const auto& protoPathId : protoLock.GetReadTables()) {
                    row.ReadTables.push_back(TPathId::FromProto(protoPathId));
                }
                for (const auto& protoPathId : protoLock.GetWriteTables()) {
                    row.WriteTables.push_back(TPathId::FromProto(protoPathId));
                }
            }
            for (const auto& protoRange : state->GetLockRanges()) {
                auto* row = Locks.FindPtr(protoRange.GetLockId());
                if (!row) {
                    LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                        "Received lock range for a missing lock " << protoRange.GetLockId());
                    Failed();
                    return;
                }
                auto& range = row->Ranges.emplace_back();
                range.TableId = TPathId::FromProto(protoRange.GetTableId());
                range.Flags = protoRange.GetFlags();
                range.Data = protoRange.GetData();
            }
            for (const auto& protoConflict : state->GetLockConflicts()) {
                auto* row = Locks.FindPtr(protoConflict.GetLockId());
                if (!row) {
                    LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                        "Received lock conflict for a missing lock " << protoConflict.GetLockId());
                    Failed();
                    return;
                }
                row->Conflicts.push_back(protoConflict.GetConflictId());
            }
            for (const auto& protoVolatileDep : state->GetLockVolatileDependencies()) {
                auto* row = Locks.FindPtr(protoVolatileDep.GetLockId());
                if (!row) {
                    LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                        "Received volatile dependency for a missing lock " << protoVolatileDep.GetLockId());
                    Failed();
                    return;
                }
                row->VolatileDependencies.push_back(protoVolatileDep.GetTxId());
            }

            Arena.Reset();
        }

        if (msg->Record.HasContinuationToken()) {
            // Request the next data chunk
            // Note: we have subscribed in the first request already
            Send(PrevStateActorId,
                new TEvDataShard::TEvInMemoryStateRequest(Owner->Generation(), msg->Record.GetContinuationToken()),
                IEventHandle::FlagTrackDelivery);
            return;
        }

        Restored();
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        Failed();
    }

    void Handle(TEvents::TEvUndelivered::TPtr&) {
        Failed();
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        Failed();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvInMemoryStateResponse, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        }
    }

private:
    struct TVars {
        TRowVersion ImmediateWriteEdge;
        TRowVersion ImmediateWriteEdgeReplied;
        TRowVersion UnprotectedReadEdge;
    };

private:
    TDataShard* Owner;
    TActorId PrevStateActorId;

    TRope Buffer;
    TVector<size_t> Checkpoints;

    google::protobuf::Arena Arena;
    THashMap<ui64, ILocksDb::TLockRow> Locks;
    std::optional<TVars> Vars;
};

void TDataShard::StartInMemoryRestoreActor() {
    if (InMemoryStatePrevActorId &&
        AppData()->FeatureFlags.GetEnableDataShardInMemoryStateMigration() &&
        (InMemoryStatePrevGeneration == Generation() - 1 ||
         AppData()->FeatureFlags.GetEnableDataShardInMemoryStateMigrationAcrossGenerations()))
    {
        auto* actor = new TDataShardInMemoryRestoreActor(this, InMemoryStatePrevActorId);
        InMemoryRestoreActor = actor;
        RegisterWithSameMailbox(actor);
        // Note: avoid unnecessary bootstrap round-trip cost on the same mailbox
        actor->Start();
        return;
    }

    OnInMemoryStateRestored();
}

class TDataShardInMemoryStateActor
    : public IDataShardInMemoryStateActor
    , public TActor<TDataShardInMemoryStateActor>
{
public:
    TDataShardInMemoryStateActor(TDataShard* owner, const TActorId& prevStateActorId)
        : TActor(&TThis::StateWork)
        , Owner(owner)
        , PrevStateActorId(prevStateActorId)
    {}

    ~TDataShardInMemoryStateActor() {
        Detach();
    }

    void ConfirmPersistent() override {
        if (PrevStateActorId) {
            Send(PrevStateActorId, new TEvents::TEvPoison);
            PrevStateActorId = {};
        }
    }

    void PreserveState() {
        Y_ABORT_UNLESS(Owner, "Unexpected call from a detached tablet");
        Y_ABORT_UNLESS(Owner->InMemoryStateActor == this, "Unexpected call while state actor is not attached");

        auto state = Owner->PreserveInMemoryState();

        Chunks.reserve(state.Chunks.size());
        for (TString& chunk : state.Chunks) {
            Chunks.emplace_back(std::move(chunk));
        }

        Checkpoints = std::move(state.Checkpoints);

        size_t size = 0;
        size_t checkpointIndex = 0;
        CheckpointsByChunk.reserve(Chunks.size());
        for (const TRope& chunk : Chunks) {
            size_t offset = size;
            size += chunk.size();
            while (checkpointIndex < Checkpoints.size() && Checkpoints[checkpointIndex] <= size) {
                // Convert included checkpoint to a relative offset
                Checkpoints[checkpointIndex] -= offset;
                checkpointIndex++;
            }
            CheckpointsByChunk.push_back(checkpointIndex);
        }
    }

    void Detach() {
        if (Owner) {
            Owner->InMemoryStateActor = nullptr;
            Owner = nullptr;
        }
    }

    void OnTabletDestroyed() override {
        // Note: this should only be called on actor system shutdown
        // We don't preserve state and expect to be destroyed soon
        Detach();

        // Not strictly necessary, but make sure we ignore all messages and
        // reply with undelivery notifications, as if this actor doesn't exist.
        Become(&TThis::StateDead);
    }

    void OnTabletDead() override {
        PreserveState();
        Detach();

        TDuration timeout = TDuration::MilliSeconds(AppData()->DataShardConfig.GetInMemoryStateMigrationTimeoutMs());
        Schedule(timeout, new TEvents::TEvPoison);
    }

    void Handle(TEvDataShard::TEvInMemoryStateRequest::TPtr& ev) {
        auto* msg = ev->Get();

        // Receiving a request implies our address is persistent
        ConfirmPersistent();

        if (Owner) {
            // Tablet might not be dead yet, but it will be very soon
            OnTabletDead();
        }

        size_t nextIndex = 0;
        if (msg->Record.HasContinuationToken()) {
            NKikimrTxDataShard::TInMemoryStateContinuationToken token;
            bool ok = token.ParseFromString(msg->Record.GetContinuationToken());
            Y_ABORT_UNLESS(ok, "Cannot parse continuation token");
            nextIndex = token.GetNextIndex();
        }

        auto res = std::make_unique<TEvDataShard::TEvInMemoryStateResponse>();
        if (nextIndex < Chunks.size()) {
            res->Record.SetSerializedStatePayloadIndex(res->AddPayload(TRope(Chunks[nextIndex])));
            size_t checkBegin = nextIndex > 0 ? CheckpointsByChunk[nextIndex - 1] : 0;
            size_t checkEnd = CheckpointsByChunk[nextIndex];
            for (size_t i = checkBegin; i != checkEnd; ++i) {
                res->Record.AddSerializedStateCheckpoints(Checkpoints[i]);
            }
            ++nextIndex;
            if (nextIndex < Chunks.size()) {
                NKikimrTxDataShard::TInMemoryStateContinuationToken token;
                token.SetNextIndex(nextIndex);
                bool ok = token.SerializeToString(res->Record.MutableContinuationToken());
                Y_ABORT_UNLESS(ok, "Cannot serialize continuation token");
            }
        }

        Send(ev->Sender, res.release());
    }

    void HandlePoison() {
        Detach();
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvInMemoryStateRequest, Handle);
            sFunc(TEvents::TEvPoison, HandlePoison);
        }
    }

    STFUNC(StateDead) {
        TActivationContext::Send(IEventHandle::ForwardOnNondelivery(ev, TEvents::TEvUndelivered::ReasonActorUnknown));
    }

private:
    TDataShard* Owner;
    TActorId PrevStateActorId;
    TVector<TRope> Chunks;
    TVector<size_t> Checkpoints;
    TVector<size_t> CheckpointsByChunk;
};

bool TDataShard::StartInMemoryStateActor() {
    if (!InMemoryStateActorId && AppData()->FeatureFlags.GetEnableDataShardInMemoryStateMigration()) {
        auto* actor = new TDataShardInMemoryStateActor(this, InMemoryStatePrevActorId);
        InMemoryStateActor = actor;
        InMemoryStateActorId = RegisterWithSameMailbox(actor);
        return true;
    }

    return false;
}

class TDataShardPreservedInMemoryStateOutputStream
    : public google::protobuf::io::ZeroCopyOutputStream
{
public:
    TDataShardPreservedInMemoryStateOutputStream() = default;

    TDataShardPreservedInMemoryStateOutputStream(const TDataShardPreservedInMemoryStateOutputStream&) = delete;
    TDataShardPreservedInMemoryStateOutputStream& operator=(const TDataShardPreservedInMemoryStateOutputStream&) = delete;

    void Reserve(size_t size) {
        while (Reserved < size) {
            if (ReserveIndex == Buffers.size()) {
                // Note: new buffer may have some initial capacity
                Reserved += Buffers.emplace_back().capacity();
                if (size <= Reserved) {
                    // Initial capacity is enough
                    break;
                }
            }
            size_t currentCapacity = Buffers.back().capacity();
            size_t targetCapacity = FastClp2(currentCapacity + (size - Reserved));
            if (targetCapacity > MAX_DATASHARD_STATE_CHUNK_SIZE) {
                targetCapacity = MAX_DATASHARD_STATE_CHUNK_SIZE;
                Buffers.back().reserve(targetCapacity);
                Reserved += Buffers.back().capacity() - currentCapacity;
                ReserveIndex++;
                continue;
            }
            Buffers.back().reserve(targetCapacity);
            Reserved += Buffers.back().capacity() - currentCapacity;
        }
    }

    bool Next(void** data, int* size) override {
        // Make sure we have at least 16 bytes available
        if (Reserved == 0) {
            Reserve(16);
        }

        // We should have at least 1 byte available
        Y_ABORT_UNLESS(WriteIndex < Buffers.size());
        if (Buffers[WriteIndex].size() == Buffers[WriteIndex].capacity()) {
            ++WriteIndex;
            Y_ABORT_UNLESS(WriteIndex < Buffers.size());
        }
        TString& buffer = Buffers[WriteIndex];
        Y_ABORT_UNLESS(buffer.size() < buffer.capacity());
        size_t oldSize = buffer.size();
        size_t newSize = buffer.capacity();
        buffer.resize(newSize);
        Y_ABORT_UNLESS(buffer.capacity() == newSize);
        Written += newSize - oldSize;
        Reserved -= newSize - oldSize;
        *data = buffer.Detach() + oldSize;
        *size = newSize - oldSize;
        return true;
    }

    void BackUp(int count) override {
        Y_ABORT_UNLESS(count >= 0);
        Y_ABORT_UNLESS(WriteIndex < Buffers.size());
        TString& buffer = Buffers[WriteIndex];
        Y_ABORT_UNLESS(buffer.size() >= (size_t)count);
        size_t oldCapacity = buffer.capacity();
        buffer.resize(buffer.size() - (size_t)count);
        Y_ABORT_UNLESS(buffer.capacity() == oldCapacity);
        Reserved += count;
        Written -= count;
    }

    int64_t ByteCount() const override {
        return Written;
    }

    TVector<TString> Finish() {
        if (WriteIndex + 1 < Buffers.size()) {
            Buffers.resize(WriteIndex + 1);
        }
        if (!Buffers.empty() && Buffers.back().empty()) {
            Buffers.pop_back();
        }
        return std::move(Buffers);
    }

private:
    TVector<TString> Buffers;
    size_t ReserveIndex = 0;
    size_t Reserved = 0;
    size_t WriteIndex = 0;
    size_t Written = 0;
};

TDataShard::TPreservedInMemoryState TDataShard::PreserveInMemoryState() {
    TDataShardPreservedInMemoryStateOutputStream stream;
    TVector<size_t> checkpoints;

    google::protobuf::Arena arena;
    NKikimrTxDataShard::TInMemoryState* state = google::protobuf::Arena::CreateMessage<NKikimrTxDataShard::TInMemoryState>(&arena);
    size_t currentStateSize = 0;
    bool needCheckpoint = false;

    auto flushState = [&]() {
        stream.Reserve(state->ByteSizeLong());
        bool ok = state->SerializeToZeroCopyStream(&stream);
        Y_ABORT_UNLESS(ok, "Unexpected failure to serialize in-memory state");
    };

    auto resetState = [&]() {
        arena.Reset();
        state = google::protobuf::Arena::CreateMessage<NKikimrTxDataShard::TInMemoryState>(&arena);
        currentStateSize = 0;
    };

    auto addedMessage = [&](size_t messageSize) {
        currentStateSize += 1 + google::protobuf::io::CodedOutputStream::VarintSize32(messageSize) + messageSize;
        if (currentStateSize >= MAX_DATASHARD_STATE_CHUNK_SIZE) {
            flushState();
            resetState();
            needCheckpoint = true;
        }
    };

    auto maybeCheckpoint = [&]() {
        if (needCheckpoint) {
            if (currentStateSize > 0) {
                flushState();
                resetState();
            }
            checkpoints.push_back(stream.ByteCount());
            needCheckpoint = false;
        }
    };

    // Serialize important in-memory vars
    {
        auto* vars = state->MutableVars();
        SnapshotManager.GetImmediateWriteEdge().ToProto(vars->MutableImmediateWriteEdge());
        SnapshotManager.GetImmediateWriteEdgeReplied().ToProto(vars->MutableImmediateWriteEdgeReplied());
        SnapshotManager.GetUnprotectedReadEdge().ToProto(vars->MutableUnprotectedReadEdge());
        addedMessage(vars->ByteSizeLong());
        maybeCheckpoint();

        // Note: we mark in-memory vars as frozen, but it doesn't affect much.
        // PreserveInMemoryState is called either before destruction or when
        // a newer generation unexpectedly requests our state. The latter may
        // only happen after a newer generation has locked the storage and
        // obtained the tablet lease, so this generation shouldn't be able
        // to commit anything new or even serve any new read-only requests,
        // otherwise it would be possible to read stale data.
        InMemoryVarsFrozen = true;
    }

    for (const auto& pr : SysLocks.GetLocks()) {
        const auto& lockInfo = *pr.second;
        auto* protoLockInfo = state->AddLocks();
        protoLockInfo->SetLockId(lockInfo.GetLockId());
        protoLockInfo->SetLockNodeId(lockInfo.GetLockNodeId());
        protoLockInfo->SetGeneration(lockInfo.GetGeneration());
        protoLockInfo->SetCounter(lockInfo.GetRawCounter());
        protoLockInfo->SetCreateTs(lockInfo.GetCreationTime().MicroSeconds());
        protoLockInfo->SetFlags((ui64)lockInfo.GetFlags());
        if (const auto& version = lockInfo.GetBreakVersion()) {
            version->ToProto(protoLockInfo->MutableBreakVersion());
        }
        for (const auto& pathId : lockInfo.GetReadTables()) {
            pathId.ToProto(protoLockInfo->AddReadTables());
        }
        for (const auto& pathId : lockInfo.GetWriteTables()) {
            pathId.ToProto(protoLockInfo->AddWriteTables());
        }
        addedMessage(protoLockInfo->ByteSizeLong());
        for (const auto& point : lockInfo.GetPoints()) {
            auto serialized = point.ToSerializedLockRange();
            auto* protoRange = state->AddLockRanges();
            protoRange->SetLockId(lockInfo.GetLockId());
            serialized.TableId.ToProto(protoRange->MutableTableId());
            protoRange->SetFlags(serialized.Flags);
            protoRange->SetData(std::move(serialized.Data));
            addedMessage(protoRange->ByteSizeLong());
        }
        for (const auto& range : lockInfo.GetRanges()) {
            auto serialized = range.ToSerializedLockRange();
            auto* protoRange = state->AddLockRanges();
            protoRange->SetLockId(lockInfo.GetLockId());
            serialized.TableId.ToProto(protoRange->MutableTableId());
            protoRange->SetFlags(serialized.Flags);
            protoRange->SetData(std::move(serialized.Data));
            addedMessage(protoRange->ByteSizeLong());
        }
        lockInfo.ForAllConflicts(
            [&](const auto* conflict) {
                auto* protoConflict = state->AddLockConflicts();
                protoConflict->SetLockId(lockInfo.GetLockId());
                protoConflict->SetConflictId(conflict->GetLockId());
                addedMessage(protoConflict->ByteSizeLong());
            },
            ELockConflictFlags::BreakThemOnOurCommit);
        lockInfo.ForAllVolatileDependencies(
            [&](ui64 txId) {
                auto* protoDep = state->AddLockVolatileDependencies();
                protoDep->SetLockId(lockInfo.GetLockId());
                protoDep->SetTxId(txId);
                addedMessage(protoDep->ByteSizeLong());
            });
        maybeCheckpoint();
    }

    if (state->ByteSizeLong()) {
        flushState();
    }

    return { stream.Finish(), std::move(checkpoints) };
}

} // namespace NKikimr::NDataShard
