#include "pdisk_mock.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/util/stlog.h>
#include <ydb/core/util/interval_set.h>

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>

namespace NKikimr {

#ifdef _MSC_VER
#define PDISK_MOCK_LOG(...)
#else
#define PDISK_MOCK_LOG(PRI, MARKER, ...) STLOG(NLog::PRI_##PRI, BS_PDISK, MARKER, Prefix << __VA_ARGS__)
#endif

struct TPDiskMockState::TImpl {
    struct TChunkData {
        std::unordered_map<ui32, const TString*> Blocks;
    };
    struct TOwner {
        TVDiskID VDiskId;
        ui32 SlotId;
        std::set<ui32> ReservedChunks, CommittedChunks;
        std::map<ui32, TChunkData> ChunkData;
        NPDisk::TOwnerRound OwnerRound = 0;
        TActorId CutLogId;
        std::deque<NPDisk::TLogRecord> Log;
        TMap<TLogSignature, NPDisk::TLogRecord> StartingPoints;
        ui64 LogDataSize = 0;
        bool Slain = false;
        ui64 LastLsn = 0;
    };

    const ui32 NodeId;
    const ui32 PDiskId;
    const ui64 PDiskGuid;
    const ui64 Size;
    const ui32 ChunkSize;
    const ui32 TotalChunks;
    const ui32 AppendBlockSize;
    std::map<ui8, TOwner> Owners;
    std::set<ui32> FreeChunks;
    ui32 NextFreeChunk = 1;
    std::unordered_map<TString, ui32> Blocks;
    TIntervalSet<ui64> Corrupted;
    NPDisk::TStatusFlags StatusFlags;
    THashSet<ui32> ReadOnlyVDisks;
    TString StateErrorReason;
    NPDisk::EDeviceType DeviceType;
    std::optional<TRcBuf> Metadata;

    TImpl(ui32 nodeId, ui32 pdiskId, ui64 pdiskGuid, ui64 size, ui32 chunkSize, NPDisk::EDeviceType deviceType)
        : NodeId(nodeId)
        , PDiskId(pdiskId)
        , PDiskGuid(pdiskGuid)
        , Size(size)
        , ChunkSize(chunkSize)
        , TotalChunks(Size / ChunkSize)
        , AppendBlockSize(4096)
        , NextFreeChunk(1)
        , StatusFlags(NPDisk::TStatusFlags{})
        , DeviceType(deviceType)
    {}

    TImpl(const TImpl&) = default;

    ui32 GetNumFreeChunks() const {
        return FreeChunks.size() + TotalChunks - NextFreeChunk;
    }

    void AdjustFreeChunks() {
        for (auto it = FreeChunks.end(); it != FreeChunks.begin() && *--it == NextFreeChunk - 1; it = FreeChunks.erase(it)) {
            --NextFreeChunk;
        }
    }

    ui32 AllocateChunk(TOwner& to) {
        ui32 chunkIdx = TotalChunks;

        if (FreeChunks.empty()) {
            chunkIdx = NextFreeChunk++;
            to.ReservedChunks.insert(chunkIdx);
        } else {
            auto it = FreeChunks.begin();
            chunkIdx = *it;
            to.ReservedChunks.insert(FreeChunks.extract(it));
        }

        Y_ABORT_UNLESS(chunkIdx != TotalChunks);
        return chunkIdx;
    }

    void AdjustRefs() {
        for (auto& [ownerId, owner] : Owners) {
            for (auto& [chunkIdx, chunk] : owner.ChunkData) {
                for (auto& [blockIdx, ref] : chunk.Blocks) {
                    const auto it = Blocks.find(*ref);
                    Y_ABORT_UNLESS(it != Blocks.end());
                    ref = &it->first;
                }
            }
        }
    }

    template<typename TQuery>
    std::variant<TOwner*, std::tuple<NKikimrProto::EReplyStatus, TString>> FindOwner(TQuery *msg) {
        if (const auto it = Owners.find(msg->Owner); it == Owners.end()) {
            Y_ABORT("invalid Owner");
        } else if (it->second.Slain) {
            return std::make_tuple(NKikimrProto::INVALID_OWNER, "VDisk is slain");
        } else if (msg->OwnerRound != it->second.OwnerRound) {
            return std::make_tuple(NKikimrProto::INVALID_ROUND, "invalid OwnerRound");
        } else {
            return &it->second;
        }
        return nullptr;
    }

    template<typename TQuery, typename TResult>
    TOwner *FindOwner(TQuery *msg, std::unique_ptr<TResult>& res) {
        auto owner = FindOwner(msg);
        if (std::holds_alternative<TOwner*>(owner)) {
            return std::get<0>(owner);
        }
        std::tie(res->Status, res->ErrorReason) = std::get<1>(owner);
        return nullptr;
    }

    template<typename TQuery>
    bool CheckIsReadOnlyOwner(TQuery *msg) {
        auto owner = FindOwner(msg);
        if (std::holds_alternative<TOwner*>(owner)) {
            if (auto o = std::get<0>(owner)) {
                return IsReadOnly(o->VDiskId);
            }
        }
        return false;
    }

    std::tuple<ui8, TOwner*> FindOrCreateOwner(const TVDiskID& vdiskId, ui32 slotId, bool *created) {
        bool slotIsValid = (slotId != Max<ui32>());
        for (auto& [ownerId, owner] : Owners) {
            if (slotIsValid) {
                if (slotId == owner.SlotId) {
                    Y_ABORT_UNLESS(owner.VDiskId.SameExceptGeneration(vdiskId));
                    *created = false;
                    return std::make_tuple(ownerId, &owner);
                }
            } else {
                if (owner.VDiskId.SameExceptGeneration(vdiskId)) {
                    *created = false;
                    return std::make_tuple(ownerId, &owner);
                }
            }
        }

        ui8 ownerId = 1;
        std::map<ui8, TOwner>::iterator it;
        for (it = Owners.begin(); it != Owners.end() && it->first == ownerId; ++it, ++ownerId)
        {}
        Y_ABORT_UNLESS(ownerId);
        it = Owners.emplace_hint(it, ownerId, TOwner());
        it->second.VDiskId = vdiskId;
        it->second.SlotId = slotId;
        *created = true;
        return std::make_tuple(ownerId, &it->second);
    }

    void ResetOwnerReservedChunks(TOwner& owner) {
        for (const TChunkIdx chunkIdx : owner.ReservedChunks) {
            owner.ChunkData.erase(chunkIdx);
        }
        FreeChunks.merge(owner.ReservedChunks);
        AdjustFreeChunks();
    }

    void CommitChunk(TOwner& owner, TChunkIdx chunkIdx) {
        const ui32 num = owner.ReservedChunks.erase(chunkIdx) + owner.CommittedChunks.erase(chunkIdx);
        Y_ABORT_UNLESS(num);
        const bool inserted = owner.CommittedChunks.insert(chunkIdx).second;
        Y_ABORT_UNLESS(inserted);
    }

    void DeleteChunk(TOwner& owner, TChunkIdx chunkIdx) {
        const ui32 num = owner.ReservedChunks.erase(chunkIdx) + owner.CommittedChunks.erase(chunkIdx);
        Y_ABORT_UNLESS(num);
        owner.ChunkData.erase(chunkIdx);
        const bool inserted = FreeChunks.insert(chunkIdx).second;
        Y_ABORT_UNLESS(inserted);
        AdjustFreeChunks();
    }

    void UncommitChunk(TOwner& owner, TChunkIdx chunkIdx) {
        if (owner.ReservedChunks.contains(chunkIdx)) {
        } else if (owner.CommittedChunks.erase(chunkIdx)) {
            owner.ReservedChunks.insert(chunkIdx);
        } else {
            Y_ABORT();
        }
    }

    void SetCorruptedArea(ui32 chunkIdx, ui32 begin, ui32 end, bool enabled) {
        const ui64 chunkBegin = ui64(chunkIdx) * ChunkSize;
        const ui64 diskBegin = chunkBegin + begin;
        const ui64 diskEnd = chunkBegin + end;
        if (enabled) {
            Corrupted |= {diskBegin, diskEnd};
        } else {
            Corrupted -= {diskBegin, diskEnd};
        }
    }

    bool HasCorruptedArea(ui32 chunkIdx, ui32 begin, ui32 end) {
        const ui64 chunkBegin = ui64(chunkIdx) * ChunkSize;
        return static_cast<bool>(Corrupted & TIntervalSet{chunkBegin + begin, chunkBegin + end});
    }

    std::set<ui32> GetChunks() {
        std::set<ui32> res;
        for (auto& [ownerId, owner] : Owners) {
            for (auto& [chunkIdx, data] : owner.ChunkData) {
                const bool inserted = res.insert(chunkIdx).second;
                Y_ABORT_UNLESS(inserted);
            }
        }
        return res;
    }

    ui32 GetChunkSize() const {
        return ChunkSize;
    }

    TMaybe<TOwner> GetOwner(const TVDiskID& vDiskId) const {
        for (auto& [ownerId, owner] : Owners) {
            if (owner.VDiskId.GroupID == vDiskId.GroupID && owner.VDiskId.VDisk == vDiskId.VDisk) {
                return owner;
            }
        }
        return Nothing();
    }

    TIntervalSet<i64> GetWrittenAreas(ui32 chunkIdx) const {
        TIntervalSet<i64> res;
        for (auto& [ownerId, owner] : Owners) {
            if (const auto it = owner.ChunkData.find(chunkIdx); it != owner.ChunkData.end()) {
                for (const auto& [idx, data] : it->second.Blocks) {
                    const ui32 offset = idx * AppendBlockSize;
                    res |= TIntervalSet<i64>(offset, offset + AppendBlockSize);
                }
                break;
            }
        }
        return res;
    }

    void TrimQuery() {
        for (auto& [ownerId, owner] : Owners) {
            if (!owner.Log.empty()) {
                TActivationContext::Send(new IEventHandle(owner.CutLogId, {}, new NPDisk::TEvCutLog(ownerId,
                    owner.OwnerRound, owner.Log.back().Lsn + 1, 0, 0, 0, 0)));
            }
        }
    }

    void SetStatusFlags(NPDisk::TStatusFlags flags) {
        StatusFlags = flags;
    }

    void SetStatusFlags(NKikimrBlobStorage::TPDiskSpaceColor::E spaceColor) {
        StatusFlags = SpaceColorToStatusFlag(spaceColor);
    }

    void SetReadOnly(const TVDiskID& vDiskId, bool isReadOnly) {
        if (isReadOnly) {
            ReadOnlyVDisks.insert(vDiskId.GroupID.GetRawId());
        } else {
            ReadOnlyVDisks.erase(vDiskId.GroupID.GetRawId());
        }
    }

    bool IsReadOnly(const TVDiskID& vDiskId) const {
        return ReadOnlyVDisks.contains(vDiskId.GroupID.GetRawId());
    }
};

TPDiskMockState::TPDiskMockState(ui32 nodeId, ui32 pdiskId, ui64 pdiskGuid, ui64 size, ui32 chunkSize,
        NPDisk::EDeviceType deviceType)
    : TPDiskMockState(std::make_unique<TImpl>(nodeId, pdiskId, pdiskGuid, size, chunkSize, deviceType))
{}

TPDiskMockState::TPDiskMockState(std::unique_ptr<TImpl>&& impl)
    : Impl(std::move(impl))
{}

TPDiskMockState::~TPDiskMockState()
{}

void TPDiskMockState::SetCorruptedArea(ui32 chunkIdx, ui32 begin, ui32 end, bool enabled) {
    Impl->SetCorruptedArea(chunkIdx, begin, end, enabled);
}

bool TPDiskMockState::HasCorruptedArea(ui32 chunkIdx, ui32 begin, ui32 end) {
    return Impl->HasCorruptedArea(chunkIdx, begin, end);
}

std::set<ui32> TPDiskMockState::GetChunks() {
    return Impl->GetChunks();
}

ui32 TPDiskMockState::GetChunkSize() const {
    return Impl->GetChunkSize();
}

TIntervalSet<i64> TPDiskMockState::GetWrittenAreas(ui32 chunkIdx) const {
    return Impl->GetWrittenAreas(chunkIdx);
}

void TPDiskMockState::TrimQuery() {
    Impl->TrimQuery();
}

void TPDiskMockState::SetStatusFlags(NKikimrBlobStorage::TPDiskSpaceColor::E spaceColor) {
    Impl->SetStatusFlags(spaceColor);
}

void TPDiskMockState::SetStatusFlags(NPDisk::TStatusFlags flags) {
    Impl->SetStatusFlags(flags);
}

void TPDiskMockState::SetReadOnly(const TVDiskID& vDiskId, bool isReadOnly) {
    Impl->SetReadOnly(vDiskId, isReadOnly);
}

TString& TPDiskMockState::GetStateErrorReason() {
    return Impl->StateErrorReason;
}

TPDiskMockState::TPtr TPDiskMockState::Snapshot() {
    auto res = MakeIntrusive<TPDiskMockState>(std::make_unique<TImpl>(*Impl));
    res->Impl->AdjustRefs();
    return res;
}

TMaybe<NPDisk::TOwnerRound> TPDiskMockState::GetOwnerRound(const TVDiskID& vDiskId) const {
    if (auto owner = Impl->GetOwner(vDiskId)) {
        return owner->OwnerRound;
    }
    return Nothing();
}

class TPDiskMockActor : public TActorBootstrapped<TPDiskMockActor> {
    enum {
        EvResume = EventSpaceBegin(TEvents::ES_PRIVATE),
    };

    using TImpl = TPDiskMockState::TImpl;

    TPDiskMockState::TPtr State;
    TImpl& Impl;
    const TString Prefix;

public:
    TPDiskMockActor(TPDiskMockState::TPtr state)
        : State(std::move(state)) // to keep ownership
        , Impl(*State->Impl)
        , Prefix(TStringBuilder() << "PDiskMock[" << Impl.NodeId << ":" << Impl.PDiskId << "] ")
    {
        for (auto& [ownerId, owner] : Impl.Owners) { // reset runtime parameters to default values
            owner.OwnerRound = 0;
            owner.CutLogId = TActorId();
            Impl.ResetOwnerReservedChunks(owner); // return reserved, but not committed chunks to free pool
        }
    }

    void Bootstrap() {
        Become(&TThis::StateNormal);
        ReportMetrics();
    }

    void ReportMetrics() {
        ui32 usedChunks = 0;
        for (const auto& [ownerId, owner] : Impl.Owners) {
            usedChunks += owner.CommittedChunks.size() + owner.ReservedChunks.size();
        }
        Y_ABORT_UNLESS(usedChunks <= Impl.TotalChunks);

        auto ev = std::make_unique<TEvBlobStorage::TEvControllerUpdateDiskStatus>();
        auto& record = ev->Record;
        auto *p = record.AddPDisksMetrics();
        p->SetPDiskId(Impl.PDiskId);
        p->SetAvailableSize((Impl.TotalChunks - usedChunks) * Impl.ChunkSize);
        p->SetTotalSize(Impl.TotalChunks * Impl.ChunkSize);
        p->SetState(NKikimrBlobStorage::TPDiskState::Normal);
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), ev.release());

        Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup);
    }

    void Handle(NPDisk::TEvYardInit::TPtr ev) {
        // report message and validate PDisk guid
        auto *msg = ev->Get();
        PDISK_MOCK_LOG(NOTICE, PDM01, "received TEvYardInit", (Msg, msg->ToString()));
        Y_ABORT_UNLESS(msg->PDiskGuid == Impl.PDiskGuid, "PDiskGuid mismatch");

        // find matching owner or create a new one
        ui8 ownerId;
        TImpl::TOwner *owner;
        bool created;
        std::tie(ownerId, owner) = Impl.FindOrCreateOwner(msg->VDisk, msg->SlotId, &created);
        std::unique_ptr<NPDisk::TEvYardInitResult> res;
        if (ev->Get()->OwnerRound > owner->OwnerRound) {
            // fill in runtime owner parameters
            owner->OwnerRound = ev->Get()->OwnerRound;
            owner->CutLogId = ev->Get()->CutLogID;
            owner->Slain = false;

            // drop data from any reserved chunks and return them to free pool
            Impl.ResetOwnerReservedChunks(*owner);

            // fill in the response
            TVector<TChunkIdx> ownedChunks(owner->CommittedChunks.begin(), owner->CommittedChunks.end());
            const auto& performanceParams = NPDisk::DevicePerformance.at(Impl.DeviceType);
            const ui64 seekTimeUs = (performanceParams.SeekTimeNs + 1000) / 1000 - 1;
            const ui64 readSpeedBps = performanceParams.FirstSectorReadBytesPerSec;
            const ui64 writeSpeedBps = performanceParams.FirstSectorWriteBytesPerSec;
            const ui64 readBlockSize = 65536;
            const ui64 writeBlockSize = 65536;
            const ui64 bulkWriteBlockSize = 65536;
            res = std::make_unique<NPDisk::TEvYardInitResult>(NKikimrProto::OK, seekTimeUs, readSpeedBps, writeSpeedBps,
                readBlockSize, writeBlockSize, bulkWriteBlockSize, Impl.ChunkSize, Impl.AppendBlockSize, ownerId,
                owner->OwnerRound, GetStatusFlags(), std::move(ownedChunks), NPDisk::DEVICE_TYPE_NVME, TString());
            res->StartingPoints = owner->StartingPoints;
        } else {
            res = std::make_unique<NPDisk::TEvYardInitResult>(NKikimrProto::INVALID_ROUND, "invalid owner round");
        }

        PDISK_MOCK_LOG(INFO, PDM02, "sending TEvYardInitResult", (Msg, res->ToString()), (Created, created));
        Send(ev->Sender, res.release());
    }

    void Handle(NPDisk::TEvSlay::TPtr ev) {
        auto *msg = ev->Get();
        PDISK_MOCK_LOG(INFO, PDM17, "received TEvSlay", (Msg, msg->ToString()));
        auto res = std::make_unique<NPDisk::TEvSlayResult>(NKikimrProto::OK, GetStatusFlags(), msg->VDiskId,
                msg->SlayOwnerRound, msg->PDiskId, msg->VSlotId, TString());
        bool found = false;
        for (auto& [ownerId, owner] : Impl.Owners) {
            if (!owner.VDiskId.SameExceptGeneration(msg->VDiskId)) {
                // not our disk
            } else if (owner.Slain) {
                res->Status = NKikimrProto::ALREADY; // already slain or not found
                res->ErrorReason = "already slain or not found";
                found = true;
                break;
            } else if (msg->SlayOwnerRound <= owner.OwnerRound) {
                res->Status = NKikimrProto::RACE;
                res->ErrorReason = TStringBuilder() << "SlayOwnerRound# " << msg->SlayOwnerRound << " actual OwnerRound# "
                    << owner.OwnerRound << " race detected";
                found = true;
                break;
            } else {
                owner.Slain = true;
                Impl.FreeChunks.merge(owner.ReservedChunks);
                Impl.FreeChunks.merge(owner.CommittedChunks);
                Impl.AdjustFreeChunks();
                owner.ChunkData.clear();
                owner.Log.clear();
                owner.LogDataSize = 0;
                owner.LastLsn = 0;
                owner.StartingPoints.clear();
                found = true;
                break;
            }
        }
        if (!found) {
            // a race is possible
            res->Status = NKikimrProto::ALREADY;
            res->ErrorReason = "not found";
        }
        Send(ev->Sender, res.release());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    std::deque<std::tuple<TActorId, THolder<NPDisk::TEvLog>>> LogQ;

    void Handle(NPDisk::TEvLog::TPtr ev) {
        Y_ABORT_UNLESS(!Impl.CheckIsReadOnlyOwner(ev->Get()));
        if (LogQ.empty()) {
            TActivationContext::Send(new IEventHandle(EvResume, 0, SelfId(), TActorId(), nullptr, 0));
        }
        LogQ.emplace_back(ev->Sender, ev->Release());
    }

    void Handle(NPDisk::TEvMultiLog::TPtr ev) {
        if (LogQ.empty()) {
            TActivationContext::Send(new IEventHandle(EvResume, 0, SelfId(), TActorId(), nullptr, 0));
        }
        for (auto& [msg, _] : ev->Get()->Logs) {
            Y_ABORT_UNLESS(!Impl.CheckIsReadOnlyOwner(msg.Get()));
            LogQ.emplace_back(ev->Sender, std::move(msg));
        }
    }

    void HandleLogQ() {
        std::deque<std::unique_ptr<IEventHandle>> results; // per sender actor
        std::deque<std::tuple<NPDisk::TEvLog::TCallback, NPDisk::TEvLogResult*>> callbacks; // just queue
        for (auto& item : std::exchange(LogQ, {})) {
            auto& recipient = std::get<0>(item);
            auto& msg = std::get<1>(item);
            NPDisk::TEvLogResult *res = nullptr;
            auto addRes = [&](NKikimrProto::EReplyStatus status, const TString& errorReason = TString()) {
                auto p = std::make_unique<NPDisk::TEvLogResult>(status, GetStatusFlags(), errorReason);
                res = p.get();
                results.emplace_back(new IEventHandle(recipient, SelfId(), p.release()));
            };
            if (const auto it = Impl.Owners.find(msg->Owner); it == Impl.Owners.end()) {
                Y_ABORT("invalid Owner");
            } else if (it->second.Slain) {
                addRes(NKikimrProto::INVALID_OWNER, "VDisk is slain");
            } else if (msg->OwnerRound != it->second.OwnerRound) {
                addRes(NKikimrProto::INVALID_ROUND, "invalid OwnerRound");
            } else {
                TImpl::TOwner& owner = it->second;
                PDISK_MOCK_LOG(DEBUG, PDM11, "received TEvLog", (Msg, msg->ToString()), (VDiskId, owner.VDiskId));

                Y_ABORT_UNLESS(msg->Lsn > std::exchange(owner.LastLsn, msg->Lsn));

                // add successful result to the actor's result queue if there is no such last one
                if (!results.empty() && results.back()->Recipient == recipient) {
                    res = results.back()->CastAsLocal<NPDisk::TEvLogResult>();
                    if (res->Status != NKikimrProto::OK) {
                        res = nullptr;
                    }
                }
                if (!res) {
                    addRes(NKikimrProto::OK);
                }
                res->Results.emplace_back(msg->Lsn, msg->Cookie);

                // process the log entry
                bool isStartingPoint = false;
                if (msg->Signature.HasCommitRecord()) {
                    const auto& cr = msg->CommitRecord;
                    if (cr.FirstLsnToKeep) { // trim log
                        std::deque<NPDisk::TLogRecord>::iterator it;
                        for (it = owner.Log.begin(); it != owner.Log.end() && it->Lsn < cr.FirstLsnToKeep; ++it)
                        {}
                        size_t num = std::distance(owner.Log.begin(), it);
                        num = RandomNumber(num + 1);
                        for (size_t i = 0; i < num; ++i) {
                            owner.LogDataSize -= owner.Log.front().Data.size();
                            owner.Log.pop_front();
                        }
                    }
                    for (const TChunkIdx chunk : cr.CommitChunks) {
                        Impl.CommitChunk(owner, chunk);
                    }
                    for (const TChunkIdx chunk : cr.DeleteChunks) {
                        if (cr.DeleteToDecommitted) {
                            Impl.UncommitChunk(owner, chunk);
                        } else {
                            Impl.DeleteChunk(owner, chunk);
                        }
                    }
                    isStartingPoint = cr.IsStartingPoint;
                }
                owner.Log.emplace_back(msg->Signature.GetUnmasked(), TRcBuf(msg->Data), msg->Lsn);
                owner.LogDataSize += msg->Data.size();
                if (isStartingPoint) {
                    owner.StartingPoints[msg->Signature.GetUnmasked()] = owner.Log.back();
                }
            }
            Y_ABORT_UNLESS(res);
            if (auto&& cb = std::move(msg->LogCallback)) { // register callback in the queue if there is one
                callbacks.emplace_back(std::move(cb), res);
            }
        }
        // invoke all accumulated callbacks with fully filled response messages
        for (auto& item : callbacks) {
            (*std::get<0>(item))(TlsActivationContext->ExecutorThread.ActorSystem, *std::get<1>(item));
        }
        // send the results
        for (auto& msg : results) {
            auto *ev = msg->CastAsLocal<NPDisk::TEvLogResult>();
            const TActorId& recipient = msg->Recipient;
            PDISK_MOCK_LOG(DEBUG, PDM12, "sending TEvLogResult", (Msg, ev->ToString()), (Recipient, recipient));
            TActivationContext::Send(msg.release());
        }
        // issue cut log events on log overflow
        for (auto& [ownerId, owner] : Impl.Owners) {
            const ui64 maxLogDataSize = 1048576;
            if (owner.LogDataSize >= maxLogDataSize) {
                ui64 temp = owner.LogDataSize;
                ui64 lsn = 0;
                for (auto it = owner.Log.begin(); it != owner.Log.end() && temp >= maxLogDataSize / 2; ++it) {
                    temp -= it->Data.size();
                    lsn = it->Lsn;
                }
                Send(owner.CutLogId, new NPDisk::TEvCutLog(ownerId, owner.OwnerRound, lsn, 0, 0, 0, 0));
            }
        }
    }

    void Handle(NPDisk::TEvChunkForget::TPtr ev) {
        auto *msg = ev->Get();
        NKikimrProto::EReplyStatus status = NKikimrProto::OK;
        TString errorReason;
        if (const auto it = Impl.Owners.find(msg->Owner); it == Impl.Owners.end()) {
            Y_ABORT("invalid Owner");
        } else if (it->second.Slain) {
            status = NKikimrProto::INVALID_OWNER;
            errorReason = "VDisk is slain";
        } else if (msg->OwnerRound != it->second.OwnerRound) {
            status = NKikimrProto::INVALID_ROUND;
            errorReason = "invalid OwnerRound";
        } else {
            TImpl::TOwner& owner = it->second;
            PDISK_MOCK_LOG(DEBUG, PDMxx, "received TEvChunkForget", (Msg, msg->ToString()), (VDiskId, owner.VDiskId));
            for (const TChunkIdx chunkIdx : msg->ForgetChunks) {
                Impl.DeleteChunk(owner, chunkIdx);
            }
        }
        Send(ev->Sender, new NPDisk::TEvChunkForgetResult(status, {}, errorReason), 0, ev->Cookie);
    }

    void Handle(NPDisk::TEvReadLog::TPtr ev) {
        auto *msg = ev->Get();
        auto res = std::make_unique<NPDisk::TEvReadLogResult>(NKikimrProto::OK, msg->Position, msg->Position,
            true, GetStatusFlags(), TString(), msg->Owner);
        if (TImpl::TOwner *owner = Impl.FindOwner(msg, res)) {
            PDISK_MOCK_LOG(INFO, PDM05, "received TEvReadLog", (Msg, msg->ToString()), (VDiskId, owner->VDiskId));
            ui64 size = 0;
            Y_ABORT_UNLESS(msg->Position.OffsetInChunk <= owner->Log.size());
            for (auto it = owner->Log.begin() + msg->Position.OffsetInChunk; it != owner->Log.end(); ++it) {
                res->Results.push_back(*it);
                res->IsEndOfLog = ++res->NextPosition.OffsetInChunk == owner->Log.size();
                size += it->Data.size();
                if (size >= msg->SizeLimit) {
                    break;
                }
            }
            PDISK_MOCK_LOG(INFO, PDM06, "sending TEvReadLogResult", (Msg, res->ToString()));
        }
        Send(ev->Sender, res.release());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void Handle(NPDisk::TEvChunkReserve::TPtr ev) {
        auto *msg = ev->Get();
        Y_ABORT_UNLESS(!Impl.CheckIsReadOnlyOwner(msg));
        auto res = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, GetStatusFlags());
        if (TImpl::TOwner *owner = Impl.FindOwner(msg, res)) {
            if (Impl.GetNumFreeChunks() < msg->SizeChunks) {
                PDISK_MOCK_LOG(NOTICE, PDM09, "received TEvChunkReserve", (Msg, msg->ToString()), (Error, "no free chunks"));
                res->Status = NKikimrProto::OUT_OF_SPACE;
                res->ErrorReason = "no free chunks";
            } else {
                PDISK_MOCK_LOG(DEBUG, PDM07, "received TEvChunkReserve", (Msg, msg->ToString()), (VDiskId, owner->VDiskId));
                for (ui32 i = 0; i < msg->SizeChunks; ++i) {
                    res->ChunkIds.push_back(Impl.AllocateChunk(*owner));
                }
                PDISK_MOCK_LOG(DEBUG, PDM10, "sending TEvChunkReserveResult", (Msg, res->ToString()));
            }
        }
        Send(ev->Sender, res.release());
    }

    void Handle(NPDisk::TEvChunkRead::TPtr ev) {
        auto *msg = ev->Get();
        auto res = std::make_unique<NPDisk::TEvChunkReadResult>(NKikimrProto::OK, msg->ChunkIdx, msg->Offset,
            msg->Cookie, GetStatusFlags(), TString());
        if (TImpl::TOwner *owner = Impl.FindOwner(msg, res)) {
            PDISK_MOCK_LOG(DEBUG, PDM13, "received TEvChunkRead", (Msg, msg->ToString()), (VDiskId, owner->VDiskId));
            Y_VERIFY_S(owner->ReservedChunks.count(msg->ChunkIdx) || owner->CommittedChunks.count(msg->ChunkIdx),
                "VDiskId# " << owner->VDiskId << " ChunkIdx# " << msg->ChunkIdx);
            ui32 offset = msg->Offset;
            ui32 size = msg->Size;
            Y_ABORT_UNLESS(offset < Impl.ChunkSize && offset + size <= Impl.ChunkSize && size);
            auto data = TRcBuf::Uninitialized(size);

            const auto chunkIt = owner->ChunkData.find(msg->ChunkIdx);
            if (chunkIt == owner->ChunkData.end()) {
                res->Data.AddGap(0, size); // no data at all
            } else {
                TImpl::TChunkData& chunk = chunkIt->second;
                const ui64 chunkOffset = (ui64)msg->ChunkIdx * Impl.ChunkSize;
                if (Impl.Corrupted & TIntervalSet<ui64>(chunkOffset + offset, chunkOffset + offset + size)) {
                    res->Status = NKikimrProto::CORRUPTED;
                } else {
                    char *begin = data.GetDataMut(), *ptr = begin;
                    while (size) {
                        const ui32 blockIdx = offset / Impl.AppendBlockSize;
                        const ui32 offsetInBlock = offset % Impl.AppendBlockSize;
                        const ui32 num = Min(size, Impl.AppendBlockSize - offsetInBlock);
                        const auto it = chunk.Blocks.find(blockIdx);
                        if (it == chunk.Blocks.end()) {
                            const ui32 base = ptr - begin;
                            res->Data.AddGap(base, base + num);
                        } else {
                            memcpy(ptr, it->second->data() + offsetInBlock, num);
                        }
                        ptr += num;
                        offset += num;
                        size -= num;
                    }
                }
            }

            if (res->Status == NKikimrProto::OK) {
                res->Data.SetData(std::move(data));
                res->Data.Commit();
            }
            PDISK_MOCK_LOG(DEBUG, PDM14, "sending TEvChunkReadResult", (Msg, res->ToString()));
        }
        Send(ev->Sender, res.release());
    }

    void Handle(NPDisk::TEvChunkWrite::TPtr ev) {
        Y_ABORT_UNLESS(!Impl.CheckIsReadOnlyOwner(ev->Get()));
        auto *msg = ev->Get();
        auto res = std::make_unique<NPDisk::TEvChunkWriteResult>(NKikimrProto::OK, msg->ChunkIdx, msg->Cookie,
            GetStatusFlags(), TString());
        if (TImpl::TOwner *owner = Impl.FindOwner(msg, res)) {
            PDISK_MOCK_LOG(DEBUG, PDM15, "received TEvChunkWrite", (Msg, msg->ToString()), (VDiskId, owner->VDiskId));
            if (!msg->ChunkIdx) { // allocate chunk
                if (!Impl.GetNumFreeChunks()) {
                    res->Status = NKikimrProto::OUT_OF_SPACE;
                    res->ErrorReason = "no free chunks";
                } else {
                    msg->ChunkIdx = res->ChunkIdx = Impl.AllocateChunk(*owner);
                }
            }
            if (msg->ChunkIdx) {
                // allow reads only from owned chunks
                Y_ABORT_UNLESS(owner->ReservedChunks.count(msg->ChunkIdx) || owner->CommittedChunks.count(msg->ChunkIdx));
                // ensure offset and write sizes are granular
                Y_ABORT_UNLESS(msg->Offset % Impl.AppendBlockSize == 0);
                Y_ABORT_UNLESS(msg->PartsPtr);
                Y_ABORT_UNLESS(msg->PartsPtr->ByteSize() % Impl.AppendBlockSize == 0);
                Y_ABORT_UNLESS(msg->Offset + msg->PartsPtr->ByteSize() <= Impl.ChunkSize);
                // issue write
                const ui32 offset = msg->Offset;
                TImpl::TChunkData& chunk = owner->ChunkData[msg->ChunkIdx];
                if (msg->PartsPtr && Impl.Corrupted) {
                    const ui64 chunkOffset = (ui64)msg->ChunkIdx * Impl.ChunkSize;
                    Impl.Corrupted -= {chunkOffset + offset, chunkOffset + offset + msg->PartsPtr->ByteSize()};
                }
                // create queue of blocks to write
                ui32 blockIdx = offset / Impl.AppendBlockSize;
                TString currentBlock;
                char *ptr, *end;
                auto push = [&](const auto& kv) {
                    auto&& [data, len] = kv;
                    ui32 offset = 0;
                    while (offset != len) {
                        if (!currentBlock) {
                            currentBlock = TString::Uninitialized(Impl.AppendBlockSize);
                            ptr = currentBlock.Detach();
                            end = ptr + currentBlock.size();
                        }
                        const ui32 num = Min<ui32>(end - ptr, len - offset); // calculate number of bytes to move
                        if (data) {
                            memcpy(ptr, static_cast<const char*>(data) + offset, num);
                        } else {
                            memset(ptr, 0, num);
                        }
                        offset += num;
                        ptr += num;
                        if (ptr == end) { // commit full block
                            auto&& [it, inserted] = Impl.Blocks.try_emplace(std::move(currentBlock), 0);
                            ++it->second;
                            if (const TString *prev = std::exchange(chunk.Blocks[blockIdx++], &it->first)) {
                                const auto it = Impl.Blocks.find(*prev);
                                Y_ABORT_UNLESS(it != Impl.Blocks.end());
                                if (!--it->second) {
                                    Impl.Blocks.erase(it);
                                }
                            }
                            currentBlock = {};
                        }
                    }
                };
                for (ui32 i = 0; i < msg->PartsPtr->Size(); ++i) {
                    push((*msg->PartsPtr)[i]);
                }
            }
            PDISK_MOCK_LOG(DEBUG, PDM16, "received TEvChunkWriteResult", (Msg, res->ToString()));
        }
        Send(ev->Sender, res.release());
    }

    void Handle(NPDisk::TEvHarakiri::TPtr ev) {
        auto *msg = ev->Get();
        PDISK_MOCK_LOG(INFO, PDM18, "received TEvHarakiri", (Msg, msg->ToString()));

        TString errorReason = "";
        auto res = std::make_unique<NPDisk::TEvHarakiriResult>(NKikimrProto::OK, GetStatusFlags(), errorReason);
        auto it = Impl.Owners.find(msg->Owner);

        if (it == Impl.Owners.end()) {
            res->Status = NKikimrProto::ALREADY;
            res->ErrorReason = "not found";
        }

        auto owner = it->second;

        if (owner.Slain) {
            res->Status = NKikimrProto::ALREADY; // already slain or not found
            res->ErrorReason = "already slain or not found";
        } else if (msg->OwnerRound <= owner.OwnerRound) {
            res->Status = NKikimrProto::RACE;
            res->ErrorReason = TStringBuilder() << "Message OwnerRound# " << msg->OwnerRound << " actual OwnerRound# "
                << owner.OwnerRound << " race detected";
        } else {
            owner.Slain = true;
            Impl.FreeChunks.merge(owner.ReservedChunks);
            Impl.FreeChunks.merge(owner.CommittedChunks);
            Impl.AdjustFreeChunks();
            owner.ChunkData.clear();
            owner.Log.clear();
            owner.LogDataSize = 0;
            owner.LastLsn = 0;
            owner.StartingPoints.clear();
        }

        Send(ev->Sender, res.release());
    }

    void Handle(NPDisk::TEvCheckSpace::TPtr ev) {
        auto *msg = ev->Get();
        auto res = std::make_unique<NPDisk::TEvCheckSpaceResult>(NKikimrProto::OK, GetStatusFlags(),
            Impl.GetNumFreeChunks(), Impl.TotalChunks, Impl.TotalChunks - Impl.GetNumFreeChunks(),
            Impl.Owners.size(), TString());
        res->Occupancy = (double)res->UsedChunks / res->TotalChunks;
        Impl.FindOwner(msg, res); // to ensure correct owner/round
        Send(ev->Sender, res.release());
    }

    void Handle(TEvBlobStorage::TEvAskWardenRestartPDiskResult::TPtr &ev) {
        bool restartAllowed = ev->Get()->RestartAllowed;

        if (restartAllowed) {
            Send(ev->Sender, new TEvBlobStorage::TEvNotifyWardenPDiskRestarted(Impl.PDiskId));
        }
    }

    void Handle(NPDisk::TEvConfigureScheduler::TPtr ev) {
        auto *msg = ev->Get();
        auto res = std::make_unique<NPDisk::TEvConfigureSchedulerResult>(NKikimrProto::OK, TString());
        Impl.FindOwner(msg, res); // to ensure correct owner/round
        Send(ev->Sender, res.release());
    }

    NPDisk::TStatusFlags GetStatusFlags() {
        return Impl.StatusFlags;
    }

    void ErrorHandle(NPDisk::TEvYardInit::TPtr &ev) {
        Send(ev->Sender, new NPDisk::TEvYardInitResult(NKikimrProto::CORRUPTED, State->GetStateErrorReason()));
    }

    void ErrorHandle(NPDisk::TEvCheckSpace::TPtr &ev) {
        Send(ev->Sender, new NPDisk::TEvCheckSpaceResult(NKikimrProto::CORRUPTED, 0, 0, 0, 0, 0, State->GetStateErrorReason()));
    }

    void ErrorHandle(NPDisk::TEvLog::TPtr &ev) {
        const NPDisk::TEvLog &evLog = *ev->Get();
        THolder<NPDisk::TEvLogResult> result(new NPDisk::TEvLogResult(NKikimrProto::CORRUPTED, 0, State->GetStateErrorReason()));
        result->Results.push_back(NPDisk::TEvLogResult::TRecord(evLog.Lsn, evLog.Cookie));
        Send(ev->Sender, result.Release());
    }

    void ErrorHandle(NPDisk::TEvMultiLog::TPtr &ev) {
        const NPDisk::TEvMultiLog &evMultiLog = *ev->Get();
        THolder<NPDisk::TEvLogResult> result(new NPDisk::TEvLogResult(NKikimrProto::CORRUPTED, 0, State->GetStateErrorReason()));
        for (auto &[log, _] : evMultiLog.Logs) {
            result->Results.push_back(NPDisk::TEvLogResult::TRecord(log->Lsn, log->Cookie));
        }
        Send(ev->Sender, result.Release());
    }

    void ErrorHandle(NPDisk::TEvReadLog::TPtr &ev) {
        const NPDisk::TEvReadLog &evReadLog = *ev->Get();
        THolder<NPDisk::TEvReadLogResult> result(new NPDisk::TEvReadLogResult(
            NKikimrProto::CORRUPTED, evReadLog.Position, evReadLog.Position, true, 0, State->GetStateErrorReason(), evReadLog.Owner));
        Send(ev->Sender, result.Release());
    }

    void ErrorHandle(NPDisk::TEvChunkWrite::TPtr &ev) {
        const NPDisk::TEvChunkWrite &evChunkWrite = *ev->Get();
        Send(ev->Sender, new NPDisk::TEvChunkWriteResult(NKikimrProto::CORRUPTED,
            evChunkWrite.ChunkIdx, evChunkWrite.Cookie, 0, State->GetStateErrorReason()));
    }

    void ErrorHandle(NPDisk::TEvChunkRead::TPtr &ev) {
        const NPDisk::TEvChunkRead &evChunkRead = *ev->Get();
        THolder<NPDisk::TEvChunkReadResult> result = MakeHolder<NPDisk::TEvChunkReadResult>(NKikimrProto::CORRUPTED,
            evChunkRead.ChunkIdx, evChunkRead.Offset, evChunkRead.Cookie, 0, "PDisk is in error state");
        Send(ev->Sender, result.Release());
    }

    void ErrorHandle(NPDisk::TEvHarakiri::TPtr &ev) {
        Send(ev->Sender, new NPDisk::TEvHarakiriResult(NKikimrProto::CORRUPTED, 0, State->GetStateErrorReason()));
    }

    void ErrorHandle(NPDisk::TEvSlay::TPtr &ev) {
        const NPDisk::TEvSlay &evSlay = *ev->Get();
        Send(ev->Sender, new NPDisk::TEvSlayResult(NKikimrProto::CORRUPTED, 0,
                    evSlay.VDiskId, evSlay.SlayOwnerRound, evSlay.PDiskId, evSlay.VSlotId, State->GetStateErrorReason()));
    }

    void ErrorHandle(NPDisk::TEvChunkReserve::TPtr &ev) {
        Send(ev->Sender, new NPDisk::TEvChunkReserveResult(NKikimrProto::CORRUPTED, 0, State->GetStateErrorReason()));
    }

    void ErrorHandle(NPDisk::TEvChunkForget::TPtr &ev) {
        Send(ev->Sender, new NPDisk::TEvChunkForgetResult(NKikimrProto::CORRUPTED, 0, State->GetStateErrorReason()));
    }

    void ErrorHandle(NPDisk::TEvYardControl::TPtr &ev) {
        const NPDisk::TEvYardControl &evControl = *ev->Get();
        Send(ev->Sender, new NPDisk::TEvYardControlResult(NKikimrProto::CORRUPTED, evControl.Cookie, State->GetStateErrorReason()));
    }

    void ErrorHandle(NPDisk::TEvAskForCutLog::TPtr &ev) {
        // Just ignore the event, can't send cut log in this state.
        Y_UNUSED(ev);
    }

    void ErrorHandle(NPDisk::TEvReadMetadata::TPtr& ev) {
        Send(ev->Sender, new NPDisk::TEvReadMetadataResult(NPDisk::EPDiskMetadataOutcome::ERROR, std::nullopt), 0, ev->Cookie);
    }

    void ErrorHandle(NPDisk::TEvWriteMetadata::TPtr& ev) {
        Send(ev->Sender, new NPDisk::TEvWriteMetadataResult(NPDisk::EPDiskMetadataOutcome::ERROR, std::nullopt), 0, ev->Cookie);
    }

    void Handle(NPDisk::TEvReadMetadata::TPtr& ev) {
        if (Impl.Metadata) {
            Send(ev->Sender, new NPDisk::TEvReadMetadataResult(TRcBuf(*Impl.Metadata), Impl.PDiskGuid), 0, ev->Cookie);
        } else {
            Send(ev->Sender, new NPDisk::TEvReadMetadataResult(NPDisk::EPDiskMetadataOutcome::NO_METADATA, Impl.PDiskGuid), 0, ev->Cookie);
        }
    }

    void Handle(NPDisk::TEvWriteMetadata::TPtr& ev) {
        Impl.Metadata.emplace(std::move(ev->Get()->Metadata));
        Send(ev->Sender, new NPDisk::TEvWriteMetadataResult(NPDisk::EPDiskMetadataOutcome::OK, Impl.PDiskGuid), 0, ev->Cookie);
    }

    void HandleMoveToErrorState() {
        Impl.StateErrorReason = "Some error reason";
        Become(&TThis::StateError);
    }

    void HandleMoveToNormalState() {
        Impl.StateErrorReason = "";
        Become(&TThis::StateNormal);
    }

    STRICT_STFUNC(StateNormal,
        hFunc(NPDisk::TEvYardInit, Handle);
        hFunc(NPDisk::TEvLog, Handle);
        hFunc(NPDisk::TEvChunkForget, Handle);
        hFunc(NPDisk::TEvMultiLog, Handle);
        cFunc(EvResume, HandleLogQ);
        hFunc(NPDisk::TEvReadLog, Handle);
        hFunc(NPDisk::TEvChunkReserve, Handle);
        hFunc(NPDisk::TEvChunkRead, Handle);
        hFunc(NPDisk::TEvChunkWrite, Handle);
        hFunc(NPDisk::TEvCheckSpace, Handle);
        hFunc(NPDisk::TEvSlay, Handle);
        hFunc(NPDisk::TEvHarakiri, Handle);
        hFunc(NPDisk::TEvConfigureScheduler, Handle);
        hFunc(TEvBlobStorage::TEvAskWardenRestartPDiskResult, Handle);
        cFunc(TEvents::TSystem::Wakeup, ReportMetrics);
        hFunc(NPDisk::TEvReadMetadata, Handle);
        hFunc(NPDisk::TEvWriteMetadata, Handle);

        cFunc(EvBecomeError, HandleMoveToErrorState);
    )

    STRICT_STFUNC(StateError,
        hFunc(NPDisk::TEvYardInit, ErrorHandle);
        hFunc(NPDisk::TEvCheckSpace, ErrorHandle);
        hFunc(NPDisk::TEvLog, ErrorHandle);
        hFunc(NPDisk::TEvMultiLog, ErrorHandle);
        hFunc(NPDisk::TEvReadLog, ErrorHandle);
        hFunc(NPDisk::TEvChunkWrite, ErrorHandle);
        hFunc(NPDisk::TEvChunkRead, ErrorHandle);
        hFunc(NPDisk::TEvHarakiri, ErrorHandle);
        hFunc(NPDisk::TEvSlay, ErrorHandle);
        hFunc(NPDisk::TEvChunkReserve, ErrorHandle);
        hFunc(NPDisk::TEvChunkForget, ErrorHandle);
        hFunc(NPDisk::TEvReadMetadata, ErrorHandle);
        hFunc(NPDisk::TEvWriteMetadata, ErrorHandle);

        cFunc(TEvents::TSystem::Wakeup, ReportMetrics);
        cFunc(EvBecomeNormal, HandleMoveToNormalState);
    )
};

IActor *CreatePDiskMockActor(TPDiskMockState::TPtr state) {
    return new TPDiskMockActor(std::move(state));
}

} // NKikimr
