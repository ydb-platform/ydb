#include "spilling_ddisk.h"

#include "spilling.h"
#include "spilling_file.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/ddisk/persistent_buffer_header.h>

#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/util/rope.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

#include <atomic>

namespace NYql::NDq {

using namespace NActors;
using namespace NKikimr;

namespace {

std::atomic<ui32> SpillingBackendAtomic{static_cast<ui32>(EDqSpillingBackend::LocalFile)};
TDDiskSpillingConfig DDiskSpillingConfigHolder;
TIntrusivePtr<TSpillingCounters> SpillingCountersHolder;

#define LOG_D(s) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ". " << s)
#define LOG_I(s) \
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ". " << s)
#define LOG_E(s) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ". " << s)
#define LOG_T(s) \
    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ". " << s)

constexpr ui32 SectorSize = NDDisk::MinSectorSize;
constexpr ui32 MaxPartPayloadBytes =
    NDDisk::TPersistentBufferLsnRecordHeader::MaxSectorsPerBufferRecord * SectorSize;
// LSN layout: blobId * MaxPartsPerBlob + partIndex. 256 parts → up to 128 MiB per blob.
constexpr ui32 MaxPartsPerBlob = 256;

ui64 AlignUp(ui64 size, ui64 align) {
    return (size + align - 1) / align * align;
}

ui64 PartLsn(ui64 blobId, ui32 partIndex) {
    return blobId * MaxPartsPerBlob + partIndex;
}

ui64 MakeSpillTabletId(const TTxId& txId, const TString& details, ui32 nodeId) {
    // High bit set to stay out of real tablet id space.
    const TString txKey = TStringBuilder() << txId;
    const ui64 hash = MultiHash(txKey, details, nodeId);
    return hash | (ui64(1) << 63);
}

TRope ChunkedBufferToAlignedRope(TChunkedBuffer&& blob, ui64 alignedSize) {
    TRcBuf buf = TRcBuf::Uninitialized(alignedSize);
    char* dst = buf.GetDataMut();
    memset(dst, 0, alignedSize);

    TString contiguous;
    contiguous.reserve(blob.Size());
    {
        TStringOutput out(contiguous);
        blob.CopyTo(out);
    }
    Y_ABORT_UNLESS(contiguous.size() <= alignedSize);
    memcpy(dst, contiguous.data(), contiguous.size());
    return TRope(std::move(buf));
}

TBuffer RopeToBuffer(const TRope& rope, ui64 originalSize) {
    Y_ABORT_UNLESS(rope.size() >= originalSize);
    TBuffer result;
    result.Reserve(originalSize);
    auto it = rope.Begin();
    ui64 remaining = originalSize;
    while (remaining > 0) {
        Y_ABORT_UNLESS(it.Valid());
        const char* data = it.ContiguousData();
        const size_t contiguous = it.ContiguousSize();
        const size_t n = Min<ui64>(remaining, contiguous);
        result.Append(data, n);
        remaining -= n;
        it += n;
    }
    return result;
}

struct TBlobMeta {
    ui64 OriginalSize = 0;
    ui32 PartCount = 0;
    ui64 AlignedTotalSize = 0;
};

struct TWriteState {
    TBlobMeta Meta;
    ui32 PartsCompleted = 0;
    bool Failed = false;
};

struct TReadState {
    TBlobMeta Meta;
    bool RemoveBlob = false;
    ui32 PartsCompleted = 0;
    bool Failed = false;
    TVector<TRope> Parts;
};

class TDqDDiskSpillingActor : public TActorBootstrapped<TDqDDiskSpillingActor> {
public:
    TDqDDiskSpillingActor(
            TTxId txId,
            const TString& details,
            const TActorId& client,
            bool removeBlobsAfterRead,
            ESpillingType spillingType,
            TDDiskSpillingConfig config,
            std::optional<TActorId> pbActorIdOverride)
        : TxId_(std::move(txId))
        , Details_(details)
        , ClientActorId_(client)
        , RemoveBlobsAfterRead_(removeBlobsAfterRead)
        , SpillingType_(spillingType)
        , Config_(config)
        , PBActorIdOverride_(pbActorIdOverride)
        , Counters_(GetDqSpillingCounters())
    {
        Y_UNUSED(Config_);
    }

    void Bootstrap() {
        TabletId_ = MakeSpillTabletId(TxId_, Details_, SelfId().NodeId());
        Credentials_ = NDDisk::TQueryCredentials::ToPersistentBuffer(TabletId_, Generation_, std::nullopt);
        if (Counters_) {
            Counters_->DDisk.ActiveSessions->Inc();
        }

        if (PBActorIdOverride_) {
            ConnectToPersistentBuffer(*PBActorIdOverride_);
            return;
        }

        LOG_I("Discovering local PersistentBuffer via NodeWarden for DDiskSpillingActor "
            << SelfId() << " tabletId " << TabletId_);
        if (Counters_) {
            Counters_->DDisk.Discoveries->Inc();
        }
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()),
            new TEvNodeWardenListLocalDDisks(),
            IEventHandle::FlagTrackDelivery);
        Become(&TDqDDiskSpillingActor::DiscoveringState);
    }

    static constexpr char ActorName[] = "DQ_DDISK_SPILLING";

private:
    void ConnectToPersistentBuffer(const TActorId& pbActorId) {
        PBActorId_ = pbActorId;
        LOG_I("Register DDiskSpillingActor " << SelfId()
            << " at PB " << PBActorId_
            << " tabletId " << TabletId_);
        Send(PBActorId_, new NDDisk::TEvConnect(Credentials_), IEventHandle::FlagTrackDelivery);
        Become(&TDqDDiskSpillingActor::ConnectingState);
    }

    STRICT_STFUNC(DiscoveringState,
        hFunc(TEvNodeWardenListLocalDDisksResult, HandleListLocalDDisks)
        sFunc(TEvents::TEvUndelivered, HandleUndeliveredDiscovery)
        hFunc(TEvents::TEvPoison, HandlePoisonConnecting)
        hFunc(TEvDqSpilling::TEvWrite, HandleWriteWhileConnecting)
        hFunc(TEvDqSpilling::TEvRead, HandleReadWhileConnecting)
    )

    STRICT_STFUNC(ConnectingState,
        hFunc(NDDisk::TEvConnectResult, HandleConnectResult)
        sFunc(TEvents::TEvUndelivered, HandleUndelivered)
        hFunc(TEvents::TEvPoison, HandlePoisonConnecting)
        hFunc(TEvDqSpilling::TEvWrite, HandleWriteWhileConnecting)
        hFunc(TEvDqSpilling::TEvRead, HandleReadWhileConnecting)
    )

    STRICT_STFUNC(WorkState,
        hFunc(TEvDqSpilling::TEvWrite, HandleWrite)
        hFunc(TEvDqSpilling::TEvRead, HandleRead)
        hFunc(NDDisk::TEvWritePersistentBufferResult, HandleWriteResult)
        hFunc(NDDisk::TEvReadPersistentBufferResult, HandleReadResult)
        hFunc(NDDisk::TEvErasePersistentBufferResult, HandleEraseResult)
        hFunc(NDDisk::TEvDisconnectResult, HandleDisconnectResult)
        sFunc(TEvents::TEvUndelivered, HandleUndelivered)
        hFunc(TEvents::TEvPoison, HandlePoison)
    )

    STRICT_STFUNC(StoppingState,
        hFunc(NDDisk::TEvErasePersistentBufferResult, HandleEraseResultStopping)
        hFunc(NDDisk::TEvDisconnectResult, HandleDisconnectResult)
        IgnoreFunc(TEvDqSpilling::TEvWrite)
        IgnoreFunc(TEvDqSpilling::TEvRead)
        IgnoreFunc(NDDisk::TEvWritePersistentBufferResult)
        IgnoreFunc(NDDisk::TEvReadPersistentBufferResult)
        sFunc(TEvents::TEvUndelivered, HandleUndeliveredStopping)
        IgnoreFunc(TEvents::TEvPoison)
    )

    void HandleListLocalDDisks(TEvNodeWardenListLocalDDisksResult::TPtr& ev) {
        if (ev->Get()->Infos.empty()) {
            if (Counters_) {
                Counters_->DDisk.DiscoveryErrors->Inc();
            }
            FailClient("No local DDisk PersistentBuffer available for spilling");
            return;
        }
        // Prefer the first local PB; NodeWarden already filters to DDisk-backed slots.
        ConnectToPersistentBuffer(ev->Get()->Infos.front().PersistentBufferId);
    }

    void HandleUndeliveredDiscovery() {
        if (Counters_) {
            Counters_->DDisk.DiscoveryErrors->Inc();
        }
        FailClient("NodeWarden is not available; cannot discover PersistentBuffer for spilling");
    }

    void HandleConnectResult(NDDisk::TEvConnectResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            if (Counters_) {
                Counters_->DDisk.ConnectErrors->Inc();
                Counters_->GetTypeCounters(SpillingType_).IoErrors->Inc();
            }
            FailClient(TStringBuilder() << "DDisk PersistentBuffer connect failed: "
                << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(record.GetStatus())
                << (record.HasErrorReason() ? (", " + record.GetErrorReason()) : TString()));
            return;
        }
        Credentials_.DDiskInstanceGuid = record.GetDDiskInstanceGuid();
        Connected_ = true;
        if (Counters_) {
            Counters_->DDisk.Connects->Inc();
        }
        LOG_D("Connected to PersistentBuffer, guid=" << record.GetDDiskInstanceGuid());
        Become(&TDqDDiskSpillingActor::WorkState);
        FlushPending();
    }

    void HandleWriteWhileConnecting(TEvDqSpilling::TEvWrite::TPtr& ev) {
        ValidateSender(ev->Sender);
        PendingWrites_.emplace_back(ev);
    }

    void HandleReadWhileConnecting(TEvDqSpilling::TEvRead::TPtr& ev) {
        ValidateSender(ev->Sender);
        PendingReads_.emplace_back(ev);
    }

    void FlushPending() {
        for (auto& ev : PendingWrites_) {
            HandleWrite(ev);
        }
        PendingWrites_.clear();
        for (auto& ev : PendingReads_) {
            HandleRead(ev);
        }
        PendingReads_.clear();
    }

    void HandleWrite(TEvDqSpilling::TEvWrite::TPtr& ev) {
        ValidateSender(ev->Sender);
        auto& msg = *ev->Get();
        LOG_T("[Write] blobId: " << msg.BlobId << ", bytes: " << msg.Blob.Size());

        if (Blobs_.contains(msg.BlobId) || Writing_.contains(msg.BlobId)) {
            FailClient(TStringBuilder() << "Duplicate write for blobId " << msg.BlobId);
            return;
        }

        const ui64 originalSize = msg.Blob.Size();
        if (originalSize == 0) {
            FailClient(TStringBuilder() << "Empty blob write, blobId " << msg.BlobId);
            return;
        }

        const ui64 alignedTotal = AlignUp(originalSize, SectorSize);
        const ui32 partCount = (alignedTotal + MaxPartPayloadBytes - 1) / MaxPartPayloadBytes;
        if (partCount == 0 || partCount > MaxPartsPerBlob) {
            FailClient(TStringBuilder() << "Blob too large for DDisk spilling: " << originalSize
                << " bytes (parts " << partCount << ")");
            return;
        }

        TRope full = ChunkedBufferToAlignedRope(std::move(msg.Blob), alignedTotal);

        TWriteState state;
        state.Meta = TBlobMeta{originalSize, partCount, alignedTotal};
        Writing_.emplace(msg.BlobId, std::move(state));
        if (Counters_) {
            Counters_->DDisk.InFlightWrites->Inc();
        }

        for (ui32 part = 0; part < partCount; ++part) {
            const ui64 partSize = Min<ui64>(MaxPartPayloadBytes, full.size());
            Y_ABORT_UNLESS(partSize > 0 && partSize % SectorSize == 0);

            auto begin = full.Begin();
            TRope partRope = full.Extract(begin, begin + partSize);

            const ui64 lsn = PartLsn(msg.BlobId, part);
            auto writeEv = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                Credentials_,
                NDDisk::TBlockSelector(/*vChunkIndex=*/0, /*offsetInBytes=*/0, static_cast<ui32>(partSize)),
                lsn,
                NDDisk::TWriteInstruction(0));
            writeEv->AddPayloadThenChecksum(std::move(partRope));

            const ui64 cookie = NextCookie_++;
            CookieMap_[cookie] = TOpRef{msg.BlobId, part};
            if (Counters_) {
                Counters_->DDisk.WriteParts->Inc();
            }
            Send(PBActorId_, writeEv.release(), IEventHandle::FlagTrackDelivery, cookie);
        }
        Y_ABORT_UNLESS(full.IsEmpty());
    }

    void HandleWriteResult(NDDisk::TEvWritePersistentBufferResult::TPtr& ev) {
        auto cookieIt = CookieMap_.find(ev->Cookie);
        if (cookieIt == CookieMap_.end()) {
            LOG_E("Unexpected write result cookie " << ev->Cookie);
            return;
        }
        const ui64 blobId = cookieIt->second.BlobId;
        const ui32 part = cookieIt->second.Part;
        CookieMap_.erase(cookieIt);

        auto it = Writing_.find(blobId);
        if (it == Writing_.end()) {
            LOG_E("Unexpected write result for blobId " << blobId);
            return;
        }

        auto& state = it->second;
        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            state.Failed = true;
            LOG_E("[WriteResult] blobId: " << blobId << " part: " << part << " failed: "
                << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(record.GetStatus())
                << (record.HasErrorReason() ? (", " + record.GetErrorReason()) : TString()));
        }

        ++state.PartsCompleted;
        if (state.PartsCompleted < state.Meta.PartCount) {
            return;
        }

        if (Counters_) {
            Counters_->DDisk.InFlightWrites->Dec();
        }

        if (state.Failed) {
            if (Counters_) {
                Counters_->GetTypeCounters(SpillingType_).IoErrors->Inc();
            }
            Writing_.erase(it);
            FailClient(TStringBuilder() << "DDisk PersistentBuffer write failed for blobId " << blobId);
            return;
        }

        Blobs_.emplace(blobId, state.Meta);
        StoredBytes_ += state.Meta.AlignedTotalSize;
        if (Counters_) {
            auto& tc = Counters_->GetTypeCounters(SpillingType_);
            tc.WriteBlobs->Inc();
            tc.StoredBlobs->Inc();
            tc.TotalSpaceUsed->Add(state.Meta.OriginalSize);
            Counters_->DDisk.WriteBytes->Add(state.Meta.OriginalSize);
        }
        Writing_.erase(it);

        LOG_T("[WriteResult] blobId: " << blobId << " ok");
        if (!Send(ClientActorId_, new TEvDqSpilling::TEvWriteResult(blobId))) {
            ClientLost();
        }
    }

    void HandleRead(TEvDqSpilling::TEvRead::TPtr& ev) {
        ValidateSender(ev->Sender);
        auto& msg = *ev->Get();
        LOG_T("[Read] blobId: " << msg.BlobId << ", remove: " << msg.RemoveBlob);

        auto blobIt = Blobs_.find(msg.BlobId);
        if (blobIt == Blobs_.end()) {
            FailClient(TStringBuilder() << "Blob not found: " << msg.BlobId);
            return;
        }
        if (Reading_.contains(msg.BlobId)) {
            FailClient(TStringBuilder() << "Duplicate read for blobId " << msg.BlobId);
            return;
        }

        const bool removeBlob = msg.RemoveBlob || RemoveBlobsAfterRead_;
        TReadState state;
        state.Meta = blobIt->second;
        state.RemoveBlob = removeBlob;
        state.Parts.resize(state.Meta.PartCount);
        Reading_.emplace(msg.BlobId, std::move(state));
        if (Counters_) {
            Counters_->DDisk.InFlightReads->Inc();
        }

        for (ui32 part = 0; part < blobIt->second.PartCount; ++part) {
            auto readEv = std::make_unique<NDDisk::TEvReadPersistentBuffer>();
            Credentials_.Serialize(readEv->Record.MutableCredentials());
            readEv->Record.SetLsn(PartLsn(msg.BlobId, part));
            readEv->Record.SetGeneration(Generation_);
            const ui64 cookie = NextCookie_++;
            CookieMap_[cookie] = TOpRef{msg.BlobId, part};
            if (Counters_) {
                Counters_->DDisk.ReadParts->Inc();
            }
            Send(PBActorId_, readEv.release(), IEventHandle::FlagTrackDelivery, cookie);
        }
    }

    void HandleReadResult(NDDisk::TEvReadPersistentBufferResult::TPtr& ev) {
        auto cookieIt = CookieMap_.find(ev->Cookie);
        if (cookieIt == CookieMap_.end()) {
            LOG_E("Unexpected read result cookie " << ev->Cookie);
            return;
        }
        const ui64 blobId = cookieIt->second.BlobId;
        const ui32 part = cookieIt->second.Part;
        CookieMap_.erase(cookieIt);

        auto it = Reading_.find(blobId);
        if (it == Reading_.end()) {
            LOG_E("Unexpected read result for blobId " << blobId);
            return;
        }

        auto& state = it->second;
        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            state.Failed = true;
            LOG_E("[ReadResult] blobId: " << blobId << " part: " << part << " failed: "
                << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(record.GetStatus())
                << (record.HasErrorReason() ? (", " + record.GetErrorReason()) : TString()));
        } else if (record.HasReadResult() && record.GetReadResult().HasPayloadId()) {
            state.Parts[part] = ev->Get()->GetPayload(record.GetReadResult().GetPayloadId());
        } else {
            state.Failed = true;
        }

        ++state.PartsCompleted;
        if (state.PartsCompleted < state.Meta.PartCount) {
            return;
        }

        if (Counters_) {
            Counters_->DDisk.InFlightReads->Dec();
        }

        if (state.Failed) {
            if (Counters_) {
                Counters_->GetTypeCounters(SpillingType_).IoErrors->Inc();
            }
            Reading_.erase(it);
            FailClient(TStringBuilder() << "DDisk PersistentBuffer read failed for blobId " << blobId);
            return;
        }

        TRope combined;
        for (auto& partRope : state.Parts) {
            combined.Insert(combined.End(), std::move(partRope));
        }
        TBuffer blob = RopeToBuffer(combined, state.Meta.OriginalSize);
        const bool removeBlob = state.RemoveBlob;
        const TBlobMeta meta = state.Meta;
        Reading_.erase(it);

        if (Counters_) {
            auto& tc = Counters_->GetTypeCounters(SpillingType_);
            tc.ReadBlobs->Inc();
            Counters_->DDisk.ReadBytes->Add(meta.OriginalSize);
        }

        if (!Send(ClientActorId_, new TEvDqSpilling::TEvReadResult(blobId, std::move(blob)))) {
            ClientLost();
            return;
        }

        if (removeBlob) {
            EraseBlob(blobId, meta);
        }
    }

    void EraseBlob(ui64 blobId, const TBlobMeta& meta) {
        auto eraseEv = std::make_unique<NDDisk::TEvBatchErasePersistentBuffer>(Credentials_);
        for (ui32 part = 0; part < meta.PartCount; ++part) {
            eraseEv->AddErase(PartLsn(blobId, part), Generation_);
        }
        Erasing_.emplace(blobId, meta);
        if (Counters_) {
            Counters_->DDisk.Erases->Inc();
        }
        Send(PBActorId_, eraseEv.release(), IEventHandle::FlagTrackDelivery, blobId);
    }

    void HandleEraseResult(NDDisk::TEvErasePersistentBufferResult::TPtr& ev) {
        const ui64 blobId = ev->Cookie;
        auto it = Erasing_.find(blobId);
        if (it == Erasing_.end()) {
            return;
        }
        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            LOG_E("[EraseResult] blobId: " << blobId << " failed: "
                << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(record.GetStatus()));
            if (Counters_) {
                Counters_->GetTypeCounters(SpillingType_).IoErrors->Inc();
            }
        } else {
            StoredBytes_ -= it->second.AlignedTotalSize;
            if (Counters_) {
                auto& tc = Counters_->GetTypeCounters(SpillingType_);
                tc.StoredBlobs->Dec();
                tc.TotalSpaceUsed->Sub(it->second.OriginalSize);
            }
            Blobs_.erase(blobId);
        }
        Erasing_.erase(it);
    }

    void HandlePoisonConnecting(TEvents::TEvPoison::TPtr& ev) {
        ValidateSender(ev->Sender);
        PassAway();
    }

    void HandlePoison(TEvents::TEvPoison::TPtr& ev) {
        ValidateSender(ev->Sender);
        BeginShutdown();
    }

    void BeginShutdown() {
        Become(&TDqDDiskSpillingActor::StoppingState);
        if (!Connected_) {
            PassAway();
            return;
        }

        // Wipe all records for this spill session tablet, then disconnect.
        ui64 maxLsn = 0;
        for (const auto& [blobId, meta] : Blobs_) {
            maxLsn = Max(maxLsn, PartLsn(blobId, meta.PartCount > 0 ? meta.PartCount - 1 : 0));
        }
        for (const auto& [blobId, state] : Writing_) {
            maxLsn = Max(maxLsn, PartLsn(blobId, state.Meta.PartCount > 0 ? state.Meta.PartCount - 1 : 0));
        }

        if (!Blobs_.empty() || !Writing_.empty()) {
            auto eraseEv = std::make_unique<NDDisk::TEvErasePersistentBuffer>(Credentials_, maxLsn);
            Send(PBActorId_, eraseEv.release(), IEventHandle::FlagTrackDelivery, Max<ui64>());
            ShutdownErasePending_ = true;
        } else {
            SendDisconnect();
        }
    }

    void HandleEraseResultStopping(NDDisk::TEvErasePersistentBufferResult::TPtr& ev) {
        Y_UNUSED(ev);
        if (ShutdownErasePending_) {
            ShutdownErasePending_ = false;
            if (Counters_) {
                auto& tc = Counters_->GetTypeCounters(SpillingType_);
                for (const auto& [blobId, meta] : Blobs_) {
                    Y_UNUSED(blobId);
                    tc.StoredBlobs->Dec();
                    tc.TotalSpaceUsed->Sub(meta.OriginalSize);
                }
            }
            Blobs_.clear();
            Writing_.clear();
            StoredBytes_ = 0;
            SendDisconnect();
        }
    }

    void SendDisconnect() {
        if (DisconnectSent_) {
            return;
        }
        DisconnectSent_ = true;
        auto ev = std::make_unique<NDDisk::TEvDisconnect>();
        Credentials_.Serialize(ev->Record.MutableCredentials());
        Send(PBActorId_, ev.release(), IEventHandle::FlagTrackDelivery);
    }

    void HandleDisconnectResult(NDDisk::TEvDisconnectResult::TPtr&) {
        PassAway();
    }

    void HandleUndelivered() {
        if (Counters_) {
            Counters_->GetTypeCounters(SpillingType_).IoErrors->Inc();
        }
        FailClient("DDisk PersistentBuffer service not available");
    }

    void HandleUndeliveredStopping() {
        PassAway();
    }

    void FailClient(const TString& message) {
        LOG_E(message);
        Send(ClientActorId_, new TEvDqSpilling::TEvError(message));
        if (Connected_) {
            BeginShutdown();
        } else {
            PassAway();
        }
    }

    void ClientLost() {
        LOG_E("Client lost");
        if (Connected_) {
            BeginShutdown();
        } else {
            PassAway();
        }
    }

    void ValidateSender(const TActorId& sender) {
        Y_ABORT_UNLESS(ClientActorId_ == sender, "%s != %s",
            ClientActorId_.ToString().c_str(), sender.ToString().c_str());
    }

    void PassAway() override {
        if (Counters_ && !SessionAccountedDown_) {
            Counters_->DDisk.ActiveSessions->Dec();
            SessionAccountedDown_ = true;
        }
        TActorBootstrapped::PassAway();
    }

private:
    const TTxId TxId_;
    const TString Details_;
    const TActorId ClientActorId_;
    const bool RemoveBlobsAfterRead_;
    const ESpillingType SpillingType_;
    const TDDiskSpillingConfig Config_;
    const std::optional<TActorId> PBActorIdOverride_;
    const TIntrusivePtr<TSpillingCounters> Counters_;

    TActorId PBActorId_;
    ui64 TabletId_ = 0;
    ui32 Generation_ = 1;
    NDDisk::TQueryCredentials Credentials_;
    bool Connected_ = false;
    bool DisconnectSent_ = false;
    bool ShutdownErasePending_ = false;
    bool SessionAccountedDown_ = false;

    struct TOpRef {
        ui64 BlobId = 0;
        ui32 Part = 0;
    };

    ui64 NextCookie_ = 1;
    THashMap<ui64, TOpRef> CookieMap_;

    THashMap<ui64, TBlobMeta> Blobs_;
    THashMap<ui64, TWriteState> Writing_;
    THashMap<ui64, TReadState> Reading_;
    THashMap<ui64, TBlobMeta> Erasing_;
    ui64 StoredBytes_ = 0;

    TVector<TEvDqSpilling::TEvWrite::TPtr> PendingWrites_;
    TVector<TEvDqSpilling::TEvRead::TPtr> PendingReads_;
};

} // anonymous namespace

void ConfigureDqSpillingBackend(
    EDqSpillingBackend backend,
    TDDiskSpillingConfig ddiskConfig,
    TIntrusivePtr<TSpillingCounters> counters)
{
    DDiskSpillingConfigHolder = ddiskConfig;
    if (counters) {
        SpillingCountersHolder = std::move(counters);
    }
    SpillingBackendAtomic.store(static_cast<ui32>(backend), std::memory_order_release);
}

EDqSpillingBackend GetDqSpillingBackend() {
    return static_cast<EDqSpillingBackend>(SpillingBackendAtomic.load(std::memory_order_acquire));
}

const TDDiskSpillingConfig& GetDqDDiskSpillingConfig() {
    return DDiskSpillingConfigHolder;
}

TIntrusivePtr<TSpillingCounters> GetDqSpillingCounters() {
    return SpillingCountersHolder;
}

namespace {

class TDqDDiskSpillingMonActor : public TActorBootstrapped<TDqDDiskSpillingMonActor> {
public:
    explicit TDqDDiskSpillingMonActor(TIntrusivePtr<TSpillingCounters> counters)
        : Counters_(std::move(counters))
    {}

    void Bootstrap() {
        Become(&TDqDDiskSpillingMonActor::StateFunc);
    }

    static constexpr char ActorName[] = "DQ_DDISK_SPILLING_MON";

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NMon::TEvHttpInfo, Handle)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream s;
        HTML(s) {
            TAG(TH2) { s << "KQP DDisk Spilling"; }
            PRE() {
                s << "Backend: DDisk PersistentBuffer" << Endl;
                s << "Enable: " << GetDqDDiskSpillingConfig().Enable << Endl;
            }
            if (Counters_) {
                TAG(TH2) { s << "DDisk counters"; }
                PRE() {
                    s << "ActiveSessions: " << Counters_->DDisk.ActiveSessions->Val() << Endl;
                    s << "Discoveries: " << Counters_->DDisk.Discoveries->Val() << Endl;
                    s << "DiscoveryErrors: " << Counters_->DDisk.DiscoveryErrors->Val() << Endl;
                    s << "Connects: " << Counters_->DDisk.Connects->Val() << Endl;
                    s << "ConnectErrors: " << Counters_->DDisk.ConnectErrors->Val() << Endl;
                    s << "WriteBytes: " << Counters_->DDisk.WriteBytes->Val() << Endl;
                    s << "ReadBytes: " << Counters_->DDisk.ReadBytes->Val() << Endl;
                    s << "WriteParts: " << Counters_->DDisk.WriteParts->Val() << Endl;
                    s << "ReadParts: " << Counters_->DDisk.ReadParts->Val() << Endl;
                    s << "Erases: " << Counters_->DDisk.Erases->Val() << Endl;
                    s << "InFlightWrites: " << Counters_->DDisk.InFlightWrites->Val() << Endl;
                    s << "InFlightReads: " << Counters_->DDisk.InFlightReads->Val() << Endl;
                }
                TAG(TH2) { s << "Compute spilling"; }
                PRE() {
                    s << "WriteBlobs: " << Counters_->ComputeSpilling.WriteBlobs->Val() << Endl;
                    s << "ReadBlobs: " << Counters_->ComputeSpilling.ReadBlobs->Val() << Endl;
                    s << "StoredBlobs: " << Counters_->ComputeSpilling.StoredBlobs->Val() << Endl;
                    s << "TotalSpaceUsed: " << Counters_->ComputeSpilling.TotalSpaceUsed->Val() << Endl;
                    s << "IoErrors: " << Counters_->ComputeSpilling.IoErrors->Val() << Endl;
                }
                TAG(TH2) { s << "Channel spilling"; }
                PRE() {
                    s << "WriteBlobs: " << Counters_->ChannelSpilling.WriteBlobs->Val() << Endl;
                    s << "ReadBlobs: " << Counters_->ChannelSpilling.ReadBlobs->Val() << Endl;
                    s << "StoredBlobs: " << Counters_->ChannelSpilling.StoredBlobs->Val() << Endl;
                    s << "TotalSpaceUsed: " << Counters_->ChannelSpilling.TotalSpaceUsed->Val() << Endl;
                    s << "IoErrors: " << Counters_->ChannelSpilling.IoErrors->Val() << Endl;
                }
            }
        }
        Send(ev->Sender, new NMon::TEvHttpInfoRes(s.Str()));
    }

    TIntrusivePtr<TSpillingCounters> Counters_;
};

} // anonymous namespace

IActor* CreateDqDDiskSpillingMonActor(TIntrusivePtr<TSpillingCounters> counters) {
    return new TDqDDiskSpillingMonActor(std::move(counters));
}

IActor* CreateDqDDiskSpillingActor(
    TTxId txId,
    const TString& details,
    const TActorId& client,
    bool removeBlobsAfterRead,
    ESpillingType spillingType,
    TDDiskSpillingConfig config,
    std::optional<TActorId> pbActorIdOverride)
{
    return new TDqDDiskSpillingActor(
        std::move(txId), details, client, removeBlobsAfterRead, spillingType, config, pbActorIdOverride);
}

IActor* CreateDqSpillingActor(
    TTxId txId,
    const TString& details,
    const TActorId& client,
    bool removeBlobsAfterRead,
    ESpillingType spillingType)
{
    if (GetDqSpillingBackend() == EDqSpillingBackend::DDisk) {
        const auto& cfg = GetDqDDiskSpillingConfig();
        if (cfg.Enable) {
            return CreateDqDDiskSpillingActor(
                std::move(txId), details, client, removeBlobsAfterRead, spillingType, cfg);
        }
    }
    return CreateDqLocalFileSpillingActor(
        std::move(txId), details, client, removeBlobsAfterRead, spillingType);
}

} // namespace NYql::NDq
