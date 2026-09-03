#pragma once

#include "defs.h"
#include "ddisk_config.h"
#include "ddisk_checksums.h"

#include <ydb/core/base/events.h>

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>

#include <ydb/library/actors/util/rope.h>

#include <vector>

namespace NKikimr::NDDisk {

    constexpr size_t MinSectorSize = 4096;
    constexpr size_t DataAlignment = MinSectorSize;

    static_assert(MinSectorSize == IntegrityUnitSize);

    struct TConnectionToken {
        ui64 Low = 0;
        ui64 High = 0;

        TConnectionToken() = default;

        TConnectionToken(ui64 low, ui64 high)
            : Low(low)
            , High(high)
        {}

        static TConnectionToken Make(
                ui32 connectionIndex,
                ui8 sequenceNo,
                ui32 tabletIdSuffix,
                ui16 nodeId,
                ui16 pdiskId,
                ui16 vslotId,
                ui8 random
        ) {
            return {
                connectionIndex | static_cast<ui64>(tabletIdSuffix) << 32,
                sequenceNo |
                    static_cast<ui64>(random) << 8 |
                    static_cast<ui64>(nodeId) << 16 |
                    static_cast<ui64>(pdiskId) << 32 |
                    static_cast<ui64>(vslotId) << 48,
            };
        }

        explicit TConnectionToken(const NKikimrBlobStorage::NDDisk::TConnectionToken& pb)
            : Low(pb.GetLow())
            , High(pb.GetHigh())
        {}

        [[nodiscard]] ui32 GetConnectionIndex() const {
            return static_cast<ui32>(Low);
        }

        [[nodiscard]] ui8 GetSequenceNo() const {
            return static_cast<ui8>(High);
        }

        [[nodiscard]] ui32 GetTabletIdSuffix() const {
            return static_cast<ui32>(Low >> 32);
        }

        [[nodiscard]] ui16 GetNodeId() const {
            return static_cast<ui16>(High >> 16);
        }

        [[nodiscard]] ui16 GetPDiskId() const {
            return static_cast<ui16>(High >> 32);
        }

        [[nodiscard]] ui16 GetVSlotId() const {
            return static_cast<ui16>(High >> 48);
        }

        [[nodiscard]] ui8 GetRandom() const {
            return static_cast<ui8>(High >> 8);
        }

        explicit operator bool() const {
            return Low || High;
        }

        bool operator==(const TConnectionToken&) const = default;

        void Serialize(NKikimrBlobStorage::NDDisk::TConnectionToken* pb) const
        {
            pb->SetLow(Low);
            pb->SetHigh(High);
        }
    };

    struct TEv {
        enum {
            EvConnect = EventSpaceBegin(TKikimrEvents::ES_DDISK),
            EvConnectResult,
            EvDisconnect,
            EvDisconnectResult,
            EvWrite,
            EvWriteResult,
            EvRead,
            EvReadResult,
            EvSync,
            EvSyncResult,
            EvSyncReserved1,
            EvSyncReserved2,
            EvWritePersistentBuffer,
            EvWritePersistentBufferResult,
            EvReadPersistentBuffer,
            EvReadPersistentBufferResult,
            EvErasePersistentBuffer,
            EvBatchErasePersistentBuffer,
            EvErasePersistentBufferResult,
            EvListPersistentBuffer,
            EvListPersistentBufferResult,
            EvWritePersistentBuffers,
            EvWritePersistentBuffersResult,
            EvReadThenWritePersistentBuffers,
            EvGetPersistentBufferInfo,
            EvPersistentBufferInfo,
            EvDeleteTabletChunks,
            EvDeleteTabletChunksResult,
        };
    };

    struct TQueryCredentials {
        using TRequestCredentials = NKikimrBlobStorage::NDDisk::TRequestCredentials;
        using ERequestKind = NKikimrBlobStorage::NDDisk::TQueryCredentials::ERequestKind;

        ui64 TabletId = 0;
        ui32 Generation = 0;
        ui32 DirectBlockGroupIndex = 0;
        std::optional<ui64> DDiskInstanceGuid;
        ui64 DDiskSessionSeqNo = 0;
        ERequestKind RequestKind = NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_TO_DDISK;
        std::optional<TConnectionToken> ConnectionToken;
        bool ServerContext = false;

        TQueryCredentials() = default;

        TQueryCredentials(
                ui64 tabletId,
                ui32 generation,
                ui64 ddiskSessionSeqNo,
                std::optional<ui64> ddiskInstanceGuid,
                ERequestKind requestKind,
                ui32 directBlockGroupIndex)
            : TabletId(tabletId)
            , Generation(generation)
            , DirectBlockGroupIndex(directBlockGroupIndex)
            , DDiskInstanceGuid(ddiskInstanceGuid)
            , DDiskSessionSeqNo(ddiskSessionSeqNo)
            , RequestKind(requestKind)
        {}

        // Connection metadata for a DDisk actor. Ordinary requests serialize
        // only the token returned by TEvConnectResult.
        static TQueryCredentials ToDDisk(
                ui64 tabletId,
                ui32 generation,
                ui64 ddiskSessionSeqNo,
                std::optional<ui64> ddiskInstanceGuid,
                ui32 directBlockGroupIndex
        ) {
            return TQueryCredentials(
                tabletId,
                generation,
                ddiskSessionSeqNo,
                ddiskInstanceGuid,
                NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_TO_DDISK,
                directBlockGroupIndex);
        }

        // Connection metadata for a PersistentBuffer actor. Ordinary requests
        // serialize only the token returned by TEvConnectResult.
        static TQueryCredentials ToPersistentBuffer(
                ui64 tabletId,
                ui32 generation,
                std::optional<ui64> ddiskInstanceGuid,
                ui32 directBlockGroupIndex
        ) {
            return TQueryCredentials(
                tabletId,
                generation,
                0,
                ddiskInstanceGuid,
                NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_TO_PERSISTENT_BUFFER,
                directBlockGroupIndex);
        }

        // Internal DDisk/PersistentBuffer forwarding.
        // Validation allows the request to bypass sender IC session checks and to pass without a registered
        // tablet connection on the receiver. DDiskSessionSeqNo is not checked: each DDisk has its own session
        // sequence number, so a forwarding actor cannot know the right value for every target.
        static TQueryCredentials ForInternal(
                ui64 tabletId,
                ui32 generation,
                std::optional<ui64> ddiskInstanceGuid,
                ui32 directBlockGroupIndex
        ) {
            return TQueryCredentials(
                tabletId,
                generation,
                0,
                ddiskInstanceGuid,
                NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_INTERNAL,
                directBlockGroupIndex);
        }

        static TQueryCredentials ToDDisk(const TConnectionToken& connectionToken) {
            TQueryCredentials creds;
            creds.ConnectionToken.emplace(connectionToken);
            return creds;
        }

        static TQueryCredentials ToDDisk(const NKikimrBlobStorage::NDDisk::TConnectionToken& connectionToken) {
            return ToDDisk(TConnectionToken(connectionToken));
        }

        static TQueryCredentials ToPersistentBuffer(const TConnectionToken& connectionToken) {
            return ToDDisk(connectionToken);
        }

        static TQueryCredentials ToPersistentBuffer(const NKikimrBlobStorage::NDDisk::TConnectionToken& connectionToken) {
            return ToDDisk(connectionToken);
        }


        TQueryCredentials(const NKikimrBlobStorage::NDDisk::TQueryCredentials& pb)
            : TabletId(pb.GetTabletId())
            , Generation(pb.GetGeneration())
            , DirectBlockGroupIndex(pb.GetDirectBlockGroupIndex())
            , DDiskInstanceGuid(pb.HasDDiskInstanceGuid() ? std::make_optional(pb.GetDDiskInstanceGuid()) : std::nullopt)
            , DDiskSessionSeqNo(pb.GetDDiskSessionSeqNo())
            , RequestKind(pb.GetRequestKind())
        {}

        TQueryCredentials(const TRequestCredentials& pb)
        {
            if (pb.HasInternal()) {
                ServerContext = true;
                const auto& internal = pb.GetInternal();
                TabletId = internal.GetTabletId();
                Generation = internal.GetGeneration();
                DirectBlockGroupIndex = internal.GetDirectBlockGroupIndex();
                DDiskInstanceGuid = internal.HasDDiskInstanceGuid()
                    ? std::make_optional(internal.GetDDiskInstanceGuid())
                    : std::nullopt;
                DDiskSessionSeqNo = internal.GetDDiskSessionSeqNo();
                RequestKind = internal.GetRequestKind();
            } else if (pb.HasConnectionToken()) {
                ConnectionToken.emplace(pb.GetConnectionToken());
            }
        }

        bool IsInternal() const {
            return RequestKind == NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_INTERNAL;
        }

        bool HasServerContext() const {
            return ServerContext;
        }

        bool RequiresDDiskSessionSeqNoCheck() const {
            return RequestKind == NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_TO_DDISK;
        }

        void Serialize(NKikimrBlobStorage::NDDisk::TQueryCredentials *pb) const {
            pb->SetTabletId(TabletId);
            pb->SetGeneration(Generation);
            pb->SetDirectBlockGroupIndex(DirectBlockGroupIndex);
            if (DDiskInstanceGuid) {
                pb->SetDDiskInstanceGuid(*DDiskInstanceGuid);
            }
            if (DDiskSessionSeqNo) {
                pb->SetDDiskSessionSeqNo(DDiskSessionSeqNo);
            }
            if (RequestKind != NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_TO_DDISK) {
                pb->SetRequestKind(RequestKind);
            }
        }

        void SerializeForRequest(TRequestCredentials* pb) const
        {
            pb->Clear();
            if (ConnectionToken) {
                ConnectionToken->Serialize(pb->MutableConnectionToken());
            } else if (IsInternal()) {
                Serialize(pb->MutableInternal());
            }
        }

        void SerializeResolvedForRequest(TRequestCredentials* pb) const
        {
            pb->Clear();
            Serialize(pb->MutableInternal());
        }
    };

    struct TBlockSelector {
        ui64 VChunkIndex;
        ui32 OffsetInBytes;
        ui32 Size;

        TBlockSelector() = default;

        TBlockSelector(ui64 vChunkIndex, ui32 offsetInBytes, ui32 size)
            : VChunkIndex(vChunkIndex)
            , OffsetInBytes(offsetInBytes)
            , Size(size)
        {}

        TBlockSelector(const NKikimrBlobStorage::NDDisk::TBlockSelector& pb)
            : VChunkIndex(pb.GetVChunkIndex())
            , OffsetInBytes(pb.GetOffsetInBytes())
            , Size(pb.GetSize())
        {}

        void Serialize(NKikimrBlobStorage::NDDisk::TBlockSelector *pb) const {
            pb->SetVChunkIndex(VChunkIndex);
            pb->SetOffsetInBytes(OffsetInBytes);
            pb->SetSize(Size);
        }

        void Print(IOutputStream& os) const {
            os << "{VChunkIndex:" << VChunkIndex << " OffsetInBytes:" << OffsetInBytes << " Size:" << Size << "}";
        }
    };

    struct TChecksumValidationResult {
        NKikimrBlobStorage::NDDisk::TReplyStatus::E Status;
        TString ErrorReason;
        ui32 ChecksumCount = 0;
        std::optional<ui32> MismatchedBlockIdx; // set only when Status == CORRUPTED
    };

    // True when the request is 4 KiB-aligned and carries exactly one checksum per block.
    inline bool HasRequiredBlockChecksums(ui32 checksumCount, ui32 offsetInBytes, ui32 size) {
        return offsetInBytes % IntegrityUnitSize == 0
            && size > 0
            && size % IntegrityUnitSize == 0
            && checksumCount > 0
            && static_cast<ui64>(checksumCount) * IntegrityUnitSize == size;
    }

    // Validates a sender-supplied per-block payload checksum list against the payload actually received.
    // Callers must reject writes without HasRequiredBlockChecksums first. This function then returns
    // std::nullopt when every checksum matches, otherwise:
    // * INCORRECT_REQUEST if the checksum count does not match the payload size
    // * CORRUPTED at the first mismatching MinSectorSize block.
    template<typename TRecord>
    [[nodiscard]]
    std::optional<TChecksumValidationResult> ValidatePayloadChecksums(const TRecord& record, const TRope& payload) {
        const ui32 checksumCount = static_cast<ui32>(record.ChecksumsSize());
        if (checksumCount == 0) {
            return std::nullopt;
        }

        if (static_cast<ui64>(checksumCount) * MinSectorSize != payload.size()) {
            return TChecksumValidationResult{
                NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                TStringBuilder() << "checksum count " << checksumCount << " does not match payload size "
                    << payload.size() << " (block size " << MinSectorSize << ")",
                checksumCount,
                std::nullopt,
            };
        }

        auto it = payload.Begin();
        for (ui32 i = 0; i < checksumCount; ++i) {
            if (record.GetChecksums(i) != CalculateBlockChecksum(it, MinSectorSize)) {
                return TChecksumValidationResult{
                    NKikimrBlobStorage::NDDisk::TReplyStatus::CORRUPTED,
                    TStringBuilder() << "checksum mismatch at block " << i << " of " << checksumCount,
                    checksumCount,
                    i,
                };
            }
            it += MinSectorSize;
        }
        return std::nullopt;
    }

    struct TWriteInstruction {
        std::optional<ui32> PayloadId;

        TWriteInstruction() = default;

        TWriteInstruction(ui32 payloadId)
            : PayloadId(payloadId)
        {}

        TWriteInstruction(const NKikimrBlobStorage::NDDisk::TWriteInstruction& pb)
            : PayloadId(pb.HasPayloadId() ? std::make_optional(pb.GetPayloadId()) : std::nullopt)
        {}

        void Serialize(NKikimrBlobStorage::NDDisk::TWriteInstruction *pb) const {
            if (PayloadId) {
                pb->SetPayloadId(*PayloadId);
            }
        }
    };

    struct TReadInstruction {
        bool ReturnInRopePayload;

        TReadInstruction() = default;

        TReadInstruction(bool returnInRopePayload)
            : ReturnInRopePayload(returnInRopePayload)
        {}

        TReadInstruction(const NKikimrBlobStorage::NDDisk::TReadInstruction& pb)
            : ReturnInRopePayload(pb.GetReturnInRopePayload())
        {}

        void Serialize(NKikimrBlobStorage::NDDisk::TReadInstruction *pb) const {
            if (ReturnInRopePayload) {
                pb->SetReturnInRopePayload(ReturnInRopePayload);
            }
        }
    };

    struct TReadResult {
        std::optional<ui32> PayloadId;

        TReadResult() = default;

        TReadResult(ui32 payloadId)
            : PayloadId(payloadId)
        {}

        TReadResult(const NKikimrBlobStorage::NDDisk::TReadResult& pb)
            : PayloadId(pb.HasPayloadId() ? std::make_optional(pb.GetPayloadId()) : std::nullopt)
        {}

        void Serialize(NKikimrBlobStorage::NDDisk::TReadResult *pb) const {
            if (PayloadId) {
                pb->SetPayloadId(*PayloadId);
            }
        }
    };

struct TPersistentBufferFormat {
    ui32 MaxChunks = 256;
    ui32 InitChunks = 4;
    ui64 MaxInMemoryCache = 128_MB;
    ui32 MaxChunkRestoreInflight = 8;
    ui32 UpdateFreeSpaceInfoMilliseconds = 5000;
    ui64 PerTabletStorageLimit = 4096_MB;
    ui32 MaxBarriersLimit = 128;
    ui32 MaxPendingEventsQueueSize = 1024;
    bool EnableFastErases = true;
    ui32 WritesBatchingPeriodMicroseconds = 40;
    bool EnableWritesBatching = true;
    // Minimum number of free sectors to keep reserved so that barrier movement
    // and fast erases (which write to a new sector before freeing the old one)
    // always have space available. New plain writes are rejected with OVERFILL
    // when the free sector count drops below this threshold. Defaults to 256
    // (= default disk operations max inflight size).
    ui32 MinFreeSectorsReserve = 256;
    // Allocate a new chunk proactively when free space drops below this percentage
    // of the currently owned capacity. 0 disables proactive allocation.
    ui32 PreallocateFreeSpaceThresholdPercent = 10;
    // Deallocate a chunk proactively when free space is over this percentage
    // of the currently owned capacity. 100% disables proactive deallocation.
    ui32 DeallocateFreeSpaceThresholdPercent = 90;
    // Deallocate a chunk proactively when it has been freed for this many seconds.
    ui32 DeallocateThresholdSeconds = 30;
    // TEvListPersistentBuffer must not observe a partially-applied write/erase for its tablet: the
    // listing is deferred (queued and retried) while any disk operation is in flight for the
    // requesting tablet. These parameters bound how long/how often we wait before giving up and
    // replying with an OVERLOADED error to avoid returning a potentially-stale view.
    ui32 ListPersistentBufferMaxRetries = 10;
    ui32 ListPersistentBufferRetryPeriodMilliseconds = 20;
    // Controls persistent-buffer on-disk integrity format. When enabled, every data
    // sector and its header use salted checksums. When disabled, a data sector starts
    // with its record header's unique ID; its original first eight bytes
    // are saved in the header. Existing checksum-formatted records remain readable.
    // Kept last to preserve existing positional aggregate initialization.
    bool EnableChecksums = true;
};

#define DECLARE_DDISK_EVENT(NAME) \
    struct TEv##NAME : TEventPB<TEv##NAME, NKikimrBlobStorage::NDDisk::TEv##NAME, TEv::Ev##NAME>

    struct TEvConnect;
    struct TEvConnectResult;
    struct TEvDisconnect;
    struct TEvDisconnectResult;
    struct TEvWrite;
    struct TEvWriteResult;
    struct TEvRead;
    struct TEvReadResult;
    struct TEvSync;
    struct TEvSyncResult;
    struct TEvWritePersistentBuffer;
    struct TEvWritePersistentBufferResult;
    struct TEvWritePersistentBuffers;
    struct TEvWritePersistentBuffersResult;
    struct TEvReadPersistentBuffer;
    struct TEvReadPersistentBufferResult;
    struct TEvErasePersistentBuffer;
    struct TEvBatchErasePersistentBuffer;
    struct TEvErasePersistentBufferResult;
    struct TEvListPersistentBuffer;
    struct TEvListPersistentBufferResult;
    struct TEvReadThenWritePersistentBuffers;
    struct TEvGetPersistentBufferInfo;
    struct TEvPersistentBufferInfo;
    struct TEvDeleteTabletChunks;
    struct TEvDeleteTabletChunksResult;

    DECLARE_DDISK_EVENT(Connect) {
        using TResult = TEvConnectResult;

        TEvConnect() = default;

        TEvConnect(const TQueryCredentials& creds) {
            creds.Serialize(Record.MutableCredentials());
        }
    };

    DECLARE_DDISK_EVENT(ConnectResult) {
        TEvConnectResult() = default;

        TEvConnectResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt,
                std::optional<ui64> ddiskInstanceGuid = std::nullopt,
                std::optional<TConnectionToken> connectionToken = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
            if (ddiskInstanceGuid) {
                Record.SetDDiskInstanceGuid(*ddiskInstanceGuid);
            }
            if (connectionToken) {
                connectionToken->Serialize(Record.MutableConnectionToken());
            }
        }
    };

    DECLARE_DDISK_EVENT(Disconnect) {
        using TResult = TEvDisconnectResult;
    };

    DECLARE_DDISK_EVENT(DisconnectResult) {
        TEvDisconnectResult() = default;

        TEvDisconnectResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
        }
    };

    DECLARE_DDISK_EVENT(Write) {
        using TResult = TEvWriteResult;

        TEvWrite() = default;

        TEvWrite(const TQueryCredentials& creds, const TBlockSelector& selector, const TWriteInstruction& instruction) {
            creds.SerializeForRequest(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            instruction.Serialize(Record.MutableInstruction());
        }

        static constexpr size_t GetPayloadAlignment() {
            return DataAlignment;
        }

        ui32 AddPayloadWithChecksum(TRope&& rope, ui64 checksum) {
            const ui32 id = AddPayload(std::move(rope));
            Record.AddChecksums(checksum);
            return id;
        }

        ui32 AddPayloadWithChecksum(TRope&& rope, const std::vector<ui64>& checksums) {
            const ui32 id = AddPayload(std::move(rope));
            for (ui64 checksum : checksums) {
                Record.AddChecksums(checksum);
            }
            return id;
        }

        // Attaches the payload and computes a checksum for each MinSectorSize block of payload 0.
        ui32 AddPayloadThenChecksum(TRope&& rope);
    };

    DECLARE_DDISK_EVENT(WriteResult) {
        TEvWriteResult() = default;

        TEvWriteResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
        }
    };

    DECLARE_DDISK_EVENT(Read) {
        using TResult = TEvReadResult;

        TEvRead() = default;

        TEvRead(const TQueryCredentials& creds, const TBlockSelector& selector, const TReadInstruction& instruction) {
            creds.SerializeForRequest(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            instruction.Serialize(Record.MutableInstruction());
        }
    };

    DECLARE_DDISK_EVENT(ReadResult) {
        TEvReadResult() = default;

        TEvReadResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt,
                TRope data = {}, const std::vector<ui64>& checksums = {}) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
            if (data) {
                TReadResult(AddPayload(std::move(data))).Serialize(Record.MutableReadResult());
            }
            for (const ui64 checksum : checksums) {
                Record.AddChecksums(checksum);
            }
        }
    };

    DECLARE_DDISK_EVENT(WritePersistentBuffer) {
        using TResult = TEvWritePersistentBufferResult;

        TEvWritePersistentBuffer() = default;

        TEvWritePersistentBuffer(const TQueryCredentials& creds, const TBlockSelector& selector, ui64 lsn,
                const TWriteInstruction& instruction) {
            creds.SerializeForRequest(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            Record.SetLsn(lsn);
            instruction.Serialize(Record.MutableInstruction());
        }

        static constexpr size_t GetPayloadAlignment() {
            return DataAlignment;
        }

        static constexpr size_t GetPayloadHeaderSize() {
            return MinSectorSize;
        }

        ui32 AddPayloadWithChecksum(TRope&& rope, ui64 checksum) {
            const ui32 id = AddPayload(std::move(rope));
            Record.AddChecksums(checksum);
            return id;
        }

        ui32 AddPayloadWithChecksum(TRope&& rope, const std::vector<ui64>& checksums) {
            const ui32 id = AddPayload(std::move(rope));
            for (ui64 checksum : checksums) {
                Record.AddChecksums(checksum);
            }
            return id;
        }

        // Attaches the payload and computes a checksum for each MinSectorSize block of payload 0.
        ui32 AddPayloadThenChecksum(TRope&& rope);
    };

    DECLARE_DDISK_EVENT(WritePersistentBufferResult) {
        TEvWritePersistentBufferResult() = default;

        TEvWritePersistentBufferResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt, double freeSpace = -1, double normalizedOccupancy = -1) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
            Record.SetFreeSpace(freeSpace);
            Record.SetPDiskNormalizedOccupancy(normalizedOccupancy);
        }
    };

    DECLARE_DDISK_EVENT(WritePersistentBuffersResult) {
        TEvWritePersistentBuffersResult() {
        }
    };

    DECLARE_DDISK_EVENT(ReadThenWritePersistentBuffers) {
        using TResult = TEvWritePersistentBuffersResult;

        TEvReadThenWritePersistentBuffers() = default;

        TEvReadThenWritePersistentBuffers(const TQueryCredentials& creds, ui64 lsn, ui32 generation,
                const std::vector<std::tuple<ui32, ui32, ui32>>& persistentBufferIds,
                ui32 replyTimeoutMicroseconds) {
            creds.SerializeForRequest(Record.MutableCredentials());
            Record.SetLsn(lsn);
            Record.SetGeneration(generation);
            Record.SetReplyTimeoutMicroseconds(replyTimeoutMicroseconds);
            for (auto id : persistentBufferIds) {
                auto* pbId = Record.AddPersistentBufferIds();
                pbId->SetNodeId(std::get<0>(id));
                pbId->SetPDiskId(std::get<1>(id));
                pbId->SetDDiskSlotId(std::get<2>(id));
            }
        }
    };

    DECLARE_DDISK_EVENT(WritePersistentBuffers) {
        using TResult = TEvWritePersistentBuffersResult;

        TEvWritePersistentBuffers() = default;

        TEvWritePersistentBuffers(const TQueryCredentials& creds, const TBlockSelector& selector, ui64 lsn,
                const TWriteInstruction& instruction, const std::vector<std::tuple<ui32, ui32, ui32>>& persistentBufferIds,
                ui32 replyTimeoutMicroseconds) {
            creds.SerializeForRequest(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            Record.SetLsn(lsn);
            Record.SetReplyTimeoutMicroseconds(replyTimeoutMicroseconds);
            instruction.Serialize(Record.MutableInstruction());
            for (auto id : persistentBufferIds) {
                auto* pbId = Record.AddPersistentBufferIds();
                pbId->SetNodeId(std::get<0>(id));
                pbId->SetPDiskId(std::get<1>(id));
                pbId->SetDDiskSlotId(std::get<2>(id));
            }
        }

        TEvWritePersistentBuffers(const TQueryCredentials& creds, const TBlockSelector& selector, ui64 lsn,
                const TWriteInstruction& instruction, const std::vector<NKikimrBlobStorage::NDDisk::TDDiskId>& persistentBufferIds,
                ui32 replyTimeoutMicroseconds) {
            creds.SerializeForRequest(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            Record.SetLsn(lsn);
            Record.SetReplyTimeoutMicroseconds(replyTimeoutMicroseconds);
            instruction.Serialize(Record.MutableInstruction());
            for (auto id : persistentBufferIds) {
                auto* pbId = Record.AddPersistentBufferIds();
                *pbId = id;
            }
        }

        static constexpr size_t GetPayloadAlignment() {
            return DataAlignment;
        }

        ui32 AddPayloadWithChecksum(TRope&& rope, ui64 checksum) {
            const ui32 id = AddPayload(std::move(rope));
            Record.AddChecksums(checksum);
            return id;
        }

        ui32 AddPayloadWithChecksum(TRope&& rope, const std::vector<ui64>& checksums) {
            const ui32 id = AddPayload(std::move(rope));
            for (ui64 checksum : checksums) {
                Record.AddChecksums(checksum);
            }
            return id;
        }

        // Attaches the payload and computes a checksum for each MinSectorSize block of payload 0.
        ui32 AddPayloadThenChecksum(TRope&& rope);
    };

    DECLARE_DDISK_EVENT(ReadPersistentBuffer) {
        using TResult = TEvReadPersistentBufferResult;

        TEvReadPersistentBuffer() = default;

        TEvReadPersistentBuffer(const TQueryCredentials& creds, const TBlockSelector& selector,
                ui64 lsn, ui32 generation, const TReadInstruction& instruction) {
            creds.SerializeForRequest(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            Record.SetLsn(lsn);
            Record.SetGeneration(generation);
            instruction.Serialize(Record.MutableInstruction());
        }
    };

    DECLARE_DDISK_EVENT(ReadPersistentBufferResult) {
        TEvReadPersistentBufferResult() = default;

        TEvReadPersistentBufferResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt,
                ui64 vChunkIndex = 0, ui32 offsetInBytes = 0, ui32 sizeInBytes = 0,
                TRope data = {}, const std::vector<ui64>& checksums = {}) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
            if (data) {
                Record.SetVChunkIndex(vChunkIndex);
                Record.SetOffsetInBytes(offsetInBytes);
                Record.SetSizeInBytes(sizeInBytes);
                TReadResult(AddPayload(std::move(data))).Serialize(Record.MutableReadResult());
                // Raw XXH3_64(data) per MinSectorSize block, copied from the persisted record.
                // Successful writes always store checksums, so a successful read of a live record
                // returns exactly one value per aligned block.
                for (ui64 checksum : checksums) {
                    Record.AddChecksums(checksum);
                }
            }
        }
    };

    DECLARE_DDISK_EVENT(ErasePersistentBuffer) {
        using TResult = TEvErasePersistentBufferResult;

        TEvErasePersistentBuffer() = default;

        TEvErasePersistentBuffer(const TQueryCredentials& creds, ui64 lsn) {
            creds.SerializeForRequest(Record.MutableCredentials());
            Record.SetLsn(lsn);
        }
    };

    DECLARE_DDISK_EVENT(BatchErasePersistentBuffer) {
        using TResult = TEvErasePersistentBufferResult;

        TEvBatchErasePersistentBuffer() = default;

        TEvBatchErasePersistentBuffer(const TQueryCredentials& creds) {
            creds.SerializeForRequest(Record.MutableCredentials());
        }

        TEvBatchErasePersistentBuffer(const TQueryCredentials& creds, const std::vector<std::tuple<ui64, ui32>>& erases) {
            creds.SerializeForRequest(Record.MutableCredentials());
            for (auto& [lsn, generation] : erases) {
                auto* erase = Record.AddErases();
                erase->SetLsn(lsn);
                erase->SetGeneration(generation);
            }
        }

        void AddErase(ui64 lsn, ui32 generation) {
            auto *erase = Record.AddErases();
            erase->SetLsn(lsn);
            erase->SetGeneration(generation);
        }
    };

    DECLARE_DDISK_EVENT(ErasePersistentBufferResult) {
        TEvErasePersistentBufferResult() = default;

        TEvErasePersistentBufferResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt, double freeSpace = -1,
                double normalizedOccupancy = -1) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
            Record.SetFreeSpace(freeSpace);
            Record.SetPDiskNormalizedOccupancy(normalizedOccupancy);
        }
    };


    struct TEvPersistentBufferInfo : public TEventLocal<TEvPersistentBufferInfo, TEv::EvPersistentBufferInfo> {
        struct TTabletInfo {
            ui64 TabletId;
            ui32 Generation;
            ui64 FirstLsn;
            ui64 LastLsn;
            TInstant FirstLsnTimestamp;
            TInstant LastLsnTimestamp;
            ui32 LsnsCount;
            ui64 Size;
            ui32 FastErasesCount;
            // Direct block group number this info entry belongs to. See TPersistentBufferId for
            // rationale; defaults to 0 to preserve the pre-existing single-namespace-per-tablet
            // behavior.
            ui8 DirectBlockGroupIndex = 0;
        };

        struct TOpStats {
            TString Name;
            ui64 RequestsInFlight = 0;
            ui64 Requests = 0; // requests in the measurement window
            double LatencyP50Ms = 0;
            double LatencyP99Ms = 0;
            double LatencyMaxMs = 0;
            double WindowSeconds = 0; // measurement window for Requests / latencies
        };

        TInstant StartedAt;
        ui32 AllocatedChunks;
        ui32 MaxChunks;
        ui32 SectorSize;
        ui32 ChunkSize;
        ui32 FreeSectors;
        ui64 InMemoryCacheSize;
        ui64 InMemoryCacheLimit;
        ui32 DiskOperationsInflight;
        ui32 PendingEvents;
        ui64 PerTabletStorageLimit;
        std::vector<TTabletInfo> TabletInfos;
        // Keyed by (TabletId, DirectBlockGroupIndex), matching TPersistentBufferBarriersManager::GetBarriers().
        std::map<std::pair<ui64, ui8>, ui64> EraseBarriers;
        std::vector<std::vector<std::tuple<ui32, ui32>>> FreeSpace;
        std::vector<TOpStats> OpStats;
    };

    struct TEvGetPersistentBufferInfo : public TEventLocal<TEvGetPersistentBufferInfo, TEv::EvGetPersistentBufferInfo> {
        bool DescribeFreeSpace = false;
        bool DescribeTablets = false;
        TEvGetPersistentBufferInfo(bool describeFreeSpace = false, bool describeTablets = false)
            : DescribeFreeSpace(describeFreeSpace)
            , DescribeTablets(describeTablets)
        {}
    };

    DECLARE_DDISK_EVENT(ListPersistentBuffer) {
        using TResult = TEvListPersistentBufferResult;

        TEvListPersistentBuffer() = default;

        TEvListPersistentBuffer(const TQueryCredentials& creds) {
            creds.SerializeForRequest(Record.MutableCredentials());
        }
    };

    DECLARE_DDISK_EVENT(ListPersistentBufferResult) {
        TEvListPersistentBufferResult() = default;

        TEvListPersistentBufferResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
        }
    };

    DECLARE_DDISK_EVENT(Sync) {
        using TResult = TEvSyncResult;
        using TDDiskId = std::tuple<ui32, ui32, ui32>;
        using TSource = NKikimrBlobStorage::NDDisk::TEvSync::TSource;
        using TSegment = NKikimrBlobStorage::NDDisk::TEvSync::TSegment;

        TEvSync() = default;

        explicit TEvSync(const TQueryCredentials& creds) {
            creds.SerializeForRequest(Record.MutableCredentials());
        }

        static void SetSource(TSource *source, const TDDiskId& ddiskId, ui64 ddiskInstanceGuid) {
            const auto& [nodeId, pdiskId, ddiskSlotId] = ddiskId;
            auto *m = source->MutableDDiskId();
            m->SetNodeId(nodeId);
            m->SetPDiskId(pdiskId);
            m->SetDDiskSlotId(ddiskSlotId);
            source->SetDDiskInstanceGuid(ddiskInstanceGuid);
        }

        TSegment* AddSegmentFromDDisk(
                const TDDiskId& ddiskId,
                ui64 ddiskInstanceGuid,
                const TBlockSelector& selector) {
            auto *source = GetOrAddSource(ddiskId, ddiskInstanceGuid);
            auto *segment = source->AddSegments();
            selector.Serialize(segment->MutableSelector());
            segment->MutableDDiskSegment();
            return segment;
        }

        TSegment* AddSegmentFromPB(
                const TDDiskId& ddiskId,
                ui64 ddiskInstanceGuid,
                const TBlockSelector& selector,
                ui64 lsn,
                ui32 generation) {
            auto *source = GetOrAddSource(ddiskId, ddiskInstanceGuid);
            auto *segment = source->AddSegments();
            selector.Serialize(segment->MutableSelector());
            auto *persistentBufferSegment =
                segment->MutablePersistentBufferSegment();
            persistentBufferSegment->SetLsn(lsn);
            persistentBufferSegment->SetGeneration(generation);
            return segment;
        }

    private:
        static bool IsSameSource(const TSource& source, const TDDiskId& ddiskId, ui64 ddiskInstanceGuid) {
            if (!source.HasDDiskId() || source.GetDDiskInstanceGuid() != ddiskInstanceGuid) {
                return false;
            }

            const auto& [nodeId, pdiskId, ddiskSlotId] = ddiskId;
            const auto& sourceDDiskId = source.GetDDiskId();
            return sourceDDiskId.GetNodeId() == nodeId
                && sourceDDiskId.GetPDiskId() == pdiskId
                && sourceDDiskId.GetDDiskSlotId() == ddiskSlotId;
        }

        TSource* GetOrAddSource(const TDDiskId& ddiskId, ui64 ddiskInstanceGuid) {
            // Coalesce only consecutive same-source segments to preserve request order.
            if (Record.SourcesSize()) {
                auto *source = Record.MutableSources(Record.SourcesSize() - 1);
                if (IsSameSource(*source, ddiskId, ddiskInstanceGuid)) {
                    return source;
                }
            }

            auto *source = Record.AddSources();
            SetSource(source, ddiskId, ddiskInstanceGuid);
            return source;
        }
    };

    DECLARE_DDISK_EVENT(SyncResult) {
        TEvSyncResult() = default;

        TEvSyncResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
        }

        void AddSegmentResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status, TString errorReason) {
            auto *result = Record.AddSegmentResults();
            result->SetStatus(status);
            if (errorReason) {
                result->SetErrorReason(errorReason);
            }
        }
    };

    DECLARE_DDISK_EVENT(DeleteTabletChunks) {
        using TResult = TEvDeleteTabletChunksResult;

        TEvDeleteTabletChunks() = default;

        TEvDeleteTabletChunks(const TQueryCredentials& creds) {
            creds.SerializeForRequest(Record.MutableCredentials());
        }
    };

    DECLARE_DDISK_EVENT(DeleteTabletChunksResult) {
        TEvDeleteTabletChunksResult() = default;

        TEvDeleteTabletChunksResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
        }
    };

    IActor *CreateDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
        TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters);

    IActor *CreatePersistentBufferActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
        TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters);

} // NKikimr::NDDisk
