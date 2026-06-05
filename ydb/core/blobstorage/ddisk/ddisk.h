#pragma once

#include "defs.h"
#include "ddisk_config.h"

#include <ydb/core/base/events.h>

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>

namespace NKikimr::NDDisk {

    constexpr size_t MinSectorSize = 4096;
    constexpr size_t DataAlignment = MinSectorSize;

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
            EvSyncWithPersistentBuffer,
            EvSyncWithPersistentBufferResult,
            EvSyncWithDDisk,
            EvSyncWithDDiskResult,
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
        using ERequestKind = NKikimrBlobStorage::NDDisk::TQueryCredentials::ERequestKind;

        ui64 TabletId;
        ui32 Generation;
        std::optional<ui64> DDiskInstanceGuid;
        ui64 DDiskSessionSeqNo = 0;
        ERequestKind RequestKind = NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_TO_DDISK;

        TQueryCredentials() = default;

        TQueryCredentials(
                ui64 tabletId,
                ui32 generation,
                ui64 ddiskSessionSeqNo,
                std::optional<ui64> ddiskInstanceGuid,
                ERequestKind requestKind)
            : TabletId(tabletId)
            , Generation(generation)
            , DDiskInstanceGuid(ddiskInstanceGuid)
            , DDiskSessionSeqNo(ddiskSessionSeqNo)
            , RequestKind(requestKind)
        {}

        // Tablet-originated request sent to a DDisk actor.
        // Validation requires a registered tablet connection with matching generation and DDiskSessionSeqNo,
        // matching DDiskInstanceGuid when it is set, and matching sender IC session.
        static TQueryCredentials ToDDisk(
                ui64 tabletId,
                ui32 generation,
                ui64 ddiskSessionSeqNo,
                std::optional<ui64> ddiskInstanceGuid) {
            return TQueryCredentials(
                tabletId,
                generation,
                ddiskSessionSeqNo,
                ddiskInstanceGuid,
                NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_TO_DDISK);
        }

        // Tablet-originated request sent to a PersistentBuffer actor.
        // Validation still requires a registered tablet connection, matching generation,
        // matching DDiskInstanceGuid when it is set, and matching sender node. Interconnect session
        // and DDiskSessionSeqNo are skipped because persistent buffers are not bound to a particular
        // DDisk session.
        static TQueryCredentials ToPersistentBuffer(
                ui64 tabletId,
                ui32 generation,
                std::optional<ui64> ddiskInstanceGuid) {
            return TQueryCredentials(
                tabletId,
                generation,
                0,
                ddiskInstanceGuid,
                NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_TO_PERSISTENT_BUFFER);
        }

        // Internal DDisk/PersistentBuffer forwarding.
        // Validation allows the request to bypass sender IC session checks and to pass without a registered
        // tablet connection on the receiver. DDiskSessionSeqNo is not checked: each DDisk has its own session
        // sequence number, so a forwarding actor cannot know the right value for every target.
        static TQueryCredentials ForInternal(
                ui64 tabletId,
                ui32 generation,
                std::optional<ui64> ddiskInstanceGuid) {
            return TQueryCredentials(
                tabletId,
                generation,
                0,
                ddiskInstanceGuid,
                NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_INTERNAL);
        }

        TQueryCredentials(const NKikimrBlobStorage::NDDisk::TQueryCredentials& pb)
            : TabletId(pb.GetTabletId())
            , Generation(pb.GetGeneration())
            , DDiskInstanceGuid(pb.HasDDiskInstanceGuid() ? std::make_optional(pb.GetDDiskInstanceGuid()) : std::nullopt)
            , DDiskSessionSeqNo(pb.GetDDiskSessionSeqNo())
            , RequestKind(pb.GetRequestKind())
        {}

        bool IsInternal() const {
            return RequestKind == NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_INTERNAL;
        }

        bool RequiresDDiskSessionSeqNoCheck() const {
            return RequestKind == NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_TO_DDISK;
        }

        bool RequiresSenderCheck() const {
            return !IsInternal();
        }

        bool RequiresInterconnectSessionCheck() const {
            return RequestKind == NKikimrBlobStorage::NDDisk::TQueryCredentials::REQUEST_KIND_TO_DDISK;
        }

        void Serialize(NKikimrBlobStorage::NDDisk::TQueryCredentials *pb) const {
            pb->SetTabletId(TabletId);
            pb->SetGeneration(Generation);
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
    struct TEvSyncWithPersistentBuffer;
    struct TEvSyncWithPersistentBufferResult;
    struct TEvSyncWithDDisk;
    struct TEvSyncWithDDiskResult;
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
                std::optional<ui64> ddiskInstanceGuid = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
            if (ddiskInstanceGuid) {
                Record.SetDDiskInstanceGuid(*ddiskInstanceGuid);
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
            creds.Serialize(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            instruction.Serialize(Record.MutableInstruction());
        }

        static constexpr size_t GetPayloadAlignment() {
            return DataAlignment;
        }
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
            creds.Serialize(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            instruction.Serialize(Record.MutableInstruction());
        }
    };

    DECLARE_DDISK_EVENT(ReadResult) {
        TEvReadResult() = default;

        TEvReadResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt,
                TRope data = {}) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
            if (data) {
                TReadResult(AddPayload(std::move(data))).Serialize(Record.MutableReadResult());
            }
        }
    };

    DECLARE_DDISK_EVENT(WritePersistentBuffer) {
        using TResult = TEvWritePersistentBufferResult;

        TEvWritePersistentBuffer() = default;

        TEvWritePersistentBuffer(const TQueryCredentials& creds, const TBlockSelector& selector, ui64 lsn,
                const TWriteInstruction& instruction) {
            creds.Serialize(Record.MutableCredentials());
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
            creds.Serialize(Record.MutableCredentials());
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
            creds.Serialize(Record.MutableCredentials());
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
            creds.Serialize(Record.MutableCredentials());
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
    };

    DECLARE_DDISK_EVENT(ReadPersistentBuffer) {
        using TResult = TEvReadPersistentBufferResult;

        TEvReadPersistentBuffer() = default;

        TEvReadPersistentBuffer(const TQueryCredentials& creds, const TBlockSelector& selector,
                ui64 lsn, ui32 generation, const TReadInstruction& instruction) {
            creds.Serialize(Record.MutableCredentials());
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
                TRope data = {}) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
            if (data) {
                Record.SetVChunkIndex(vChunkIndex);
                Record.SetOffsetInBytes(offsetInBytes);
                Record.SetSizeInBytes(sizeInBytes);
                TReadResult(AddPayload(std::move(data))).Serialize(Record.MutableReadResult());
            }
        }
    };

    DECLARE_DDISK_EVENT(ErasePersistentBuffer) {
        using TResult = TEvErasePersistentBufferResult;

        TEvErasePersistentBuffer() = default;

        TEvErasePersistentBuffer(const TQueryCredentials& creds, ui64 lsn) {
            creds.Serialize(Record.MutableCredentials());
            Record.SetLsn(lsn);
        }
    };

    DECLARE_DDISK_EVENT(BatchErasePersistentBuffer) {
        using TResult = TEvErasePersistentBufferResult;

        TEvBatchErasePersistentBuffer() = default;

        TEvBatchErasePersistentBuffer(const TQueryCredentials& creds) {
            creds.Serialize(Record.MutableCredentials());
        }

        TEvBatchErasePersistentBuffer(const TQueryCredentials& creds, const std::vector<std::tuple<ui64, ui32>>& erases) {
            creds.Serialize(Record.MutableCredentials());
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
        std::unordered_map<ui64, ui64> EraseBarriers;
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
            creds.Serialize(Record.MutableCredentials());
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

    DECLARE_DDISK_EVENT(SyncWithPersistentBuffer) {
        using TResult = TEvSyncWithPersistentBufferResult;

        TEvSyncWithPersistentBuffer() = default;

        TEvSyncWithPersistentBuffer(const TQueryCredentials& creds, std::optional<std::tuple<ui32, ui32, ui32>> ddiskId,
                std::optional<ui64> ddiskInstanceGuid) {
            creds.Serialize(Record.MutableCredentials());
            if (ddiskId) {
                const auto& [nodeId, pdiskId, ddiskSlotId] = *ddiskId;
                auto *m = Record.MutableDDiskId();
                m->SetNodeId(nodeId);
                m->SetPDiskId(pdiskId);
                m->SetDDiskSlotId(ddiskSlotId);
            }
            if (ddiskInstanceGuid) {
                Record.SetDDiskInstanceGuid(*ddiskInstanceGuid);
            }
        }

        void AddSegment(const TBlockSelector& selector, ui64 lsn, ui32 generation) {
            auto *segment = Record.AddSegments();
            selector.Serialize(segment->MutableSelector());
            segment->SetLsn(lsn);
            segment->SetGeneration(generation);
        }
    };

    DECLARE_DDISK_EVENT(SyncWithPersistentBufferResult) {
        TEvSyncWithPersistentBufferResult() = default;

        TEvSyncWithPersistentBufferResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
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

    DECLARE_DDISK_EVENT(SyncWithDDisk) {
        using TResult = TEvSyncWithDDiskResult;

        TEvSyncWithDDisk() = default;

        TEvSyncWithDDisk(const TQueryCredentials& creds, std::optional<std::tuple<ui32, ui32, ui32>> ddiskId,
                std::optional<ui64> ddiskInstanceGuid) {
            creds.Serialize(Record.MutableCredentials());
            if (ddiskId) {
                const auto& [nodeId, pdiskId, ddiskSlotId] = *ddiskId;
                auto *m = Record.MutableDDiskId();
                m->SetNodeId(nodeId);
                m->SetPDiskId(pdiskId);
                m->SetDDiskSlotId(ddiskSlotId);
            }
            if (ddiskInstanceGuid) {
                Record.SetDDiskInstanceGuid(*ddiskInstanceGuid);
            }
        }

        void AddSegment(const TBlockSelector& selector) {
            auto *segment = Record.AddSegments();
            selector.Serialize(segment->MutableSelector());
        }
    };

    DECLARE_DDISK_EVENT(SyncWithDDiskResult) {
        TEvSyncWithDDiskResult() = default;

        TEvSyncWithDDiskResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
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
            creds.Serialize(Record.MutableCredentials());
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
