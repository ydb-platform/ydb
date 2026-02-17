#pragma once

#include "defs.h"

#include <ydb/core/base/events.h>

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>

namespace NKikimr::NDDisk {

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
        };
    };

    struct TQueryCredentials {
        ui64 TabletId;
        ui32 Generation;
        std::optional<ui64> DDiskInstanceGuid;
        bool FromPersistentBuffer = false;

        TQueryCredentials() = default;

        TQueryCredentials(ui64 tabletId, ui32 generation, std::optional<ui64> ddiskInstanceGuid, bool fromPersistentBuffer = false)
            : TabletId(tabletId)
            , Generation(generation)
            , DDiskInstanceGuid(ddiskInstanceGuid)
            , FromPersistentBuffer(fromPersistentBuffer)
        {}

        TQueryCredentials(const NKikimrBlobStorage::NDDisk::TQueryCredentials& pb)
            : TabletId(pb.GetTabletId())
            , Generation(pb.GetGeneration())
            , DDiskInstanceGuid(pb.HasDDiskInstanceGuid() ? std::make_optional(pb.GetDDiskInstanceGuid()) : std::nullopt)
            , FromPersistentBuffer(pb.GetFromPersistentBuffer())
        {}

        void Serialize(NKikimrBlobStorage::NDDisk::TQueryCredentials *pb) const {
            pb->SetTabletId(TabletId);
            pb->SetGeneration(Generation);
            if (DDiskInstanceGuid) {
                pb->SetDDiskInstanceGuid(*DDiskInstanceGuid);
            }
            if (FromPersistentBuffer) {
                pb->SetFromPersistentBuffer(FromPersistentBuffer);
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
    struct TEvReadPersistentBuffer;
    struct TEvReadPersistentBufferResult;
    struct TEvErasePersistentBuffer;
    struct TEvBatchErasePersistentBuffer;
    struct TEvErasePersistentBufferResult;
    struct TEvListPersistentBuffer;
    struct TEvListPersistentBufferResult;

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
    };

    DECLARE_DDISK_EVENT(WritePersistentBufferResult) {
        TEvWritePersistentBufferResult() = default;

        TEvWritePersistentBufferResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt, double freeSpace = -1) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
            Record.SetFreeSpace(freeSpace);
        }
    };

    DECLARE_DDISK_EVENT(ReadPersistentBuffer) {
        using TResult = TEvReadPersistentBufferResult;

        TEvReadPersistentBuffer() = default;

        TEvReadPersistentBuffer(const TQueryCredentials& creds, const TBlockSelector& selector, ui64 lsn,
                const TReadInstruction& instruction) {
            creds.Serialize(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            Record.SetLsn(lsn);
            instruction.Serialize(Record.MutableInstruction());
        }
    };

    DECLARE_DDISK_EVENT(ReadPersistentBufferResult) {
        TEvReadPersistentBufferResult() = default;

        TEvReadPersistentBufferResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
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

    DECLARE_DDISK_EVENT(ErasePersistentBuffer) {
        using TResult = TEvErasePersistentBufferResult;

        TEvErasePersistentBuffer() = default;

        TEvErasePersistentBuffer(const TQueryCredentials& creds, const TBlockSelector& selector, ui64 lsn) {
            creds.Serialize(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            Record.SetLsn(lsn);
        }
    };

    DECLARE_DDISK_EVENT(BatchErasePersistentBuffer) {
        using TResult = TEvErasePersistentBufferResult;

        TEvBatchErasePersistentBuffer() = default;

        TEvBatchErasePersistentBuffer(const TQueryCredentials& creds, const std::vector<std::tuple<TBlockSelector, ui64>>& erases) {
            creds.Serialize(Record.MutableCredentials());
            for (auto& [selector, lsn] : erases) {
                auto* erase = Record.AddErases();
                selector.Serialize(erase->MutableSelector());
                erase->SetLsn(lsn);
            }
        }
    };

    DECLARE_DDISK_EVENT(ErasePersistentBufferResult) {
        TEvErasePersistentBufferResult() = default;

        TEvErasePersistentBufferResult(NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt, double freeSpace = -1) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
            Record.SetFreeSpace(freeSpace);
        }
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

        void AddSegment(const TBlockSelector& selector, ui64 lsn) {
            auto *segment = Record.AddSegments();
            selector.Serialize(segment->MutableSelector());
            segment->SetLsn(lsn);
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

    IActor *CreateDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters);

} // NKikimr::NDDisk
