#pragma once

#include "defs.h"

#include <ydb/core/base/events.h>

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>

namespace NKikimr::NDDisk {

    struct TEv {
        enum {
            EvDDiskConnect = EventSpaceBegin(TKikimrEvents::ES_DDISK),
            EvDDiskConnectResult,
            EvDDiskDisconnect,
            EvDDiskDisconnectResult,
            EvDDiskWrite,
            EvDDiskWriteResult,
            EvDDiskRead,
            EvDDiskReadResult,
            EvDDiskWritePersistentBuffer,
            EvDDiskWritePersistentBufferResult,
            EvDDiskReadPersistentBuffer,
            EvDDiskReadPersistentBufferResult,
            EvDDiskFlushPersistentBuffer,
            EvDDiskFlushPersistentBufferResult,
            EvDDiskListPersistentBuffer,
            EvDDiskListPersistentBufferResult,
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

        TQueryCredentials(const NKikimrBlobStorage::TDDiskQueryCredentials& pb)
            : TabletId(pb.GetTabletId())
            , Generation(pb.GetGeneration())
            , DDiskInstanceGuid(pb.HasDDiskInstanceGuid() ? std::make_optional(pb.GetDDiskInstanceGuid()) : std::nullopt)
            , FromPersistentBuffer(pb.GetFromPersistentBuffer())
        {}

        void Serialize(NKikimrBlobStorage::TDDiskQueryCredentials *pb) const {
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

        TBlockSelector(const NKikimrBlobStorage::TDDiskBlockSelector& pb)
            : VChunkIndex(pb.GetVChunkIndex())
            , OffsetInBytes(pb.GetOffsetInBytes())
            , Size(pb.GetSize())
        {}

        void Serialize(NKikimrBlobStorage::TDDiskBlockSelector *pb) const {
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

        TWriteInstruction(const NKikimrBlobStorage::TDDiskWriteInstruction& pb)
            : PayloadId(pb.HasPayloadId() ? std::make_optional(pb.GetPayloadId()) : std::nullopt)
        {}

        void Serialize(NKikimrBlobStorage::TDDiskWriteInstruction *pb) const {
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

        TReadInstruction(const NKikimrBlobStorage::TDDiskReadInstruction& pb)
            : ReturnInRopePayload(pb.GetReturnInRopePayload())
        {}

        void Serialize(NKikimrBlobStorage::TDDiskReadInstruction *pb) const {
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

        TReadResult(const NKikimrBlobStorage::TDDiskReadResult& pb)
            : PayloadId(pb.HasPayloadId() ? std::make_optional(pb.GetPayloadId()) : std::nullopt)
        {}

        void Serialize(NKikimrBlobStorage::TDDiskReadResult *pb) const {
            if (PayloadId) {
                pb->SetPayloadId(*PayloadId);
            }
        }
    };

#define DECLARE_DDISK_EVENT(NAME) \
    struct TEvDDisk##NAME : TEventPB<TEvDDisk##NAME, NKikimrBlobStorage::TEvDDisk##NAME, TEv::EvDDisk##NAME>

    struct TEvDDiskConnect;
    struct TEvDDiskConnectResult;
    struct TEvDDiskDisconnect;
    struct TEvDDiskDisconnectResult;
    struct TEvDDiskWrite;
    struct TEvDDiskWriteResult;
    struct TEvDDiskRead;
    struct TEvDDiskReadResult;
    struct TEvDDiskWritePersistentBuffer;
    struct TEvDDiskWritePersistentBufferResult;
    struct TEvDDiskReadPersistentBuffer;
    struct TEvDDiskReadPersistentBufferResult;
    struct TEvDDiskFlushPersistentBuffer;
    struct TEvDDiskFlushPersistentBufferResult;
    struct TEvDDiskListPersistentBuffer;
    struct TEvDDiskListPersistentBufferResult;

    DECLARE_DDISK_EVENT(Connect) {
        using TResult = TEvDDiskConnectResult;

        TEvDDiskConnect() = default;

        TEvDDiskConnect(const TQueryCredentials& creds) {
            creds.Serialize(Record.MutableCredentials());
        }
    };

    DECLARE_DDISK_EVENT(ConnectResult) {
        TEvDDiskConnectResult() = default;

        TEvDDiskConnectResult(NKikimrBlobStorage::TDDiskReplyStatus::E status,
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
        using TResult = TEvDDiskDisconnectResult;
    };

    DECLARE_DDISK_EVENT(DisconnectResult) {
        TEvDDiskDisconnectResult() = default;

        TEvDDiskDisconnectResult(NKikimrBlobStorage::TDDiskReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
        }
    };

    DECLARE_DDISK_EVENT(Write) {
        using TResult = TEvDDiskWriteResult;

        TEvDDiskWrite() = default;

        TEvDDiskWrite(const TQueryCredentials& creds, const TBlockSelector& selector, const TWriteInstruction& instruction) {
            creds.Serialize(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            instruction.Serialize(Record.MutableInstruction());
        }
    };

    DECLARE_DDISK_EVENT(WriteResult) {
        TEvDDiskWriteResult() = default;

        TEvDDiskWriteResult(NKikimrBlobStorage::TDDiskReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
        }
    };

    DECLARE_DDISK_EVENT(Read) {
        using TResult = TEvDDiskReadResult;

        TEvDDiskRead() = default;

        TEvDDiskRead(const TQueryCredentials& creds, const TBlockSelector& selector, const TReadInstruction& instruction) {
            creds.Serialize(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            instruction.Serialize(Record.MutableInstruction());
        }
    };

    DECLARE_DDISK_EVENT(ReadResult) {
        TEvDDiskReadResult() = default;

        TEvDDiskReadResult(NKikimrBlobStorage::TDDiskReplyStatus::E status,
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
        using TResult = TEvDDiskWritePersistentBufferResult;

        TEvDDiskWritePersistentBuffer() = default;

        TEvDDiskWritePersistentBuffer(const TQueryCredentials& creds, const TBlockSelector& selector, ui64 lsn,
                const TWriteInstruction& instruction) {
            creds.Serialize(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            Record.SetLsn(lsn);
            instruction.Serialize(Record.MutableInstruction());
        }
    };

    DECLARE_DDISK_EVENT(WritePersistentBufferResult) {
        TEvDDiskWritePersistentBufferResult() = default;

        TEvDDiskWritePersistentBufferResult(NKikimrBlobStorage::TDDiskReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
        }
    };

    DECLARE_DDISK_EVENT(ReadPersistentBuffer) {
        using TResult = TEvDDiskReadPersistentBufferResult;

        TEvDDiskReadPersistentBuffer() = default;

        TEvDDiskReadPersistentBuffer(const TQueryCredentials& creds, const TBlockSelector& selector, ui64 lsn,
                const TReadInstruction& instruction) {
            creds.Serialize(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            Record.SetLsn(lsn);
            instruction.Serialize(Record.MutableInstruction());
        }
    };

    DECLARE_DDISK_EVENT(ReadPersistentBufferResult) {
        TEvDDiskReadPersistentBufferResult() = default;

        TEvDDiskReadPersistentBufferResult(NKikimrBlobStorage::TDDiskReplyStatus::E status,
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

    DECLARE_DDISK_EVENT(FlushPersistentBuffer) {
        using TResult = TEvDDiskFlushPersistentBufferResult;

        TEvDDiskFlushPersistentBuffer() = default;

        TEvDDiskFlushPersistentBuffer(const TQueryCredentials& creds, const TBlockSelector& selector, ui64 lsn,
                std::optional<std::tuple<ui32, ui32, ui32>> ddiskId, std::optional<ui64> ddiskInstanceGuid) {
            creds.Serialize(Record.MutableCredentials());
            selector.Serialize(Record.MutableSelector());
            Record.SetLsn(lsn);
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
    };

    DECLARE_DDISK_EVENT(FlushPersistentBufferResult) {
        TEvDDiskFlushPersistentBufferResult() = default;

        TEvDDiskFlushPersistentBufferResult(NKikimrBlobStorage::TDDiskReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
        }
    };

    DECLARE_DDISK_EVENT(ListPersistentBuffer) {
        using TResult = TEvDDiskListPersistentBufferResult;

        TEvDDiskListPersistentBuffer() = default;

        TEvDDiskListPersistentBuffer(const TQueryCredentials& creds) {
            creds.Serialize(Record.MutableCredentials());
        }
    };

    DECLARE_DDISK_EVENT(ListPersistentBufferResult) {
        TEvDDiskListPersistentBufferResult() = default;

        TEvDDiskListPersistentBufferResult(NKikimrBlobStorage::TDDiskReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
        }
    };

    IActor *CreateDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters);

} // NKikimr::NDDisk
