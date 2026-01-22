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

        TQueryCredentials() = default;

        TQueryCredentials(const NKikimrBlobStorage::TDDiskQueryCredentials& pb)
            : TabletId(pb.GetTabletId())
            , Generation(pb.GetGeneration())
            , DDiskInstanceGuid(pb.HasDDiskInstanceGuid() ? std::make_optional(pb.GetDDiskInstanceGuid()) : std::nullopt)
        {}

        void Serialize(NKikimrBlobStorage::TDDiskQueryCredentials *pb) const {
            pb->SetTabletId(TabletId);
            pb->SetGeneration(Generation);
            if (DDiskInstanceGuid) {
                pb->SetDDiskInstanceGuid(*DDiskInstanceGuid);
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

    DECLARE_DDISK_EVENT(Connect) {
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
    };

    DECLARE_DDISK_EVENT(DisconnectResult) {
    };

    DECLARE_DDISK_EVENT(Write) {
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
    };

    DECLARE_DDISK_EVENT(WritePersistentBufferResult) {
    };

    DECLARE_DDISK_EVENT(ReadPersistentBuffer) {
    };

    DECLARE_DDISK_EVENT(ReadPersistentBufferResult) {
    };

    DECLARE_DDISK_EVENT(FlushPersistentBuffer) {
    };

    DECLARE_DDISK_EVENT(FlushPersistentBufferResult) {
    };

    DECLARE_DDISK_EVENT(ListPersistentBuffer) {
    };

    DECLARE_DDISK_EVENT(ListPersistentBufferResult) {
    };

    IActor *CreateDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters);

} // NKikimr::NDDisk
