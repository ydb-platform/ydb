#pragma once

#include "protos.h"
#include <ydb/core/protos/blobstorage.pb.h>
#include <stdexcept>

#include <stdexcept>

namespace NKikimrCapnProtoUtil {

    inline NKikimrCapnProto::EVDiskQueueId convertToCapnProto(NKikimrBlobStorage::EVDiskQueueId value) {
        switch (value) {
            case NKikimrBlobStorage::EVDiskQueueId::Unknown:
                return NKikimrCapnProto::EVDiskQueueId::Unknown;
            case NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob:
                return NKikimrCapnProto::EVDiskQueueId::PutAsyncBlob;
            case NKikimrBlobStorage::EVDiskQueueId::PutUserData:
                return NKikimrCapnProto::EVDiskQueueId::PutUserData;
            case NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead:
                return NKikimrCapnProto::EVDiskQueueId::GetAsyncRead;
            case NKikimrBlobStorage::EVDiskQueueId::GetFastRead:
                return NKikimrCapnProto::EVDiskQueueId::GetFastRead;
            case NKikimrBlobStorage::EVDiskQueueId::GetDiscover:
                return NKikimrCapnProto::EVDiskQueueId::GetDiscover;
            case NKikimrBlobStorage::EVDiskQueueId::GetLowRead:
                return NKikimrCapnProto::EVDiskQueueId::GetLowRead;
            case NKikimrBlobStorage::EVDiskQueueId::Begin:
                return NKikimrCapnProto::EVDiskQueueId::Begin;
            case NKikimrBlobStorage::EVDiskQueueId::End:
                return NKikimrCapnProto::EVDiskQueueId::End;
            default:
                throw std::runtime_error("Invalid Protobuf EVDiskQueueId value");
        }
    }

    inline NKikimrCapnProto::EVDiskQueueId convertToCapnProto(NKikimrCapnProto::EVDiskQueueId value) {
        return value;
    }

    inline NKikimrBlobStorage::EVDiskQueueId convertToProtobuf(NKikimrCapnProto::EVDiskQueueId value) {
        switch (value) {
            case NKikimrCapnProto::EVDiskQueueId::Unknown:
                return NKikimrBlobStorage::Unknown;
            case NKikimrCapnProto::EVDiskQueueId::PutTabletLog:
                return NKikimrBlobStorage::PutTabletLog;
            case NKikimrCapnProto::EVDiskQueueId::PutAsyncBlob:
                return NKikimrBlobStorage::PutAsyncBlob;
            case NKikimrCapnProto::EVDiskQueueId::PutUserData:
                return NKikimrBlobStorage::PutUserData;
            case NKikimrCapnProto::EVDiskQueueId::GetAsyncRead:
                return NKikimrBlobStorage::GetAsyncRead;
            case NKikimrCapnProto::EVDiskQueueId::GetFastRead:
                return NKikimrBlobStorage::GetFastRead;
            case NKikimrCapnProto::EVDiskQueueId::GetDiscover:
                return NKikimrBlobStorage::GetDiscover;
            case NKikimrCapnProto::EVDiskQueueId::GetLowRead:
                return NKikimrBlobStorage::GetLowRead;
            default:
                throw std::runtime_error("Invalid Protobuf EVDiskQueueId value");
        }
    }

    inline NKikimrBlobStorage::EVDiskQueueId convertToProtobuf(NKikimrBlobStorage::EVDiskQueueId value) {
        return value;
    }

    inline NKikimrCapnProto::EStatus convertToCapnProto(const NKikimrBlobStorage::TWindowFeedback_EStatus &value) {
        switch (value) {
            case NKikimrBlobStorage::TWindowFeedback_EStatus_Unknown:
                return NKikimrCapnProto::EStatus::Unknown;
            case NKikimrBlobStorage::TWindowFeedback_EStatus_Success:
                return NKikimrCapnProto::EStatus::Success;
            case NKikimrBlobStorage::TWindowFeedback_EStatus_WindowUpdate:
                return NKikimrCapnProto::EStatus::WindowUpdate;
            case NKikimrBlobStorage::TWindowFeedback_EStatus_Processed:
                return NKikimrCapnProto::EStatus::Processed;
            case NKikimrBlobStorage::TWindowFeedback_EStatus_IncorrectMsgId:
                return NKikimrCapnProto::EStatus::IncorrectMsgId;
            case NKikimrBlobStorage::TWindowFeedback_EStatus_HighWatermarkOverflow:
                return NKikimrCapnProto::EStatus::HighWatermarkOverflow;
            default:
                throw std::runtime_error("Invalid Protobuf EStatus value");
        }
    }

    inline NKikimrCapnProto::EStatus convertToCapnProto(const NKikimrCapnProto::EStatus &value) {
        return value;
    }

    inline NKikimrBlobStorage::TWindowFeedback_EStatus convertToProtobuf(const NKikimrCapnProto::EStatus &value) {
        switch (value) {
            case NKikimrCapnProto::EStatus::Unknown:
                return NKikimrBlobStorage::TWindowFeedback_EStatus_Unknown;
            case NKikimrCapnProto::EStatus::Success:
                return NKikimrBlobStorage::TWindowFeedback_EStatus_Success;
            case NKikimrCapnProto::EStatus::WindowUpdate:
                return NKikimrBlobStorage::TWindowFeedback_EStatus_WindowUpdate;
            case NKikimrCapnProto::EStatus::Processed:
                return NKikimrBlobStorage::TWindowFeedback_EStatus_Processed;
            case NKikimrCapnProto::EStatus::IncorrectMsgId:
                return NKikimrBlobStorage::TWindowFeedback_EStatus_IncorrectMsgId;
            case NKikimrCapnProto::EStatus::HighWatermarkOverflow:
                return NKikimrBlobStorage::TWindowFeedback_EStatus_HighWatermarkOverflow;
            default:
                throw std::runtime_error("Invalid Cap'n Proto EStatus value");
        }
    }

    inline NKikimrBlobStorage::TWindowFeedback_EStatus convertToProtobuf(const NKikimrBlobStorage::TWindowFeedback_EStatus &value) {
        return value;
    }

    inline NKikimrCapnProto::EGetHandleClass convertToCapnProto(const NKikimrBlobStorage::EGetHandleClass &value) {
        switch (value) {
            case NKikimrBlobStorage::AsyncRead:
                return NKikimrCapnProto::EGetHandleClass::AsyncRead;
            case NKikimrBlobStorage::FastRead:
                return NKikimrCapnProto::EGetHandleClass::FastRead;
            case NKikimrBlobStorage::Discover:
                return NKikimrCapnProto::EGetHandleClass::Discover;
            case NKikimrBlobStorage::LowRead:
                return NKikimrCapnProto::EGetHandleClass::LowRead;
            default:
                throw std::runtime_error("Invalid Protobuf EGetHandleClass value");
        }
    }

    inline NKikimrCapnProto::EGetHandleClass convertToCapnProto(const NKikimrCapnProto::EGetHandleClass &value) {
        return value;
    }

    inline NKikimrBlobStorage::EGetHandleClass convertToProtobuf(const NKikimrCapnProto::EGetHandleClass &value) {
        switch (value) {
            case NKikimrCapnProto::EGetHandleClass::AsyncRead:
                return NKikimrBlobStorage::AsyncRead;
            case NKikimrCapnProto::EGetHandleClass::FastRead:
                return NKikimrBlobStorage::FastRead;
            case NKikimrCapnProto::EGetHandleClass::Discover:
                return NKikimrBlobStorage::Discover;
            case NKikimrCapnProto::EGetHandleClass::LowRead:
                return NKikimrBlobStorage::LowRead;
            default:
                throw std::runtime_error("Invalid Cap'n Proto EGetHandleClass value");
        }
    }

    inline NKikimrBlobStorage::EGetHandleClass convertToProtobuf(const NKikimrBlobStorage::EGetHandleClass &value) {
        return value;
    }

    inline NKikimrCapnProto::EVDiskInternalQueueId convertToCapnProto(const NKikimrBlobStorage::EVDiskInternalQueueId &value) {
        switch (value) {
            case NKikimrBlobStorage::IntUnknown:
                return NKikimrCapnProto::EVDiskInternalQueueId::IntUnknown;
            case NKikimrBlobStorage::IntBegin:
                return NKikimrCapnProto::EVDiskInternalQueueId::IntBegin;
            case NKikimrBlobStorage::IntGetFast:
                return NKikimrCapnProto::EVDiskInternalQueueId::IntGetFast;
            case NKikimrBlobStorage::IntPutLog:
                return NKikimrCapnProto::EVDiskInternalQueueId::IntPutLog;
            case NKikimrBlobStorage::IntPutHugeForeground:
                return NKikimrCapnProto::EVDiskInternalQueueId::IntPutHugeForeground;
            case NKikimrBlobStorage::IntPutHugeBackground:
                return NKikimrCapnProto::EVDiskInternalQueueId::IntPutHugeBackground;
            case NKikimrBlobStorage::IntGetDiscover:
                return NKikimrCapnProto::EVDiskInternalQueueId::IntGetDiscover;
            case NKikimrBlobStorage::IntLowRead:
                return NKikimrCapnProto::EVDiskInternalQueueId::IntLowRead;
            case NKikimrBlobStorage::IntEnd:
                return NKikimrCapnProto::EVDiskInternalQueueId::IntEnd;
            default:
                throw std::runtime_error("Invalid Protobuf EVDiskInternalQueueId value");
        }
    }

    inline NKikimrCapnProto::EVDiskInternalQueueId convertToCapnProto(const NKikimrCapnProto::EVDiskInternalQueueId &value) {
        return value;
    }

    inline NKikimrBlobStorage::EVDiskInternalQueueId convertToProtobuf(const NKikimrCapnProto::EVDiskInternalQueueId &value) {
        switch (value) {
            case NKikimrCapnProto::EVDiskInternalQueueId::IntUnknown:
                return NKikimrBlobStorage::IntUnknown;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntBegin:
                return NKikimrBlobStorage::IntBegin;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntGetAsync:
                return NKikimrBlobStorage::IntGetAsync;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntGetFast:
                return NKikimrBlobStorage::IntGetFast;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntPutLog:
                return NKikimrBlobStorage::IntPutLog;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntPutHugeForeground:
                return NKikimrBlobStorage::IntPutHugeForeground;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntPutHugeBackground:
                return NKikimrBlobStorage::IntPutHugeBackground;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntGetDiscover:
                return NKikimrBlobStorage::IntGetDiscover;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntLowRead:
                return NKikimrBlobStorage::IntLowRead;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntEnd:
                return NKikimrBlobStorage::EVDiskInternalQueueId::IntEnd;
            default:
                throw std::runtime_error("Invalid Protobuf EVDiskInternalQueueId value");
        }
    }

    inline NKikimrBlobStorage::EVDiskInternalQueueId convertToProtobuf(const NKikimrBlobStorage::EVDiskInternalQueueId &value) {
        return value;
    }
}