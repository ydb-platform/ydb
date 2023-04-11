#include "protos.h"

#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimrCapnProto {
    NKikimrBlobStorage::EGetHandleClass ConvertEGetHandleClass(NKikimrCapnProto::EGetHandleClass capnProtoGetHandleClass) {
        switch (capnProtoGetHandleClass) {
            case NKikimrCapnProto::EGetHandleClass::AsyncRead: return NKikimrBlobStorage::EGetHandleClass::AsyncRead;
            case NKikimrCapnProto::EGetHandleClass::FastRead: return NKikimrBlobStorage::EGetHandleClass::FastRead;
            case NKikimrCapnProto::EGetHandleClass::Discover: return NKikimrBlobStorage::EGetHandleClass::Discover;
            case NKikimrCapnProto::EGetHandleClass::LowRead: return NKikimrBlobStorage::EGetHandleClass::LowRead;
            default: return static_cast<NKikimrBlobStorage::EGetHandleClass>(-1); // Return an invalid value for unrecognized cases
        }
    }

    NKikimrBlobStorage::TWindowFeedback_EStatus ConvertEStatus(NKikimrCapnProto::EStatus capnProtoEStatus) {
        switch (capnProtoEStatus) {
            case NKikimrCapnProto::EStatus::Unknown: return NKikimrBlobStorage::TWindowFeedback_EStatus_Unknown;
            case NKikimrCapnProto::EStatus::Success: return NKikimrBlobStorage::TWindowFeedback_EStatus_Success;
            case NKikimrCapnProto::EStatus::WindowUpdate: return NKikimrBlobStorage::TWindowFeedback_EStatus_WindowUpdate;
            case NKikimrCapnProto::EStatus::Processed: return NKikimrBlobStorage::TWindowFeedback_EStatus_Processed;
            case NKikimrCapnProto::EStatus::IncorrectMsgId: return NKikimrBlobStorage::TWindowFeedback_EStatus_IncorrectMsgId;
            case NKikimrCapnProto::EStatus::HighWatermarkOverflow: return NKikimrBlobStorage::TWindowFeedback_EStatus_HighWatermarkOverflow;
            default: return NKikimrBlobStorage::TWindowFeedback_EStatus_Unknown;
        }
    }

    NKikimrBlobStorage::EVDiskQueueId ConvertEVDiskQueueId(NKikimrCapnProto::EVDiskQueueId capnProtoQueueId) {
        switch (capnProtoQueueId) {
            case NKikimrCapnProto::EVDiskQueueId::Unknown: return NKikimrBlobStorage::EVDiskQueueId::Unknown;
            case NKikimrCapnProto::EVDiskQueueId::PutTabletLog: return NKikimrBlobStorage::EVDiskQueueId::PutTabletLog;
            case NKikimrCapnProto::EVDiskQueueId::PutAsyncBlob: return NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob;
            case NKikimrCapnProto::EVDiskQueueId::PutUserData: return NKikimrBlobStorage::EVDiskQueueId::PutUserData;
            case NKikimrCapnProto::EVDiskQueueId::GetAsyncRead: return NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead;
            case NKikimrCapnProto::EVDiskQueueId::GetFastRead: return NKikimrBlobStorage::EVDiskQueueId::GetFastRead;
            case NKikimrCapnProto::EVDiskQueueId::GetDiscover: return NKikimrBlobStorage::EVDiskQueueId::GetDiscover;
            case NKikimrCapnProto::EVDiskQueueId::GetLowRead: return NKikimrBlobStorage::EVDiskQueueId::GetLowRead;
            case NKikimrCapnProto::EVDiskQueueId::Begin: return NKikimrBlobStorage::EVDiskQueueId::Begin;
            case NKikimrCapnProto::EVDiskQueueId::End: return NKikimrBlobStorage::EVDiskQueueId::End;
            default: return NKikimrBlobStorage::EVDiskQueueId::Unknown;
        }
    }

    NKikimrBlobStorage::EVDiskInternalQueueId ConvertEVDiskInternalQueueId(NKikimrCapnProto::EVDiskInternalQueueId capnProtoInternalQueueId) {
        switch (capnProtoInternalQueueId) {
            case NKikimrCapnProto::EVDiskInternalQueueId::IntUnknown: return NKikimrBlobStorage::EVDiskInternalQueueId::IntUnknown;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntBegin: return NKikimrBlobStorage::EVDiskInternalQueueId::IntBegin;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntGetAsync: return NKikimrBlobStorage::EVDiskInternalQueueId::IntGetAsync;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntGetFast: return NKikimrBlobStorage::EVDiskInternalQueueId::IntGetFast;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntPutLog: return NKikimrBlobStorage::EVDiskInternalQueueId::IntPutLog;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntPutHugeForeground: return NKikimrBlobStorage::EVDiskInternalQueueId::IntPutHugeForeground;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntPutHugeBackground: return NKikimrBlobStorage::EVDiskInternalQueueId::IntPutHugeBackground;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntGetDiscover: return NKikimrBlobStorage::EVDiskInternalQueueId::IntGetDiscover;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntLowRead: return NKikimrBlobStorage::EVDiskInternalQueueId::IntLowRead;
            case NKikimrCapnProto::EVDiskInternalQueueId::IntEnd: return NKikimrBlobStorage::EVDiskInternalQueueId::IntEnd;
            default: return NKikimrBlobStorage::EVDiskInternalQueueId::IntUnknown;
        }
    }
}
