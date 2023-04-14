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

    NKikimrCapnProto::EGetHandleClass ConvertEGetHandleClassToCapnProto(NKikimrBlobStorage::EGetHandleClass cls) {
        switch (cls) {
            case NKikimrBlobStorage::EGetHandleClass::AsyncRead: return NKikimrCapnProto::EGetHandleClass::AsyncRead;
            case NKikimrBlobStorage::EGetHandleClass::FastRead: return NKikimrCapnProto::EGetHandleClass::FastRead;
            case NKikimrBlobStorage::EGetHandleClass::Discover: return NKikimrCapnProto::EGetHandleClass::Discover;
            case NKikimrBlobStorage::EGetHandleClass::LowRead: return NKikimrCapnProto::EGetHandleClass::LowRead;
            default: return static_cast<NKikimrCapnProto::EGetHandleClass>(-1); // Return an invalid value for unrecognized cases
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

//    NKikimrCapnProto::EStatus ConvertEStatusToCapnProto(NKikimrBlobStorage::TWindowFeedback_EStatus cls) {
//        switch (cls) {
//            case NKikimrBlobStorage::TWindowFeedback_EStatus_Unknown: return NKikimrCapnProto::EStatus::Unknown;
//            case NKikimrBlobStorage::TWindowFeedback_EStatus_Success: return NKikimrCapnProto::EStatus::Success;
//            case NKikimrBlobStorage::TWindowFeedback_EStatus_WindowUpdate: return NKikimrCapnProto::EStatus::WindowUpdate;
//            case NKikimrBlobStorage::TWindowFeedback_EStatus_Processed: return NKikimrCapnProto::EStatus::Processed;
//            case NKikimrBlobStorage::TWindowFeedback_EStatus_IncorrectMsgId: return NKikimrCapnProto::EStatus::IncorrectMsgId;
//            case NKikimrBlobStorage::TWindowFeedback_EStatus_HighWatermarkOverflow: return NKikimrCapnProto::EStatus::HighWatermarkOverflow;
//            default: return NKikimrCapnProto::EStatus::Unknown;
//        }
//    }

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

    NKikimrBlobStorage::EVDiskQueueId ConvertEVDiskQueueId(NKikimrBlobStorage::EVDiskQueueId capnProtoQueueId) {
        return capnProtoQueueId;
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
