#pragma once
#include <ydb/core/base/events.h>
#include <util/generic/ptr.h>

namespace NKikimr::NWrappers::NExternalStorage {

class IRequestContext {
public:
    using TPtr = std::shared_ptr<IRequestContext>;
    virtual ~IRequestContext() = default;
};

#define EV_REQUEST_RESPONSE(name) \
        Ev##name##Request, \
        Ev##name##Response

    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_S3_WRAPPER),

        EV_REQUEST_RESPONSE(GetObject),
        EV_REQUEST_RESPONSE(HeadObject),
        EV_REQUEST_RESPONSE(PutObject),
        EV_REQUEST_RESPONSE(DeleteObject),
        EV_REQUEST_RESPONSE(DeleteObjects),
        EV_REQUEST_RESPONSE(CreateMultipartUpload),
        EV_REQUEST_RESPONSE(UploadPart),
        EV_REQUEST_RESPONSE(CompleteMultipartUpload),
        EV_REQUEST_RESPONSE(AbortMultipartUpload),
        EV_REQUEST_RESPONSE(ListObjects),
        EV_REQUEST_RESPONSE(CheckObjectExists),
        EV_REQUEST_RESPONSE(UploadPartCopy),
        EvEnd,
    };

#undef EV_REQUEST_RESPONSE

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_S3_WRAPPER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_S3_WRAPPER)");
}
