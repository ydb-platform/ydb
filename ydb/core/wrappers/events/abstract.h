#pragma once

#include <ydb/core/base/events.h>

#include <memory>

namespace NKikimr::NWrappers::NExternalStorage {

class IRequestContext {
public:
    using TPtr = std::shared_ptr<IRequestContext>;
    virtual ~IRequestContext() = default;
};

#define Y_FOR_EACH_S3_WRAPPER_OP(XX) \
    XX(GetObject) \
    XX(HeadObject) \
    XX(PutObject) \
    XX(DeleteObject) \
    XX(DeleteObjects) \
    XX(CreateMultipartUpload) \
    XX(UploadPart) \
    XX(CompleteMultipartUpload) \
    XX(AbortMultipartUpload) \
    XX(ListObjects) \
    XX(CheckObjectExists) \
    XX(UploadPartCopy)

#define EV_REQUEST_RESPONSE(name) Ev##name##Request, Ev##name##Response,

enum EEv {
    EvBegin = EventSpaceBegin(TKikimrEvents::ES_S3_WRAPPER),

    Y_FOR_EACH_S3_WRAPPER_OP(EV_REQUEST_RESPONSE)

    EvEnd,
};

#undef EV_REQUEST_RESPONSE

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_S3_WRAPPER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_S3_WRAPPER)");

}
