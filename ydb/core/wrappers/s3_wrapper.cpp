#if defined(KIKIMR_DISABLE_S3_WRAPPER)
#error "s3 wrapper is disabled"
#endif

#include "s3_wrapper.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/ResponseStream.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/curl/include/curl/curl.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/generic/singleton.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>

namespace NKikimr::NWrappers {

namespace NExternalStorage {

class TS3Wrapper: public TActor<TS3Wrapper> {
    void Handle(TEvGetObjectRequest::TPtr& ev) {
        CSOperator->Execute(ev);
    }

    void Handle(TEvCheckObjectExistsRequest::TPtr& ev) {
        CSOperator->Execute(ev);
    }

    void Handle(TEvHeadObjectRequest::TPtr& ev) {
        CSOperator->Execute(ev);
    }

    void Handle(TEvPutObjectRequest::TPtr& ev) {
        CSOperator->Execute(ev);
    }

    void Handle(TEvDeleteObjectRequest::TPtr& ev) {
        CSOperator->Execute(ev);
    }

    void Handle(TEvCreateMultipartUploadRequest::TPtr& ev) {
        CSOperator->Execute(ev);
    }

    void Handle(TEvUploadPartRequest::TPtr& ev) {
        CSOperator->Execute(ev);
    }

    void Handle(TEvCompleteMultipartUploadRequest::TPtr& ev) {
        CSOperator->Execute(ev);
    }

    void Handle(TEvAbortMultipartUploadRequest::TPtr& ev) {
        CSOperator->Execute(ev);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::S3_WRAPPER_ACTOR;
    }

    explicit TS3Wrapper(IExternalStorageOperator::TPtr csOperator)
        : TActor(&TThis::StateWork)
        , CSOperator(csOperator) {
    }

    virtual ~TS3Wrapper() = default;

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvGetObjectRequest, Handle);
            hFunc(TEvHeadObjectRequest, Handle);
            hFunc(TEvPutObjectRequest, Handle);
            hFunc(TEvDeleteObjectRequest, Handle);
            hFunc(TEvCreateMultipartUploadRequest, Handle);
            hFunc(TEvUploadPartRequest, Handle);
            hFunc(TEvCompleteMultipartUploadRequest, Handle);
            hFunc(TEvAbortMultipartUploadRequest, Handle);
            hFunc(TEvCheckObjectExistsRequest, Handle);


            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    IExternalStorageOperator::TPtr CSOperator;

}; // TS3Wrapper

} // NExternalStorage

IActor* CreateS3Wrapper(NExternalStorage::IExternalStorageOperator::TPtr storage) {
    return new NExternalStorage::TS3Wrapper(storage);
}

} // NKikimr::NWrappers
