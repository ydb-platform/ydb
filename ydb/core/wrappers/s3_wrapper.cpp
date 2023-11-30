#if defined(KIKIMR_DISABLE_S3_WRAPPER)
#error "s3 wrapper is disabled"
#endif

#include "s3_wrapper.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/ResponseStream.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/curl/include/curl/curl.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/singleton.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>

namespace NKikimr::NWrappers {

namespace NExternalStorage {

class TS3Wrapper: public TActor<TS3Wrapper> {

    template <class T>
    void Handle(T& ev) {
        StorageOperator->Execute(ev);
    }

public:
    explicit TS3Wrapper(IExternalStorageOperator::TPtr storageOperator)
        : TActor(&TThis::StateWork)
        , StorageOperator(storageOperator)
    {
        Y_ABORT_UNLESS(!!StorageOperator, "not initialized operator. incorrect config.");
    }

    virtual ~TS3Wrapper() = default;

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvListObjectsRequest, Handle);
            hFunc(TEvGetObjectRequest, Handle);
            hFunc(TEvHeadObjectRequest, Handle);
            hFunc(TEvPutObjectRequest, Handle);
            hFunc(TEvDeleteObjectRequest, Handle);
            hFunc(TEvDeleteObjectsRequest, Handle);
            hFunc(TEvCreateMultipartUploadRequest, Handle);
            hFunc(TEvUploadPartRequest, Handle);
            hFunc(TEvCompleteMultipartUploadRequest, Handle);
            hFunc(TEvAbortMultipartUploadRequest, Handle);
            hFunc(TEvCheckObjectExistsRequest, Handle);
            hFunc(TEvUploadPartCopyRequest, Handle);

            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    IExternalStorageOperator::TPtr StorageOperator;

}; // TS3Wrapper

} // NExternalStorage

IActor* CreateS3Wrapper(NExternalStorage::IExternalStorageOperator::TPtr storage) {
    return new NExternalStorage::TS3Wrapper(storage);
}

} // NKikimr::NWrappers
