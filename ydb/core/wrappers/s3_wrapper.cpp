#if defined(KIKIMR_DISABLE_S3_WRAPPER)
#error "s3 wrapper is disabled"
#endif

#include "s3_wrapper.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NWrappers {

namespace NExternalStorage {

class TStorageWrapper: public TActor<TStorageWrapper> {
    template <typename T>
    void Handle(T& ev) {
        StorageOperator->Execute(ev);
    }

public:
    explicit TStorageWrapper(IExternalStorageOperator::TPtr storageOperator)
        : TActor(&TThis::StateWork)
        , StorageOperator(storageOperator)
    {
        Y_ABORT_UNLESS(!!StorageOperator, "not initialized operator. incorrect config.");
    }

    virtual ~TStorageWrapper() = default;

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

            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    IExternalStorageOperator::TPtr StorageOperator;

}; // TS3Wrapper

} // NExternalStorage

IActor* CreateStorageWrapper(NExternalStorage::IExternalStorageOperator::TPtr storage) {
    return new NExternalStorage::TStorageWrapper(storage);
}

} // NKikimr::NWrappers
