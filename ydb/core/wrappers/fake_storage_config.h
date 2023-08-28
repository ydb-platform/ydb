#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "abstract.h"

#include <ydb/core/base/events.h>

#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr::NWrappers::NExternalStorage {

class TFakeExternalStorageConfig: public IExternalStorageConfig {
private:
    const TString Bucket;
    const TString SecretKey;
protected:
    virtual TString DoGetStorageId() const override;
    virtual IExternalStorageOperator::TPtr DoConstructStorageOperator(bool verbose) const override;
public:
    TFakeExternalStorageConfig(const TString& bucket, const TString& secretKey)
        : Bucket(bucket)
        , SecretKey(secretKey)
    {
    }
};
} // NKikimr::NWrappers::NExternalStorage

#endif // KIKIMR_DISABLE_S3_OPS
