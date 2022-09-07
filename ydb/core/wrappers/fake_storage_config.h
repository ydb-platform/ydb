#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "abstract.h"

#include <ydb/core/base/events.h>

#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr::NWrappers::NExternalStorage {

class TFakeExternalStorageConfig: public IExternalStorageConfig {
protected:
    virtual TString DoGetStorageId() const override;
    virtual IExternalStorageOperator::TPtr DoConstructStorageOperator() const override;
public:
};
} // NKikimr::NWrappers::NExternalStorage

#endif // KIKIMR_DISABLE_S3_OPS
