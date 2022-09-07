#include "abstract.h"
#include "fake_storage_config.h"
#include "s3_storage_config.h"

#include <util/system/rwlock.h>

namespace NKikimr::NWrappers::NExternalStorage {

IExternalStorageOperator::TPtr IExternalStorageConfig::ConstructStorageOperator() const {
    return DoConstructStorageOperator();
}

IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrSchemeOp::TS3Settings& settings) {
    if (settings.GetEndpoint() == "fake") {
        return std::make_shared<TFakeExternalStorageConfig>();
    } else {
        return std::make_shared <TS3ExternalStorageConfig>(settings);
    }
}
}
