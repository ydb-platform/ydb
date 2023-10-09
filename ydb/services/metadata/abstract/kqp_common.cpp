#include "initialization.h"
#include "kqp_common.h"
#include <ydb/core/base/appdata.h>
#include <ydb/services/metadata/manager/abstract.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata {

TString IClassBehaviour::GetStorageTableDirectory() const {
    return TFsPath(GetStorageTablePath()).Fix().Parent().GetPath();
}

TString IClassBehaviour::GetStorageTablePath() const {
    return "/" + AppData()->TenantName + "/" + NMetadata::NProvider::TServiceOperator::GetPath() + "/" + GetInternalStorageTablePath();
}

TString IClassBehaviour::GetStorageHistoryTablePath() const {
    const TString internalTablePath = GetInternalStorageHistoryTablePath();
    if (!internalTablePath) {
        return "";
    }
    return "/" + AppData()->TenantName + "/" + NMetadata::NProvider::TServiceOperator::GetPath() + "/" + internalTablePath;
}

NInitializer::IInitializationBehaviour::TPtr IClassBehaviour::GetInitializer() const {
    if (!Initializer) {
        Initializer = ConstructInitializer();
    }
    Y_ABORT_UNLESS(Initializer);
    return Initializer;
}

TString IClassBehaviour::GetInternalStorageHistoryTablePath() const {
    return GetInternalStorageTablePath() + "_history";
}

}
