#include "initialization.h"
#include "kqp_common.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/services/metadata/manager/abstract.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata {
namespace {

TString GetStoragePrefix() {
    TString path = NMetadata::NProvider::TServiceOperator::GetPath();
    Y_ENSURE(path, "Service operator path must not be empty");
    return CanonizePath("/" + AppData()->TenantName + "/" + path);
}

} // anonymous namespace

TString IClassBehaviour::GetStorageTableDirectory() const {
    return TFsPath(GetStorageTablePath()).Fix().Parent().GetPath();
}

TString IClassBehaviour::GetStorageTablePath() const {
    return GetStoragePrefix() + "/" + GetInternalStorageTablePath();
}

TString IClassBehaviour::GetStorageHistoryTablePath() const {
    const TString internalTablePath = GetInternalStorageHistoryTablePath();
    if (!internalTablePath) {
        return "";
    }

    return GetStoragePrefix() + "/" + internalTablePath;
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

TString IClassBehaviour::FormPathToResourceTable(const TString& resourceDatabasePath) const {
    return "/" + resourceDatabasePath + "/.metadata/" + GetInternalStorageTablePath();
}

std::shared_ptr<NModifications::IOptimizationManager> IClassBehaviour::ConstructOptimizationManager() const {
    return nullptr;
}

}
