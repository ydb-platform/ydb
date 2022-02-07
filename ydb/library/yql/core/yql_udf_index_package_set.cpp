#include "yql_udf_index_package_set.h"

#include <ydb/library/yql/utils/yql_panic.h>

using namespace NYql;

TUdfIndexPackageSet::TUdfIndexPackageSet() {
}

TUdfIndexPackageSet::TUdfIndexPackageSet(const TSet<TString>& knownPackages, const THashMap<TString, ui32>& packageDefaultVersions, const TMap<std::pair<TString, ui32>, TResourceInfo::TPtr>& resources)
    : KnownPackages_(knownPackages)
    , PackageDefaultVersions_(packageDefaultVersions)
    , Resources_(resources)
{
}

void TUdfIndexPackageSet::RegisterPackage(const TString& package) {
    KnownPackages_.insert(package);
}

bool TUdfIndexPackageSet::SetPackageDefaultVersion(const TString& package, ui32 version) {
    if (!KnownPackages_.contains(package)) {
        return false;
    }

    PackageDefaultVersions_[package] = version;
    return true;
}

void TUdfIndexPackageSet::RegisterResource(const TString& package, ui32 version, const TResourceInfo::TPtr& resource) {
    YQL_ENSURE(resource != nullptr);
    YQL_ENSURE(KnownPackages_.contains(package), "Unknown package " << package);
    Resources_.emplace(std::make_pair(package, version), resource);
}

bool TUdfIndexPackageSet::AddResourceTo(const TString& package, ui32 version, const TUdfIndex::TPtr& target) const {
    auto it = Resources_.find(std::make_pair(package, version));
    if (it == Resources_.end()) {
        return KnownPackages_.contains(package);
    }
    target->RegisterResource(it->second, TUdfIndex::EOverrideMode::ReplaceWithNew);
    return true;
}

void TUdfIndexPackageSet::AddResourcesTo(const TUdfIndex::TPtr& target) const {
    YQL_ENSURE(target != nullptr);
    for (auto& package : KnownPackages_) {
        auto vIt = PackageDefaultVersions_.find(package);
        const ui32 version = (vIt != PackageDefaultVersions_.end()) ? vIt->second : 0;
        auto it = Resources_.find(std::make_pair(package, version));
        if (it == Resources_.end()) {
            // todo: consider error?
            continue;
        }
        target->RegisterResource(it->second, TUdfIndex::EOverrideMode::ReplaceWithNew);
    }
}

TUdfIndexPackageSet::TPtr TUdfIndexPackageSet::Clone() const {
    return new TUdfIndexPackageSet(KnownPackages_, PackageDefaultVersions_, Resources_);
}
