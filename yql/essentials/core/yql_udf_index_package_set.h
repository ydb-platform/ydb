#pragma once

#include "yql_udf_index.h"

namespace NYql {

class TUdfIndexPackageSet : public TThrRefBase {
public:
    typedef TIntrusivePtr<TUdfIndexPackageSet> TPtr;

private:
    TUdfIndexPackageSet(const TSet<TString>& knownPackages, const THashMap<TString, ui32>& packageDefaultVersions, const TMap<std::pair<TString, ui32>, TResourceInfo::TPtr>& resources);

public:
    TUdfIndexPackageSet();
    void RegisterPackage(const TString& package);
    bool SetPackageDefaultVersion(const TString& package, ui32 version);
    void RegisterResource(const TString& package, ui32 version, const TResourceInfo::TPtr& resource);
    bool AddResourceTo(const TString& package, ui32 version, const TUdfIndex::TPtr& target) const;
    void AddResourcesTo(const TUdfIndex::TPtr& target) const;
    TPtr Clone() const;

private:
    TSet<TString> KnownPackages_;
    THashMap<TString, ui32> PackageDefaultVersions_;
    TMap<std::pair<TString, ui32>, TResourceInfo::TPtr> Resources_;
};

}
