#pragma once

#include <util/generic/maybe.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/ptr.h>

namespace NYql {

class TYtClusterConfig;
class TYtGatewayConfig;

class TConfigClusters: public TThrRefBase {
private:
    struct TClusterInfo {
        TString RealName;
        TString YtName;
        TMaybe<TString> Token;
    };
public:
    using TPtr = TIntrusivePtr<TConfigClusters>;

    explicit TConfigClusters(const TYtGatewayConfig& config);

    void AddCluster(const TYtClusterConfig& cluster, bool checkDuplicate);

    const TString& GetServer(const TString& name) const;
    TString TryGetServer(const TString& name) const;
    const TString& GetYtName(const TString& name) const;
    TString GetNameByYtName(const TString& ytName) const;
    TMaybe<TString> GetAuth(const TString& name) const;
    void GetAllClusters(TVector<TString>& names) const;
    const TString& GetDefaultClusterName() const;

    static TString GetDefaultYtServer(const TYtGatewayConfig& config);

private:
    THashMap<TString, TClusterInfo> Clusters_;
    // ytName.to_lower() -> Name
    THashMap<TString, TString> YtName2Name_;
    TString DefaultClusterName_;
};

} // NYql
