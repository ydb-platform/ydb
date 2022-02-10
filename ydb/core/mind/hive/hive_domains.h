#pragma once

#include "hive.h"

namespace NKikimr {
namespace NHive {

struct TTabletInfo;
struct TNodeInfo;

class TDomainsView {
public:
    ui32 CountNodes(const TSubDomainKey& domain) const {
        auto it = TotalCount.find(domain);
        if (it == TotalCount.end()) {
            return 0;
        }
        return it->second;
    }

    bool IsEmpty(const TSubDomainKey& domain) const {
        return CountNodes(domain) == 0;
    }

    void RegisterNode(const TNodeInfo& node);
    void DeregisterNode(const TNodeInfo& node);

    TString AsString() const {
        TStringBuilder result;
        result << '[';
        for (auto it = TotalCount.begin(); it != TotalCount.end(); ++it) {
            const auto &x = *it;
            if (it != TotalCount.begin()) {
                result << ", ";
            }
            result << x.first << '=' << x.second;
        }
        result << ']';
        return result;
    }

private:
    std::unordered_map<TSubDomainKey, ui32, THash<TSubDomainKey>> TotalCount;
};


} // NHive
} // NKikimr
