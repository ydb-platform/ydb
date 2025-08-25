#pragma once

#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NSQLTranslation {
class TClusterMapping {
public:
    explicit TClusterMapping(const THashMap<TString, TString>& mapping);
    TMaybe<TString> GetClusterProvider(const TString& cluster, TString& normalizedClusterName) const;

private:
    THashMap<TString, TString> CaseSensitiveClusters_;
    THashMap<TString, TString> CaseInsensitiveClusters_;
};
}
