#pragma once

#include <yql/essentials/sql/v1/complete/name/cluster/discovery.h>

namespace NSQLComplete {

    IClusterDiscovery::TPtr MakeStaticClusterDiscovery(TVector<TString> instances);

} // namespace NSQLComplete
