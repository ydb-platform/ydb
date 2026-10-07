#pragma once

#include <yt/yql/providers/yt/lib/config_clusters/config_clusters.h>

#include <yql/essentials/core/url_lister/interface/url_lister.h>


namespace NYql {

IUrlListerPtr MakeYtUrlLister(TConfigClusters::TPtr clusters = {});

}
