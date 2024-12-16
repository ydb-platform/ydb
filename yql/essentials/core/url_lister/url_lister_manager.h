#pragma once

#include <yql/essentials/core/url_lister/interface/url_lister.h>
#include <yql/essentials/core/url_lister/interface/url_lister_manager.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>


namespace NYql {

IUrlListerManagerPtr MakeUrlListerManager(
    TVector<IUrlListerPtr> urlListers
);

}
