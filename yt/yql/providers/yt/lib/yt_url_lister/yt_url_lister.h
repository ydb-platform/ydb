#pragma once

#include <yql/essentials/core/url_lister/interface/url_lister.h>


namespace NYql {

IUrlListerPtr MakeYtUrlLister();

}
