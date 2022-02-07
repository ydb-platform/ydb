#pragma once

#include "hive.h"

namespace NKikimr {
namespace NHive {

struct TDomainInfo {
    TString Path;
    TTabletId HiveId = 0;
};

}
}
