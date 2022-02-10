#pragma once

#include "defs.h"

namespace NKikimr {
namespace NKesus {

TActorId MakeKesusProxyServiceId();

IActor* CreateKesusProxyService();

}
}
