#pragma once

#include "defs.h"

#include <library/cpp/monlib/service/pages/mon_page.h>

namespace NKikimr {

    NMonitoring::IMonPage *CreateMonVDiskStreamPage(const TString& path, TActorSystem *actorSystem);

} // NKikimr
