#pragma once

#include <library/cpp/actors/interconnect/interconnect_common.h>

extern TMaybe<NActors::TInterconnectProxyCommon::TVersionInfo> VERSION;

void CheckVersionTag();
TString GetBranchName(TString url);
