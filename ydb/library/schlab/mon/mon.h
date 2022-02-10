#pragma once

#include <library/cpp/monlib/service/monservice.h>

namespace NKikimr {

void CreateSchLabCommonPages(NMonitoring::TMonService2* mon);
void CreateSchArmPages(NMonitoring::TMonService2* mon, const TString& path, const TString& schVizUrl);
void CreateSchVizPages(NMonitoring::TMonService2* mon, const TString& path, const TString& dataUrl);

} // namespace NKikimr
