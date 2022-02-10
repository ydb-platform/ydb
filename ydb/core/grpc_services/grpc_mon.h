#pragma once

#include <ydb/core/base/defs.h>

#include <util/generic/string.h>

namespace NKikimr {
namespace NGRpcService {

TActorId GrpcMonServiceId();
IActor* CreateGrpcMonService();

void ReportGrpcReqToMon(NActors::TActorSystem&, const TString& fromAddress);
void ReportGrpcReqToMon(NActors::TActorSystem&, const TString& fromAddress, const TString& buildInfo);

}}
