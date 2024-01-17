#pragma once
#include "defs.h"

#include <ydb/library/actors/core/actor.h>

#include <library/cpp/logger/file.h>

#include <util/generic/strbuf.h>

namespace NKikimr::NSQS {

inline TActorId MakeSqsServiceID(ui32 nodeId) {
    Y_ABORT_UNLESS(nodeId != 0);
    return TActorId(nodeId, TStringBuf("SQS_SERVICE"));
}

inline TActorId MakeSqsProxyServiceID(ui32 nodeId) {
    Y_ABORT_UNLESS(nodeId != 0);
    return TActorId(nodeId, TStringBuf("SQS_PROXY"));
}

inline TActorId MakeSqsAccessServiceID() {
    return TActorId(0, TStringBuf("SQS_ACCESS"));
}

inline TActorId MakeSqsFolderServiceID() {
    return TActorId(0, TStringBuf("SQS_FOLDER"));
}

inline TActorId MakeSqsMeteringServiceID() {
    return TActorId(0, TStringBuf("SQS_METER"));
}

IActor* CreateSqsService();
IActor* CreateSqsProxyService();
IActor* CreateSqsAccessService(const TString& address, const TString& pathToRootCA);
IActor* CreateSqsFolderService(const TString& address, const TString& pathToRootCA);
IActor* CreateMockSqsFolderService();
IActor* CreateSqsMeteringService();

} // namespace NKikimr::NSQS
