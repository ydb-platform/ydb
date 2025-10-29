#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimr::NReplication::NController {

IActor* CreateResourceIdResolver(const TActorId& parent, ui64 rid,
    const TString& endpoint, const TString& database, bool ssl, const TString& caCert, const TString& token);

}
