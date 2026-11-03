#pragma once

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

namespace NKikimr::NReplication::NService {

enum class EAuthFailurePolicy {
    SourceRejectionOnly,
    StaticLoginInitialization,
};

IActor* CreateRemoteTopicReader(const TActorId& ydbProxy, const TEvYdbProxy::TTopicReaderSettings& opts,
    EAuthFailurePolicy authFailurePolicy = EAuthFailurePolicy::SourceRejectionOnly);

}
