#pragma once

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

#include <memory>

namespace NKikimr::NReplication::NService {

enum class EAuthFailurePolicy {
    SourceRejectionOnly,
    StaticLoginInitialization,
};

IActor* CreateRemoteTopicReader(const TActorId& ydbProxy, const TEvYdbProxy::TTopicReaderSettings& opts,
    EAuthFailurePolicy authFailurePolicy = EAuthFailurePolicy::SourceRejectionOnly);

// A service-owned handle keeps the proxy usable through reader recreation and
// completion of a pending create request after the worker has stopped.
IActor* CreateRemoteTopicReader(std::shared_ptr<const TActorId> ydbProxy, const TEvYdbProxy::TTopicReaderSettings& opts,
    EAuthFailurePolicy authFailurePolicy = EAuthFailurePolicy::SourceRejectionOnly);

}
