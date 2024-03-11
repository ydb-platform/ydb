#pragma once

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

namespace NKikimr::NReplication::NService {

IActor* CreateRemoteTopicReader(const TActorId& ydbProxy, const TEvYdbProxy::TTopicReaderSettings& opts);

}
