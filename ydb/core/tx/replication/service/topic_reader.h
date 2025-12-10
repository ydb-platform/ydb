#pragma once

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

namespace NKikimr::NReplication {

struct TTransferReadStats {
    TDuration ReadTime = TDuration::Zero();
    TDuration DecompressCpu = TDuration::Zero();
    i64 Partition = -1;
    ui64 Offset = 0;
    ui64 Messages = 0;
    ui64 Bytes = 0;
};

namespace NService {

IActor* CreateRemoteTopicReader(const TActorId& ydbProxy, const TEvYdbProxy::TTopicReaderSettings& opts);

}
}
