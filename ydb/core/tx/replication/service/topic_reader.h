#pragma once

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

namespace NKikimr::NReplication {

struct TTransferReadStats {
    ui64 TotalReadTimeMs = 0;
    ui64 TotalDecompressTimeMs = 0;
    ui64 ReadCpuMs = 0;
    ui64 DecompressCpuMs = 0;
};

namespace NService {

IActor* CreateRemoteTopicReader(const TActorId& ydbProxy, const TEvYdbProxy::TTopicReaderSettings& opts);

}
}
