#pragma once

#include <ydb/core/tx/replication/checksum/checksum.h>

namespace NKikimr::NReplication::NController {

IActor* CreateChecksumSource(const TActorId& parent, const TActorId& proxy,
    Ydb::Replication::StreamChecksumTablesInternalRequest request, TInstant deadline);

}
