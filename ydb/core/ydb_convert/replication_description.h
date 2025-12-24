#pragma once

#include <util/generic/fwd.h>

namespace Ydb {
    namespace Replication {
        class DescribeReplicationResult;
        class DescribeTransferResult;
    }
    enum StatusIds_StatusCode : int;
}

namespace NKikimrSchemeOp {
    class TReplicationDescription;
    class TDirEntry;
}

namespace NKikimrReplication {
    class TEvDescribeReplicationResult;
}

namespace NKikimr {

void FillReplicationDescription(
    Ydb::Replication::DescribeReplicationResult& out,
    const NKikimrReplication::TEvDescribeReplicationResult& inDesc);

void FillTransferDescription(
    Ydb::Replication::DescribeTransferResult& out,
    const NKikimrReplication::TEvDescribeReplicationResult& inDesc);

} // namespace NKikimr
