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

bool FillReplicationDescription(
    Ydb::Replication::DescribeReplicationResult& out,
    const NKikimrSchemeOp::TReplicationDescription& inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry,
    Ydb::StatusIds_StatusCode& status,
    TString& error);

void FillTransferDescription(
    Ydb::Replication::DescribeTransferResult& out,
    const NKikimrReplication::TEvDescribeReplicationResult& inDesc);

bool FillTransferDescription(
    Ydb::Replication::DescribeTransferResult& out,
    const NKikimrSchemeOp::TReplicationDescription& inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry,
    Ydb::StatusIds_StatusCode& status,
    TString& error);

} // namespace NKikimr
