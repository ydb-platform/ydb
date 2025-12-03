#pragma once

#include <util/generic/fwd.h>

namespace Ydb {
    namespace Replication {
        class DescribeReplicationResult;
    }
    class StatusIds;
    enum StatusIds_StatusCode : int;
}

namespace NKikimrSchemeOp {
    class TReplicationDescription;
    class TDirEntry;
}

namespace NYql {
    class TIssue;
}

namespace NKikimr {

bool FillReplicationDescription(
    Ydb::Replication::DescribeReplicationResult& out,
    const NKikimrSchemeOp::TReplicationDescription inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry,
    Ydb::StatusIds_StatusCode& status,
    TString& error);

} // namespace NKikimr
