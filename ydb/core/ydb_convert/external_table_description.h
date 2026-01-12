#pragma once

#include <util/generic/fwd.h>

namespace Ydb {
    namespace Table {
        class DescribeExternalTableResult;
    }
    enum StatusIds_StatusCode : int;
}

namespace NKikimrSchemeOp {
    class TExternalTableDescription;
    class TDirEntry;
}

namespace NKikimr {

bool FillExternalTableDescription(
    Ydb::Table::DescribeExternalTableResult& out,
    const NKikimrSchemeOp::TExternalTableDescription& inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry,
    Ydb::StatusIds_StatusCode& status,
    TString& error);

} // namespace NKikimr
