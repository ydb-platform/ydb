#pragma once

namespace Ydb {
    namespace Table {
        class DescribeExternalDataSourceResult;
    }
    enum StatusIds_StatusCode : int;
}

namespace NKikimrSchemeOp {
    class TExternalDataSourceDescription;
    class TDirEntry;
}

namespace NKikimr {

void FillExternalDataSourceDescription(
    Ydb::Table::DescribeExternalDataSourceResult& out,
    const NKikimrSchemeOp::TExternalDataSourceDescription& inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry);

} // namespace NKikimr
