#include "compatibility.h"

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <util/string/builder.h>

namespace NYdb::NDump {

TStatus CheckSysViewCompatibility(
    const Ydb::Table::DescribeSystemViewResult& dumpedProto,
    const Ydb::Table::DescribeSystemViewResult& actualProto)
{
    NYdb::NIssue::TIssues issues;
    if (dumpedProto.sys_view_id() != actualProto.sys_view_id()) {
        issues.AddIssue(NYdb::NIssue::TIssue(TStringBuilder()
            << "System view ID mismatch"
            << ": dumped = '" << dumpedProto.sys_view_id() << "'"
            << ", actual = '" << actualProto.sys_view_id() << "'"));
        return TStatus(EStatus::SCHEME_ERROR, std::move(issues));
    }

    return TStatus(EStatus::SUCCESS, std::move(issues));
}

} // namespace NYdb::NDump
