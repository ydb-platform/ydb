#include "error_helpers.h"

#include <library/cpp/yson/writer.h>

#include <ydb/library/yql/public/issue/yql_issue_id.h>

#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

const int IssueToErrorCodesShift = 30000;

////////////////////////////////////////////////////////////////////////////////

TString MessageToYtErrorYson(const TString& message)
{
    TStringStream yson;
    ::NYson::TYsonWriter writer(&yson);

    writer.OnBeginMap();
    writer.OnKeyedItem("code");
    writer.OnInt64Scalar(1); // Generic error
    writer.OnKeyedItem("message");
    writer.OnStringScalar(message);
    writer.OnKeyedItem("attributes");
    writer.OnBeginMap();
    writer.OnEndMap();

    writer.OnEndMap();

    return yson.Str();
}

TString IssuesToYtErrorYson(const NYql::TIssues& issues)
{
    TStringStream yson;
    ::NYson::TYsonWriter writer(&yson);

    auto serializePosition = [&] (const NYql::TPosition& position) {
        writer.OnBeginMap();
        {
            writer.OnKeyedItem("column");
            writer.OnInt64Scalar(position.Column);

            writer.OnKeyedItem("row");
            writer.OnInt64Scalar(position.Row - 1); // First line is for required pragmas.

            if (!position.File.empty()) {
                writer.OnKeyedItem("file");
                writer.OnStringScalar(position.File);
            }
        }
        writer.OnEndMap();
    };

    auto fn = [&] (const NYql::TIssue& issue, ui16 /*level*/) {
        writer.OnListItem();
        writer.OnBeginMap();
        {
            writer.OnKeyedItem("code");

            NYql::TIssueCode code = IssueToErrorCodesShift + issue.GetCode();
            writer.OnInt64Scalar(code);

            writer.OnKeyedItem("message");
            writer.OnStringScalar(issue.GetMessage());

            writer.OnKeyedItem("attributes");
            writer.OnBeginMap();
            {
                if (issue.Range().Position) {
                    writer.OnKeyedItem("start_position");
                    serializePosition(issue.Range().Position);
                }

                if (issue.Range().EndPosition) {
                    writer.OnKeyedItem("end_position");
                    serializePosition(issue.Range().EndPosition);
                }

                writer.OnKeyedItem("yql_status");
                writer.OnStringScalar(NYql::IssueCodeToString<NYql::TIssuesIds>(issue.GetCode()));

                writer.OnKeyedItem("severity");
                writer.OnStringScalar(SeverityToString(issue.GetSeverity()));
            }
            writer.OnEndMap();

            writer.OnKeyedItem("inner_errors");
            writer.OnBeginList();
        }
    };

    auto afterChildrenFn = [&] (const NYql::TIssue& /*issue*/, ui16 /*level*/) {
        writer.OnEndList();
        writer.OnEndMap();
    };

    writer.OnBeginMap();
    writer.OnKeyedItem("message");
    writer.OnStringScalar("There are some issues");
    writer.OnKeyedItem("code");
    writer.OnInt64Scalar(1);
    writer.OnKeyedItem("inner_errors");
    writer.OnBeginList();
    for (const auto& issue : issues) {
        WalkThroughIssues(issue, /*leafOnly*/ false, fn, afterChildrenFn);
    }
    writer.OnEndList();
    writer.OnKeyedItem("attributes");
    writer.OnBeginMap();
    writer.OnEndMap();
    writer.OnEndMap();
    return yson.Str();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPLugin
