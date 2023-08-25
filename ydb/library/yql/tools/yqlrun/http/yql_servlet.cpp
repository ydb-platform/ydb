#include "yql_servlet.h"

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <util/string/builder.h>


namespace NYql {
namespace NHttp {

void TYqlAction::WriteStatus(bool success, const TIssues& errors) const {
    Writer.Write("succeeded", success);
    static const TVector<TString> issueQueue = {"errors", "warnings", "infos"};
    static const THashMap<NYql::TSeverityIds::ESeverityId, TStringBuf> severityMap = {
        {TSeverityIds::ESeverityId::TSeverityIds_ESeverityId_S_FATAL, "errors"},
        {TSeverityIds::ESeverityId::TSeverityIds_ESeverityId_S_ERROR, "errors"},
        {TSeverityIds::ESeverityId::TSeverityIds_ESeverityId_S_WARNING, "warnings"},
        {TSeverityIds::ESeverityId::TSeverityIds_ESeverityId_S_INFO, "infos"}
    };

    auto writeIssues = [this](const TString& severity, const TIssues& errors) {
        Writer.Write(severity);
        Writer.OpenArray();

        for (const auto& topIssue: errors) {
            if (severity != severityMap.at(topIssue.Severity)) {
                continue;
            }
            WalkThroughIssues(topIssue, false, [this, severity](const TIssue& issue, ui16 level) {
                TStringBuilder sb;
                sb << TString(level, '>') << issue;
                Cerr << sb << Endl;
                Writer.Write(sb);
            });
        }

        Writer.CloseArray();
    };

    for (auto severety: issueQueue) {
        writeIssues(severety, errors);
    }
}

void TYqlAction::WriteAstTree(const TAstNode* node) {
    if (node == nullptr) return;

    Writer.OpenMap();
    if (node->IsAtom()) {
        Writer.Write(TStringBuf("type"), TStringBuf("atom"));
        Writer.Write(TStringBuf("content"), node->GetContent());
        Writer.Write(TStringBuf("flags"), node->GetFlags());
    } else if (node->IsList()) {
        Writer.Write(TStringBuf("type"), TStringBuf("list"));
        Writer.Write(TStringBuf("content"), TStringBuf("( )"));
        if (node->GetChildrenCount() > 0) {
            Writer.Write(TStringBuf("children"));
            Writer.OpenArray();
            for (ui32 index = 0; index < node->GetChildrenCount(); ++index) {
                WriteAstTree(node->GetChild(index));
            }
            Writer.CloseArray();
        }
    }
    Writer.CloseMap();
}

} // namspace NNttp
} // namspace NYql
