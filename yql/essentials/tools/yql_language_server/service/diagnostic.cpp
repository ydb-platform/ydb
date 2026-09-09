#include "diagnostic.h"

#include <yql/essentials/tools/yql_language_server/lsp/message/exception.h>

#include <yql/essentials/public/fastcheck/linter.h>
#include <yql/essentials/core/issue/yql_issue.h>

namespace NLsp::NYql {

namespace {

using ::NYql::TIssue;
using ::NYql::NFastCheck::RunChecks;
using ::NYql::NFastCheck::TCheckResponse;
using ::NYql::NFastCheck::TChecksRequest;
using ::NYql::NFastCheck::TChecksResponse;

class TDiagnosticService final: public IDiagnosticService {
public:
    TDocumentDiagnosticReport Analyze(
        TTextDocumentItemPtr textDocument,
        TMaybe<TString> previousResultId) override {
        auto currentVersion = textDocument->Version;
        auto previousVersion = previousResultId.Transform(FromResultId);
        if (previousVersion && currentVersion == *previousVersion) {
            TRelatedUnchangedDocumentDiagnosticReport report;
            report.ResultId = std::move(*previousResultId);
            return report;
        }

        TChecksRequest request = ToRequest(std::move(textDocument));
        TChecksResponse response = RunChecks(request);
        TVector<TDiagnostic> items = ToMessage(std::move(response));

        TRelatedFullDocumentDiagnosticReport report;
        report.ResultId = ToString(currentVersion);
        report.Items = std::move(items);
        return report;
    }

private:
    TChecksRequest ToRequest(TTextDocumentItemPtr textDocument) {
        return {
            .Program = textDocument->Text,
            .File = textDocument->Uri,
            .ClusterMode = ::NYql::NFastCheck::Unknown,
            .LangVer = ::NYql::GetMaxReleasedLangVersion(),
            .SuppressPrerequisiteIssues = true,
        };
    }

    static TVector<TDiagnostic> ToMessage(TChecksResponse response) {
        size_t size = 0;
        ForEachIssue(response, [&](TStringBuf source, const NYql::TIssue& issue) {
            Y_UNUSED(source, issue);
            size += 1;
        });

        TVector<TDiagnostic> items(Reserve(size));
        ForEachIssue(response, [&](TStringBuf source, const NYql::TIssue& issue) {
            items.emplace_back(ToMessage(source, issue));
        });

        return items;
    }

    static TDiagnostic ToMessage(TStringBuf source, const NYql::TIssue& issue) {
        auto code = ToMessage(issue.GetCode());
        auto tags = Tags(code);
        return {
            .Range = ToMessage(issue.Range()),
            .Severity = ToMessage(issue.GetSeverity()),
            .Code = std::move(code),
            .Source = TString(source),
            .Message = issue.GetMessage(),
            .Tags = std::move(tags),
        };
    }

    static TRange ToMessage(const ::NYql::TRange& range) {
        return {
            .Start = ToMessage(range.Position),
            .End = ToMessage(range.EndPosition, /*isEnd=*/true),
        };
    }

    static TPosition ToMessage(const ::NYql::TPosition& position, bool isEnd = false) {
        TPosition message = {
            .Line = position.Row,
            .Character = position.Column,
        };

        if (0 < message.Line) {
            message.Line -= 1;
        }

        if (0 < message.Character) {
            message.Character -= 1;
        }

        message.Character += isEnd;

        return message;
    }

    static TString ToMessage(::NYql::TIssueCode code) {
        return ::NYql::TIssuesIds::EIssueCode_Name(code);
    }

    static TMaybe<TVector<EDiagnosticTag>> Tags(TString code) {
        TVector<EDiagnosticTag> tags;
        if (code.Contains("UNUSED")) {
            tags.push_back(EDiagnosticTag::Unnecessary);
        }
        if (code.Contains("DEPRECATED")) {
            tags.push_back(EDiagnosticTag::Deprecated);
        }
        if (tags.empty()) {
            return Nothing();
        }
        return tags;
    }

    static EDiagnosticSeverity ToMessage(::NYql::ESeverity severity) {
        switch (severity) {
            case ::NYql::TSeverityIds_ESeverityId_S_FATAL:
                return EDiagnosticSeverity::Error;
            case ::NYql::TSeverityIds_ESeverityId_S_ERROR:
                return EDiagnosticSeverity::Error;
            case ::NYql::TSeverityIds_ESeverityId_S_WARNING:
                return EDiagnosticSeverity::Warning;
            case ::NYql::TSeverityIds_ESeverityId_S_INFO:
                return EDiagnosticSeverity::Information;
            case ::NYql::TSeverityIds_ESeverityId_TSeverityIds_ESeverityId_INT_MIN_SENTINEL_DO_NOT_USE_:
            case ::NYql::TSeverityIds_ESeverityId_TSeverityIds_ESeverityId_INT_MAX_SENTINEL_DO_NOT_USE_:
                Y_ENSURE(false, "Unreachable");
        }
    }

    static void ForEachIssue(const TChecksResponse& response, const auto& f) {
        for (const TCheckResponse& check : response.Checks) {
            const TStringBuf source = check.CheckName;
            for (const NYql::TIssue& issue : check.Issues) {
                ForEachIssue(source, issue, f);
            }
        }
    }

    static void ForEachIssue(TStringBuf source, const NYql::TIssue& issue, const auto& f) {
        auto children = issue.GetSubIssues();
        for (const auto& child : children) {
            ForEachIssue(source, *child, f);
        }
        if (children.empty()) {
            f(source, issue);
        }
    }

    static TTextDocumentVersion FromResultId(TString resultId) {
        TTextDocumentVersion version;
        if (!TryFromString<TTextDocumentVersion>(resultId, version)) {
            throw TLspException::BadRequest()
                << "got an invalid TextDocumentVersion as a resultId";
        }

        return version;
    }
};

} // namespace

IDiagnosticService::TPtr MakeDiagnosticService() {
    return new TDiagnosticService();
}

} // namespace NLsp::NYql
