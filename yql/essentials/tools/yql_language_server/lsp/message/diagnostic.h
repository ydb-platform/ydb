#pragma once

#include "text_document.h"

namespace NLsp {

struct TDocumentDiagnosticParams {
    TTextDocumentIdentifier TextDocument;
    TMaybe<TString> Identifier;
    TMaybe<TString> PreviousResultId;
};

enum class EDiagnosticSeverity {
    Error,
    Warning,
    Information,
    Hint,
};

enum class EDiagnosticTag {
    Unnecessary,
    Deprecated,
};

struct TDiagnostic {
    using TCode = std::variant<i64, TString>;

    TRange Range;
    TMaybe<EDiagnosticSeverity> Severity;
    TMaybe<TCode> Code;
    TMaybe<TString> Source;
    TString Message;
    TMaybe<TVector<EDiagnosticTag>> Tags;
};

enum class EDocumentDiagnosticReportKind {
    Full /* "full" */,
    Unchanged /* "unchanged" */,
};

struct TFullDocumentDiagnosticReport {
    TMaybe<TString> ResultId;
    TVector<TDiagnostic> Items;

    EDocumentDiagnosticReportKind Kind();
};

struct TRelatedFullDocumentDiagnosticReport: TFullDocumentDiagnosticReport {
};

struct TUnchangedDocumentDiagnosticReport {
    TString ResultId;

    EDocumentDiagnosticReportKind Kind();
};

struct TRelatedUnchangedDocumentDiagnosticReport: TUnchangedDocumentDiagnosticReport {
};

using TDocumentDiagnosticReport = std::variant<
    TRelatedFullDocumentDiagnosticReport,
    TRelatedUnchangedDocumentDiagnosticReport>;

} // namespace NLsp

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(NLsp::TDocumentDiagnosticParams, (TextDocument)(Identifier)(PreviousResultId));
YQL_DEFINE_REFLECTING(NLsp::TDiagnostic, (Range)(Severity)(Code)(Source)(Message)(Tags));
YQL_DEFINE_REFLECTING(NLsp::TFullDocumentDiagnosticReport, (ResultId)(Items));
YQL_DEFINE_REFLECTING(NLsp::TUnchangedDocumentDiagnosticReport, (ResultId));

} // namespace NYql::NReflection

namespace NYql::NJson {

JSON_DECLARE_FROM(NLsp::TDocumentDiagnosticParams, value);
JSON_DECLARE_TO(NLsp::EDiagnosticSeverity, value);
JSON_DECLARE_TO(NLsp::EDiagnosticTag, value);
JSON_DECLARE_TO(NLsp::TDiagnostic, value);
JSON_DECLARE_TO(NLsp::TDiagnostic::TCode, value);
JSON_DECLARE_TO(NLsp::TFullDocumentDiagnosticReport, value);
JSON_DECLARE_TO(NLsp::TRelatedFullDocumentDiagnosticReport, value);
JSON_DECLARE_TO(NLsp::TUnchangedDocumentDiagnosticReport, value);
JSON_DECLARE_TO(NLsp::TRelatedUnchangedDocumentDiagnosticReport, value);
JSON_DECLARE_TO(NLsp::EDocumentDiagnosticReportKind, value);
JSON_DECLARE_TO(NLsp::TDocumentDiagnosticReport, value);

} // namespace NYql::NJson
