#include "diagnostic.h"

#include <yql/essentials/utils/json/reflection.h>

namespace NLsp {

EDocumentDiagnosticReportKind TFullDocumentDiagnosticReport::Kind() {
    return EDocumentDiagnosticReportKind::Full;
}

EDocumentDiagnosticReportKind TUnchangedDocumentDiagnosticReport::Kind() {
    return EDocumentDiagnosticReportKind::Unchanged;
}

} // namespace NLsp

namespace NYql::NJson {

YQL_DERIVE_JSON_FROM(NLsp::TDocumentDiagnosticParams);

JSON_DEFINE_TO(NLsp::EDiagnosticSeverity, value) {
    return TJsonValue(static_cast<int>(value) + 1);
}

JSON_DEFINE_TO(NLsp::EDiagnosticTag, value) {
    return TJsonValue(static_cast<int>(value) + 1);
}

YQL_DERIVE_JSON_TO(NLsp::TDiagnostic);

JSON_DEFINE_TO(NLsp::TDiagnostic::TCode, value) {
    return std::visit([&](auto&& x) {
        return ToJson(std::forward<decltype(x)>(x));
    }, std::move(value));
}

YQL_DERIVE_JSON_TO(NLsp::TFullDocumentDiagnosticReport);

JSON_DEFINE_TO(NLsp::TRelatedFullDocumentDiagnosticReport, value) {
    static_assert(
        sizeof(NLsp::TRelatedFullDocumentDiagnosticReport) ==
        sizeof(NLsp::TFullDocumentDiagnosticReport));
    return ToJson(static_cast<NLsp::TFullDocumentDiagnosticReport&&>(std::move(value)));
}

YQL_DERIVE_JSON_TO(NLsp::TUnchangedDocumentDiagnosticReport);

JSON_DEFINE_TO(NLsp::TRelatedUnchangedDocumentDiagnosticReport, value) {
    static_assert(
        sizeof(NLsp::TRelatedUnchangedDocumentDiagnosticReport) ==
        sizeof(NLsp::TUnchangedDocumentDiagnosticReport));
    return ToJson(static_cast<NLsp::TUnchangedDocumentDiagnosticReport&&>(std::move(value)));
}

JSON_DEFINE_TO(NLsp::EDocumentDiagnosticReportKind, value) {
    return ToString(value);
}

JSON_DEFINE_TO(NLsp::TDocumentDiagnosticReport, value) {
    return std::visit([&](auto&& x) {
        using T = std::decay_t<decltype(x)>;

        auto kind = ToJson(x.Kind());
        auto json = ToJson(std::forward<T>(x));
        json["kind"] = std::move(kind);
        return json;
    }, std::move(value));
}

} // namespace NYql::NJson
