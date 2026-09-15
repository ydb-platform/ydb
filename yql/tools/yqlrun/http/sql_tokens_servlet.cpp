#include "sql_tokens_servlet.h"

#include <library/cpp/json/json_reader.h>
#include <yql/essentials/sql/v1/format/sql_format.h>

namespace {

THashSet<TString> ExtractNamesFromJson(const NJson::TJsonValue& json) {
    THashSet<TString> names;
    for (const auto& value : json.GetArraySafe()) {
        names.insert(value["name"].GetStringSafe());
    }
    return names;
}

NJson::TJsonValue LoadJsonResource(TStringBuf filename) {
    TString text;
    Y_ENSURE(NResource::FindExact(filename, &text));
    return NJson::ReadJsonFastTree(text);
}

void OutputJsArray(TStringStream& out, TStringBuf varName, const THashSet<TString>& names) {
    out << varName << " = [";
    for (const auto& name : names) {
        out << '"' << name << "\",";
    }
    out << "];\n";
}

void OutputKnownTokens(TStringStream& out)
{
    // See type_id in the grammar.
    THashSet<TString> compositeTypes = {
        "OPTIONAL",
        "TUPLE",
        "STRUCT",
        "VARIANT",
        "LIST",
        "STREAM",
        "FLOW",
        "DICT",
        "SET",
        "ENUM",
        "RESOURCE",
        "TAGGED",
        "CALLABLE",
    };

    out << "window.sql = {};\n";

    auto kws = NSQLFormat::GetKeywords();
    for (const auto& ty : compositeTypes) {
        kws.erase(ty);
    }
    OutputJsArray(out, "window.sql.keywords", kws);

    THashSet<TString> types = ExtractNamesFromJson(LoadJsonResource("types.json"));
    types.insert(compositeTypes.begin(), compositeTypes.end());
    OutputJsArray(out, "window.sql.types", types);

    THashSet<TString> builtinFuncs = ExtractNamesFromJson(LoadJsonResource("sql_functions.json"));
    OutputJsArray(out, "window.sql.builtinFunctions", builtinFuncs);
}

} // namespace

namespace NYql {
namespace NHttp {

TSqlTokensServlet::TSqlTokensServlet()
{
    TStringStream out;
    OutputKnownTokens(out);
    Script_ = out.Str();
}

void TSqlTokensServlet::DoGet(const TRequest& req, TResponse& resp) const {
    Y_UNUSED(req);

    resp.Body = TBlob::FromString(Script_);
    resp.Headers.AddHeader(THttpInputHeader("Content-Type: text/javascript"));
}

} // namespace NHttp
} // namespace NYql
