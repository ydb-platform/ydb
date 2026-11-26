#include "serialization.h"

#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NYql::NSqlInspect::NAbi {

NProto::TSymbolsRequest EncodeRequest(const TString& sql, const TSymbolSettings& settings) {
    NProto::TSymbolsRequest request;
    request.SetSql(sql);
    request.SetAnsi(settings.Ansi);
    request.SetLangVer(settings.LangVer);
    request.SetImports(!settings.Filter || settings.Filter->contains(ESymbolType::Import));
    request.SetExports(!settings.Filter || settings.Filter->contains(ESymbolType::Export));
    request.SetParameters(!settings.Filter || settings.Filter->contains(ESymbolType::Parameter));
    return request;
}

TSymbolSettings DecodeSettings(const NProto::TSymbolsRequest& request) {
    TSymbolSettings settings{.Ansi = request.GetAnsi(), .Filter = TSet<ESymbolType>{}};
    if (request.HasLangVer()) {
        settings.LangVer = request.GetLangVer();
    }
    if (request.GetImports()) {
        settings.Filter->insert(ESymbolType::Import);
    }
    if (request.GetExports()) {
        settings.Filter->insert(ESymbolType::Export);
    }
    if (request.GetParameters()) {
        settings.Filter->insert(ESymbolType::Parameter);
    }
    return settings;
}

NProto::TSymbolsResponse EncodeResult(const TSymbolsResult& result) {
    NProto::TSymbolsResponse response;
    if (const auto* issues = std::get_if<TIssues>(&result)) {
        response.SetSuccess(false);
        IssuesToMessage(*issues, response.MutableIssues());
        return response;
    }
    response.SetSuccess(true);
    const auto& symbols = std::get<TSymbols>(result);
    for (const auto& symbol : symbols.Imports) {
        auto* imported = response.AddImports();
        imported->SetName(symbol.Name);
        imported->SetIsAbsolute(symbol.IsAbsolute);
        for (const auto& component : symbol.Module) {
            imported->AddModule(component);
        }
    }
    for (const auto& symbol : symbols.Exports) {
        response.AddExports()->SetName(symbol.Name);
    }
    for (const auto& symbol : symbols.Parameters) {
        response.AddParameters()->SetName(symbol.Name);
    }
    return response;
}

TSymbolsResult DecodeResult(const NProto::TSymbolsResponse& response) {
    if (!response.GetSuccess()) {
        TIssues issues;
        IssuesFromMessage(response.GetIssues(), issues);
        return issues;
    }
    TSymbols symbols;
    for (const auto& symbol : response.GetImports()) {
        symbols.Imports.push_back({.Name = symbol.GetName(), .Module = {symbol.GetModule().begin(), symbol.GetModule().end()}, .IsAbsolute = symbol.GetIsAbsolute()});
    }
    for (const auto& symbol : response.GetExports()) {
        symbols.Exports.push_back({.Name = symbol.GetName()});
    }
    for (const auto& symbol : response.GetParameters()) {
        symbols.Parameters.push_back({.Name = symbol.GetName()});
    }
    return symbols;
}

} // namespace NYql::NSqlInspect::NAbi
