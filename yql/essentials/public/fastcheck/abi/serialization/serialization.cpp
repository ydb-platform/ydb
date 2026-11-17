#include "serialization.h"

namespace NYql::NFastCheck::NAbi {

void Encode(const TOptions& value, NProto::TFastCheckRequest& message) {
    message.SetIsSql(value.IsSql);
    message.SetParseOnly(value.ParseOnly);
    message.SetSyntaxVersion(value.SyntaxVersion);
    message.SetLangVer(value.LangVer);
    message.SetIsLibrary(value.IsLibrary);
}

void Decode(const NProto::TFastCheckRequest& message, TOptions& value) {
    Y_ENSURE(message.GetSyntaxVersion() <= Max<ui16>(), "SQL syntax version exceeds ui16");
    value.IsSql = message.GetIsSql();
    value.ParseOnly = message.GetParseOnly();
    value.SyntaxVersion = message.GetSyntaxVersion();
    value.LangVer = message.GetLangVer();
    value.IsLibrary = message.GetIsLibrary();
}

void Encode(const TChecksRequest& value, NProto::TLinterRequest& message) {
    message.SetProgram(value.Program);
    message.SetFile(value.File);
    message.SetClusterMode(static_cast<int>(value.ClusterMode));
    message.SetClusterSystem(value.ClusterSystem);
    message.SetSyntax(static_cast<int>(value.Syntax));
    message.SetSyntaxVersion(value.SyntaxVersion);
    message.SetLangVer(value.LangVer);
    message.SetIsAnsiLexer(value.IsAnsiLexer);
    message.SetMode(static_cast<int>(value.Mode));
    message.SetIssueReportTarget(value.IssueReportTarget);
    message.SetSuppressPrerequisiteIssues(value.SuppressPrerequisiteIssues);
    message.SetLimitStrictnessFactor(value.LimitStrictnessFactor);
}

void Decode(const NProto::TLinterRequest& message, TChecksRequest& value) {
    Y_ENSURE(message.GetClusterMode() >= Many && message.GetClusterMode() <= Unknown, "Invalid cluster mode");
    Y_ENSURE(message.GetSyntax() >= static_cast<int>(ESyntax::SExpr) && message.GetSyntax() <= static_cast<int>(ESyntax::PG), "Invalid syntax");
    Y_ENSURE(message.GetMode() >= static_cast<int>(EMode::Default) && message.GetMode() <= static_cast<int>(EMode::View), "Invalid mode");
    Y_ENSURE(message.GetSyntaxVersion() <= Max<ui16>(), "SQL syntax version exceeds ui16");
    value.Program = message.GetProgram();
    value.File = message.GetFile();
    value.ClusterMode = static_cast<decltype(value.ClusterMode)>(message.GetClusterMode());
    value.ClusterSystem = message.GetClusterSystem();
    value.Syntax = static_cast<decltype(value.Syntax)>(message.GetSyntax());
    value.SyntaxVersion = message.GetSyntaxVersion();
    value.LangVer = message.GetLangVer();
    value.IsAnsiLexer = message.GetIsAnsiLexer();
    value.Mode = static_cast<decltype(value.Mode)>(message.GetMode());
    value.IssueReportTarget = message.GetIssueReportTarget();
    value.SuppressPrerequisiteIssues = message.GetSuppressPrerequisiteIssues();
    value.LimitStrictnessFactor = message.GetLimitStrictnessFactor();
}

void EncodeMap(const THashMap<TString, TString>& value, google::protobuf::RepeatedPtrField<NProto::TStringPair>& message) {
    for (const auto& [key, item] : value) {
        auto* pair = message.Add();
        pair->SetKey(key);
        pair->SetValue(item);
    }
}

void DecodeMap(const google::protobuf::RepeatedPtrField<NProto::TStringPair>& message, THashMap<TString, TString>& value) {
    for (const auto& pair : message) {
        value.emplace(pair.GetKey(), pair.GetValue());
    }
}

void EncodeFilters(const TMaybe<TVector<TCheckFilter>>& value, NProto::TLinterRequest& message) {
    message.SetHasFilters(value.Defined());
    if (value) {
        for (const auto& filter : *value) {
            auto* item = message.AddFilters();
            item->SetInclude(filter.Include);
            item->SetCheckNameGlob(filter.CheckNameGlob);
        }
    }
}

void DecodeFilters(const NProto::TLinterRequest& message, TMaybe<TVector<TCheckFilter>>& value) {
    if (message.GetHasFilters()) {
        value.ConstructInPlace();
        for (const auto& filter : message.GetFilters()) {
            value->push_back({.Include = filter.GetInclude(), .CheckNameGlob = filter.GetCheckNameGlob()});
        }
    }
}

} // namespace NYql::NFastCheck::NAbi
