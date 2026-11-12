#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <yql/essentials/core/pg_settings/guc_settings.h>
#include <yql/essentials/ast/yql_ast.h>

namespace NKikimr {
namespace NKqp {

class TKqpTranslationSettingsBuilder {
public:
    TKqpTranslationSettingsBuilder(NYql::EKikimrQueryType queryType, const TString& cluster,
            const TString& queryText, const NSQLTranslation::EBindingsMode& bindingsMode, const TGUCSettings::TPtr& gUCSettings)
        : QueryType(queryType)
        , Cluster(cluster)
        , QueryText(queryText)
        , BindingsMode(bindingsMode)
        , GUCSettings(gUCSettings)
    {}

    NSQLTranslation::TTranslationSettings Build(NYql::TExprContext& ctx);
    TKqpTranslationSettingsBuilder& SetFromConfig(const NYql::TKikimrConfiguration& config);

    TKqpTranslationSettingsBuilder& SetKqpTablePathPrefix(const TString& value) {
        KqpTablePathPrefix = value;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetIsEnableExternalDataSources(bool value) {
        IsEnableExternalDataSources = value;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetSqlAutoCommit(bool value) {
        SqlAutoCommit = value;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetApplicationName(const TMaybe<TString>& value) {
        ApplicationName = value;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetQueryParameters(const std::shared_ptr<std::map<TString, Ydb::Type>>& value) {
        QueryParameters = value;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetLangVer(ui32 langVer) {
        LangVer = langVer;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetBackportMode(NYql::EBackportCompatibleFeaturesMode backportMode) {
        BackportMode = backportMode;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetIsAmbiguityError(bool isAmbiguityError) {
        IsAmbiguityError = isAmbiguityError;
        return *this;
    }

    bool GetIsAmbiguityError() const {
        return IsAmbiguityError;
    }

    TKqpTranslationSettingsBuilder& SetYqlSelect(TMaybe<NSQLTranslation::EYqlSelect> yqlSelect) {
        YqlSelect = yqlSelect;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetValidateViewStatement(bool value) {
        ValidateViewStatement = value;
        return *this;
    }

private:
    const NYql::EKikimrQueryType QueryType;
    const TString Cluster;
    const TString QueryText;
    const NSQLTranslation::EBindingsMode BindingsMode;

    TString KqpTablePathPrefix = {};
    bool IsEnableExternalDataSources = false;
    TMaybe<bool> SqlAutoCommit = {};
    TGUCSettings::TPtr GUCSettings;
    TMaybe<TString> ApplicationName = {};
    std::shared_ptr<std::map<TString, Ydb::Type>> QueryParameters = {};
    NYql::TLangVersion LangVer = NYql::MinLangVersion;
    NYql::EBackportCompatibleFeaturesMode BackportMode = NYql::EBackportCompatibleFeaturesMode::Released;
    bool IsAmbiguityError = false;
    TMaybe<NSQLTranslation::EYqlSelect> YqlSelect = {};
    bool ValidateViewStatement = true;
};

NYql::EKikimrQueryType ConvertType(NKikimrKqp::EQueryType type);

NYql::TAstParseResult ParseQuery(const TString& queryText, bool isSql, TMaybe<ui16>& sqlVersion, bool& deprecatedSQL,
    NYql::TExprContext& ctx, TKqpTranslationSettingsBuilder& settingsBuilder, bool& keepInCache, TMaybe<TString>& commandTagName,
    NSQLTranslation::TTranslationSettings* effectiveSettings = nullptr);

} // namespace NKqp
} // namespace NKikimr
