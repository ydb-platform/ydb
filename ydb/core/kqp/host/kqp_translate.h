#pragma once

#include <ydb/core/kqp/common/simple/query_ast.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <yql/essentials/core/pg_settings/guc_settings.h>

namespace NKikimr {
namespace NKqp {

class TKqpAutoParamBuilder : public NYql::IAutoParamBuilder {
public:
    TKqpAutoParamBuilder();
    THashMap<TString, Ydb::TypedValue> Values;

    ui32 Size() const final;

    bool Contains(const TString& name) const final;

    NYql::IAutoParamTypeBuilder& Add(const TString& name) final;

private:
    class TTypeProxy : public NYql::IAutoParamTypeBuilder {
    public:
        TTypeProxy(TKqpAutoParamBuilder& owner);

        void Pg(const TString& name) final;

        void BeginList() final;

        void EndList() final;

        void BeginTuple() final;

        void EndTuple() final;

        void BeforeItem() final;

        void AfterItem() final;

        NYql::IAutoParamDataBuilder& FinishType() final;

        void Push();
        void Pop();

        TKqpAutoParamBuilder& Owner;
        Ydb::Type* CurrentType = nullptr;
        TVector<Ydb::Type*> Stack;
    };

    class TDataProxy : public NYql::IAutoParamDataBuilder {
    public:
        TDataProxy(TKqpAutoParamBuilder& owner);

        void Pg(const TMaybe<TString>& value) final;

        void BeginList() final;

        void EndList() final;

        void BeginTuple() final;

        void EndTuple() final;

        void BeforeItem() final;

        void AfterItem() final;

        NYql::IAutoParamBuilder& FinishData() final;

        void Push();
        void Pop();

        TKqpAutoParamBuilder& Owner;
        Ydb::Value* CurrentValue = nullptr;
        TVector<Ydb::Value*> Stack;
    };

    Ydb::TypedValue* CurrentParam = nullptr;
    TTypeProxy TypeProxy;
    TDataProxy DataProxy;
};

class TKqpAutoParamBuilderFactory : public NYql::IAutoParamBuilderFactory {
public:
    NYql::IAutoParamBuilderPtr MakeBuilder() final;
};

class TKqpTranslationSettingsBuilder {
public:
    TKqpTranslationSettingsBuilder(NYql::EKikimrQueryType queryType, ui16 kqpYqlSyntaxVersion, const TString& cluster,
            const TString& queryText, const NSQLTranslation::EBindingsMode& bindingsMode, const TGUCSettings::TPtr& gUCSettings)
        : QueryType(queryType)
        , KqpYqlSyntaxVersion(kqpYqlSyntaxVersion)
        , Cluster(cluster)
        , QueryText(queryText)
        , BindingsMode(bindingsMode)
        , GUCSettings(gUCSettings)
    {}

    NSQLTranslation::TTranslationSettings Build(NYql::TExprContext& ctx);
    TKqpTranslationSettingsBuilder& SetFromConfig(const NYql::TKikimrConfiguration& config);

    TKqpTranslationSettingsBuilder& SetUsePgParser(const TMaybe<bool> value) {
        UsePgParser = value;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetKqpTablePathPrefix(const TString& value) {
        KqpTablePathPrefix = value;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetIsEnableExternalDataSources(bool value) {
        IsEnableExternalDataSources = value;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetIsEnablePgConstsToParams(bool value) {
        IsEnablePgConstsToParams = value;
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

    TKqpTranslationSettingsBuilder& SetSqlVersion(const TMaybe<ui16>& value) {
        SqlVersion = value;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetIsEnablePgSyntax(bool value) {
        IsEnablePgSyntax = value;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetIsEnableAntlr4Parser(bool value) {
        IsEnableAntlr4Parser = value;
        return *this;
    }

    TKqpTranslationSettingsBuilder& SetLangVer(ui32 langVer) {
        LangVer = langVer;
        return *this;
    }

private:
    const NYql::EKikimrQueryType QueryType;
    const ui16 KqpYqlSyntaxVersion;
    const TString Cluster;
    const TString QueryText;
    const NSQLTranslation::EBindingsMode BindingsMode;

    TMaybe<bool> UsePgParser = {};
    TString KqpTablePathPrefix = {};
    bool IsEnableExternalDataSources = false;
    bool IsEnablePgConstsToParams = false;
    bool IsEnablePgSyntax = false;
    bool IsEnableAntlr4Parser = false;
    TMaybe<bool> SqlAutoCommit = {};
    TGUCSettings::TPtr GUCSettings;
    TMaybe<TString> ApplicationName = {};
    std::shared_ptr<std::map<TString, Ydb::Type>> QueryParameters = {};
    TMaybe<ui16> SqlVersion = {};
    NYql::TLangVersion LangVer = NYql::MinLangVersion;
    NYql::EBackportCompatibleFeaturesMode BackportMode = NYql::EBackportCompatibleFeaturesMode::Released;
};

NSQLTranslation::EBindingsMode RemapBindingsMode(NKikimrConfig::TTableServiceConfig::EBindingsMode mode);

NYql::EKikimrQueryType ConvertType(NKikimrKqp::EQueryType type);

NYql::TAstParseResult ParseQuery(const TString& queryText, bool isSql, TMaybe<ui16>& sqlVersion, bool& deprecatedSQL,
    NYql::TExprContext& ctx, TKqpTranslationSettingsBuilder& settingsBuilder, bool& keepInCache, TMaybe<TString>& commandTagName,
    NSQLTranslation::TTranslationSettings* effectiveSettings = nullptr);

TVector<TQueryAst> ParseStatements(const TString& queryText, const TMaybe<Ydb::Query::Syntax>& syntax, bool isSql, TKqpTranslationSettingsBuilder& settingsBuilder, bool perStatementExecution);

} // namespace NKqp
} // namespace NKikimr
