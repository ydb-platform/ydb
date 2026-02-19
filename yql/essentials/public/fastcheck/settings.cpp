#include "settings.h"
#include "utils.h"

#include <yql/essentials/sql/settings/translation_settings.h>

#include <library/cpp/resource/resource.h>

namespace NYql::NFastCheck {

namespace {

NJson::TJsonValue LoadJsonResource(TStringBuf filename) {
    TString text;
    Y_ENSURE(NResource::FindExact(filename, &text), filename);
    return NJson::ReadJsonFastTree(text);
}

TUdfFilter LoadDefaultUdfFilter() {
    auto json = LoadJsonResource("udfs_basic.json");
    return ParseUdfFilter(json);
}

struct TDefaultUdfFilterLoader {
    TDefaultUdfFilterLoader()
        : Data(LoadDefaultUdfFilter())
    {
    }

    TUdfFilter Data;
};

const TUdfFilter& GetDefaultUdfFilter() {
    return Singleton<TDefaultUdfFilterLoader>()->Data;
}

} // namespace

THashSet<TString> TranslationFlags() {
    return {
        "AnsiOrderByLimitInUnionAll",
        "DisableCoalesceJoinKeysOnQualifiedAll",
        "AnsiRankForNullableKeys",
        "DisableUnorderedSubqueries",
        "DisableAnsiOptionalAs",
        "FlexibleTypes",
        "CompactNamedExprs",
        "DistinctOverWindow",
    };
}

bool BuildSqlTranslationSettings(
    const TChecksRequest& request,
    google::protobuf::Arena* arena,
    NSQLTranslation::TTranslationSettings& settings,
    TIssues& issues,
    TMaybe<TParsedSettingsCache>& cache)
{
    settings.Arena = arena;
    settings.File = request.File;
    FillClusters(request, settings);
    settings.EmitReadsForExists = true;
    settings.Antlr4Parser = true;
    settings.AnsiLexer = request.IsAnsiLexer;
    settings.SyntaxVersion = request.SyntaxVersion;
    settings.Flags = TranslationFlags();
    settings.LangVer = request.LangVer;

    if (!request.UdfFilter) {
        settings.UdfFilter = &GetDefaultUdfFilter().Modules;
    } else {
        settings.UdfFilter = &request.UdfFilter->Modules;
    }

    switch (request.Mode) {
        case EMode::Default:
            settings.AlwaysAllowExports = true;
            break;
        case EMode::Library:
            settings.Mode = NSQLTranslation::ESqlMode::LIBRARY;
            break;
        case EMode::Main:
            break;
        case EMode::View:
            settings.Mode = NSQLTranslation::ESqlMode::LIMITED_VIEW;
            break;
    }

    if (cache) {
        const auto& cached = *cache;
        issues.AddIssues(cached.Issues);
        if (!cached.Success) {
            return false;
        }
        return cached.Settings.ApplyTo(settings, issues);
    }

    TParsedSettingsCache result;
    result.Success = NSQLTranslation::ParseTranslationSettingsFromComments(
        request.Program,
        result.Settings,
        result.Issues);

    cache = result;
    issues.AddIssues(result.Issues);

    if (!result.Success) {
        return false;
    }

    return result.Settings.ApplyTo(settings, issues);
}

void BuildPgTranslationSettings(
    const TChecksRequest& request,
    google::protobuf::Arena* arena,
    NSQLTranslation::TTranslationSettings& settings)
{
    settings.Arena = arena;
    settings.PgParser = true;
    FillClusters(request, settings);
}

bool BuildSqlParsingSettings(
    const TChecksRequest& request,
    google::protobuf::Arena* arena,
    NSQLTranslation::TTranslationSettings& settings,
    TIssues& issues,
    TMaybe<TParsedSettingsCache>& cache)
{
    settings.SyntaxVersion = request.SyntaxVersion;
    settings.AnsiLexer = request.IsAnsiLexer;
    settings.Antlr4Parser = true;
    settings.File = request.File;
    settings.Arena = arena;

    if (cache) {
        const auto& cached = *cache;
        issues.AddIssues(cached.Issues);
        if (!cached.Success) {
            return false;
        }
        return cached.Settings.ApplyTo(settings, issues);
    }

    TParsedSettingsCache result;
    result.Success = NSQLTranslation::ParseTranslationSettingsFromComments(
        request.Program,
        result.Settings,
        result.Issues);

    cache = result;
    issues.AddIssues(result.Issues);

    if (!result.Success) {
        return false;
    }

    return result.Settings.ApplyTo(settings, issues);
}

bool BuildLexerSettings(
    const TChecksRequest& request,
    NSQLTranslation::TTranslationSettings& settings,
    TIssues& issues,
    TMaybe<TParsedSettingsCache>& cache)
{
    settings.SyntaxVersion = request.SyntaxVersion;
    settings.AnsiLexer = request.IsAnsiLexer;
    settings.File = request.File;

    if (cache) {
        const auto& cached = *cache;
        issues.AddIssues(cached.Issues);
        if (!cached.Success) {
            return false;
        }
        return cached.Settings.ApplyTo(settings, issues);
    }

    TParsedSettingsCache result;
    result.Success = NSQLTranslation::ParseTranslationSettingsFromComments(
        request.Program,
        result.Settings,
        result.Issues);

    cache = result;
    issues.AddIssues(result.Issues);

    if (!result.Success) {
        return false;
    }

    return result.Settings.ApplyTo(settings, issues);
}

} // namespace NYql::NFastCheck
