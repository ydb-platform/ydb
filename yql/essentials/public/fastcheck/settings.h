#pragma once

#include "linter.h"

#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/ast/yql_errors.h>

#include <google/protobuf/arena.h>
#include <util/generic/maybe.h>

namespace NYql::NFastCheck {

struct TParsedSettingsCache {
    bool Success;
    NSQLTranslation::TParsedSettings Settings;
    TIssues Issues;
};

THashSet<TString> TranslationFlags();

bool BuildSqlTranslationSettings(
    const TChecksRequest& request,
    google::protobuf::Arena* arena,
    NSQLTranslation::TTranslationSettings& settings,
    TIssues& issues,
    TMaybe<TParsedSettingsCache>& cache);

void BuildPgTranslationSettings(
    const TChecksRequest& request,
    google::protobuf::Arena* arena,
    NSQLTranslation::TTranslationSettings& settings);

bool BuildSqlParsingSettings(
    const TChecksRequest& request,
    google::protobuf::Arena* arena,
    NSQLTranslation::TTranslationSettings& settings,
    TIssues& issues,
    TMaybe<TParsedSettingsCache>& cache);

bool BuildLexerSettings(
    const TChecksRequest& request,
    NSQLTranslation::TTranslationSettings& settings,
    TIssues& issues,
    TMaybe<TParsedSettingsCache>& cache);

} // namespace NYql::NFastCheck
