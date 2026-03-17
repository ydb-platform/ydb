#pragma once

#include <DBPoco/Util/AbstractConfiguration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

using DictionaryConfigurationPtr = DBPoco::AutoPtr<DBPoco::Util::AbstractConfiguration>;

/// Convert dictionary AST to DBPoco::AbstractConfiguration
/// This function is necessary because all loadable objects configuration are DBPoco::AbstractConfiguration
/// Can throw exception if query is ill-formed
DictionaryConfigurationPtr
getDictionaryConfigurationFromAST(const ASTCreateQuery & query, ContextPtr context, const std::string & database_ = "");

struct ClickHouseDictionarySourceInfo
{
    QualifiedTableName table_name;
    String query;
    bool is_local = false;
};

std::optional<ClickHouseDictionarySourceInfo>
getInfoIfClickHouseDictionarySource(DictionaryConfigurationPtr & config, ContextPtr global_context);

}
