#include "configuration.h"

#include <yql/essentials/sql/v1/complete/syntax/grammar.h>

namespace NSQLComplete {

    TConfiguration MakeConfiguration(THashSet<TString> allowedStmts) {
        allowedStmts.emplace("sql_stmt");

        TConfiguration config;
        for (const std::string& name : GetSqlGrammar().GetAllRules()) {
            if (name.ends_with("_stmt") && !allowedStmts.contains(name)) {
                config.IgnoredRules_.emplace(name);
            }
        }
        return config;
    }

    TConfiguration MakeYDBConfiguration() {
        TConfiguration config;
        config.IgnoredRules_ = {
            "use_stmt",
            "import_stmt",
            "export_stmt",
        };
        return config;
    }

    TConfiguration MakeYQLConfiguration() {
        auto config = MakeConfiguration(/* allowedStmts = */ {
            "lambda_stmt",
            "pragma_stmt",
            "select_stmt",
            "named_nodes_stmt",
            "drop_table_stmt",
            "use_stmt",
            "into_table_stmt",
            "commit_stmt",
            "declare_stmt",
            "import_stmt",
            "export_stmt",
            "do_stmt",
            "define_action_or_subquery_stmt",
            "if_stmt",
            "for_stmt",
            "values_stmt",
        });

        config.DisabledPreviousByToken_ = {};

        config.ForcedPreviousByToken_ = {
            {"PARALLEL", {}},
            {"TABLESTORE", {}},
            {"FOR", {"EVALUATE"}},
            {"IF", {"EVALUATE"}},
            {"EXTERNAL", {"USING"}},
        };

        return config;
    }

} // namespace NSQLComplete
