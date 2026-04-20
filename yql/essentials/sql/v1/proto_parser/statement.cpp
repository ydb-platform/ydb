#include "statement.h"

#include "parse_tree.h"

#include <util/string/split.h>

#include <ranges>

namespace NSQLTranslationV1 {

TStatementName TStatementName::FromAltDescription(const TString& alt) {
    TVector<TString> parts;
    Split(alt, "_", parts);
    Y_DEBUG_ABORT_UNLESS(parts.size() > 1);
    parts.pop_back();

    TStatementName name;

    for (TString& part : parts) {
        part.to_upper(0, 1);

        name.Internal += part;

        if (!name.Human.empty()) {
            name.Human += ' ';
        }
        name.Human += to_upper(part);
    }

    return name;
}

TVector<TStatementName> StatementNames(const TRule_sql_query& rule) {
    auto statements = Statements(rule);

    TVector<TStatementName> names(Reserve(statements.size()));
    for (const auto* statement : statements) {
        names.emplace_back(TStatementName::From(*statement));
    }
    return names;
}

} // namespace NSQLTranslationV1
