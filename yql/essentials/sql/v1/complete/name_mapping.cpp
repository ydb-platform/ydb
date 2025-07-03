#include "name_mapping.h"

#include <yql/essentials/sql/v1/complete/syntax/format.h>

namespace NSQLComplete {

    namespace {

        TString ToIdentifier(TString content, const TLocalSyntaxContext& context) {
            if (IsPlain(content) && !context.IsQuoted.AtLhs && !context.IsQuoted.AtRhs) {
                return content;
            }

            SubstGlobal(content, "`", "``");
            if (!context.IsQuoted.AtLhs) {
                content.prepend('`');
            }
            if (!context.IsQuoted.AtRhs) {
                content.append('`');
            }
            return content;
        }

    } // namespace

    TCandidate ToCandidate(TKeyword name, TLocalSyntaxContext& context) {
        TVector<TString>& seq = context.Keywords[name.Content];
        seq.insert(std::begin(seq), name.Content);

        TCandidate candidate = {
            .Kind = ECandidateKind::Keyword,
            .Content = FormatKeywords(seq),
        };

        if (candidate.Content.EndsWith('(')) {
            candidate.Content += ')';
            candidate.CursorShift = 1;
        }

        return candidate;
    }

    TCandidate ToCandidate(TPragmaName name) {
        return {ECandidateKind::PragmaName, std::move(name.Indentifier)};
    }

    TCandidate ToCandidate(TTypeName name) {
        TCandidate candidate = {
            .Kind = ECandidateKind::TypeName,
            .Content = std::move(name.Indentifier),
        };

        switch (name.Kind) {
            case TTypeName::EKind::Simple: {
            } break;
            case TTypeName::EKind::Container: {
                candidate.Content += "<>";
                candidate.CursorShift = 1;
            } break;
            case TTypeName::EKind::Parameterized: {
                candidate.Content += "()";
                candidate.CursorShift = 1;
            } break;
        }

        return candidate;
    }

    TCandidate ToCandidate(TFunctionName name) {
        TCandidate candidate = {
            .Kind = ECandidateKind::FunctionName,
            .Content = std::move(name.Indentifier),
        };

        candidate.Content += "()";
        candidate.CursorShift = 1;

        return candidate;
    }

    TCandidate ToCandidate(THintName name) {
        return {ECandidateKind::HintName, std::move(name.Indentifier)};
    }

    TCandidate ToCandidate(TFolderName name, TLocalSyntaxContext& context) {
        TCandidate candidate = {
            .Kind = ECandidateKind::FolderName,
            .Content = std::move(name.Indentifier),
        };

        if (!context.IsQuoted.AtLhs) {
            candidate.Content.prepend('`');
        }

        candidate.Content.append('/');

        if (!context.IsQuoted.AtRhs) {
            candidate.Content.append('`');
            candidate.CursorShift = 1;
        }

        return candidate;
    }

    TCandidate ToCandidate(TTableName name, TLocalSyntaxContext& context) {
        if (!context.IsQuoted.AtLhs) {
            name.Indentifier.prepend('`');
        }
        if (!context.IsQuoted.AtRhs) {
            name.Indentifier.append('`');
        }
        return {ECandidateKind::TableName, std::move(name.Indentifier)};
    }

    TCandidate ToCandidate(TClusterName name) {
        return {ECandidateKind::ClusterName, std::move(name.Indentifier)};
    }

    TCandidate ToCandidate(TColumnName name, TLocalSyntaxContext& context) {
        name.Indentifier = ToIdentifier(std::move(name.Indentifier), context);

        if (context.Column->Table.empty() && !name.TableAlias.empty()) {
            name.Indentifier.prepend('.');
            name.Indentifier.prepend(name.TableAlias);
        }

        return {ECandidateKind::ColumnName, std::move(name.Indentifier)};
    }

    TCandidate ToCandidate(TBindingName name, TLocalSyntaxContext& context) {
        if (!context.Binding) {
            name.Indentifier.prepend('$');
        }
        return {ECandidateKind::BindingName, std::move(name.Indentifier)};
    }

    TCandidate ToCandidate(TUnknownName name, TLocalSyntaxContext& context) {
        name.Content = ToIdentifier(std::move(name.Content), context);

        return {ECandidateKind::UnknownName, std::move(name.Content)};
    }

    TCandidate ToCandidate(TGenericName generic, TLocalSyntaxContext& context) {
        return std::visit([&](auto&& name) -> TCandidate {
            using T = std::decay_t<decltype(name)>;
            constexpr bool IsContextSensitive =
                std::is_same_v<T, TKeyword> ||
                std::is_same_v<T, TFolderName> ||
                std::is_same_v<T, TTableName> ||
                std::is_same_v<T, TColumnName> ||
                std::is_same_v<T, TBindingName> ||
                std::is_same_v<T, TUnknownName>;

            if constexpr (IsContextSensitive) {
                return ToCandidate(std::move(name), context);
            } else {
                return ToCandidate(std::move(name));
            }
        }, std::move(generic));
    }

    TVector<TCandidate> ToCandidate(TVector<TGenericName> names, TLocalSyntaxContext context) {
        TVector<TCandidate> candidates;
        candidates.reserve(names.size());
        for (auto& name : names) {
            candidates.emplace_back(ToCandidate(std::move(name), context));
        }
        return candidates;
    }

} // namespace NSQLComplete
