#include "name_mapping.h"

#include <yql/essentials/sql/v1/complete/syntax/format.h>

namespace NSQLComplete {

    namespace {

        TString ToIdentifier(TString content, const TLocalSyntaxContext& local) {
            if (IsPlain(content) && !local.IsQuoted.AtLhs && !local.IsQuoted.AtRhs) {
                return content;
            }

            SubstGlobal(content, "`", "``");
            if (!local.IsQuoted.AtLhs) {
                content.prepend('`');
            }
            if (!local.IsQuoted.AtRhs) {
                content.append('`');
            }
            return content;
        }

    } // namespace

    TCandidate ToCandidate(TKeyword name, TLocalSyntaxContext& local) {
        TVector<TString>& seq = local.Keywords[name.Content];
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
        return {ECandidateKind::PragmaName, std::move(name.Identifier)};
    }

    TCandidate ToCandidate(TTypeName name) {
        TCandidate candidate = {
            .Kind = ECandidateKind::TypeName,
            .Content = std::move(name.Identifier),
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
            .Content = std::move(name.Identifier),
        };

        candidate.Content += "()";
        candidate.CursorShift = 1;

        return candidate;
    }

    TCandidate ToCandidate(THintName name) {
        return {ECandidateKind::HintName, std::move(name.Identifier)};
    }

    TCandidate ToCandidate(TFolderName name, TLocalSyntaxContext& local) {
        TCandidate candidate = {
            .Kind = ECandidateKind::FolderName,
            .Content = std::move(name.Identifier),
        };

        if (!local.IsQuoted.AtLhs) {
            candidate.Content.prepend('`');
        }

        candidate.Content.append('/');

        if (!local.IsQuoted.AtRhs) {
            candidate.Content.append('`');
            candidate.CursorShift = 1;
        }

        return candidate;
    }

    TCandidate ToCandidate(TTableName name, TLocalSyntaxContext& local) {
        if (!local.IsQuoted.AtLhs) {
            name.Identifier.prepend('`');
        }
        if (!local.IsQuoted.AtRhs) {
            name.Identifier.append('`');
        }
        return {ECandidateKind::TableName, std::move(name.Identifier)};
    }

    TCandidate ToCandidate(TClusterName name) {
        return {ECandidateKind::ClusterName, std::move(name.Identifier)};
    }

    TCandidate ToCandidate(TColumnName name, TLocalSyntaxContext& local) {
        name.Identifier = ToIdentifier(std::move(name.Identifier), local);

        if (local.Column->Table.empty() && !name.TableAlias.empty()) {
            name.Identifier.prepend('.');
            name.Identifier.prepend(name.TableAlias);
        }

        return {ECandidateKind::ColumnName, std::move(name.Identifier)};
    }

    TCandidate ToCandidate(TBindingName name, TLocalSyntaxContext& local) {
        if (!local.Binding) {
            name.Identifier.prepend('$');
        }
        return {ECandidateKind::BindingName, std::move(name.Identifier)};
    }

    TCandidate ToCandidate(TUnknownName name, TLocalSyntaxContext& local) {
        name.Content = ToIdentifier(std::move(name.Content), local);

        return {ECandidateKind::UnknownName, std::move(name.Content)};
    }

    TCandidate ToCandidate(TGenericName generic, TLocalSyntaxContext& local) {
        return std::visit([&](auto&& name) -> TCandidate {
            using T = std::decay_t<decltype(name)>;

            constexpr bool IsContextSensitive =
                std::is_same_v<T, TKeyword> ||
                std::is_same_v<T, TFolderName> ||
                std::is_same_v<T, TTableName> ||
                std::is_same_v<T, TColumnName> ||
                std::is_same_v<T, TBindingName> ||
                std::is_same_v<T, TUnknownName>;

            constexpr bool IsDocumented =
                std::is_base_of_v<TDescribed, T>;

            TMaybe<TString> documentation;
            if constexpr (IsDocumented) {
                documentation = std::move(name.Description);
            }

            TCandidate candidate;
            if constexpr (IsContextSensitive) {
                candidate = ToCandidate(std::move(name), local);
            } else {
                candidate = ToCandidate(std::move(name));
            }

            candidate.Documentation = std::move(documentation);

            return candidate;
        }, std::move(generic));
    }

    TVector<TCandidate> ToCandidate(TVector<TGenericName> names, TLocalSyntaxContext local) {
        TVector<TCandidate> candidates;
        candidates.reserve(names.size());
        for (auto& name : names) {
            candidates.emplace_back(ToCandidate(std::move(name), local));
        }
        return candidates;
    }

} // namespace NSQLComplete
