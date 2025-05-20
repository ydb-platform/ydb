#include "sql_complete.h"

#include <yql/essentials/sql/v1/complete/text/word.h>
#include <yql/essentials/sql/v1/complete/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/complete/syntax/local.h>
#include <yql/essentials/sql/v1/complete/syntax/format.h>
#include <yql/essentials/sql/v1/complete/analysis/global/global.h>

#include <util/generic/algorithm.h>
#include <util/charset/utf8.h>

namespace NSQLComplete {

    class TSqlCompletionEngine: public ISqlCompletionEngine {
    public:
        explicit TSqlCompletionEngine(
            TLexerSupplier lexer,
            INameService::TPtr names,
            ISqlCompletionEngine::TConfiguration configuration)
            : Configuration_(std::move(configuration))
            , SyntaxAnalysis_(MakeLocalSyntaxAnalysis(lexer))
            , GlobalAnalysis_(MakeGlobalAnalysis())
            , Names_(std::move(names))
        {
        }

        TCompletion Complete(TCompletionInput input) override {
            return CompleteAsync(std::move(input)).ExtractValueSync();
        }

        virtual NThreading::TFuture<TCompletion> CompleteAsync(TCompletionInput input) override {
            if (
                input.CursorPosition < input.Text.length() &&
                    IsUTF8ContinuationByte(input.Text.at(input.CursorPosition)) ||
                input.Text.length() < input.CursorPosition) {
                ythrow yexception()
                    << "invalid cursor position " << input.CursorPosition
                    << " for input size " << input.Text.size();
            }

            TLocalSyntaxContext context = SyntaxAnalysis_->Analyze(input);
            auto keywords = context.Keywords;

            TGlobalContext global = GlobalAnalysis_->Analyze(input);

            TNameRequest request = NameRequestFrom(input, context, global);
            if (request.IsEmpty()) {
                return NThreading::MakeFuture<TCompletion>({
                    .CompletedToken = GetCompletedToken(input, context.EditRange),
                    .Candidates = {},
                });
            }

            return Names_->Lookup(std::move(request))
                .Apply([this, input, context = std::move(context)](auto f) {
                    return ToCompletion(input, context, f.ExtractValue());
                });
        }

    private:
        TCompletedToken GetCompletedToken(TCompletionInput input, TEditRange editRange) const {
            return {
                .Content = input.Text.SubStr(editRange.Begin, editRange.Length),
                .SourcePosition = editRange.Begin,
            };
        }

        TNameRequest NameRequestFrom(
            TCompletionInput input,
            const TLocalSyntaxContext& context,
            const TGlobalContext& global) const {
            TNameRequest request = {
                .Prefix = TString(GetCompletedToken(input, context.EditRange).Content),
                .Limit = Configuration_.Limit,
            };

            for (const auto& [first, _] : context.Keywords) {
                request.Keywords.emplace_back(first);
            }

            if (context.Pragma) {
                TPragmaName::TConstraints constraints;
                constraints.Namespace = context.Pragma->Namespace;
                request.Constraints.Pragma = std::move(constraints);
            }

            if (context.Type) {
                request.Constraints.Type = TTypeName::TConstraints();
            }

            if (context.Function) {
                TFunctionName::TConstraints constraints;
                constraints.Namespace = context.Function->Namespace;
                request.Constraints.Function = std::move(constraints);
            }

            if (context.Hint) {
                THintName::TConstraints constraints;
                constraints.Statement = context.Hint->StatementKind;
                request.Constraints.Hint = std::move(constraints);
            }

            if (context.Object) {
                request.Constraints.Object = TObjectNameConstraints();
                request.Constraints.Object->Kinds = context.Object->Kinds;
                request.Prefix = context.Object->Path;
            }

            if (context.Object && global.Use) {
                request.Constraints.Object->Provider = global.Use->Provider;
                request.Constraints.Object->Cluster = global.Use->Cluster;
            }

            if (context.Object && context.Object->HasCluster()) {
                request.Constraints.Object->Provider = context.Object->Provider;
                request.Constraints.Object->Cluster = context.Object->Cluster;
            }

            if (context.Cluster) {
                TClusterName::TConstraints constraints;
                constraints.Namespace = context.Cluster->Provider;
                request.Constraints.Cluster = std::move(constraints);
            }

            return request;
        }

        TCompletion ToCompletion(
            TCompletionInput input,
            TLocalSyntaxContext context,
            TNameResponse response) const {
            TCompletion completion = {
                .CompletedToken = GetCompletedToken(input, context.EditRange),
                .Candidates = Convert(std::move(response.RankedNames), std::move(context)),
            };

            if (response.NameHintLength) {
                const auto length = *response.NameHintLength;
                TEditRange editRange = {
                    .Begin = input.CursorPosition - length,
                    .Length = length,
                };
                completion.CompletedToken = GetCompletedToken(input, editRange);
            }

            return completion;
        }

        static TVector<TCandidate> Convert(TVector<TGenericName> names, TLocalSyntaxContext context) {
            TVector<TCandidate> candidates;
            candidates.reserve(names.size());
            for (auto& name : names) {
                candidates.emplace_back(Convert(std::move(name), context));
            }
            return candidates;
        }

        static TCandidate Convert(TGenericName name, TLocalSyntaxContext& context) {
            return std::visit([&](auto&& name) -> TCandidate {
                using T = std::decay_t<decltype(name)>;

                if constexpr (std::is_base_of_v<TKeyword, T>) {
                    TVector<TString>& seq = context.Keywords[name.Content];
                    seq.insert(std::begin(seq), name.Content);
                    return {ECandidateKind::Keyword, FormatKeywords(seq)};
                }

                if constexpr (std::is_base_of_v<TPragmaName, T>) {
                    return {ECandidateKind::PragmaName, std::move(name.Indentifier)};
                }

                if constexpr (std::is_base_of_v<TTypeName, T>) {
                    return {ECandidateKind::TypeName, std::move(name.Indentifier)};
                }

                if constexpr (std::is_base_of_v<TFunctionName, T>) {
                    name.Indentifier += "(";
                    return {ECandidateKind::FunctionName, std::move(name.Indentifier)};
                }

                if constexpr (std::is_base_of_v<THintName, T>) {
                    return {ECandidateKind::HintName, std::move(name.Indentifier)};
                }

                if constexpr (std::is_base_of_v<TFolderName, T>) {
                    name.Indentifier.append('/');
                    if (!context.Object->IsQuoted) {
                        name.Indentifier = Quoted(std::move(name.Indentifier));
                    }
                    return {ECandidateKind::FolderName, std::move(name.Indentifier)};
                }

                if constexpr (std::is_base_of_v<TTableName, T>) {
                    if (!context.Object->IsQuoted) {
                        name.Indentifier = Quoted(std::move(name.Indentifier));
                    }
                    return {ECandidateKind::TableName, std::move(name.Indentifier)};
                }

                if constexpr (std::is_base_of_v<TClusterName, T>) {
                    return {ECandidateKind::ClusterName, std::move(name.Indentifier)};
                }

                if constexpr (std::is_base_of_v<TUnkownName, T>) {
                    return {ECandidateKind::UnknownName, std::move(name.Content)};
                }
            }, std::move(name));
        }

        TConfiguration Configuration_;
        ILocalSyntaxAnalysis::TPtr SyntaxAnalysis_;
        IGlobalAnalysis::TPtr GlobalAnalysis_;
        INameService::TPtr Names_;
    };

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine(
        TLexerSupplier lexer,
        INameService::TPtr names,
        ISqlCompletionEngine::TConfiguration configuration) {
        return MakeHolder<TSqlCompletionEngine>(
            lexer, std::move(names), std::move(configuration));
    }

} // namespace NSQLComplete

template <>
void Out<NSQLComplete::ECandidateKind>(IOutputStream& out, NSQLComplete::ECandidateKind kind) {
    switch (kind) {
        case NSQLComplete::ECandidateKind::Keyword:
            out << "Keyword";
            break;
        case NSQLComplete::ECandidateKind::PragmaName:
            out << "PragmaName";
            break;
        case NSQLComplete::ECandidateKind::TypeName:
            out << "TypeName";
            break;
        case NSQLComplete::ECandidateKind::FunctionName:
            out << "FunctionName";
            break;
        case NSQLComplete::ECandidateKind::HintName:
            out << "HintName";
            break;
        case NSQLComplete::ECandidateKind::FolderName:
            out << "FolderName";
            break;
        case NSQLComplete::ECandidateKind::TableName:
            out << "TableName";
            break;
        case NSQLComplete::ECandidateKind::ClusterName:
            out << "ClusterName";
            break;
        case NSQLComplete::ECandidateKind::UnknownName:
            out << "UnknownName";
            break;
    }
}

template <>
void Out<NSQLComplete::TCandidate>(IOutputStream& out, const NSQLComplete::TCandidate& candidate) {
    out << "{" << candidate.Kind << ", \"" << candidate.Content << "\"}";
}
