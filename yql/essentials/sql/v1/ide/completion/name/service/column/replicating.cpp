#include "replicating.h"

#include <yql/essentials/sql/v1/ide/completion/name/object/simple/static/schema.h>

#include <library/cpp/case_insensitive_string/case_insensitive_string.h>

#include <util/generic/hash.h>

namespace NSQLComplete {

namespace {

class TNameService: public INameService {
public:
    TNameService(INameService::TPtr origin, IRanking::TPtr ranking)
        : Origin_(std::move(origin))
        , Ranking_(std::move(ranking))
    {
    }

    NThreading::TFuture<TNameResponse> Lookup(const TNameRequest& request) const final {
        NThreading::TFuture<TNameResponse> response = Origin_->Lookup(request);

        if (!request.Constraints.Column || request.Constraints.Column->TableAlias) {
            return response;
        }

        // TODO(YQL-20095): Explore real problem to fix this.
        // NOLINTNEXTLINE(bugprone-exception-escape)
        return std::move(response).Apply([request, ranking = Ranking_](auto f) -> TNameResponse {
            TNameResponse response = f.ExtractValue();

            THashMap<TString, size_t> references = CountAsUnqualified(response.RankedNames);
            response.RankedNames = Replicated(std::move(response.RankedNames));
            response.RankedNames = Disambiguated(std::move(response.RankedNames), references);
            response.RankedNames = Filtered(std::move(response.RankedNames), request.Prefix);

            ranking->CropToSortedPrefix(response.RankedNames, request.Constraints, request.Limit);

            return response;
        });
    }

private:
    static THashMap<TString, size_t> CountAsUnqualified(const TVector<TGenericName>& names) {
        THashMap<TString, size_t> references;
        for (const TGenericName& name : names) {
            const auto* column = std::get_if<TColumnName>(&name);
            if (column == nullptr) {
                continue;
            }

            references[column->Identifier] += 1;
        }

        return references;
    }

    static TVector<TGenericName> Replicated(TVector<TGenericName> names) {
        const size_t size = names.size();

        for (size_t i = 0; i < size; ++i) {
            const auto* column = std::get_if<TColumnName>(&names[i]);
            if (column == nullptr || column->TableAlias.empty()) {
                continue;
            }

            TColumnName unqualified;
            unqualified.Identifier = column->Identifier;
            names.emplace_back(std::move(unqualified));
        }

        return names;
    }

    static TVector<TGenericName> Disambiguated(
        TVector<TGenericName> names,
        const THashMap<TString, size_t>& references)
    {
        EraseIf(names, [&](const TGenericName& name) {
            const auto* column = std::get_if<TColumnName>(&name);
            if (column == nullptr) {
                return false;
            }

            const auto* count = references.FindPtr(column->Identifier);
            return column->TableAlias.empty() && 1 < *count;
        });

        return names;
    }

    static TVector<TGenericName> Filtered(TVector<TGenericName> names, TStringBuf prefix) {
        EraseIf(names, [&](const TGenericName& name) {
            const auto* column = std::get_if<TColumnName>(&name);

            // TODO(YQL-19747): introduce a single source of truth of filtration policy
            return column &&
                   !TCaseInsensitiveStringBuf(column->Identifier).StartsWith(prefix) &&
                   !TCaseInsensitiveStringBuf(column->TableAlias).StartsWith(prefix);
        });

        return names;
    }

    INameService::TPtr Origin_;
    IRanking::TPtr Ranking_;
};

} // namespace

INameService::TPtr MakeColumnReplicatingService(INameService::TPtr origin, IRanking::TPtr ranking) {
    return new TNameService(std::move(origin), std::move(ranking));
}

} // namespace NSQLComplete
