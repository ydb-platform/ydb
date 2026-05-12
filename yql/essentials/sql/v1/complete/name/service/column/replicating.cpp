#include "replicating.h"

#include <yql/essentials/sql/v1/complete/name/object/simple/static/schema.h>

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

            TVector<TGenericName> replicatable = ReplicatableColumns(response.RankedNames, request.Prefix);
            std::ranges::move(std::move(replicatable), std::back_inserter(response.RankedNames));

            ranking->CropToSortedPrefix(response.RankedNames, request.Constraints, request.Limit);

            return response;
        });
    }

private:
    static TVector<TGenericName> ReplicatableColumns(const TVector<TGenericName>& names, TStringBuf prefix) {
        THashMap<TString, size_t> references;
        for (const TGenericName& name : names) {
            if (!std::holds_alternative<TColumnName>(name)) {
                continue;
            }

            const TColumnName& column = std::get<TColumnName>(name);
            if (column.TableAlias.empty()) {
                continue;
            }

            references[column.Identifier] += 1;
        }

        TVector<TGenericName> replicatable;
        for (auto& [column, count] : references) {
            if (count != 1) {
                continue;
            }

            // TODO(YQL-19747): introduce a single source of truth of filtration policy
            if (!TCaseInsensitiveStringBuf(column).StartsWith(prefix)) {
                continue;
            }

            TColumnName name;
            name.Identifier = column;
            replicatable.emplace_back(std::move(name));
        }

        return replicatable;
    }

    INameService::TPtr Origin_;
    IRanking::TPtr Ranking_;
};

} // namespace

INameService::TPtr MakeColumnReplicatingService(INameService::TPtr origin, IRanking::TPtr ranking) {
    return new TNameService(std::move(origin), std::move(ranking));
}

} // namespace NSQLComplete
