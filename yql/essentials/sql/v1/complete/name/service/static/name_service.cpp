#include "name_service.h"

#include "name_index.h"

#include <yql/essentials/sql/v1/complete/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/complete/name/service/union/name_service.h>
#include <yql/essentials/sql/v1/complete/text/case.h>

namespace NSQLComplete {

    const TVector<TStringBuf> FilteredByPrefix(
        const TString& prefix,
        const TVector<TString>& sorted Y_LIFETIME_BOUND) {
        auto [first, last] = EqualRange(
            std::begin(sorted), std::end(sorted),
            prefix, NoCaseCompareLimit(prefix.size()));
        return TVector<TStringBuf>(first, last);
    }

    template <class T, class S = TStringBuf>
    void AppendAs(TVector<TGenericName>& target, const TVector<S>& source) {
        for (const auto& element : source) {
            T name;

            TString* content = nullptr;
            if constexpr (std::is_same_v<TKeyword, T>) {
                content = &name.Content;
            } else {
                content = &name.Indentifier;
            }

            *content = element;

            target.emplace_back(std::move(name));
        }
    }

    template <class T>
    void NameIndexScan(
        const TNameIndex& index,
        const TString& prefix,
        const TNameConstraints& constraints,
        TVector<TGenericName>& out) {
        T name;
        name.Indentifier = prefix;
        name = std::get<T>(constraints.Qualified(std::move(name)));

        AppendAs<T>(out, FilteredByPrefix(name.Indentifier, index, NormalizeName));
        out = constraints.Unqualified(std::move(out));
    }

    class IRankingNameService: public INameService {
    private:
        auto Ranking(TNameRequest request) const {
            return [request = std::move(request), this](auto f) {
                TNameResponse response = f.ExtractValue();
                Ranking_->CropToSortedPrefix(
                    response.RankedNames,
                    request.Constraints,
                    request.Limit);
                return response;
            };
        }

    public:
        explicit IRankingNameService(IRanking::TPtr ranking)
            : Ranking_(std::move(ranking))
        {
        }

        NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) const override {
            return LookupAllUnranked(request).Apply(Ranking(request));
        }

        virtual NThreading::TFuture<TNameResponse> LookupAllUnranked(TNameRequest request) const = 0;

    private:
        IRanking::TPtr Ranking_;
    };

    class TKeywordNameService: public IRankingNameService {
    public:
        explicit TKeywordNameService(IRanking::TPtr ranking)
            : IRankingNameService(std::move(ranking))
        {
        }

        NThreading::TFuture<TNameResponse> LookupAllUnranked(TNameRequest request) const override {
            TNameResponse response;
            Sort(request.Keywords, NoCaseCompare);
            AppendAs<TKeyword>(
                response.RankedNames,
                FilteredByPrefix(request.Prefix, request.Keywords));
            return NThreading::MakeFuture<TNameResponse>(std::move(response));
        }
    };

    class TPragmaNameService: public IRankingNameService {
    public:
        TPragmaNameService(IRanking::TPtr ranking, TVector<TString> pragmas)
            : IRankingNameService(std::move(ranking))
            , Pragmas_(BuildNameIndex(std::move(pragmas), NormalizeName))
        {
        }

        NThreading::TFuture<TNameResponse> LookupAllUnranked(TNameRequest request) const override {
            TNameResponse response;
            if (request.Constraints.Pragma) {
                NameIndexScan<TPragmaName>(
                    Pragmas_,
                    request.Prefix,
                    request.Constraints,
                    response.RankedNames);
            }
            return NThreading::MakeFuture<TNameResponse>(std::move(response));
        }

    private:
        TNameIndex Pragmas_;
    };

    class TTypeNameService: public IRankingNameService {
    public:
        TTypeNameService(IRanking::TPtr ranking, TVector<TString> types)
            : IRankingNameService(std::move(ranking))
            , SimpleTypes_(BuildNameIndex(std::move(types), NormalizeName))
            , ContainerTypes_(BuildNameIndex(
                  {
                      "Optional",
                      "Tuple",
                      "Struct",
                      "Variant",
                      "List",
                      "Stream",
                      "Flow",
                      "Dict",
                      "Set",
                      "Enum",
                      "Resource",
                      "Tagged",
                      "Callable",
                  }, NormalizeName))
            , ParameterizedTypes_(BuildNameIndex(
                  {
                      "Decimal",
                  }, NormalizeName))
        {
        }

        NThreading::TFuture<TNameResponse> LookupAllUnranked(TNameRequest request) const override {
            TNameResponse response;
            if (request.Constraints.Type) {
                NameIndexScan<TTypeName>(SimpleTypes_, request.Prefix, request.Constraints, response.RankedNames);

                size_t previousSize = response.RankedNames.size();
                NameIndexScan<TTypeName>(ContainerTypes_, request.Prefix, request.Constraints, response.RankedNames);
                for (size_t i = previousSize; i < response.RankedNames.size(); ++i) {
                    std::get<TTypeName>(response.RankedNames[i]).Kind = TTypeName::EKind::Container;
                }

                previousSize = response.RankedNames.size();
                NameIndexScan<TTypeName>(ParameterizedTypes_, request.Prefix, request.Constraints, response.RankedNames);
                for (size_t i = previousSize; i < response.RankedNames.size(); ++i) {
                    std::get<TTypeName>(response.RankedNames[i]).Kind = TTypeName::EKind::Parameterized;
                }
            }
            return NThreading::MakeFuture<TNameResponse>(std::move(response));
        }

    private:
        TNameIndex SimpleTypes_;
        TNameIndex ContainerTypes_;
        TNameIndex ParameterizedTypes_;
    };

    class TFunctionNameService: public IRankingNameService {
    public:
        TFunctionNameService(IRanking::TPtr ranking, TVector<TString> functions)
            : IRankingNameService(std::move(ranking))
            , Functions_(BuildNameIndex(std::move(functions), NormalizeName))
        {
        }

        NThreading::TFuture<TNameResponse> LookupAllUnranked(TNameRequest request) const override {
            TNameResponse response;
            if (request.Constraints.Function) {
                NameIndexScan<TFunctionName>(
                    Functions_,
                    request.Prefix,
                    request.Constraints,
                    response.RankedNames);
            }
            return NThreading::MakeFuture<TNameResponse>(std::move(response));
        }

    private:
        TNameIndex Functions_;
    };

    class THintNameService: public IRankingNameService {
    public:
        THintNameService(
            IRanking::TPtr ranking,
            THashMap<EStatementKind, TVector<TString>> hints)
            : IRankingNameService(std::move(ranking))
            , Hints_([hints = std::move(hints)] {
                THashMap<EStatementKind, TNameIndex> index;
                for (auto& [k, hints] : hints) {
                    index.emplace(k, BuildNameIndex(std::move(hints), NormalizeName));
                }
                return index;
            }())
        {
        }

        NThreading::TFuture<TNameResponse> LookupAllUnranked(TNameRequest request) const override {
            TNameResponse response;
            if (request.Constraints.Hint) {
                const auto stmt = request.Constraints.Hint->Statement;
                if (const auto* hints = Hints_.FindPtr(stmt)) {
                    NameIndexScan<THintName>(
                        *hints,
                        request.Prefix,
                        request.Constraints,
                        response.RankedNames);
                }
            }
            return NThreading::MakeFuture<TNameResponse>(std::move(response));
        }

    private:
        THashMap<EStatementKind, TNameIndex> Hints_;
    };

    INameService::TPtr MakeStaticNameService(TNameSet names, TFrequencyData frequency) {
        return MakeStaticNameService(
            Pruned(std::move(names), frequency),
            MakeDefaultRanking(std::move(frequency)));
    }

    INameService::TPtr MakeStaticNameService(TNameSet names, IRanking::TPtr ranking) {
        TVector<INameService::TPtr> children = {
            new TKeywordNameService(ranking),
            new TPragmaNameService(ranking, std::move(names.Pragmas)),
            new TTypeNameService(ranking, std::move(names.Types)),
            new TFunctionNameService(ranking, std::move(names.Functions)),
            new THintNameService(ranking, std::move(names.Hints)),
        };
        return MakeUnionNameService(std::move(children), ranking);
    }

} // namespace NSQLComplete
