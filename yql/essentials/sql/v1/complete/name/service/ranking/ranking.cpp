#include "ranking.h"

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

#include <util/charset/utf8.h>

namespace NSQLComplete {

    class TRanking: public IRanking {
    private:
        struct TRow {
            TGenericName Name;
            size_t Weight;
        };

    public:
        explicit TRanking(TFrequencyData frequency)
            : Frequency_(std::move(frequency))
        {
        }

        void CropToSortedPrefix(
            TVector<TGenericName>& names,
            const TNameConstraints& constraints,
            size_t limit) const override {
            limit = Min(limit, names.size());

            TVector<TRow> rows;
            rows.reserve(names.size());
            for (TGenericName& name : names) {
                name = constraints.Qualified(std::move(name));
                size_t weight = Weight(name);
                rows.emplace_back(std::move(name), weight);
            }

            ::PartialSort(
                std::begin(rows), std::begin(rows) + limit, std::end(rows),
                [this](const TRow& lhs, const TRow& rhs) {
                    const size_t lhs_weight = ReversedWeight(lhs.Weight);
                    const auto lhs_content = ContentView(lhs.Name);

                    const size_t rhs_weight = ReversedWeight(rhs.Weight);
                    const auto rhs_content = ContentView(rhs.Name);

                    return std::tie(lhs_weight, lhs_content) <
                           std::tie(rhs_weight, rhs_content);
                });

            names.crop(limit);
            rows.crop(limit);

            for (size_t i = 0; i < limit; ++i) {
                names[i] = constraints.Unqualified(std::move(rows[i].Name));
            }
        }

    private:
        size_t Weight(const TGenericName& name) const {
            return std::visit([this](const auto& name) -> size_t {
                using T = std::decay_t<decltype(name)>;

                auto content = NormalizeName(ContentView(name));

                if constexpr (std::is_same_v<T, TKeyword>) {
                    if (auto weight = Frequency_.Keywords.FindPtr(content)) {
                        return *weight;
                    }
                }

                if constexpr (std::is_same_v<T, TPragmaName>) {
                    if (auto weight = Frequency_.Pragmas.FindPtr(content)) {
                        return *weight;
                    }
                }

                if constexpr (std::is_same_v<T, TFunctionName>) {
                    if (auto weight = Frequency_.Functions.FindPtr(content)) {
                        return *weight;
                    }
                }

                if constexpr (std::is_same_v<T, TTypeName>) {
                    if (auto weight = Frequency_.Types.FindPtr(content)) {
                        return *weight;
                    }
                }

                if constexpr (std::is_same_v<T, THintName>) {
                    if (auto weight = Frequency_.Hints.FindPtr(content)) {
                        return *weight;
                    }
                }

                if constexpr (std::is_same_v<T, TFolderName> ||
                              std::is_same_v<T, TTableName> ||
                              std::is_same_v<T, TColumnName>) {
                    return std::numeric_limits<size_t>::max();
                }

                if constexpr (std::is_same_v<T, TClusterName>) {
                    return std::numeric_limits<size_t>::max() - 8;
                }

                return 0;
            }, name);
        }

        static size_t ReversedWeight(size_t weight) {
            return std::numeric_limits<size_t>::max() - weight;
        }

        TStringBuf ContentView(const TGenericName& name Y_LIFETIME_BOUND) const {
            return std::visit([](const auto& name) -> TStringBuf {
                using T = std::decay_t<decltype(name)>;
                if constexpr (std::is_base_of_v<TKeyword, T>) {
                    return name.Content;
                }
                if constexpr (std::is_base_of_v<TIdentifier, T>) {
                    return name.Identifier;
                }
                if constexpr (std::is_base_of_v<TUnknownName, T>) {
                    return name.Content;
                }
            }, name);
        }

        TFrequencyData Frequency_;
    };

    IRanking::TPtr MakeDefaultRanking() {
        return MakeDefaultRanking(LoadFrequencyData());
    }

    IRanking::TPtr MakeDefaultRanking(const TFrequencyData& frequency) {
        return MakeIntrusive<TRanking>(Pruned(frequency));
    }

} // namespace NSQLComplete
