#include "ranking.h"

#include "frequency.h"

#include <yql/essentials/sql/v1/complete/name/name_service.h>

#include <util/charset/utf8.h>

namespace NSQLComplete {

    class TRanking: public IRanking {
    private:
        struct TRow {
            TGenericName Name;
            size_t Weight;
        };

    public:
        TRanking(TFrequencyData frequency)
            : Frequency_(std::move(frequency))
        {
        }

        void CropToSortedPrefix(TVector<TGenericName>& names, size_t limit) override {
            limit = std::min(limit, names.size());

            TVector<TRow> rows;
            rows.reserve(names.size());
            for (TGenericName& name : names) {
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
                names[i] = std::move(rows[i].Name);
            }
        }

    private:
        size_t Weight(const TGenericName& name) const {
            return std::visit([this](const auto& name) -> size_t {
                using T = std::decay_t<decltype(name)>;

                auto identifier = ToLowerUTF8(ContentView(name));

                if constexpr (std::is_same_v<T, TFunctionName>) {
                    if (auto weight = Frequency_.Functions.FindPtr(identifier)) {
                        return *weight;
                    }
                }

                if constexpr (std::is_same_v<T, TTypeName>) {
                    if (auto weight = Frequency_.Types.FindPtr(identifier)) {
                        return *weight;
                    }
                }

                return 0;
            }, name);
        }

        static size_t ReversedWeight(size_t weight) {
            return std::numeric_limits<size_t>::max() - weight;
        }

        const TStringBuf ContentView(const TGenericName& name Y_LIFETIME_BOUND) const {
            return std::visit([](const auto& name) -> TStringBuf {
                using T = std::decay_t<decltype(name)>;
                if constexpr (std::is_base_of_v<TIndentifier, T>) {
                    return name.Indentifier;
                }
            }, name);
        }

        TFrequencyData Frequency_;
    };

    IRanking::TPtr MakeDefaultRanking() {
        return IRanking::TPtr(new TRanking(LoadFrequencyData()));
    }

    IRanking::TPtr MakeDefaultRanking(TFrequencyData frequency) {
        return IRanking::TPtr(new TRanking(frequency));
    }

} // namespace NSQLComplete
