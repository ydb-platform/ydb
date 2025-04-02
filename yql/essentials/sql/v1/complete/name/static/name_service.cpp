#include "name_service.h"

namespace NSQLComplete {

    const TVector<TStringBuf> FilteredByPrefix(
        const TString& prefix,
        const TVector<TString>& sorted Y_LIFETIME_BOUND) {
        auto [first, last] = EqualRange(
            std::begin(sorted),
            std::end(sorted),
            prefix,
            [&](const TString& lhs, const TString& rhs) {
                return strncasecmp(lhs.data(), rhs.data(), prefix.size()) < 0;
            });

        return TVector<TStringBuf>(first, last);
    }

    template <class T>
    void AppendAs(TVector<TGenericName>& target, const TVector<TStringBuf>& source) {
        for (const auto& element : source) {
            target.emplace_back(T{TString(element)});
        }
    }

    size_t KindWeight(const TGenericName& name) {
        return std::visit([](const auto& name) {
            using T = std::decay_t<decltype(name)>;
            if constexpr (std::is_same_v<T, TFunctionName>) {
                return 1;
            }
            if constexpr (std::is_same_v<T, TTypeName>) {
                return 2;
            }
        }, name);
    }

    const TStringBuf ContentView(const TGenericName& name Y_LIFETIME_BOUND) {
        return std::visit([](const auto& name) -> TStringBuf {
            using T = std::decay_t<decltype(name)>;
            if constexpr (std::is_base_of_v<TIndentifier, T>) {
                return name.Indentifier;
            }
        }, name);
    }

    void Sort(TVector<TGenericName>& names) {
        Sort(names, [](const TGenericName& lhs, const TGenericName& rhs) {
            const auto lhs_weight = KindWeight(lhs);
            const auto lhs_content = ContentView(lhs);

            const auto rhs_weight = KindWeight(rhs);
            const auto rhs_content = ContentView(rhs);

            return std::tie(lhs_weight, lhs_content) <
                   std::tie(rhs_weight, rhs_content);
        });
    }

    class TStaticNameService: public INameService {
    public:
        explicit TStaticNameService(NameSet names)
            : NameSet_(std::move(names))
        {
            Sort(NameSet_.Types);
            Sort(NameSet_.Functions);
        }

        TFuture<TNameResponse> Lookup(TNameRequest request) override {
            TNameResponse response;

            if (request.Constraints.TypeName) {
                AppendAs<TTypeName>(response.RankedNames,
                                    FilteredByPrefix(request.Prefix, NameSet_.Types));
            }

            if (request.Constraints.Function) {
                AppendAs<TFunctionName>(
                    response.RankedNames,
                    FilteredByPrefix(request.Prefix, NameSet_.Functions));
            }

            Sort(response.RankedNames);

            response.RankedNames.crop(request.Limit);
            return NThreading::MakeFuture(std::move(response));
        }

    private:
        NameSet NameSet_;
    };

    INameService::TPtr MakeStaticNameService(NameSet names) {
        return INameService::TPtr(new TStaticNameService(std::move(names)));
    }

} // namespace NSQLComplete
