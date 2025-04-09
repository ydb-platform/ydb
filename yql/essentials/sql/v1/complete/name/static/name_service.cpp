#include "name_service.h"

#include "ranking.h"

namespace NSQLComplete {

    bool NoCaseCompare(const TString& lhs, const TString& rhs) {
        return std::lexicographical_compare(
            std::begin(lhs), std::end(lhs),
            std::begin(rhs), std::end(rhs),
            [](const char lhs, const char rhs) {
                return ToLower(lhs) < ToLower(rhs);
            });
    }

    auto NoCaseCompareLimit(size_t size) {
        return [size](const TString& lhs, const TString& rhs) -> bool {
            return strncasecmp(lhs.data(), rhs.data(), size) < 0;
        };
    }

    const TVector<TStringBuf> FilteredByPrefix(
        const TString& prefix,
        const TVector<TString>& sorted Y_LIFETIME_BOUND) {
        auto [first, last] = EqualRange(
            std::begin(sorted), std::end(sorted),
            prefix, NoCaseCompareLimit(prefix.size()));
        return TVector<TStringBuf>(first, last);
    }

    template <class T>
    void AppendAs(TVector<TGenericName>& target, const TVector<TStringBuf>& source) {
        for (const auto& element : source) {
            target.emplace_back(T{TString(element)});
        }
    }

    TString Prefixed(const TStringBuf requestPrefix, const TStringBuf delimeter, const TNamespaced& namespaced) {
        TString prefix;
        if (!namespaced.Namespace.empty()) {
            prefix += namespaced.Namespace;
            prefix += delimeter;
        }
        prefix += requestPrefix;
        return prefix;
    }

    void FixPrefix(TString& name, const TStringBuf delimeter, const TNamespaced& namespaced) {
        if (namespaced.Namespace.empty()) {
            return;
        }
        name.remove(0, namespaced.Namespace.size() + delimeter.size());
    }

    void FixPrefix(TGenericName& name, const TNameRequest& request) {
        std::visit([&](auto& name) -> size_t {
            using T = std::decay_t<decltype(name)>;
            if constexpr (std::is_same_v<T, TPragmaName>) {
                FixPrefix(name.Indentifier, ".", *request.Constraints.Pragma);
            }
            if constexpr (std::is_same_v<T, TFunctionName>) {
                FixPrefix(name.Indentifier, "::", *request.Constraints.Function);
            }
            return 0;
        }, name);
    }

    class TStaticNameService: public INameService {
    public:
        explicit TStaticNameService(NameSet names, IRanking::TPtr ranking)
            : NameSet_(std::move(names))
            , Ranking_(std::move(ranking))
        {
            Sort(NameSet_.Pragmas, NoCaseCompare);
            Sort(NameSet_.Types, NoCaseCompare);
            Sort(NameSet_.Functions, NoCaseCompare);
        }

        TFuture<TNameResponse> Lookup(TNameRequest request) override {
            TNameResponse response;

            if (request.Constraints.Pragma) {
                auto prefix = Prefixed(request.Prefix, ".", *request.Constraints.Pragma);
                auto names = FilteredByPrefix(prefix, NameSet_.Pragmas);
                AppendAs<TPragmaName>(response.RankedNames, names);
            }

            if (request.Constraints.Type) {
                AppendAs<TTypeName>(
                    response.RankedNames,
                    FilteredByPrefix(request.Prefix, NameSet_.Types));
            }

            if (request.Constraints.Function) {
                auto prefix = Prefixed(request.Prefix, "::", *request.Constraints.Function);
                auto names = FilteredByPrefix(prefix, NameSet_.Functions);
                AppendAs<TFunctionName>(response.RankedNames, names);
            }

            Ranking_->CropToSortedPrefix(response.RankedNames, request.Limit);

            for (auto& name : response.RankedNames) {
                FixPrefix(name, request);
            }

            return NThreading::MakeFuture(std::move(response));
        }

    private:
        NameSet NameSet_;
        IRanking::TPtr Ranking_;
    };

    INameService::TPtr MakeStaticNameService() {
        return MakeStaticNameService(MakeDefaultNameSet(), MakeDefaultRanking());
    }

    INameService::TPtr MakeStaticNameService(NameSet names, IRanking::TPtr ranking) {
        return INameService::TPtr(new TStaticNameService(std::move(names), std::move(ranking)));
    }

} // namespace NSQLComplete
