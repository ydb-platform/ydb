#include "name_service.h"

#include <library/cpp/threading/future/wait/wait.h>

namespace NSQLComplete {

namespace {

class TNameService: public INameService {
public:
    TNameService(
        TVector<INameService::TPtr> children,
        IRanking::TPtr ranking)
        : Children_(std::move(children))
        , Ranking_(std::move(ranking))
    {
    }

    NThreading::TFuture<TNameResponse> Lookup(const TNameRequest& request) const override {
        TVector<NThreading::TFuture<TNameResponse>> fs;
        for (const auto& c : Children_) {
            fs.emplace_back(c->Lookup(request));
        }

        return NThreading::WaitAll(fs)
            // TODO(YQL-20095): Explore real problem to fix this.
            // NOLINTNEXTLINE(bugprone-exception-escape)
            .Apply([fs, ranking = Ranking_, request](auto) {
                return Union(fs, ranking, request.Constraints, request.Limit);
            });
    }

private:
    static TNameResponse Union(
        TVector<NThreading::TFuture<TNameResponse>> fs,
        IRanking::TPtr ranking,
        const TNameConstraints& constraints,
        size_t limit) {
        TNameResponse united;
        for (auto f : fs) {
            TNameResponse response = f.ExtractValue();

            std::ranges::move(
                response.RankedNames,
                std::back_inserter(united.RankedNames));

            if (!response.IsEmpty() && response.NameHintLength) {
                Y_ENSURE(
                    united.NameHintLength.Empty() ||
                    united.NameHintLength == response.NameHintLength);
                united.NameHintLength = response.NameHintLength;
            }
        }
        ranking->CropToSortedPrefix(united.RankedNames, constraints, limit);
        return united;
    }

    TVector<INameService::TPtr> Children_;
    IRanking::TPtr Ranking_;
};

} // namespace

INameService::TPtr MakeUnionNameService(
    TVector<INameService::TPtr> children,
    IRanking::TPtr ranking) {
    return INameService::TPtr(new TNameService(
        std::move(children),
        std::move(ranking)));
}

} // namespace NSQLComplete
