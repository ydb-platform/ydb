#include "name_service.h"

#include <library/cpp/case_insensitive_string/case_insensitive_string.h>

#include <util/charset/utf8.h>

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        private:
            static auto FilterByName(TString name) {
                return [name = std::move(name)](auto f) {
                    TClusterList clusters = f.ExtractValue();
                    EraseIf(clusters, [prefix = TCaseInsensitiveStringBuf(name)](const TString& instance) {
                        return !TCaseInsensitiveStringBuf(instance).StartsWith(prefix);
                    });
                    return clusters;
                };
            }

            static auto Crop(size_t limit) {
                return [limit](auto f) {
                    TClusterList clusters = f.ExtractValue();
                    clusters.crop(limit);
                    return clusters;
                };
            }

            static auto ToResponse(TNameConstraints constraints) {
                return [constraints = std::move(constraints)](auto f) {
                    TClusterList clusters = f.ExtractValue();

                    TNameResponse response;
                    response.RankedNames.reserve(clusters.size());

                    for (auto& cluster : clusters) {
                        TClusterName name;
                        name.Identifier = std::move(cluster);
                        response.RankedNames.emplace_back(std::move(name));
                    }

                    response.RankedNames = constraints.Unqualified(std::move(response.RankedNames));
                    return response;
                };
            }

        public:
            explicit TNameService(IClusterDiscovery::TPtr discovery)
                : Discovery_(std::move(discovery))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(const TNameRequest& request) const override {
                if (!request.Constraints.Cluster) {
                    return NThreading::MakeFuture<TNameResponse>({});
                }

                return Discovery_->Query()
                    .Apply(FilterByName(QualifiedClusterName(request)))
                    .Apply(Crop(request.Limit))
                    .Apply(ToResponse(request.Constraints));
            }

        private:
            static TString QualifiedClusterName(const TNameRequest& request) {
                TClusterName cluster;
                cluster.Identifier = request.Prefix;

                TGenericName generic = request.Constraints.Qualified(cluster);
                return std::get<TClusterName>(std::move(generic)).Identifier;
            }

            IClusterDiscovery::TPtr Discovery_;
        };

    } // namespace

    INameService::TPtr MakeClusterNameService(IClusterDiscovery::TPtr discovery) {
        return new TNameService(std::move(discovery));
    }

} // namespace NSQLComplete
