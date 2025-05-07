#include "discovery.h"

namespace NSQLComplete {

    namespace {

        class TClusterDiscovery: public IClusterDiscovery {
        public:
            explicit TClusterDiscovery(TVector<TString> instances)
                : ClusterList_(std::move(instances))
            {
            }

            NThreading::TFuture<TClusterList> Query() const override {
                return NThreading::MakeFuture(ClusterList_);
            }

        private:
            TVector<TString> ClusterList_;
        };

    } // namespace

    IClusterDiscovery::TPtr MakeStaticClusterDiscovery(TVector<TString> instances) {
        return new TClusterDiscovery(std::move(instances));
    }

} // namespace NSQLComplete
