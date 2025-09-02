#include "name_service.h"

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        public:
            explicit TNameService(INameService::TPtr origin)
                : Origin_(std::move(origin))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(const TNameRequest& request) const override {
                auto future = Origin_->Lookup(request);
                if (future.IsReady()) {
                    return future;
                }
                return NThreading::MakeFuture<TNameResponse>({});
            }

        private:
            INameService::TPtr Origin_;
        };

    } // namespace

    INameService::TPtr MakeImpatientNameService(INameService::TPtr origin) {
        return new TNameService(std::move(origin));
    }

} // namespace NSQLComplete
