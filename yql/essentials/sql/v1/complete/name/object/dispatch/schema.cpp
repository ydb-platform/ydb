#include "schema.h"

namespace NSQLComplete {

    namespace {

        class TSchema: public ISchema {
        public:
            explicit TSchema(THashMap<TString, ISchema::TPtr> mapping)
                : Mapping_(std::move(mapping))
            {
            }

            NThreading::TFuture<TListResponse> List(const TListRequest& request) const override {
                auto iter = Mapping_.find(request.Cluster);
                if (iter == std::end(Mapping_)) {
                    yexception e;
                    e << "unknown cluster '" << request.Cluster << "'";
                    std::exception_ptr p = std::make_exception_ptr(e);
                    return NThreading::MakeErrorFuture<TListResponse>(p);
                }

                return iter->second->List(request);
            }

        private:
            THashMap<TString, ISchema::TPtr> Mapping_;
        };

    } // namespace

    ISchema::TPtr MakeDispatchSchema(THashMap<TString, ISchema::TPtr> mapping) {
        return new TSchema(std::move(mapping));
    }

} // namespace NSQLComplete
