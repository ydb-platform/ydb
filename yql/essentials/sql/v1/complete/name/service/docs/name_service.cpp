#include "name_service.h"

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        public:
            TNameService(IDocumentation::TPtr docs, INameService::TPtr origin)
                : Docs_(std::move(docs))
                , Origin_(std::move(origin))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(const TNameRequest& request) const override {
                return Origin_->Lookup(request).Apply([docs = Docs_](auto f) {
                    TNameResponse response = f.ExtractValue();
                    for (TGenericName& name : response.RankedNames) {
                        name = std::visit([&](auto&& name) -> TGenericName {
                            using T = std::decay_t<decltype(name)>;

                            if constexpr (std::is_base_of_v<TDescribed, T> &&
                                          std::is_base_of_v<TIdentifier, T>) {
                                name.Description = docs->Lookup(name.Identifier);
                            }

                            return std::move(name);
                        }, std::move(name));
                    }
                    return response;
                });
            }

        private:
            IDocumentation::TPtr Docs_;
            INameService::TPtr Origin_;
        };

    } // namespace

    INameService::TPtr MakeDocumentingNameService(IDocumentation::TPtr docs, INameService::TPtr origin) {
        return new TNameService(std::move(docs), std::move(origin));
    }

} // namespace NSQLComplete
