#include "dummy_name_service.h"

#include <library/cpp/iterator/functools.h>

namespace NYdb::NConsoleClient {

    namespace {

        class TNameService: public NSQLComplete::INameService {
        public:
            NThreading::TFuture<NSQLComplete::TNameResponse>
            Lookup(NSQLComplete::TNameRequest request) const override {
                NSQLComplete::TNameResponse response;

                if (!request.Constraints.Object) {
                    return NThreading::MakeFuture(std::move(response));
                }

                auto kinds = request.Constraints.Object->Kinds;
                if (kinds.contains(NSQLComplete::EObjectKind::Folder)) {
                    response.RankedNames.emplace_back(Folder());
                }
                if (kinds.contains(NSQLComplete::EObjectKind::Table)) {
                    response.RankedNames.emplace_back(Table());
                }

                return NThreading::MakeFuture(std::move(response));
            }

        private:
            static NSQLComplete::TFolderName Folder() {
                NSQLComplete::TFolderName name;
                name.Indentifier = "folder_name";
                return std::move(name);
            }

            static NSQLComplete::TTableName Table() {
                NSQLComplete::TTableName name;
                name.Indentifier = "table_name";
                return std::move(name);
            }
        };

    } // namespace

    NSQLComplete::INameService::TPtr MakeDummyNameService() {
        return new TNameService();
    }

} // namespace NYdb::NConsoleClient
