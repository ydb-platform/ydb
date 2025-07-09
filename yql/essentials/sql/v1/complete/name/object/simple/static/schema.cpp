#include "schema.h"

#include <library/cpp/iterator/concatenate.h>

namespace NSQLComplete {

    namespace {

        class TSimpleSchema: public ISimpleSchema {
        public:
            explicit TSimpleSchema(
                THashMap<TString, THashMap<TString, TVector<TFolderEntry>>> folders,
                THashMap<TString, THashMap<TString, TTableDetails>> tables)
                : Folders_(std::move(folders))
                , Tables_(std::move(tables))
            {
                for (const auto& [_, paths] : Folders_) {
                    for (const auto& [k, _] : paths) {
                        Y_ENSURE(k.StartsWith("/"), k << " must start with the '/'");
                        Y_ENSURE(k.EndsWith("/"), k << " must end with the '/'");
                    }
                }

                for (const auto& [_, paths] : Tables_) {
                    for (const auto& [k, _] : paths) {
                        Y_ENSURE(k.StartsWith("/"), k << " must start with the '/'");
                        Y_ENSURE(!k.EndsWith("/"), k << " must not end with the '/'");
                    }
                }
            }

            TSplittedPath Split(TStringBuf path) const override {
                size_t pos = path.find_last_of('/');
                if (pos == TString::npos) {
                    return {"", path};
                }

                TStringBuf head, tail;
                TStringBuf(path).SplitAt(pos + 1, head, tail);
                return {head, tail};
            }

            NThreading::TFuture<TVector<TFolderEntry>> List(TString cluster, TString folder) const override {
                TVector<TFolderEntry> entries;

                const THashMap<TString, TVector<TFolderEntry>>* tables = nullptr;
                const TVector<TFolderEntry>* items = nullptr;
                if ((tables = Folders_.FindPtr(cluster)) &&
                    (items = tables->FindPtr(Qualified(folder)))) {
                    entries = *items;
                }

                return NThreading::MakeFuture(std::move(entries));
            }

            NThreading::TFuture<TMaybe<TTableDetails>>
            DescribeTable(const TString& cluster, const TString& path) const override {
                auto* tables = Tables_.FindPtr(cluster);
                if (tables == nullptr) {
                    return NThreading::MakeFuture<TMaybe<TTableDetails>>(Nothing());
                }

                auto* details = tables->FindPtr(Qualified(path));
                if (details == nullptr) {
                    return NThreading::MakeFuture<TMaybe<TTableDetails>>(Nothing());
                }

                return NThreading::MakeFuture<TMaybe<TTableDetails>>(*details);
            }

        private:
            TString Qualified(TString path) const {
                if (!path.StartsWith('/')) {
                    path.prepend('/');
                }
                return path;
            }

            THashMap<TString, THashMap<TString, TVector<TFolderEntry>>> Folders_;
            THashMap<TString, THashMap<TString, TTableDetails>> Tables_;
        };

    } // namespace

    ISimpleSchema::TPtr MakeStaticSimpleSchema(TSchemaData data) {
        return new TSimpleSchema(std::move(data.Folders), std::move(data.Tables));
    }

} // namespace NSQLComplete
