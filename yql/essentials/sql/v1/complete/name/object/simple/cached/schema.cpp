#include "schema.h"

#include <yql/essentials/sql/v1/complete/name/cache/cached.h>

namespace NSQLComplete {

    namespace {

        class TSimpleSchema: public ISimpleSchema {
        public:
            TSimpleSchema(
                ISchemaListCache::TPtr Cache,
                TString Zone,
                ISimpleSchema::TPtr Origin)
                : Zone_(std::move(Zone))
                , Origin_(std::move(Origin))
                , Query_(std::move(Cache), [origin = Origin_](const TSchemaListCacheKey& key) {
                    return origin->List(key.Cluster, key.Folder);
                })
            {
            }

            TSplittedPath Split(TStringBuf path) const override {
                return Origin_->Split(path);
            }

            NThreading::TFuture<TVector<TFolderEntry>>
            List(TString cluster, TString folder) const override {
                return Query_({
                    .Zone = Zone_,
                    .Cluster = std::move(cluster),
                    .Folder = std::move(folder),
                });
            }

        private:
            TString Zone_;
            ISimpleSchema::TPtr Origin_;
            TCachedQuery<TSchemaListCacheKey, TVector<TFolderEntry>> Query_;
        };

    } // namespace

    ISimpleSchema::TPtr MakeCachedSimpleSchema(
        ISchemaListCache::TPtr cache, TString zone, ISimpleSchema::TPtr origin) {
        return new TSimpleSchema(std::move(cache), std::move(zone), std::move(origin));
    }

} // namespace NSQLComplete
