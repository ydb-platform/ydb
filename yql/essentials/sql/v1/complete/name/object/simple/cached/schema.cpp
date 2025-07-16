#include "schema.h"

#include <yql/essentials/sql/v1/complete/name/cache/cached.h>

namespace NSQLComplete {

    namespace {

        class TSimpleSchema: public ISimpleSchema {
        public:
            TSimpleSchema(
                TSchemaCaches caches,
                TString Zone,
                ISimpleSchema::TPtr Origin)
                : Zone_(std::move(Zone))
                , Origin_(std::move(Origin))
                , QueryList_(std::move(caches.List), [origin = Origin_](const TSchemaDescribeCacheKey& key) {
                    return origin->List(key.Cluster, key.Path);
                })
                , QueryDescribeTable_(std::move(caches.DescribeTable), [origin = Origin_](const TSchemaDescribeCacheKey& key) {
                    return origin->DescribeTable(key.Cluster, key.Path);
                })
            {
            }

            TSplittedPath Split(TStringBuf path) const override {
                return Origin_->Split(path);
            }

            NThreading::TFuture<TVector<TFolderEntry>>
            List(TString cluster, TString folder) const override {
                return QueryList_({
                    .Zone = Zone_,
                    .Cluster = std::move(cluster),
                    .Path = std::move(folder),
                });
            }

            NThreading::TFuture<TMaybe<TTableDetails>>
            DescribeTable(const TString& cluster, const TString& path) const override {
                return QueryDescribeTable_({
                    .Zone = Zone_,
                    .Cluster = cluster,
                    .Path = path,
                });
            }

        private:
            TString Zone_;
            ISimpleSchema::TPtr Origin_;
            TCachedQuery<TSchemaDescribeCacheKey, TVector<TFolderEntry>> QueryList_;
            TCachedQuery<TSchemaDescribeCacheKey, TMaybe<TTableDetails>> QueryDescribeTable_;
        };

    } // namespace

    ISimpleSchema::TPtr MakeCachedSimpleSchema(
        ISchemaListCache::TPtr cache, TString zone, ISimpleSchema::TPtr origin) {
        return new TSimpleSchema({.List = std::move(cache)}, std::move(zone), std::move(origin));
    }

    ISimpleSchema::TPtr MakeCachedSimpleSchema(TSchemaCaches caches, TString zone, ISimpleSchema::TPtr origin) {
        return new TSimpleSchema(std::move(caches), std::move(zone), std::move(origin));
    }

} // namespace NSQLComplete
