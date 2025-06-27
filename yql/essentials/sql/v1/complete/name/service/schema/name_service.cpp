#include "name_service.h"

#include <library/cpp/threading/future/wait/wait.h>
#include <library/cpp/iterator/functools.h>

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        public:
            explicit TNameService(ISchema::TPtr schema)
                : Schema_(std::move(schema))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) const override {
                if (request.Constraints.Object) {
                    return Schema_
                        ->List(ToListRequest(std::move(request)))
                        .Apply(ToListNameResponse);
                }

                if (request.Constraints.Column && !request.Constraints.Column->Tables.empty()) {
                    return BatchDescribe(
                        std::move(request.Constraints.Column->Tables),
                        request.Prefix,
                        request.Limit);
                }

                return NThreading::MakeFuture<TNameResponse>({});
            }

        private:
            NThreading::TFuture<TNameResponse> BatchDescribe(
                TVector<TTableId> tables, TString prefix, ui64 limit) const {
                TVector<NThreading::TFuture<TDescribeTableResponse>> futures;
                for (const auto& table : tables) {
                    TDescribeTableRequest request = {
                        .TableCluster = table.Cluster,
                        .TablePath = table.Path,
                        .ColumnPrefix = prefix,
                        .ColumnsLimit = limit,
                    };
                    futures.emplace_back(Schema_->Describe(request));
                }

                return NThreading::WaitAll(futures).Apply([tables, futures](auto) mutable {
                    TNameResponse response;

                    for (auto [table, f] : NFuncTools::Zip(tables, futures)) {
                        TDescribeTableResponse description = f.ExtractValue();
                        for (TString& column : description.Columns) {
                            TColumnName name;
                            name.Indentifier = std::move(column);
                            name.Table = table;

                            response.RankedNames.emplace_back(std::move(name));
                        }
                    }

                    return response;
                });
            }

            static TListRequest ToListRequest(TNameRequest request) {
                return {
                    .Cluster = ClusterName(*request.Constraints.Object),
                    .Path = request.Prefix,
                    .Filter = ToListFilter(request.Constraints),
                    .Limit = request.Limit,
                };
            }

            static TString ClusterName(const TObjectNameConstraints& constraints) {
                return constraints.Cluster;
            }

            static TListFilter ToListFilter(const TNameConstraints& constraints) {
                TListFilter filter;
                filter.Types = THashSet<TString>();
                for (auto kind : constraints.Object->Kinds) {
                    filter.Types->emplace(ToFolderEntry(kind));
                }
                return filter;
            }

            static TString ToFolderEntry(EObjectKind kind) {
                switch (kind) {
                    case EObjectKind::Folder:
                        return TFolderEntry::Folder;
                    case EObjectKind::Table:
                        return TFolderEntry::Table;
                }
            }

            static TNameResponse ToListNameResponse(NThreading::TFuture<TListResponse> f) {
                TListResponse list = f.ExtractValue();

                TNameResponse response;
                for (auto& entry : list.Entries) {
                    response.RankedNames.emplace_back(ToGenericName(std::move(entry)));
                }
                response.NameHintLength = list.NameHintLength;
                return response;
            }

            static TGenericName ToGenericName(TFolderEntry entry) {
                TGenericName name;
                if (entry.Type == TFolderEntry::Folder) {
                    TFolderName local;
                    local.Indentifier = std::move(entry.Name);
                    name = std::move(local);
                } else if (entry.Type == TFolderEntry::Table) {
                    TTableName local;
                    local.Indentifier = std::move(entry.Name);
                    name = std::move(local);
                } else {
                    TUnkownName local;
                    local.Content = std::move(entry.Name);
                    local.Type = std::move(entry.Type);
                    name = std::move(local);
                }
                return name;
            }

            ISchema::TPtr Schema_;
        };

    } // namespace

    INameService::TPtr MakeSchemaNameService(ISchema::TPtr schema) {
        return new TNameService(std::move(schema));
    }

} // namespace NSQLComplete
