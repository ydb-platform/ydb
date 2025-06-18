#include "name_service.h"

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

                if (request.Constraints.Column) {
                    Y_ENSURE(request.Constraints.Column->Tables.size() == 1, "Not Implemented");
                    TTableId table = request.Constraints.Column->Tables[0];
                    return Schema_
                        ->Describe({
                            .TableCluster = table.Cluster,
                            .TablePath = table.Path,
                            .ColumnPrefix = request.Prefix,
                            .ColumnsLimit = request.Limit,
                        })
                        .Apply(ToDescribeNameResponse);
                }

                return NThreading::MakeFuture<TNameResponse>({});
            }

        private:
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

            static TNameResponse ToDescribeNameResponse(NThreading::TFuture<TDescribeTableResponse> f) {
                TDescribeTableResponse table = f.ExtractValue();

                TNameResponse response;
                for (TString& column : table.Columns) {
                    TColumnName name;
                    name.Indentifier = std::move(column);
                    response.RankedNames.emplace_back(std::move(name));
                }
                return response;
            }

            ISchema::TPtr Schema_;
        };

    } // namespace

    INameService::TPtr MakeSchemaNameService(ISchema::TPtr schema) {
        return new TNameService(std::move(schema));
    }

} // namespace NSQLComplete
