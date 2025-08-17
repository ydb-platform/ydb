#include "name_service.h"

#include <library/cpp/threading/future/wait/wait.h>
#include <library/cpp/iterator/iterate_values.h>

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        public:
            explicit TNameService(ISchema::TPtr schema)
                : Schema_(std::move(schema))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(const TNameRequest& request) const override {
                if (request.Constraints.Object) {
                    return Schema_
                        ->List(ToListRequest(std::move(request)))
                        .Apply(ToListNameResponse);
                }

                if (request.Constraints.Column && !request.Constraints.Column->Tables.empty()) {
                    return BatchDescribe(
                        std::move(request.Constraints.Column->Tables),
                        std::move(request.Constraints.Column->WithoutByTableAlias),
                        request.Prefix,
                        request.Limit);
                }

                return NThreading::MakeFuture<TNameResponse>({});
            }

        private:
            NThreading::TFuture<TNameResponse> BatchDescribe(
                TVector<TAliased<TTableId>> tables,
                THashMap<TString, THashSet<TString>> withoutByTableAlias,
                TString prefix,
                ui64 limit) const {
                THashMap<TTableId, TVector<TString>> aliasesByTable;
                for (TAliased<TTableId> table : std::move(tables)) {
                    aliasesByTable[std::move(static_cast<TTableId&>(table))]
                        .emplace_back(std::move(table.Alias));
                }

                THashMap<TTableId, NThreading::TFuture<TDescribeTableResponse>> futuresByTable;
                for (const auto& [table, aliases] : aliasesByTable) {
                    TString columnPrefix = prefix;
                    for (const auto& alias : aliases) {
                        if (alias.StartsWith(prefix)) {
                            columnPrefix = "";
                            break;
                        }
                    }

                    TDescribeTableRequest request = {
                        .TableCluster = table.Cluster,
                        .TablePath = table.Path,
                        .ColumnPrefix = columnPrefix,
                        .ColumnsLimit = limit,
                    };

                    futuresByTable.emplace(table, Schema_->Describe(request));
                }

                auto futuresIt = IterateValues(futuresByTable);
                TVector<NThreading::TFuture<TDescribeTableResponse>> futures(begin(futuresIt), end(futuresIt));

                return NThreading::WaitAll(std::move(futures))
                    .Apply([aliasesByTable = std::move(aliasesByTable),
                            futuresByTable = std::move(futuresByTable),
                            withoutByTableAlias = std::move(withoutByTableAlias)](auto) mutable {
                        TNameResponse response;

                        for (auto [table, f] : futuresByTable) {
                            TDescribeTableResponse description = f.ExtractValue();
                            for (const TString& column : description.Columns) {
                                const auto& aliases = aliasesByTable[table];
                                for (const TString& alias : aliases) {
                                    if ((withoutByTableAlias[alias].contains(column)) ||
                                        (alias.empty() && 1 < aliases.size())) {
                                        continue;
                                    }

                                    TColumnName name;
                                    name.Identifier = column;
                                    name.TableAlias = alias;

                                    response.RankedNames.emplace_back(std::move(name));
                                }
                            }
                        }

                        return response;
                    });
            }

            static TListRequest ToListRequest(const TNameRequest& request) {
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
                const auto& kinds = constraints.Object->Kinds;

                TListFilter filter;
                filter.Types = THashSet<TString>();
                filter.IsUnknownAllowed = kinds.contains(EObjectKind::Unknown);
                for (EObjectKind kind : kinds) {
                    if (kind == EObjectKind::Unknown) {
                        continue;
                    }

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
                    case EObjectKind::Unknown:
                        ythrow yexception()
                            << "Unknown is a type class and "
                            << "can not be a folder entry";
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
                    local.Identifier = std::move(entry.Name);
                    name = std::move(local);
                } else if (entry.Type == TFolderEntry::Table) {
                    TTableName local;
                    local.Identifier = std::move(entry.Name);
                    name = std::move(local);
                } else {
                    TUnknownName local;
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
