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
                if (!request.Constraints.Object) {
                    return NThreading::MakeFuture<TNameResponse>({});
                }

                return Schema_
                    ->List(ToListRequest(std::move(request)))
                    .Apply(ToNameResponse);
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

            static TNameResponse ToNameResponse(NThreading::TFuture<TListResponse> f) {
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
