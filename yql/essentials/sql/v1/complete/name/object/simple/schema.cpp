#include "schema.h"

#include <util/charset/utf8.h>

namespace NSQLComplete {

    namespace {

        class TSimpleSchema: public ISchema {
        private:
            static auto FilterEntriesByName(TString name) {
                return [name = std::move(name)](auto f) {
                    TVector<TFolderEntry> entries = f.ExtractValue();
                    EraseIf(entries, [prefix = ToLowerUTF8(name)](const TFolderEntry& entry) {
                        return !entry.Name.StartsWith(prefix);
                    });
                    return entries;
                };
            }

            static auto FilterEntriesByTypes(TMaybe<THashSet<TString>> types) {
                return [types = std::move(types)](auto f) mutable {
                    TVector<TFolderEntry> entries = f.ExtractValue();
                    EraseIf(entries, [types = std::move(types)](const TFolderEntry& entry) {
                        return types && !types->contains(entry.Type);
                    });
                    return entries;
                };
            }

            static auto CropEntries(size_t limit) {
                return [limit](auto f) {
                    TVector<TFolderEntry> entries = f.ExtractValue();
                    entries.crop(limit);
                    return entries;
                };
            }

            static auto ToListResponse(TStringBuf name) {
                const auto length = name.length();
                return [length](auto f) {
                    return TListResponse{
                        .NameHintLength = length,
                        .Entries = f.ExtractValue(),
                    };
                };
            }

            static auto FilterColumnsByName(TString name) {
                return [name = std::move(name)](auto f) {
                    return f.ExtractValue().Transform([&](auto&& table) {
                        EraseIf(table.Columns, [prefix = ToLowerUTF8(name)](const TString& name) {
                            return !name.StartsWith(prefix);
                        });
                        return table;
                    });
                };
            }

            static auto CropColumns(size_t limit) {
                return [limit](auto f) {
                    return f.ExtractValue().Transform([&](auto&& table) {
                        table.Columns.crop(limit);
                        return table;
                    });
                };
            }

            static auto ToTableDescribeResponse() {
                return [](auto f) {
                    TMaybe<TTableDetails> table = f.ExtractValue();
                    return TDescribeTableResponse{
                        .IsExisting = table.Defined(),
                        .Columns = table
                                       .Transform([](auto&& table) { return table.Columns; })
                                       .GetOrElse({}),
                    };
                };
            }

        public:
            explicit TSimpleSchema(ISimpleSchema::TPtr simple)
                : Simple_(std::move(simple))
            {
            }

            NThreading::TFuture<TListResponse> List(const TListRequest& request) const override {
                auto [path, name] = Simple_->Split(request.Path);
                return Simple_->List(request.Cluster, TString(path))
                    .Apply(FilterEntriesByName(TString(name)))
                    .Apply(FilterEntriesByTypes(request.Filter.Types))
                    .Apply(CropEntries(request.Limit))
                    .Apply(ToListResponse(name));
            }

            NThreading::TFuture<TDescribeTableResponse>
            Describe(const TDescribeTableRequest& request) const override {
                return Simple_
                    ->DescribeTable(request.TableCluster, request.TablePath)
                    .Apply(FilterColumnsByName(TString(request.ColumnPrefix)))
                    .Apply(CropColumns(request.ColumnsLimit))
                    .Apply(ToTableDescribeResponse());
            }

        private:
            ISimpleSchema::TPtr Simple_;
        };

    } // namespace

    NThreading::TFuture<TVector<TFolderEntry>>
    ISimpleSchema::List(TString folder) const {
        return List(/* cluster = */ "", std::move(folder));
    }

    NThreading::TFuture<TVector<TFolderEntry>>
    ISimpleSchema::List(TString /* cluster */, TString folder) const {
        return List(std::move(folder));
    }

    NThreading::TFuture<TMaybe<TTableDetails>>
    ISimpleSchema::DescribeTable(const TString& cluster, const TString& path) const {
        Y_UNUSED(cluster, path);
        return NThreading::MakeFuture<TMaybe<TTableDetails>>(Nothing());
    }

    ISchema::TPtr MakeSimpleSchema(ISimpleSchema::TPtr simple) {
        return ISchema::TPtr(new TSimpleSchema(std::move(simple)));
    }

} // namespace NSQLComplete
