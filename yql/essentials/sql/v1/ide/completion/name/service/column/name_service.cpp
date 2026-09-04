#include "name_service.h"

#include <yql/essentials/sql/v1/ide/completion/name/object/simple/static/schema.h>

namespace NSQLComplete {

namespace {

class TNameService: public INameService {
public:
    explicit TNameService(TVector<TColumnId> columns) {
        columns = Deduplicated(std::move(columns));

        TSchemaData data;
        for (auto& column : columns) {
            Tables_.emplace(column.TableAlias);

            data.Tables[""]["/" + Escaped(column.TableAlias)]
                .Columns
                .emplace_back(std::move(column.Name));
        }

        Schema_ = MakeSimpleSchema(MakeStaticSimpleSchema(std::move(data)));
    }

    NThreading::TFuture<TNameResponse> Lookup(const TNameRequest& request) const override {
        if (!request.Constraints.Column) {
            return NThreading::MakeFuture<TNameResponse>({});
        }

        TStringBuf alias = request.Constraints.Column->TableAlias;

        TNameResponse response;

        for (const TString& tableName : Tables_) {
            if (!alias.empty() && tableName != alias) {
                continue;
            }

            const auto& withoutByTableAlias = request.Constraints.Column->WithoutByTableAlias;

            THashSet<TString> without;
            if (auto it = withoutByTableAlias.find(tableName); it != withoutByTableAlias.end()) {
                without.insert(begin(it->second), end(it->second));
            }
            if (auto it = withoutByTableAlias.find(""); it != withoutByTableAlias.end()) {
                without.insert(begin(it->second), end(it->second));
            }

            TString columnPrefix = request.Prefix;
            if (tableName.StartsWith(request.Prefix)) {
                columnPrefix = "";
            }

            TDescribeTableRequest describeRequest = {
                .TableCluster = "",
                .TablePath = Escaped(tableName),
                .ColumnPrefix = columnPrefix,
                .ColumnsLimit = request.Limit,
            };

            TDescribeTableResponse table =
                Schema_
                    ->Describe(describeRequest)
                    .ExtractValue();

            Y_ENSURE(table.IsExisting);
            for (TString& column : table.Columns) {
                if (without.contains(column)) {
                    continue;
                }

                TColumnName name;
                name.TableAlias = tableName;
                name.Identifier = std::move(column);

                response.RankedNames.emplace_back(std::move(name));
            }
        }

        response.RankedNames.crop(request.Limit);

        return NThreading::MakeFuture(std::move(response));
    }

private:
    static TVector<TColumnId> Deduplicated(TVector<TColumnId> columns) {
        TVector<TColumnId*> ptrs(Reserve(columns.size()));
        for (auto& column : columns) {
            ptrs.emplace_back(&column);
        }

        SortUniqueBy(ptrs, [](const TColumnId* id) {
            TString alias = id->TableAlias;
            if (alias.empty()) {
                alias = ToString(reinterpret_cast<std::uintptr_t>(id));
            }

            return std::pair<TString, TStringBuf>(alias, id->Name);
        });

        TVector<TColumnId> deduplicated(Reserve(ptrs.size()));
        for (TColumnId* ptr : ptrs) {
            deduplicated.emplace_back(std::move(*ptr));
        }

        return deduplicated;
    }

    static TString Escaped(TString tableName) {
        if (tableName.empty()) {
            tableName.prepend("table_");
        }

        SubstGlobal(tableName, "/", "%2F");

        return tableName;
    }

    THashSet<TString> Tables_;
    ISchema::TPtr Schema_;
};

} // namespace

INameService::TPtr MakeColumnNameService(TVector<TColumnId> columns) {
    return new TNameService(std::move(columns));
}

} // namespace NSQLComplete
