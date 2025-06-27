#include "name_service.h"

#include <yql/essentials/sql/v1/complete/name/object/simple/static/schema.h>

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        public:
            explicit TNameService(TVector<TColumnId> columns) {
                TSchemaData data;
                for (auto& column : columns) {
                    Tables_.emplace(column.TableAlias);

                    data.Tables[""]["/" + Escaped(column.TableAlias)]
                        .Columns
                        .emplace_back(std::move(column.Name));
                }

                Schema_ = MakeSimpleSchema(MakeStaticSimpleSchema(std::move(data)));
            }

            NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) const override {
                if (!request.Constraints.Column) {
                    return NThreading::MakeFuture<TNameResponse>({});
                }

                TNameResponse response;

                for (const TString& tableName : Tables_) {
                    TDescribeTableRequest describeRequest = {
                        .TableCluster = "",
                        .TablePath = Escaped(tableName),
                        .ColumnPrefix = request.Prefix,
                        .ColumnsLimit = request.Limit,
                    };

                    TDescribeTableResponse table =
                        Schema_
                            ->Describe(std::move(describeRequest))
                            .ExtractValue();

                    Y_ENSURE(table.IsExisting);
                    for (TString& column : table.Columns) {
                        TColumnName name;
                        name.TableAlias = tableName;
                        name.Indentifier = std::move(column);

                        response.RankedNames.emplace_back(std::move(name));
                    }
                }

                response.RankedNames.crop(request.Limit);

                return NThreading::MakeFuture(std::move(response));
            }

        private:
            static TString Escaped(TString tableName) {
                // Saves when name is empty
                tableName.prepend("table_");
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
