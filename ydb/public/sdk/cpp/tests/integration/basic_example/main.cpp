#include "basic_example.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <google/protobuf/text_format.h>

#include <util/string/cast.h>

static void ValidateResultSet(const std::vector<NYdb::TColumn>& columns,
                              const std::vector<std::vector<NYdb::TValue>>& values,
                              const NYdb::TResultSet& rs) {
    Ydb::ResultSet protoResultSet;
    for (const auto& column : columns) {
        auto* protoColumn = protoResultSet.add_columns();
        protoColumn->set_name(column.Name);
        *protoColumn->mutable_type() = column.Type.GetProto();
    }

    for (const auto& row : values) {
        auto* protoRow = protoResultSet.add_rows();
        for (const auto& value : row) {
            *protoRow->add_items() = value.GetProto();
        }
    }

    NYdb::TStringType expected, result;
    google::protobuf::TextFormat::PrintToString(protoResultSet, &expected);
    google::protobuf::TextFormat::PrintToString(NYdb::TProtoAccessor::GetProto(rs), &result);

    ASSERT_EQ(expected, result);
}


static NYdb::TType MakeOptionalType(NYdb::EPrimitiveType type) {
    return NYdb::TTypeBuilder().BeginOptional().Primitive(type).EndOptional().Build();
}


TEST(BasicExample, BasicExample) {
    auto [driver, path] = GetRunArgs();

    NYdb::NTable::TTableClient client(driver);

    try {
        CreateTables(client, path);

        NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync([path](NYdb::NTable::TSession session) {
            return FillTableDataTransaction(session, path);
        }));

        {
            std::vector<NYdb::TColumn> columns = {
                {"series_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"title", MakeOptionalType(NYdb::EPrimitiveType::Utf8)},
                {"release_date", MakeOptionalType(NYdb::EPrimitiveType::String)},
            };

            std::vector<std::vector<NYdb::TValue>> values = {{
                NYdb::TValueBuilder().Uint64(1).Build(),
                NYdb::TValueBuilder().Utf8("IT Crowd").Build(),
                NYdb::TValueBuilder().String("2006-02-03").Build(),
            }};

            ValidateResultSet(columns, values, SelectSimple(client, path));
        }

        UpsertSimple(client, path);

        {
            std::vector<NYdb::TColumn> columns = {
                {"season_title", MakeOptionalType(NYdb::EPrimitiveType::Utf8)},
                {"series_title", MakeOptionalType(NYdb::EPrimitiveType::Utf8)},
            };

            std::vector<std::vector<NYdb::TValue>> values = {{
                NYdb::TValueBuilder().Utf8("Season 3").Build(),
                NYdb::TValueBuilder().Utf8("Silicon Valley").Build(),
            }};

            ValidateResultSet(columns, values, SelectWithParams(client, path));
        }

        {
            std::vector<NYdb::TColumn> columns = {
                {"air_date", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"episode_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"season_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"series_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"title", MakeOptionalType(NYdb::EPrimitiveType::Utf8)},
            };

            std::vector<std::vector<NYdb::TValue>> values = {{
                NYdb::TValueBuilder().Uint64(16957).Build(),
                NYdb::TValueBuilder().Uint64(7).Build(),
                NYdb::TValueBuilder().Uint64(3).Build(),
                NYdb::TValueBuilder().Uint64(2).Build(),
                NYdb::TValueBuilder().Utf8("To Build a Better Beta").Build(),
            }};

            ValidateResultSet(columns, values, PreparedSelect(client, path, 2, 3, 7));
        }

        {
            std::vector<NYdb::TColumn> columns = {
                {"air_date", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"episode_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"season_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"series_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"title", MakeOptionalType(NYdb::EPrimitiveType::Utf8)},
            };

            std::vector<std::vector<NYdb::TValue>> values = {{
                NYdb::TValueBuilder().Uint64(16964).Build(),
                NYdb::TValueBuilder().Uint64(8).Build(),
                NYdb::TValueBuilder().Uint64(3).Build(),
                NYdb::TValueBuilder().Uint64(2).Build(),
                NYdb::TValueBuilder().Utf8("Bachman's Earnings Over-Ride").Build(),
            }};

            ValidateResultSet(columns, values, PreparedSelect(client, path, 2, 3, 8));
        }

        {
            std::vector<NYdb::TColumn> columns = {
                {"season_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"episode_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"title", MakeOptionalType(NYdb::EPrimitiveType::Utf8)},
                {"air_date", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
            };

            std::vector<std::vector<NYdb::TValue>> values = {
                {
                    NYdb::TValueBuilder().Uint64(5).Build(),
                    NYdb::TValueBuilder().Uint64(1).Build(),
                    NYdb::TValueBuilder().Utf8("Grow Fast or Die Slow").Build(),
                    NYdb::TValueBuilder().Uint64(17615).Build(),
                },
                {
                    NYdb::TValueBuilder().Uint64(5).Build(),
                    NYdb::TValueBuilder().Uint64(2).Build(),
                    NYdb::TValueBuilder().Utf8("Reorientation").Build(),
                    NYdb::TValueBuilder().Uint64(17622).Build(),
                },
                {
                    NYdb::TValueBuilder().Uint64(5).Build(),
                    NYdb::TValueBuilder().Uint64(3).Build(),
                    NYdb::TValueBuilder().Utf8("Chief Operating Officer").Build(),
                    NYdb::TValueBuilder().Uint64(17629).Build(),
                },
            };

            ValidateResultSet(columns, values, MultiStep(client, path));
        }

        ExplicitTcl(client, path);

        {
            std::vector<NYdb::TColumn> columns = {
                {"air_date", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"episode_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"season_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"series_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"title", MakeOptionalType(NYdb::EPrimitiveType::Utf8)},
            };

            std::vector<std::vector<NYdb::TValue>> values = {{
                NYdb::TValueBuilder().Uint64(0).Build(),
                NYdb::TValueBuilder().Uint64(1).Build(),
                NYdb::TValueBuilder().Uint64(6).Build(),
                NYdb::TValueBuilder().Uint64(2).Build(),
                NYdb::TValueBuilder().Utf8("TBD").Build(),
            }};

            ValidateResultSet(columns, values, PreparedSelect(client, path, 2, 6, 1));
        }

        {
            std::vector<NYdb::TColumn> columns = {
                {"series_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"season_id", MakeOptionalType(NYdb::EPrimitiveType::Uint64)},
                {"title", MakeOptionalType(NYdb::EPrimitiveType::Utf8)},
                {"first_aired", MakeOptionalType(NYdb::EPrimitiveType::String)},
            };

            std::vector<std::vector<NYdb::TValue>> values = {
                {
                    NYdb::TValueBuilder().Uint64(1).Build(),
                    NYdb::TValueBuilder().Uint64(1).Build(),
                    NYdb::TValueBuilder().Utf8("Season 1").Build(),
                    NYdb::TValueBuilder().String("2006-02-03").Build(),
                },
                {
                    NYdb::TValueBuilder().Uint64(1).Build(),
                    NYdb::TValueBuilder().Uint64(2).Build(),
                    NYdb::TValueBuilder().Utf8("Season 2").Build(),
                    NYdb::TValueBuilder().String("2007-08-24").Build(),
                },
                {
                    NYdb::TValueBuilder().Uint64(1).Build(),
                    NYdb::TValueBuilder().Uint64(3).Build(),
                    NYdb::TValueBuilder().Utf8("Season 3").Build(),
                    NYdb::TValueBuilder().String("2008-11-21").Build(),
                },
                {
                    NYdb::TValueBuilder().Uint64(1).Build(),
                    NYdb::TValueBuilder().Uint64(4).Build(),
                    NYdb::TValueBuilder().Utf8("Season 4").Build(),
                    NYdb::TValueBuilder().String("2010-06-25").Build(),
                },
            };

            std::string expected = "{\"series_id\":1,\"season_id\":1,\"title\":\"Season 1\",\"first_aired\":\"2006-02-03\"}\n{\"series_id\":1,\"season_id\":2,\"title\":\"Season 2\",\"first_aired\":\"2007-08-24\"}\n{\"series_id\":1,\"season_id\":3,\"title\":\"Season 3\",\"first_aired\":\"2008-11-21\"}\n{\"series_id\":1,\"season_id\":4,\"title\":\"Season 4\",\"first_aired\":\"2010-06-25\"}\n";
            auto result = ScanQuerySelect(client, path);
            ASSERT_EQ(result.size(), size_t(1));
            ValidateResultSet(columns, values, result[0]);
        }
    } catch (const NYdb::NStatusHelpers::TYdbErrorException& e) {
        FAIL() << "Execution failed due to fatal error:\n" << e.what() << std::endl;
    }
}
