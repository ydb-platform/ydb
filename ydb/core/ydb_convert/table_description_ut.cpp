#include "table_description.h"

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

#include <google/protobuf/text_format.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/strip.h>

#include <optional>

namespace NKikimr {

Y_UNIT_TEST_SUITE(ConvertTableDescription) {
<<<<<<< HEAD
=======
    NKikimrSchemeOp::TModifyScheme BuildInternalAlter(const NKikimrKqp::TKqpAlterTableRequest& request) {
        NKikimrSchemeOp::TModifyScheme result;
        auto status = Ydb::StatusIds::SUCCESS;
        TString error;
        UNIT_ASSERT_C(BuildAlterTableModifyScheme(&request, &result, TTableProfiles{}, TPathId{}, status, error), error);
        return result;
    }

    NKikimrSchemeOp::TModifyScheme BuildPublicAlterWithChangefeed(Ydb::Table::Changefeed changefeed) {
        TActorSystemStub actorSystem;
        Ydb::Table::AlterTableRequest request;
        request.set_path("/Root/Table");
        changefeed.set_name("feed");
        changefeed.set_mode(Ydb::Table::ChangefeedMode::MODE_NEW_AND_OLD_IMAGES);
        changefeed.set_format(Ydb::Table::ChangefeedFormat::FORMAT_JSON);
        *request.add_add_changefeeds() = std::move(changefeed);
        NKikimrSchemeOp::TModifyScheme result;
        auto status = Ydb::StatusIds::SUCCESS;
        TString error;
        UNIT_ASSERT_C(BuildAlterTableModifyScheme(&request, &result, TTableProfiles{}, TPathId{}, status, error), error);
        return result;
    }

    Y_UNIT_TEST(ChangefeedTopicMaxDefaultsToMin) {
        Ydb::Table::Changefeed changefeed;
        auto& settings = *changefeed.mutable_topic_partitioning_settings();
        settings.set_min_active_partitions(64);
        settings.mutable_auto_partitioning_settings()->set_strategy(
            Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);

        const auto result = BuildPublicAlterWithChangefeed(std::move(changefeed));
        const auto& stream = result.GetCreateCdcStream();
        UNIT_ASSERT_VALUES_EQUAL(stream.GetTopicPartitions(), 64);
        UNIT_ASSERT(stream.GetTopicAutoPartitioning());
        UNIT_ASSERT_VALUES_EQUAL(stream.GetMaxPartitionCount(), 64);
    }

    Y_UNIT_TEST(ChangefeedTopicUnsetMinMaxDefaultToFifty) {
        Ydb::Table::Changefeed changefeed;
        changefeed.mutable_topic_partitioning_settings()->mutable_auto_partitioning_settings()->set_strategy(
            Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);

        const auto result = BuildPublicAlterWithChangefeed(std::move(changefeed));
        const auto& stream = result.GetCreateCdcStream();
        UNIT_ASSERT_VALUES_EQUAL(stream.GetTopicPartitions(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetMaxPartitionCount(), 50);
    }

    Y_UNIT_TEST(ChangefeedTopicExplicitMaxKept) {
        Ydb::Table::Changefeed changefeed;
        auto& settings = *changefeed.mutable_topic_partitioning_settings();
        settings.set_min_active_partitions(10);
        settings.set_max_active_partitions(100);
        settings.mutable_auto_partitioning_settings()->set_strategy(
            Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);

        const auto result = BuildPublicAlterWithChangefeed(std::move(changefeed));
        const auto& stream = result.GetCreateCdcStream();
        UNIT_ASSERT_VALUES_EQUAL(stream.GetTopicPartitions(), 10);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetMaxPartitionCount(), 100);
    }

    Y_UNIT_TEST(ChangefeedTopicMinActivePartitionsOverflow) {
        // 2^32 + 64 fits the public int64 field but would silently truncate
        // to 64 in the uint32 internal fields.
        TActorSystemStub actorSystem;
        Ydb::Table::AlterTableRequest request;
        request.set_path("/Root/Table");
        auto& changefeed = *request.add_add_changefeeds();
        changefeed.set_name("feed");
        changefeed.set_mode(Ydb::Table::ChangefeedMode::MODE_NEW_AND_OLD_IMAGES);
        changefeed.set_format(Ydb::Table::ChangefeedFormat::FORMAT_JSON);
        auto& settings = *changefeed.mutable_topic_partitioning_settings();
        settings.set_min_active_partitions(4294967360LL);
        settings.mutable_auto_partitioning_settings()->set_strategy(
            Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);

        NKikimrSchemeOp::TModifyScheme result;
        auto status = Ydb::StatusIds::SUCCESS;
        TString error;
        UNIT_ASSERT(!BuildAlterTableModifyScheme(&request, &result, TTableProfiles{}, TPathId{}, status, error));
        UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(error, "must not exceed");
    }

    Y_UNIT_TEST(ChangefeedTopicMaxActivePartitionsOverflow) {
        TActorSystemStub actorSystem;
        Ydb::Table::AlterTableRequest request;
        request.set_path("/Root/Table");
        auto& changefeed = *request.add_add_changefeeds();
        changefeed.set_name("feed");
        changefeed.set_mode(Ydb::Table::ChangefeedMode::MODE_NEW_AND_OLD_IMAGES);
        changefeed.set_format(Ydb::Table::ChangefeedFormat::FORMAT_JSON);
        auto& settings = *changefeed.mutable_topic_partitioning_settings();
        settings.set_min_active_partitions(10);
        settings.set_max_active_partitions(4294967296LL); // 2^32
        settings.mutable_auto_partitioning_settings()->set_strategy(
            Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);

        NKikimrSchemeOp::TModifyScheme result;
        auto status = Ydb::StatusIds::SUCCESS;
        TString error;
        UNIT_ASSERT(!BuildAlterTableModifyScheme(&request, &result, TTableProfiles{}, TPathId{}, status, error));
        UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(error, "must not exceed");
    }

    Y_UNIT_TEST(InternalGeneratedColumnPreservesExpression) {
        TActorSystemStub actorSystem;
        for (const bool stored : {false, true}) {
            NKikimrKqp::TKqpAlterTableRequest request;
            request.set_path("/Root/Table");
            auto& column = *request.add_add_columns();
            column.SetName("generated");
            column.SetType("Int32");
            column.SetTypeId(NScheme::NTypeIds::Int32);
            column.SetDefaultKind(NKikimrKqp::TKqpColumnMetadataProto::DEFAULT_KIND_EXPRESSION);
            auto& expression = *column.MutableDefaultFromExpression();
            expression.SetExprText("a + 1");
            expression.AddDependencies("a");
            expression.SetStored(stored);

            const auto scheme = BuildInternalAlter(request);
            UNIT_ASSERT_VALUES_EQUAL(scheme.GetAlterTable().ColumnsSize(), 1);
            const auto& result = scheme.GetAlterTable().GetColumns(0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetName(), "generated");
            UNIT_ASSERT_C(result.HasDefaultFromExpression(), result.DebugString());
            const auto& generated = result.GetDefaultFromExpression();
            UNIT_ASSERT_VALUES_EQUAL(generated.GetExprText(), "a + 1");
            UNIT_ASSERT_VALUES_EQUAL(generated.DependencyColumnNamesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(generated.GetDependencyColumnNames(0), "a");
            UNIT_ASSERT_VALUES_EQUAL(generated.GetStored(), stored);
        }
    }

    Y_UNIT_TEST(InternalAlterDefaultsPreserveNullability) {
        TActorSystemStub actorSystem;
        NKikimrKqp::TKqpAlterTableRequest request;
        request.set_path("/Root/Dir/Table");
        using TColumn = NKikimrKqp::TKqpColumnMetadataProto;

        auto& literal = *request.add_alter_columns();
        literal.SetName("literal");
        literal.SetDefaultKind(TColumn::DEFAULT_KIND_LITERAL);
        literal.MutableDefaultFromLiteral()->mutable_type()->set_type_id(Ydb::Type::INT32);
        literal.MutableDefaultFromLiteral()->mutable_value()->set_int32_value(42);
        auto& relative = *request.add_alter_columns();
        relative.SetName("relative");
        relative.SetDefaultKind(TColumn::DEFAULT_KIND_SEQUENCE);
        relative.SetDefaultFromSequence("sequence");
        auto& absolute = *request.add_alter_columns();
        absolute.SetName("absolute");
        absolute.SetDefaultKind(TColumn::DEFAULT_KIND_SEQUENCE);
        absolute.SetDefaultFromSequence("/Root/Other/sequence");
        auto& clear = *request.add_alter_columns();
        clear.SetName("clear");
        clear.SetDefaultKind(TColumn::DEFAULT_KIND_NULL);
        auto& unchanged = *request.add_alter_columns();
        unchanged.SetName("unchanged");
        unchanged.AddFamily("family");
        auto& nullable = *request.add_alter_columns();
        nullable.SetName("nullable");
        nullable.SetNotNull(false);

        const auto result = BuildInternalAlter(request);
        UNIT_ASSERT_VALUES_EQUAL(result.GetWorkingDir(), "/Root/Dir");
        const auto& columns = result.GetAlterTable().GetColumns();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(columns[0].GetDefaultFromLiteral().value().int32_value(), 42);
        UNIT_ASSERT_VALUES_EQUAL(columns[1].GetDefaultFromSequence(), "/Root/Dir/sequence");
        UNIT_ASSERT_VALUES_EQUAL(columns[2].GetDefaultFromSequence(), "/Root/Other/sequence");
        UNIT_ASSERT(columns[3].HasEmptyDefault());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(columns[4].DefaultValue_case()),
            static_cast<int>(NKikimrSchemeOp::TColumnDescription::DEFAULTVALUE_NOT_SET));
        UNIT_ASSERT_VALUES_EQUAL(columns[4].GetFamilyName(), "family");
        for (size_t i = 0; i < 5; ++i) {
            UNIT_ASSERT_C(!columns[i].HasNotNull(), columns[i].DebugString());
            UNIT_ASSERT_C(!columns[i].HasType(), columns[i].DebugString());
        }
        UNIT_ASSERT(columns[5].HasNotNull());
        UNIT_ASSERT(!columns[5].GetNotNull());
    }

    Y_UNIT_TEST(InternalAddedColumnTypesAndMixedSettings) {
        TActorSystemStub actorSystem;
        NKikimrKqp::TKqpAlterTableRequest request;
        request.set_path("/Root/Table");
        auto& decimal = *request.add_add_columns();
        decimal.SetName("amount");
        decimal.SetType("Decimal(22,9)");
        decimal.SetTypeId(NScheme::NTypeIds::Decimal);
        decimal.MutableTypeInfo()->SetDecimalPrecision(22);
        decimal.MutableTypeInfo()->SetDecimalScale(9);
        decimal.AddFamily("family");
        auto& pg = *request.add_add_columns();
        pg.SetName("text");
        pg.SetType("pgvarchar");
        pg.SetTypeId(NScheme::NTypeIds::Pg);
        pg.MutableTypeInfo()->SetPgTypeId(1043);
        pg.MutableTypeInfo()->SetPgTypeMod("(32)");
        request.add_add_column_families()->set_name("family");
        request.set_set_key_bloom_filter(Ydb::FeatureFlag::ENABLED);
        request.mutable_drop_ttl_settings();
        request.add_drop_columns("old");

        const auto result = BuildInternalAlter(request);
        const auto& table = result.GetAlterTable();
        UNIT_ASSERT_VALUES_EQUAL(table.GetColumns(0).GetType(), "Decimal(22,9)");
        UNIT_ASSERT_VALUES_EQUAL(table.GetColumns(0).GetTypeInfo().GetDecimalPrecision(), 22);
        UNIT_ASSERT_VALUES_EQUAL(table.GetColumns(0).GetTypeInfo().GetDecimalScale(), 9);
        UNIT_ASSERT_VALUES_EQUAL(table.GetColumns(0).GetFamilyName(), "family");
        UNIT_ASSERT_VALUES_EQUAL(table.GetColumns(1).GetTypeInfo().GetPgTypeId(), 1043);
        UNIT_ASSERT_VALUES_EQUAL(table.GetColumns(1).GetTypeInfo().GetPgTypeMod(), "(32)");
        UNIT_ASSERT(table.GetPartitionConfig().GetEnableFilterByKey());
        UNIT_ASSERT(table.GetTTLSettings().HasDisabled());
        UNIT_ASSERT_VALUES_EQUAL(table.GetDropColumns(0).GetName(), "old");
    }

    Y_UNIT_TEST(InternalSqlColumnTypes) {
        NYql::TExprContext context;
        NKikimrKqp::TKqpColumnMetadataProto decimal;
        TString error;
        UNIT_ASSERT_C(NYql::SetColumnType(
            context.MakeType<NYql::TDataExprParamsType>(NYql::NUdf::EDataSlot::Decimal, "22", "9"),
            false, decimal, error), error);
        UNIT_ASSERT_VALUES_EQUAL(decimal.GetType(), "Decimal(22,9)");
        UNIT_ASSERT_VALUES_EQUAL(decimal.GetTypeId(), NScheme::NTypeIds::Decimal);
        UNIT_ASSERT_VALUES_EQUAL(decimal.GetTypeInfo().GetDecimalPrecision(), 22);
        UNIT_ASSERT_VALUES_EQUAL(decimal.GetTypeInfo().GetDecimalScale(), 9);
        UNIT_ASSERT(decimal.HasNotNull());
        UNIT_ASSERT(!decimal.GetNotNull());

        NKikimrKqp::TKqpColumnMetadataProto pg;
        UNIT_ASSERT_C(NYql::SetColumnType(context.MakeType<NYql::TPgExprType>(1043), false, pg, error), error);
        UNIT_ASSERT_VALUES_EQUAL(pg.GetTypeId(), NScheme::NTypeIds::Pg);
        UNIT_ASSERT_VALUES_EQUAL(pg.GetTypeInfo().GetPgTypeId(), 1043);
        UNIT_ASSERT(!pg.GetNotNull());
    }

    Y_UNIT_TEST(PublicAddedColumnNullabilityValidation) {
        TActorSystemStub actorSystem;
        actorSystem.AppData.FeatureFlags.SetEnableNotNullColumns(true);
        google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta> columns;
        auto& column = *columns.Add();
        column.set_name("value");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::INT32);
        column.set_not_null(true);
        NKikimrSchemeOp::TTableDescription result;
        auto status = Ydb::StatusIds::SUCCESS;
        TString error;
        UNIT_ASSERT(!FillColumnDescription(result, columns, status, error));
        UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(error, "Not consistent column type and not_null option");
    }

    Y_UNIT_TEST(PublicNullDefaultsCreateAndAddColumns) {
        TActorSystemStub actorSystem;
        actorSystem.AppData.FeatureFlags.SetEnableNotNullColumns(true);
        for (const TString type : {"type_id: INT32", "type_id: STRING",
            "decimal_type { precision: 22 scale: 9 }", "pg_type { type_name: 'pgtext' oid: 25 }"})
        {
            for (const bool notNull : {false, true}) {
                for (const bool optionalLiteral : {false, true}) {
                    google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta> columns;
                    *columns.Add() = NullDefaultColumn(type, notNull, optionalLiteral);
                    columns.Mutable(0)->set_family("family");
                    NKikimrSchemeOp::TTableDescription result;
                    auto status = Ydb::StatusIds::SUCCESS;
                    TString error;
                    UNIT_ASSERT_C(FillColumnDescription(result, columns, status, error), error);
                    UNIT_ASSERT_VALUES_EQUAL(result.ColumnsSize(), 1);
                    const auto& column = result.GetColumns(0);
                    UNIT_ASSERT_C(!column.HasDefaultFromLiteral(), column.DebugString());
                    UNIT_ASSERT_VALUES_EQUAL(column.GetNotNull(), notNull);
                    UNIT_ASSERT_VALUES_EQUAL(column.GetFamilyName(), "family");
                    UNIT_ASSERT_VALUES_EQUAL(column.GetName(), "value");
                }
            }
        }
    }

    Y_UNIT_TEST(PublicNullDefaultsAlterColumns) {
        TActorSystemStub actorSystem;
        for (const bool optionalLiteral : {false, true}) {
            Ydb::Table::AlterTableRequest request;
            request.set_path("/Root/Table");
            auto& column = *request.add_alter_columns();
            column = NullDefaultColumn("type_id: INT32", false, optionalLiteral);
            column.clear_type();
            column.clear_not_null();
            NKikimrSchemeOp::TModifyScheme result;
            auto status = Ydb::StatusIds::SUCCESS;
            TString error;
            UNIT_ASSERT_C(BuildAlterTableModifyScheme(&request, &result, TTableProfiles{}, TPathId{}, status, error), error);
            UNIT_ASSERT_VALUES_EQUAL(result.GetAlterTable().ColumnsSize(), 1);
            const auto& altered = result.GetAlterTable().GetColumns(0);
            UNIT_ASSERT_C(altered.HasEmptyDefault(), altered.DebugString());
            UNIT_ASSERT(!altered.HasDefaultFromLiteral());
            UNIT_ASSERT(!altered.HasNotNull());
            UNIT_ASSERT(!altered.HasType());
        }
    }

    Y_UNIT_TEST(PublicNonNullLiteralDefaultsPreserved) {
        TActorSystemStub actorSystem;
        google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta> columns;
        auto& zero = *columns.Add();
        zero = NullDefaultColumn("type_id: INT32", false, false);
        zero.set_name("zero");
        zero.mutable_from_literal()->mutable_value()->set_int32_value(0);
        auto& empty = *columns.Add();
        empty = NullDefaultColumn("type_id: STRING", false, true);
        empty.set_name("empty");
        empty.mutable_from_literal()->mutable_value()->set_bytes_value("");
        NKikimrSchemeOp::TTableDescription result;
        auto status = Ydb::StatusIds::SUCCESS;
        TString error;
        UNIT_ASSERT_C(FillColumnDescription(result, columns, status, error), error);
        UNIT_ASSERT(result.GetColumns(0).HasDefaultFromLiteral());
        UNIT_ASSERT(result.GetColumns(0).GetDefaultFromLiteral().value().has_int32_value());
        UNIT_ASSERT_VALUES_EQUAL(result.GetColumns(0).GetDefaultFromLiteral().value().int32_value(), 0);
        UNIT_ASSERT(result.GetColumns(1).HasDefaultFromLiteral());
        UNIT_ASSERT(result.GetColumns(1).GetDefaultFromLiteral().value().has_bytes_value());
        UNIT_ASSERT_VALUES_EQUAL(result.GetColumns(1).GetDefaultFromLiteral().value().bytes_value(), "");
    }

    Y_UNIT_TEST(PublicNullDefaultsColumnTableCreateAndAdd) {
        TActorSystemStub actorSystem;
        for (const bool notNull : {false, true}) {
            google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta> columns;
            *columns.Add() = NullDefaultColumn("type_id: INT32", notNull, true);
            NKikimrSchemeOp::TColumnTableDescription created;
            auto status = Ydb::StatusIds::SUCCESS;
            TString error;
            UNIT_ASSERT_C(FillColumnDescription(created, columns, status, error), error);
            UNIT_ASSERT_VALUES_EQUAL(created.GetSchema().GetColumns(0).GetNotNull(), notNull);
            NKikimrSchemeOp::TAlterColumnTable altered;
            UNIT_ASSERT_C(FillColumnDescription(altered, columns, status, error), error);
            UNIT_ASSERT_VALUES_EQUAL(altered.GetAlterSchema().AddColumnsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(altered.GetAlterSchema().GetAddColumns(0).GetNotNull(), notNull);
        }
    }

    Y_UNIT_TEST(PublicColumnTablesRejectDefaultChanges) {
        TActorSystemStub actorSystem;
        for (const bool drop : {false, true}) {
            Ydb::Table::AlterTableRequest request;
            request.set_path("/Root/Table");
            auto& column = *request.add_alter_columns();
            column = NullDefaultColumn("type_id: INT32", false, true);
            column.clear_type();
            column.clear_not_null();
            if (drop) {
                column.set_empty_default(NProtoBuf::NULL_VALUE);
            }
            NKikimrSchemeOp::TModifyScheme result;
            auto status = Ydb::StatusIds::SUCCESS;
            TString error;
            UNIT_ASSERT(!BuildAlterColumnTableModifyScheme(&request, &result, {}, status, error));
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(error, "Default values are not supported in column tables");
        }
    }

    Y_UNIT_TEST(ColumnTableNullabilityFromType) {
        TActorSystemStub actorSystem;

        google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta> columns;
        {
            // not_null is not set: NOT NULL is inferred from the non-optional type, as for row tables
            auto& column = *columns.Add();
            column.set_name("key");
            column.mutable_type()->set_type_id(Ydb::Type::INT64);
        }
        {
            auto& column = *columns.Add();
            column.set_name("value");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *columns.Add();
            column.set_name("explicit_not_null");
            column.mutable_type()->set_type_id(Ydb::Type::UINT32);
            column.set_not_null(true);
        }
        {
            auto& column = *columns.Add();
            column.set_name("explicit_nullable");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
            column.set_not_null(false);
        }

        NKikimrSchemeOp::TColumnTableDescription result;
        auto status = Ydb::StatusIds::SUCCESS;
        TString error;
        UNIT_ASSERT_C(FillColumnDescription(result, columns, status, error), error);

        const auto& schema = result.GetSchema();
        UNIT_ASSERT_VALUES_EQUAL(schema.ColumnsSize(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(0).GetName(), "key");
        UNIT_ASSERT(schema.GetColumns(0).GetNotNull());
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(1).GetName(), "value");
        UNIT_ASSERT(!schema.GetColumns(1).GetNotNull());
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(2).GetName(), "explicit_not_null");
        UNIT_ASSERT(schema.GetColumns(2).GetNotNull());
        UNIT_ASSERT_VALUES_EQUAL(schema.GetColumns(3).GetName(), "explicit_nullable");
        UNIT_ASSERT(!schema.GetColumns(3).GetNotNull());
    }

    Y_UNIT_TEST(ColumnTableRejectsInconsistentNullability) {
        TActorSystemStub actorSystem;

        const auto check = [](const bool optionalType, const bool notNull) {
            google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta> columns;
            auto& column = *columns.Add();
            column.set_name("value");
            if (optionalType) {
                column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::INT32);
            } else {
                column.mutable_type()->set_type_id(Ydb::Type::INT32);
            }
            column.set_not_null(notNull);

            NKikimrSchemeOp::TColumnTableDescription result;
            auto status = Ydb::StatusIds::SUCCESS;
            TString error;
            UNIT_ASSERT(!FillColumnDescription(result, columns, status, error));
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(error, "Not consistent column type and not_null option for column: value");
        };

        check(/* optionalType */ true, /* notNull */ true);
        check(/* optionalType */ false, /* notNull */ false);
    }

    Y_UNIT_TEST(ColumnTablesRejectSetNotNull) {
        TActorSystemStub actorSystem;
        actorSystem.AppData.FeatureFlags.SetEnableSetColumnConstraint(true);

        const auto check = [](auto request) {
            request.set_path("/Root/Table");
            request.add_set_not_null()->set_column_name("value");
            NKikimrSchemeOp::TModifyScheme result;
            auto status = Ydb::StatusIds::SUCCESS;
            TString error;

            UNIT_ASSERT(!BuildAlterColumnTableModifyScheme(&request, &result, {}, status, error));
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::UNSUPPORTED);
            UNIT_ASSERT_VALUES_EQUAL(error, "SET NOT NULL is not supported for column tables");
            UNIT_ASSERT(!result.HasOperationType());
            UNIT_ASSERT(!result.HasAlterColumnTable());
        };

        check(Ydb::Table::AlterTableRequest{});
        check(NKikimrKqp::TKqpAlterTableRequest{});
    }

>>>>>>> cb753730882 (schemeshard: default changefeed topic max partitions to max(min, 50) (#60524))
    Y_UNIT_TEST(ColumnCompressionRoundTrip) {
        using TCompression = Ydb::Table::ColumnCompression;
        for (const auto algorithm : {TCompression::ALGORITHM_OFF, TCompression::ALGORITHM_LZ4, TCompression::ALGORITHM_ZSTD}) {
            for (const auto level : {std::optional<int>{}, std::optional<int>{0}, std::optional<int>{-5}, std::optional<int>{5}}) {
                if (level && algorithm != TCompression::ALGORITHM_ZSTD) {
                    continue;
                }
                Ydb::Table::CreateTableRequest original;
                auto* column = original.add_columns();
                column->set_name("value");
                column->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
                column->mutable_compression()->set_algorithm(algorithm);
                if (level) {
                    column->mutable_compression()->set_compression_level(*level);
                }
                auto* defaultColumn = original.add_columns();
                defaultColumn->set_name("default_compression");
                *defaultColumn->mutable_type() = column->type();

                NKikimrSchemeOp::TColumnTableDescription internal;
                auto status = Ydb::StatusIds::SUCCESS;
                TString error;
                UNIT_ASSERT_C(FillColumnDescription(internal, original.columns(), status, error), error);

                Ydb::Table::CreateTableRequest exported;
                FillColumnDescription(exported, internal);
                Ydb::Table::DescribeTableResult described;
                FillColumnDescription(described, internal);
                for (const auto* restored : {&exported.columns(), &described.columns()}) {
                    UNIT_ASSERT_VALUES_EQUAL(restored->size(), 2);
                    const auto& compression = restored->Get(0).compression();
                    UNIT_ASSERT(compression.has_algorithm());
                    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(compression.algorithm()), static_cast<int>(algorithm));
                    UNIT_ASSERT_VALUES_EQUAL(compression.has_compression_level(), level.has_value());
                    if (level) {
                        UNIT_ASSERT_VALUES_EQUAL(compression.compression_level(), *level);
                    }
                    UNIT_ASSERT(!restored->Get(1).has_compression());
                }
            }
        }
    }

    template <typename T>
    struct TInvoker {
        T Value;

        template <typename F, typename... Args>
        explicit TInvoker(F&& f, Args&&... args)
            : Value(std::invoke(std::forward<F>(f), std::forward<Args>(args)...))
        { }

        T Result() {
            return Value;
        }
    };

    template <>
    struct TInvoker<void> {
        template <typename F, typename... Args>
        explicit TInvoker(F&& f, Args&&... args) {
            std::invoke(std::forward<F>(f), std::forward<Args>(args)...);
        }

        void Result() {}
    };

    template <typename TOut, typename TIn, typename R, typename... Args>
    using TConvertFn = R(*)(TOut&, const TIn&, Args...);

    template <typename TIn, typename TOut, typename R = void, typename... Args>
    auto Test(TConvertFn<TOut, TIn, R, Args...>&& fn, const TString& input, const TString& output, Args&&... args) {
        TIn in;
        google::protobuf::TextFormat::ParseFromString(input, &in);

        TOut out;
        auto ret = TInvoker<R>(fn, out, in, std::forward<Args>(args)...);

        TString result;
        google::protobuf::TextFormat::PrintToString(out, &result);

        UNIT_ASSERT_NO_DIFF(StripInPlace(result), Strip(output));
        ret.Result();
    }

    Y_UNIT_TEST(StorageSettings) {
        // no default family
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillStorageSettings, R"(
PartitionConfig {
  ColumnFamilies {
    Id: 1
    StorageConfig {
      SysLog {
        PreferredPoolKind: "ssd"
        AllowOtherKinds: false
      }
    }
  }
})", R"(
)");

        // non-strict storage pool
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillStorageSettings, R"(
PartitionConfig {
  ColumnFamilies {
    Id: 0
    StorageConfig {
      SysLog {
        PreferredPoolKind: "ssd"
        AllowOtherKinds: true
      }
    }
  }
})", R"(
storage_settings {
  store_external_blobs: DISABLED
}
)");

        // strict storage pools
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillStorageSettings, R"(
PartitionConfig {
  ColumnFamilies {
    Id: 0
    StorageConfig {
      SysLog {
        PreferredPoolKind: "ssd"
        AllowOtherKinds: false
      }
      Log {
        PreferredPoolKind: "ssd"
        AllowOtherKinds: false
      }
    }
  }
})", R"(
storage_settings {
  tablet_commit_log0 {
    media: "ssd"
  }
  tablet_commit_log1 {
    media: "ssd"
  }
  store_external_blobs: DISABLED
}
)");

        // external blobs
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillStorageSettings, R"(
PartitionConfig {
  ColumnFamilies {
    Id: 0
    StorageConfig {
      External {
        PreferredPoolKind: "hdd"
        AllowOtherKinds: false
      }
      ExternalThreshold: 1
      ExternalChannelsCount: 7
    }
  }
})", R"(
storage_settings {
  external {
    media: "hdd"
  }
  store_external_blobs: ENABLED
  external_data_channels_count: 7
}
)");
    }

    Y_UNIT_TEST(ColumnFamilies) {
        // default family, explicit id: 0
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    Id: 0
    StorageConfig {
      Data {
        PreferredPoolKind: "ssd"
      }
    }
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_NONE
}
)");

        // default family, without id & name
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    StorageConfig {
      Data {
        PreferredPoolKind: "ssd"
      }
    }
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_NONE
}
)");

        // named family
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    Name: "foo"
    StorageConfig {
      Data {
        PreferredPoolKind: "ssd"
      }
    }
  }
})", R"(
column_families {
  name: "foo"
  compression: COMPRESSION_NONE
}
)");

        // identified family
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    Id: 1
    StorageConfig {
      Data {
        PreferredPoolKind: "ssd"
      }
    }
  }
})", R"(
column_families {
  name: "<id: 1>"
  compression: COMPRESSION_NONE
}
)");

        // strict storage pool
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    StorageConfig {
      Data {
        PreferredPoolKind: "hdd"
        AllowOtherKinds: false
      }
    }
  }
})", R"(
column_families {
  name: "default"
  data {
    media: "hdd"
  }
  compression: COMPRESSION_NONE
}
)");

        // compression
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    ColumnCodec: ColumnCodecLZ4
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_LZ4
}
)");

        // legacy compression
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    Codec: 1
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_LZ4
}
)");

        // caching
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    ColumnCache: ColumnCacheEver
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_NONE
  keep_in_memory: ENABLED
}
)");

        // legacy caching
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    InMemory: true
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_NONE
  keep_in_memory: ENABLED
}
)");

        // cache mode
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    ColumnCacheMode: ColumnCacheModeTryKeepInMemory
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_NONE
  cache_mode: CACHE_MODE_IN_MEMORY
}
)");
    }

    using TFillIndexDescFn = void(*)(Ydb::Table::DescribeTableResult&, const NKikimrSchemeOp::TTableDescription&);

    Y_UNIT_TEST(IndexDescriptionLocalBloomFilter) {
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(
            static_cast<TFillIndexDescFn>(&FillIndexDescription), R"(
TableIndexes {
  Name: "bloom_idx"
  Type: EIndexTypeLocalBloomFilter
  State: EIndexStateReady
  KeyColumnNames: "data"
  BloomFilterDescription {
    FalsePositiveProbability: 0.05
  }
})", R"(
indexes {
  name: "bloom_idx"
  index_columns: "data"
  status: STATUS_READY
  local_bloom_filter_index {
    false_positive_probability: 0.05
  }
})");
    }

    Y_UNIT_TEST(IndexDescriptionLocalBloomNgramFilter) {
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(
            static_cast<TFillIndexDescFn>(&FillIndexDescription), R"(
TableIndexes {
  Name: "ngram_idx"
  Type: EIndexTypeLocalBloomNgramFilter
  State: EIndexStateReady
  KeyColumnNames: "text_col"
  BloomNGrammFilterDescription {
    NGrammSize: 3
    HashesCount: 2
    FilterSizeBytes: 512
    RecordsCount: 1024
    CaseSensitive: true
  }
})", R"(
indexes {
  name: "ngram_idx"
  index_columns: "text_col"
  status: STATUS_READY
  local_bloom_ngram_filter_index {
    ngram_size: 3
    case_sensitive: true
  }
})");
    }

    Y_UNIT_TEST(BloomFilterAlterTableReportsInvalidPath) {
        Ydb::Table::AlterTableRequest request;
        request.set_path("/Root/Table/");
        NKikimrSchemeOp::TModifyScheme result;
        auto status = Ydb::StatusIds::SUCCESS;
        TString error;

        UNIT_ASSERT(!BuildAlterTableBloomFilterModifyScheme(&request, &result, status, error));
        UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(error, "wrong path format '/Root/Table/'");
    }

    Y_UNIT_TEST(ColumnTableAlterReportsInvalidPath) {
        Ydb::Table::AlterTableRequest request;
        request.set_path("/Root/Table/");
        request.add_add_columns();
        NKikimrSchemeOp::TModifyScheme result;
        auto status = Ydb::StatusIds::SUCCESS;
        TString error;

        UNIT_ASSERT(!BuildAlterColumnTableModifyScheme(&request, &result, NYql::TKikimrTableMetadataPtr{}, status, error));
        UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(error, "wrong path format '/Root/Table/'");
    }

} // ConvertTableDescription

} // NKikimr
