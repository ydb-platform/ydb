#include "table_description.h"
#include "table_settings.h"

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/protos/table_metrics_settings.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/list.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(ConvertTableMetricsSettings) {
    using TApi = Ydb::Table::MetricsSettings;
    using TInternal = NKikimrSchemeOp::TTableDetailedMetricsSettings;
    using TTableDescription = NKikimrSchemeOp::TTableDescription;

    bool Fill(TTableDescription& t, const Ydb::Table::CreateTableRequest& r, Ydb::StatusIds::StatusCode& code, TString& error) {
        TList<TString> warnings;
        return FillCreateTableSettingsDesc(t, r, code, error, warnings, false);
    }
    bool Fill(TTableDescription& t, const Ydb::Table::AlterTableRequest& r, Ydb::StatusIds::StatusCode& code, TString& error) {
        return FillAlterTableSettingsDesc(t, r, code, error, false);
    }
    template <class TRequest>
    void ExpectFill(TTableDescription& t, const TRequest& r, Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS) {
        auto code = Ydb::StatusIds::SUCCESS;
        TString error;
        const bool ok = Fill(t, r, code, error);
        UNIT_ASSERT_VALUES_EQUAL_C(ok, expected == Ydb::StatusIds::SUCCESS, error);
        if (!ok) {
            UNIT_ASSERT_VALUES_EQUAL(code, expected);
            UNIT_ASSERT(!error.empty());
        }
    }
    void ExpectLevel(const TTableDescription& t, TInternal::EMetricsLevel expected) {
        UNIT_ASSERT_C(t.GetDetailedMetricsSettings().HasConfigured(), t.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(t.GetDetailedMetricsSettings().GetConfigured().GetMetricsLevel()), static_cast<int>(expected));
    }

    struct TLevelCase {
        TApi::MetricsLevel Api;
        TInternal::EMetricsLevel Internal;
    };

    constexpr TLevelCase Levels[] = {
        {TApi::METRICS_LEVEL_UNSPECIFIED, TInternal::MetricsLevelDisabled},
        {TApi::METRICS_LEVEL_DATABASE, TInternal::MetricsLevelDisabled},
        {TApi::METRICS_LEVEL_TABLE, TInternal::MetricsLevelTable},
        {TApi::METRICS_LEVEL_PARTITION, TInternal::MetricsLevelPartition},
    };

    Y_UNIT_TEST(CreateMetricsLevels) {
        for (const auto& level : Levels) {
            Ydb::Table::CreateTableRequest request;
            request.mutable_metrics_settings()->set_metrics_level(level.Api);
            TTableDescription table;
            ExpectFill(table, request);
            ExpectLevel(table, level.Internal);

            // Export/Describe must retain explicit off as DATABASE, including
            // when the request relied on the default inside an empty message.
            Ydb::Table::DescribeTableResult described;
            FillMetricsSettings(described, table);
            const auto expected = level.Api == TApi::METRICS_LEVEL_UNSPECIFIED
                ? TApi::METRICS_LEVEL_DATABASE : level.Api;
            UNIT_ASSERT(described.has_metrics_settings());
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(described.metrics_settings().metrics_level()), static_cast<int>(expected));

            Ydb::Table::CreateTableRequest exported;
            FillMetricsSettings(exported, table);
            TTableDescription restored;
            ExpectFill(restored, exported);
            ExpectLevel(restored, level.Internal);
        }
    }

    Y_UNIT_TEST(AlterMetricsLevels) {
        for (const auto& level : Levels) {
            Ydb::Table::AlterTableRequest request;
            request.mutable_set_metrics_settings()->set_metrics_level(level.Api);
            TTableDescription table;
            ExpectFill(table, request);
            ExpectLevel(table, level.Internal);
        }
    }

    Y_UNIT_TEST(OmittedEmptyAndResetMetricsSettings) {
        Ydb::Table::CreateTableRequest create;
        TTableDescription table;
        ExpectFill(table, create);
        UNIT_ASSERT(!table.HasDetailedMetricsSettings());

        create.mutable_metrics_settings();
        ExpectFill(table, create);
        ExpectLevel(table, TInternal::MetricsLevelDisabled);

        table.MutableDetailedMetricsSettings()->MutableConfigured()->SetMetricsLevel(TInternal::MetricsLevelTable);
        Ydb::Table::AlterTableRequest alter;
        ExpectFill(table, alter);
        ExpectLevel(table, TInternal::MetricsLevelTable);

        alter.mutable_set_metrics_settings();
        ExpectFill(table, alter);
        ExpectLevel(table, TInternal::MetricsLevelDisabled);

        alter.mutable_drop_metrics_settings();
        ExpectFill(table, alter);
        UNIT_ASSERT(table.GetDetailedMetricsSettings().HasNotConfigured());
    }

    Y_UNIT_TEST(RejectInvalidMetricsLevelsWithoutClearingOverride) {
        for (const int level : {int(TApi::METRICS_LEVEL_DISABLED), 99, -1}) {
            Ydb::Table::CreateTableRequest create;
            create.mutable_metrics_settings()->set_metrics_level(static_cast<TApi::MetricsLevel>(level));
            TTableDescription table;
            ExpectFill(table, create, Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT(!table.HasDetailedMetricsSettings());

            table.MutableDetailedMetricsSettings()->MutableConfigured()->SetMetricsLevel(TInternal::MetricsLevelPartition);
            Ydb::Table::AlterTableRequest alter;
            alter.mutable_set_metrics_settings()->set_metrics_level(static_cast<TApi::MetricsLevel>(level));
            ExpectFill(table, alter, Ydb::StatusIds::BAD_REQUEST);
            ExpectLevel(table, TInternal::MetricsLevelPartition);
        }
    }

    Y_UNIT_TEST(ColumnTableMetricsSettingsRejected) {
        auto code = Ydb::StatusIds::SUCCESS;
        TString error;
        Ydb::Table::CreateTableRequest create;
        create.mutable_metrics_settings();
        NKikimrSchemeOp::TColumnTableDescription table;
        UNIT_ASSERT(!FillCreateTableSettingsDesc(table, create, code, error));
        UNIT_ASSERT_VALUES_EQUAL(code, Ydb::StatusIds::BAD_REQUEST);

        for (bool drop : {false, true}) {
            Ydb::Table::AlterTableRequest alter;
            alter.add_drop_columns("value");
            if (drop) {
                alter.mutable_drop_metrics_settings();
            } else {
                alter.mutable_set_metrics_settings()->set_metrics_level(TApi::METRICS_LEVEL_DATABASE);
            }
            NKikimrSchemeOp::TModifyScheme modify;
            const TIntrusivePtr<NYql::TKikimrTableMetadata> metadata;
            code = Ydb::StatusIds::SUCCESS;
            error.clear();
            UNIT_ASSERT(!BuildAlterColumnTableModifyScheme("/Root/Table", &alter, &modify, metadata, code, error));
            UNIT_ASSERT_VALUES_EQUAL(code, Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(error, "Metrics settings are not supported");
            UNIT_ASSERT(!modify.HasAlterColumnTable());
        }
    }
}

} // namespace NKikimr
