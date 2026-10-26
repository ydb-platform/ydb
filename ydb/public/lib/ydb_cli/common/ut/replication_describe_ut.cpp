#include <ydb/public/lib/ydb_cli/common/describe.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>
#include <util/string/strip.h>

using namespace NYdb::NConsoleClient;

namespace {
TString Render(const Ydb::Replication::DescribeReplicationResult& proto, bool stats = true) {
    TStringStream output;
    TDescribeOptions options;
    options.ShowStats = stats;
    options.Database = "/Root";
    PrintReplicationDescription(output, NYdb::NReplication::TReplicationDescription(proto), options);
    return output.Str();
}

TVector<TString> TargetCells(const TString& output, TStringBuf marker) {
    TStringBuf remaining(output);
    TStringBuf row;
    while (remaining.NextTok('\n', row)) {
        if (!row.Contains(marker)) {
            continue;
        }
        TVector<TString> cells;
        TStringBuf cell;
        while (row.NextTok(TStringBuf("│"), cell)) {
            const auto value = StripString(cell);
            if (!value.empty()) {
                cells.emplace_back(value);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(cells.size(), 8, output);
        return cells;
    }
    UNIT_FAIL("Target row not found: " << marker << " in " << output);
    return {};
}
}

Y_UNIT_TEST_SUITE(ReplicationDescribe) {
    Y_UNIT_TEST(DecimalSpeedUnits) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatDecimalSpeed(0), "0.00 B/s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDecimalSpeed(12.345), "12.35 B/s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDecimalSpeed(1000), "1.00 KB/s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDecimalSpeed(2500000), "2.50 MB/s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDecimalSpeed(3250000000), "3.25 GB/s");
    }

    Y_UNIT_TEST(SummaryAndIndependentTargetRows) {
        Ydb::Replication::DescribeReplicationResult proto;
        proto.mutable_connection_params()->set_database("/Root");
        auto* overall = proto.mutable_running()->mutable_stats();
        overall->set_write_speed_bytes_per_second(1250000);
        overall->mutable_initial_scan_eta()->set_seconds(90);
        auto* first = proto.add_items();
        first->set_id(1);
        first->set_source_path("/Root/table");
        first->set_destination_path("/Root/copy");
        first->mutable_stats()->set_write_speed_bytes_per_second(250000);
        first->mutable_stats()->mutable_initial_scan_eta()->set_seconds(0);
        auto* index = proto.add_items();
        index->set_id(2);
        index->set_source_path("/Root/table/by_value");
        index->set_destination_path("/Root/copy/by_value/indexImplTable");
        index->mutable_stats()->set_write_speed_bytes_per_second(1000000);
        index->mutable_stats()->mutable_initial_scan_eta()->set_seconds(90);
        const auto output = Render(proto);
        UNIT_ASSERT_C(output.Contains("Write speed (60s avg): 1.25 MB/s"), output);
        UNIT_ASSERT_C(output.Contains("Initial scan ETA (approx.): 1m 30s"), output);
        UNIT_ASSERT_C(output.Contains("250.00 KB/s"), output);
        UNIT_ASSERT_C(output.Contains("1.00 MB/s"), output);
        const auto base = TargetCells(output, "copy");
        UNIT_ASSERT_STRINGS_EQUAL(base[6], "250.00 KB/s");
        UNIT_ASSERT_STRINGS_EQUAL(base[7], "0s");
        const auto indexCells = TargetCells(output, "indexImplTable");
        UNIT_ASSERT_STRINGS_EQUAL(indexCells[6], "1.00 MB/s");
        UNIT_ASSERT_STRINGS_EQUAL(indexCells[7], "1m 30s");
        UNIT_ASSERT_C(output.Contains("by_value/indexImplTable"), output);
        UNIT_ASSERT_C(output.Contains("Write speed (60s avg)")
            && output.Contains("Initial scan ETA (approx.)"), output);

        const auto plain = Render(proto, false);
        UNIT_ASSERT_C(!plain.Contains("Write speed") && !plain.Contains("Initial scan ETA"), plain);
        UNIT_ASSERT_C(!plain.Contains("MB/s") && !plain.Contains("KB/s"), plain);
    }

    Y_UNIT_TEST(UnknownAndConfirmedComplete) {
        Ydb::Replication::DescribeReplicationResult proto;
        proto.mutable_running();
        auto* item = proto.add_items();
        item->set_id(1);
        item->set_source_path("unknown-source");
        item->set_destination_path("unknown-target");
        const auto unknown = Render(proto);
        UNIT_ASSERT_C(unknown.Contains("Write speed (60s avg): n/a"), unknown);
        UNIT_ASSERT_C(unknown.Contains("Initial scan ETA (approx.): n/a"), unknown);
        const auto cells = TargetCells(unknown, "unknown-target");
        UNIT_ASSERT_STRINGS_EQUAL(cells[6], "n/a");
        UNIT_ASSERT_STRINGS_EQUAL(cells[7], "n/a");
        auto* stats = proto.mutable_running()->mutable_stats();
        stats->set_write_speed_bytes_per_second(0);
        stats->mutable_initial_scan_eta()->set_seconds(0);
        const auto complete = Render(proto);
        UNIT_ASSERT_C(complete.Contains("Write speed (60s avg): 0.00 B/s"), complete);
        UNIT_ASSERT_C(complete.Contains("Initial scan ETA (approx.): 0s"), complete);
        stats->mutable_initial_scan_eta()->set_nanos(1000);
        const auto fractional = Render(proto);
        UNIT_ASSERT_C(fractional.Contains("Initial scan ETA (approx.): 1s"), fractional);
    }
}
