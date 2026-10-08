#include "mon_component_test.h"
#include <ydb/core/blobstorage/ddisk/ddisk_mon.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

namespace NKikimr::NDDisk {
ui64 SpaceValue(const TString& html, TStringBuf label) {
    const auto bar = ComponentData(html, "data-allocation");
    for (const auto& segment : bar["segments"].GetArraySafe()) {
        if (segment["label"].GetString() == label) {
            return segment["value"].GetUInteger();
        }
    }
    UNIT_FAIL("Missing space segment");
    return 0;
}

Y_UNIT_TEST_SUITE(TDDiskMonRenderer) {
    Y_UNIT_TEST(TabletSpaceUsesFairQuotaAndIncludesAllConsumers) {
        TDDiskMonInfo info;
        info.StatsAvailable = true;
        info.ChunkSize = 1024;
        info.StatsTablets = 2;
        info.StatsChunks = info.DataChunks = 20;
        info.IntegrityChunks = 1;
        info.ReservedChunks = 4;
        TDDiskMonTabletStats tablet;
        tablet.TabletId = 11;
        tablet.Chunks = 10;
        info.StatsShares.push_back(tablet);
        info.TabletStats.push_back(tablet);
        TPersistentBufferMonInfo pb;
        pb.ChunkSize = info.ChunkSize;
        pb.AllocatedChunks = 5;
        pb.PDiskSpace.emplace();
        pb.PDiskSpace->TotalChunks = 100;
        pb.PDiskSpace->UsedChunks = 30;
        pb.PDiskSpace->FreeChunks = 70;
        TDDiskMonQuery query;
        query.Tab = "tablets";
        query.RefreshRate = 1;
        query.SearchTabletId = 11;
        auto html = RenderDDiskMonPage(info, &pb, query, {});
        const auto bar = ComponentData(html, "data-allocation", 2);
        UNIT_ASSERT_VALUES_EQUAL(bar["capacity"].GetDouble(), 102400);
        const auto& segments = bar["segments"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(segments.size(), 6);
        const std::array<TStringBuf, 6> labels = {"11", "Other / 1 tablets", "Checksums", "PersistentBuffer", "Empty reserve", "Unallocated share"};
        const std::array<ui64, 6> chunks = {10, 10, 1, 5, 4, 70};
        for (size_t i = 0; i < labels.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(segments[i]["label"].GetString(), labels[i]);
            UNIT_ASSERT_VALUES_EQUAL(segments[i]["value"].GetDouble(), chunks[i] * info.ChunkSize);
        }
        for (size_t i = 2; i < segments.size(); ++i) {
            UNIT_ASSERT(!segments[i].Has("href"));
        }
        info.StatsTablets = 1;
        const auto otherData = ComponentData(RenderDDiskMonPage(info, &pb, query, {}), "data-allocation", 2);
        UNIT_ASSERT(!otherData["segments"].GetArraySafe()[1].Has("href"));
        UNIT_ASSERT(html.Contains("<th>Share of fair quota</th>"));
        UNIT_ASSERT(html.Contains("<td>10.0%</td>"));
        UNIT_ASSERT(!html.Contains("Reserve shortfall"));
        pb.PDiskSpace->FreeChunks = 40;
        const auto limited = ComponentData(RenderDDiskMonPage(info, &pb, query, {}), "data-allocation", 2);
        UNIT_ASSERT_VALUES_EQUAL(limited["segments"].GetArraySafe().back()["value"].GetDouble(), 30 * info.ChunkSize);
        UNIT_ASSERT_VALUES_EQUAL(limited["segments"].GetArraySafe().back()["pattern"].GetString(), "striped");
        const auto unavailable = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(unavailable.Contains("Fair share unavailable"));
        UNIT_ASSERT(!unavailable.Contains("<td>50.0%</td>"));
        UNIT_ASSERT(unavailable.Contains("<td>-</td>"));
    }

    Y_UNIT_TEST(WaitingRequestsExcludeBufferAndIoCounters) {
        TDDiskMonInfo info;
        info.Backend = "PDisk";
        info.PendingIntegrity = 7;
        TDDiskMonQuery query;
        query.Tab = "operations";
        auto html = RenderDDiskMonPage(info, nullptr, query, {});
        auto start = html.find("<div class=\"ddisk-waiting-requests\">");
        auto queues = html.substr(start, html.find("</div>", start) - start);
        UNIT_ASSERT(!html.Contains("<h3>In progress</h3>"));
        UNIT_ASSERT(queues.Contains("<h3>Requests waiting for</h3>"));
        UNIT_ASSERT(!queues.Contains("router I/O"));
        UNIT_ASSERT(!queues.Contains("PB initialization / free space"));
        UNIT_ASSERT(queues.Contains("Previous write to the same chunk</td><td><strong>7</strong>"));
        UNIT_ASSERT(!html.Contains("Logical requests and physical I/O"));
        UNIT_ASSERT(!html.Contains("<h3>Sync</h3>"));
        TPersistentBufferMonInfo pb;
        pb.PendingEvents = 3;
        info.Backend = "io_uring";
        html = RenderDDiskMonPage(info, &pb, query, {});
        start = html.find("<div class=\"ddisk-waiting-requests\">");
        queues = html.substr(start, html.find("</div>", start) - start);
        UNIT_ASSERT(!html.Contains("<h3>In progress</h3>"));
        UNIT_ASSERT(!queues.Contains("router I/O"));
        UNIT_ASSERT(!queues.Contains("PB initialization / free space"));
    }

    Y_UNIT_TEST(OperationHistoryUsesMonotonicIntervalsAndPreservesGapsAndResets) {
        std::vector<TDDiskMonCounterSample> samples(6);
        for (size_t i = 0; i < samples.size(); ++i) {
            samples[i].Timestamp = TInstant::Seconds(100 + i);
            samples[i].SampledAt = TMonotonic::MilliSeconds(1000 + i * 1500);
            for (auto& counter : samples[i].Counters) {
                counter = {10 + i * 3, 100 + i * 300};
            }
        }
        // A wall-clock step must not change rates.
        samples[1].Timestamp = TInstant::Seconds(103);
        samples[2].Counters[1] = {0, 0};
        // Long missing intervals and non-increasing clock readings are unavailable.
        samples[3].SampledAt = samples[2].SampledAt + TDuration::Seconds(3);
        samples[4].SampledAt = samples[3].SampledAt;
        samples[5].SampledAt = samples[4].SampledAt + TDuration::Seconds(1);
        samples[5].Counters = samples[4].Counters;
        const auto history = CalculateDDiskMonRateHistory(samples);
        UNIT_ASSERT(!history[0].Rates[0]);
        UNIT_ASSERT_VALUES_EQUAL(history[1].Rates[0]->Iops, 2);
        UNIT_ASSERT_VALUES_EQUAL(history[1].Rates[0]->BytesPerSecond, 200);
        UNIT_ASSERT(!history[2].Rates[1]);
        UNIT_ASSERT(history[2].Rates[0]);
        UNIT_ASSERT(!history[3].Rates[0]);
        UNIT_ASSERT(!history[4].Rates[0]);
        UNIT_ASSERT_VALUES_EQUAL(history[5].Rates[0]->Iops, 0);
        UNIT_ASSERT_VALUES_EQUAL(history[5].Rates[0]->BytesPerSecond, 0);
    }

    Y_UNIT_TEST(OperationsExcludeBufferAndIncludeSharedIo) {
        TDDiskMonInfo info;
        info.CollectedAt = TInstant::Seconds(600);
        TDDiskMonRateSample sample;
        sample.Timestamp = TInstant::Seconds(599);
        sample.Rates[0] = TDDiskMonRate{8, 8192};
        sample.Rates[3] = TDDiskMonRate{65536, 1ull << 30};
        info.RateHistory.push_back(sample);
        TDDiskMonQuery query;
        query.Tab = "operations";
        auto html = RenderDDiskMonPage(info, nullptr, query, "PB timeout");
        UNIT_ASSERT(html.Contains("DDisk requests per second"));
        UNIT_ASSERT(!html.Contains("PersistentBuffer interface operations"));
        UNIT_ASSERT(html.Contains("Shared DirectIO"));
        const auto io = ComponentData(html, "data-chart", 2);
        UNIT_ASSERT_VALUES_EQUAL(io["series"][0]["points"][0]["value"].GetDouble(), 65536);
        UNIT_ASSERT(!html.Contains("PB timeout"));
    }

    Y_UNIT_TEST(OperationChartsKeepUnavailableIntervalsAndZeroRatesDistinct) {
        TDDiskMonInfo info;
        info.CollectedAt = TInstant::Seconds(600);
        TDDiskMonRateSample first;
        first.Timestamp = TInstant::Seconds(590);
        first.Rates[0] = TDDiskMonRate{8, 8192};
        TDDiskMonRateSample missing;
        missing.Timestamp = TInstant::Seconds(591);
        TDDiskMonRateSample zero;
        zero.Timestamp = TInstant::Seconds(592);
        zero.Rates[0] = TDDiskMonRate{};
        info.RateHistory = {first, missing, zero};
        TDDiskMonQuery query;
        query.Tab = "operations";
        auto html = RenderDDiskMonPage(info, nullptr, query, {});
        const auto data = ComponentData(html, "data-chart");
        const auto& points = data["series"][0]["points"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(points.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(points[0]["value"].GetDouble(), 8);
        UNIT_ASSERT(points[1]["value"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(points[2]["value"].GetDouble(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ComponentData(html, "data-chart", 1)["settings"]["unit"].GetString(), "bytesPerSecond");
        info.RateHistory.clear();
        html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(html.Contains("Collecting history"));
    }

    Y_UNIT_TEST(OperationTableCombinesInFlightRequestsAndBytes) {
        TDDiskMonInfo info;
        TDDiskMonOperation read;
        read.Name = "Read";
        read.InFlight = 2;
        read.BytesInFlight = 8192;
        read.Requests = 1234567;
        read.ReplyOk = 1234565;
        read.Rate = TDDiskMonRate{1234.5, 8192};
        info.Operations.push_back(read);
        TDDiskMonQuery query;
        query.Tab = "operations";
        const auto html = RenderDDiskMonPage(info, nullptr, query, {});
        const auto start = html.find("ddisk-operation-table\">");
        UNIT_ASSERT(start != TString::npos);
        const auto table = html.substr(start, html.find("</table>", start) - start);
        UNIT_ASSERT(table.Contains("<th>Transferred total</th><th>In flight now</th>"));
        UNIT_ASSERT(table.Contains("<td>2 <span class=\"text-muted\">(8.00 KiB)</span></td>"));
        UNIT_ASSERT(table.Contains("<th>Last IOPS</th><th>Last throughput</th>"));
        UNIT_ASSERT(table.Contains("<td>1 234.5</td><td>8.00 KiB/s</td>"));
        UNIT_ASSERT(table.Contains("<td>1 234 567</td><td>1 234 565</td>"));
        UNIT_ASSERT(table.Contains("background:#377bba"));
        UNIT_ASSERT(!table.Contains("Bytes in flight"));
        UNIT_ASSERT(!table.Contains("Since start"));
    }

    Y_UNIT_TEST(MemoryChartsKeepTimeGapsAndSeparateUnknownFromZero) {
        TDDiskMonInfo info;
        info.CollectedAt = TInstant::Seconds(600);
        info.Memory.Limit = 64ull << 20;
        TPersistentBufferMonInfo pb;
        pb.Memory.Limit = 128ull << 20;
        TDDiskMonQuery query;
        query.Tab = "space";
        auto html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(html.Contains("Collecting history"));
        info.Memory.Samples = {{TInstant::Seconds(590), 1ull << 20}, {TInstant::Seconds(592), 2ull << 20}};
        pb.Memory.Samples = {{TInstant::Seconds(593), 0}};
        html = RenderDDiskMonPage(info, &pb, query, {});
        const auto memory = ComponentData(html, "data-chart", 1);
        const auto& points = memory["series"][0]["points"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(points.size(), 3);
        UNIT_ASSERT(points[1]["value"].IsNull());
        UNIT_ASSERT(html.Contains("DDisk checksum cache (estimated)"));
        UNIT_ASSERT(!html.Contains("PersistentBuffer data cache"));
        UNIT_ASSERT(!html.Contains("1s samples / last 5 minutes"));
        UNIT_ASSERT(html.Contains("Latest <strong>2.00 MiB"));
        UNIT_ASSERT(!html.Contains("Latest <strong>0 B"));
        UNIT_ASSERT(!html.Contains("Limit <strong>128.00 MiB"));
        UNIT_ASSERT(!html.Contains("ddisk-memory-bar"));
        info.Memory.Error = "timeout <memory>";
        html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(html.Contains("timeout &lt;memory&gt;"));
        query.Tab = "overview";
        html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(!html.Contains("ddisk-memory-chart"));
    }

    Y_UNIT_TEST(SpaceHistoryStacksCategoriesAndPreservesMissingSeconds) {
        TDDiskMonInfo info;
        info.CollectedAt = TInstant::Seconds(600);
        info.ChunkSize = 1ull << 20;
        TPersistentBufferMonInfo pb;
        pb.ChunkSize = info.ChunkSize;
        pb.PDiskSpace = TDDiskSpaceMonInfo{info.CollectedAt, 10, 0, 10};
        TDDiskMonQuery query;
        query.Tab = "space";
        auto html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(html.Contains("Collecting space history"));
        for (size_t i = 0; i < info.SpaceHistory.size(); ++i) {
            info.SpaceHistory[i].Samples = {
                {TInstant::Seconds(290), 1ull << 30}, // outside the window
                {TInstant::Seconds(590), 1ull << 20},
                {TInstant::Seconds(592), i == 0 ? 0 : 1ull << 20}};
        }
        info.SpaceHistory[0].Samples.push_back({TInstant::Seconds(596), 1ull << 20}); // incomplete
        html = RenderDDiskMonPage(info, &pb, query, {});
        const auto data = ComponentData(html, "data-chart");
        const auto& series = data["series"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(series.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(data["settings"]["type"].GetString(), "area");
        const auto& points = series[0]["points"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(points[0]["time"].GetUInteger(), 590000);
        UNIT_ASSERT_VALUES_EQUAL(points[0]["value"].GetDouble(), 1ull << 20);
        UNIT_ASSERT(points[1]["value"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(points[2]["value"].GetDouble(), 0);
        UNIT_ASSERT(points.back()["value"].IsNull());
        UNIT_ASSERT(!html.Contains("1.00 GiB"));
        UNIT_ASSERT(html.find("Allocated space history") < html.find("DDisk + PB"));
        UNIT_ASSERT(!html.Contains("ddisk-space-history-bar"));
        info.SpaceHistory[2].Error = "unavailable <space>";
        html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(html.Contains("unavailable &lt;space&gt;"));
        UNIT_ASSERT(!html.Contains("ddisk-space-history-chart"));
        query.Tab = "overview";
        html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(!html.Contains("Allocated space history"));
    }

    Y_UNIT_TEST(AreaChartsKeepAllocationStepsAndMemoryPeaks) {
        TDDiskMonInfo info;
        info.CollectedAt = TInstant::Seconds(600);
        info.Memory.Samples = {
            {TInstant::Seconds(590), 1ull << 20},
            {TInstant::Seconds(591), 2ull << 20},
            {TInstant::Seconds(592), 1ull << 20}};
        for (size_t category = 0; category < info.SpaceHistory.size(); ++category) {
            info.SpaceHistory[category].Samples = {
                {TInstant::Seconds(590), 1ull << 20},
                {TInstant::Seconds(591), category == 0 ? 0 : 1ull << 20},
                {TInstant::Seconds(592), category == 0 ? 2ull << 20 : 1ull << 20}};
        }
        TDDiskMonQuery query;
        query.Tab = "space";
        const auto html = RenderDDiskMonPage(info, nullptr, query, {});
        const auto space = ComponentData(html, "data-chart");
        const auto memory = ComponentData(html, "data-chart", 1);
        UNIT_ASSERT(space["series"][0]["step"].GetBoolean());
        UNIT_ASSERT(!memory["series"][0]["step"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(space["series"][0]["points"][1]["value"].GetDouble(), 0);
        UNIT_ASSERT_VALUES_EQUAL(memory["series"][0]["points"][1]["value"].GetDouble(), 2ull << 20);
        UNIT_ASSERT_VALUES_EQUAL(memory["series"][0]["display"].GetString(), "Checksum cache");
    }

    Y_UNIT_TEST(MemoryHistoryToleratesTimerJitter) {
        TDDiskMonInfo info;
        info.CollectedAt = TInstant::Seconds(600);
        info.Memory.Samples = {
            {TInstant::Seconds(590) + TDuration::MilliSeconds(100), 1ull << 20},
            {TInstant::Seconds(591) + TDuration::MilliSeconds(200), 2ull << 20},
            {TInstant::Seconds(592) + TDuration::MilliSeconds(150), 1ull << 20}};
        TDDiskMonQuery query;
        query.Tab = "space";
        const auto html = RenderDDiskMonPage(info, nullptr, query, {});
        const auto memory = ComponentData(html, "data-chart", 1);
        const auto& points = memory["series"][0]["points"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(points.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(points[1]["time"].GetUInteger(), 591200);
        UNIT_ASSERT_VALUES_EQUAL(points[2]["time"].GetUInteger(), 592150);
    }

    Y_UNIT_TEST(RatesUseMeasuredIntervalAndRejectCounterReset) {
        const auto rate = CalculateDDiskMonRate(10, 100, 25, 1124, TDuration::Seconds(5));
        UNIT_ASSERT(rate);
        UNIT_ASSERT_DOUBLES_EQUAL(rate->Iops, 3.0, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(rate->BytesPerSecond, 204.8, 1e-9);
        const auto idle = CalculateDDiskMonRate(25, 1124, 25, 1124, TDuration::Seconds(7));
        UNIT_ASSERT(idle);
        UNIT_ASSERT_VALUES_EQUAL(idle->Iops, 0);
        UNIT_ASSERT_VALUES_EQUAL(idle->BytesPerSecond, 0);
        UNIT_ASSERT(!CalculateDDiskMonRate(10, 100, 9, 100, TDuration::Seconds(5)));
        UNIT_ASSERT(!CalculateDDiskMonRate(10, 100, 10, 99, TDuration::Seconds(5)));
        UNIT_ASSERT(!CalculateDDiskMonRate(10, 100, 25, 1124, TDuration::Zero()));
        const auto delayed = CalculateDDiskMonRate(10, 100, 25, 1124, TDuration::Seconds(10));
        UNIT_ASSERT_DOUBLES_EQUAL(delayed->Iops, 1.5, 1e-9);
    }

    Y_UNIT_TEST(OverviewRatesDistinguishUnknownIdleAndFractionalRates) {
        TDDiskMonInfo info;
        TDDiskMonOperation read;
        read.Name = "Read";
        info.Operations.push_back(read);
        TDDiskMonQuery query;
        auto html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(html.Contains("Collecting rates"));
        UNIT_ASSERT(!html.Contains("Waiting requests"));
        UNIT_ASSERT(!html.Contains("<th>In flight</th>"));
        info.RateWindowSeconds = 5.0;
        info.Operations[0].Rate = TDDiskMonRate{0.2, 0.8};
        html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(html.Contains("<td>Read</td><td>0.2</td><td>0.80 B/s</td>"));
        info.Operations[0].Rate = TDDiskMonRate{};
        html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(html.Contains("<td>Read</td><td>0.0</td><td>0 B/s</td>"));
    }
    Y_UNIT_TEST(FairShareTracksSharedSpaceAndHidesZeroShortfall) {
        TDDiskMonInfo info;
        info.ChunkSize = 1ull << 30;
        info.DataChunks = 4;
        info.ReservedChunks = 1;
        TPersistentBufferMonInfo pb;
        pb.ChunkSize = info.ChunkSize;
        pb.AllocatedChunks = 1;
        pb.PDiskSpace = TDDiskSpaceMonInfo{TInstant::Seconds(100), 10, 6, 20};
        TDDiskMonQuery query;
        auto html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(html.Contains("DDisk + PB fair share <strong>10.00 GiB"));
        UNIT_ASSERT_VALUES_EQUAL(SpaceValue(html, "Unallocated share"), 4ull << 30);
        UNIT_ASSERT(!html.Contains("Reserve shortfall"));
        pb.PDiskSpace->FreeChunks = 1;
        html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT_VALUES_EQUAL(SpaceValue(html, "Reserve shortfall"), 3ull << 30);
        UNIT_ASSERT_VALUES_EQUAL(SpaceValue(html, "Unallocated share"), 1ull << 30);
        pb.PDiskSpace->UsedChunks = 8;
        html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT_VALUES_EQUAL(SpaceValue(html, "Other allocated"), 2ull << 30);
        UNIT_ASSERT_VALUES_EQUAL(SpaceValue(html, "Reserve shortfall"), 1ull << 30);
        // A newer actor snapshot must not underflow an older PDisk usage sample.
        pb.PDiskSpace->UsedChunks = 2;
        html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(!html.Contains("Other allocated"));
        UNIT_ASSERT_VALUES_EQUAL(SpaceValue(html, "Reserve shortfall"), 3ull << 30);
        pb.PDiskSpace->TotalChunks = 3;
        html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(html.Contains("Above fair share by 3.00 GiB"));
        UNIT_ASSERT(!html.Contains("Reserve shortfall"));
        pb.PDiskSpace.reset();
        html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(html.Contains("Fair share unavailable"));
        UNIT_ASSERT(!html.Contains("role=\"img\""));
    }

    Y_UNIT_TEST(EscapesActorStateAndErrors) {
        TDDiskMonInfo info;
        info.State = "<script>alert(1)</script>";
        info.Backend = "backend\"&'";
        info.BrokenReason = "<b>broken</b>";
        TDDiskMonQuery query;
        auto html = RenderDDiskMonPage(info, nullptr, query, "<img src=x onerror=alert(2)>");
        UNIT_ASSERT(!html.Contains("<script>"));
        UNIT_ASSERT(!html.Contains("<img"));
        UNIT_ASSERT(html.Contains("&lt;script&gt;"));
        UNIT_ASSERT(html.Contains("backend&quot;&amp;&#39;"));
        query.Tab = "diagnostics";
        info.Identity.push_back({"<name>", "<value>"});
        html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(html.Contains("&lt;name&gt;"));
        UNIT_ASSERT(html.Contains("&lt;value&gt;"));
        UNIT_ASSERT(!html.Contains("<b>broken"));
    }

    Y_UNIT_TEST(TabletPagesExcludeBufferAndPreserveUint64Identity) {
        TDDiskMonInfo info;
        TPersistentBufferMonInfo pb;
        constexpr ui64 base = 18446744073709551000ull;
        for (ui64 i = 0; i < 100; ++i) {
            TDDiskMonTablet disk;
            disk.TabletId = base + i * 2;
            info.Tablets.push_back(disk);
            TPersistentBufferMonInfo::TTablet buffer;
            buffer.TabletId = base + i * 2 + 1;
            pb.Tablets.push_back(buffer);
        }
        TDDiskMonQuery query;
        query.Tab = "tablets";
        auto html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(html.Contains("tabletId=" + ToString(base)));
        UNIT_ASSERT(!html.Contains("tabletId=" + ToString(base + 1)));
        UNIT_ASSERT(!html.Contains("Next tablets"));
        UNIT_ASSERT(html.Contains("tabletId=" + ToString(base + 100)));
        query.AfterTabletId = base + 99;
        html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(!html.Contains("tabletId=" + ToString(base + 99)));
        UNIT_ASSERT(html.Contains("tabletId=" + ToString(base + 100)));
    }

    Y_UNIT_TEST(SelectedTabletIsStandaloneAndFiltersSessions) {
        TDDiskMonInfo info;
        info.ChunkSize = 34ull << 20;
        info.Tablets.push_back({11, 5});
        info.Tablets.push_back({12, 9});
        TPersistentBufferMonInfo pb;
        pb.Tablets.push_back({11, 24576, 6, 1});
        info.Connections.push_back({11, 3, 8, 1, 42, "selected-session"});
        info.Connections.push_back({12, 3, 8, 2, 43, "other-session"});
        TDDiskMonQuery query;
        query.Tab = "tablets";
        query.TabletId = 11;
        query.AfterTabletId = 10;
        query.RefreshRate = 5;
        const auto html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(html.Contains("selected-session"));
        UNIT_ASSERT(!html.Contains("other-session"));
        UNIT_ASSERT(html.Contains("<h2>Tablet 11</h2>"));
        UNIT_ASSERT(html.Contains("role=\"tablist\" aria-label=\"Tablet sections\""));
        UNIT_ASSERT(html.Contains("href=\"#chunks\" role=\"tab\""));
        UNIT_ASSERT(html.Contains("href=\"#connections\" role=\"tab\""));
        UNIT_ASSERT(html.Contains("role=\"tabpanel\" aria-labelledby=\"ddisk-chunks-tab\""));
        UNIT_ASSERT(!html.Contains("&amp;dbgIndex="));
        UNIT_ASSERT(html.Contains("window.addEventListener('hashchange',selectTab)"));
        UNIT_ASSERT(!html.Contains("<h2>DBG 3</h2>"));
        UNIT_ASSERT(html.Contains("<dt>Allocated</dt><dd>170.00 MiB</dd>"));
        UNIT_ASSERT(!html.Contains("<dt>Records</dt><dd>6</dd>"));
        UNIT_ASSERT(!html.Contains("<dt>Live data</dt><dd>24.00 KiB</dd>"));
        UNIT_ASSERT(html.Contains("href=\"?tab=tablets&amp;afterTabletId=10&amp;refreshRate=5\""));
        UNIT_ASSERT(!html.Contains("nav-tabs"));
        UNIT_ASSERT(!html.Contains("<th>Tablet ID</th>"));
        UNIT_ASSERT(!html.Contains("PB records /"));
        UNIT_ASSERT(!html.Contains("Next records"));
    }

    Y_UNIT_TEST(BufferRegistrationDoesNotAppearOnDDiskPage) {
        TDDiskMonInfo info;
        TPersistentBufferMonInfo pb;
        TPersistentBufferMonInfo::TRegistration reg;
        reg.DirectBlockGroupIndex = 7;
        reg.RemovalStage = "removing <pending>";
        reg.BarrierGeneration = 0;
        reg.BarrierLsn = 0;
        pb.Registrations.push_back(reg);
        TDDiskMonQuery query;
        query.Tab = "tablets";
        query.TabletId = 19;
        query.DirectBlockGroupIndex = 7;
        const auto html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(!html.Contains("removing &lt;pending&gt;"));
        UNIT_ASSERT(!html.Contains("<dt>Barrier generation</dt><dd>0</dd>"));
        UNIT_ASSERT(!html.Contains("<dt>Barrier LSN</dt><dd>0</dd>"));
        UNIT_ASSERT(!html.Contains("<h2>DBG 7</h2>"));
        UNIT_ASSERT(!html.Contains("PB records /"));
    }

    Y_UNIT_TEST(MissingBufferDoesNotBecomeZeroSpace) {
        TDDiskMonInfo info;
        info.ChunkSize = 128ull << 20;
        info.DataChunks = 128;
        TDDiskMonQuery query;
        query.Tab = "space";
        const auto html = RenderDDiskMonPage(info, nullptr, query, "timeout");
        UNIT_ASSERT(html.Contains("Shared space unavailable: timeout"));
        UNIT_ASSERT(html.Contains("<td>PersistentBuffer</td><td>unknown</td><td>unknown</td>"));
        UNIT_ASSERT(html.Contains("<td>Classified total</td><td>unknown</td><td>unknown</td>"));
        UNIT_ASSERT(!html.Contains("<td>Growth limit</td><td>unknown</td>"));
        UNIT_ASSERT(!html.Contains("<td>Memory cache / limit</td><td>unknown</td>"));
    }

    Y_UNIT_TEST(RefreshPreservesNavigationAndClampsTimeout) {
        TDDiskMonInfo info;
        TDDiskMonQuery query;
        query.RefreshRate = 4294967295u;
        const auto html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(html.Contains("},2147483647);</script>"));
        UNIT_ASSERT(html.Contains("tab=operations&amp;refreshRate=4294967295"));
    }

    Y_UNIT_TEST(OverviewIsCompactAndDirectIoHasNoReplyCounters) {
        TDDiskMonInfo info;
        TDDiskMonOperation write;
        write.Name = "Write";
        info.Operations.push_back(write);
        write.Name = "WritePersistentBuffer";
        info.Operations.push_back(write);
        info.DirectIo.push_back(write);
        TDDiskMonQuery query;
        auto html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(!html.Contains("WritePersistentBuffer"));
        UNIT_ASSERT(!html.Contains("<th>Requests</th>"));
        query.Tab = "operations";
        html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(html.Contains("Shared DirectIO counters"));
        UNIT_ASSERT(!html.Contains("PersistentBuffer interface operations"));
    }

    Y_UNIT_TEST(AllSelectedTabletSessionsRemainAccessible) {
        TDDiskMonInfo info;
        for (ui32 dbgIndex = 0; dbgIndex < 256; ++dbgIndex) {
            info.Connections.push_back({11, dbgIndex, 8, 1, 42, "session-" + ToString(dbgIndex)});
        }
        TDDiskMonQuery query;
        query.Tab = "tablets";
        query.TabletId = 11;
        auto html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(html.Contains("session-100</td>"));
        UNIT_ASSERT(html.Contains("session-255</td>"));
        UNIT_ASSERT(!html.Contains("Session list truncated"));
        query.DirectBlockGroupIndex = 255;
        html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(html.Contains("session-255</td>"));
    }

    Y_UNIT_TEST(DiagnosticsDistinguishesRunningStoppingAndUnavailableBuffer) {
        TDDiskMonInfo info;
        info.State = "Ready";
        info.Lifecycle = {{"Stopping", "false"}, {"Waiting for PB", "false"}, {"Own I/O drained", "false"}};
        TPersistentBufferMonInfo pb;
        pb.State = "Ready";
        TDDiskMonQuery query;
        query.Tab = "diagnostics";
        auto html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(html.Contains("<td>Stopping</td><td>No</td>"));
        UNIT_ASSERT(html.Contains("<td>DDisk I/O drained</td><td>-</td>"));
        UNIT_ASSERT(!html.Contains("<td>PB I/O drained</td><td>-</td>"));
        UNIT_ASSERT(!html.Contains("Raw operation counters"));
        UNIT_ASSERT(!html.Contains("PDisk normalized occupancy"));
        info.State = "Stopping";
        info.Lifecycle = {{"Stopping", "true"}, {"Waiting for PB", "true"}, {"Own I/O drained", "false"}};
        pb.State = "Stopping";
        pb.OwnDrainComplete = true;
        html = RenderDDiskMonPage(info, &pb, query, {});
        UNIT_ASSERT(html.Contains("<td>Waiting for PB to stop</td><td>Yes</td>"));
        UNIT_ASSERT(html.Contains("<td>DDisk I/O drained</td><td>No</td>"));
        UNIT_ASSERT(!html.Contains("<td>PB I/O drained</td><td>Yes</td>"));
        html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(!html.Contains("<td>PB I/O drained</td><td>unknown</td>"));
        UNIT_ASSERT(!html.Contains("<td>PB I/O stalled</td><td>unknown</td>"));
        UNIT_ASSERT(!html.Contains("<td>PB chunks restoring</td><td>unknown</td>"));
    }

    Y_UNIT_TEST(MappingCursorAndTabFallback) {
        TDDiskMonInfo info;
        for (ui64 i = 0; i < 102; ++i) {
            TDDiskMonChunk chunk;
            chunk.VChunk = i;
            info.Chunks.push_back(chunk);
        }
        TDDiskMonQuery query;
        query.Tab = "tablets";
        query.TabletId = 9007199254740993ull;
        query.AfterVChunk = 0;
        auto html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(html.Contains("tabletId=9007199254740993&amp;afterVChunk=100"));
        query.Tab = "\"><script>";
        html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(!html.Contains("\"><script>"));
        UNIT_ASSERT(html.Contains("<h2>Tablet 9007199254740993</h2>"));
    }

    Y_UNIT_TEST(TabletConnectionsShowAllFieldsInlineAndExcludeBuffer) {
        TDDiskMonInfo info;
        info.Connections.push_back({11, 7, 8, 1, 42, "session-seven<&>"});
        info.Connections.push_back({12, 9, 3, 2, 55, "other-tablet"});
        TPersistentBufferMonInfo pb;
        TPersistentBufferMonInfo::TRegistration registration;
        registration.DirectBlockGroupIndex = 3;
        registration.Registered = true;
        pb.Registrations.push_back(registration);
        TDDiskMonQuery query;
        query.TabletId = 11;
        for (const auto oldIndex : {std::optional<ui32>{}, std::optional<ui32>{7}}) {
            query.DirectBlockGroupIndex = oldIndex;
            const auto html = RenderDDiskMonPage(info, &pb, query, {});
            UNIT_ASSERT(!html.Contains("&amp;dbgIndex="));
            UNIT_ASSERT(!html.Contains("<h2>DBG "));
            UNIT_ASSERT(html.Contains("<th>Node</th>"));
            UNIT_ASSERT(html.Contains("<th>Generation</th>"));
            UNIT_ASSERT(html.Contains("<th>Sequence</th>"));
            UNIT_ASSERT(html.Contains("<th>Interconnect session</th>"));
            UNIT_ASSERT(html.Contains("session-seven&lt;&amp;&gt;"));
            UNIT_ASSERT(!html.Contains("other-tablet"));
            UNIT_ASSERT(html.Contains("role=\"tablist\""));
            UNIT_ASSERT(!html.Contains("<th>Records</th>"));
        }
    }

    Y_UNIT_TEST(ChunkFieldsAreInlineAndOverviewIsDefault) {
        TDDiskMonInfo info;
        info.ChunkSize = 34ull << 20;
        info.Chunks.push_back({0, 240, 250, 0, 2, 3, 4});
        info.Tablets.push_back({11, 2});
        info.StatsAvailable = true;
        info.StatsChunks = 8;
        info.StatsIops = 100;
        info.StatsBytesPerSecond = 409600;
        TDDiskMonTabletStats stats;
        stats.TabletId = 11;
        stats.Rates[0] = {25, 102400};
        info.TabletStats.push_back(stats);
        TDDiskMonQuery query;
        query.TabletId = 11;
        query.VChunk = 0; // Old nested-page links still open the tablet.
        const auto html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(!html.Contains("&amp;vChunk="));
        UNIT_ASSERT(!html.Contains("<h2>Virtual chunk"));
        UNIT_ASSERT(html.Contains("<th>Checksum chunk</th>"));
        UNIT_ASSERT(html.Contains("<th>Checksum slot</th>"));
        UNIT_ASSERT(html.Contains("<th>Waiting for allocation</th>"));
        UNIT_ASSERT(html.Contains("<th>Waiting for integrity</th>"));
        UNIT_ASSERT(html.Contains("<td>0</td><td>240</td><td>34.00 MiB</td><td>250</td><td>0</td><td>2</td><td>3</td><td>4</td>"));
        UNIT_ASSERT(html.Contains("href=\"#overview\" role=\"tab\""));
        UNIT_ASSERT(html.Contains("window.location.hash:'#overview'"));
        UNIT_ASSERT(html.Contains("<dt>Share of tablet space</dt><dd>25.0%</dd>"));
        UNIT_ASSERT(html.Contains("<dt>Share of IOPS</dt><dd>25.0%</dd>"));
        UNIT_ASSERT(html.Contains("<dt>Share of throughput</dt><dd>25.0%</dd>"));
        UNIT_ASSERT(!html.Contains("Shown "));
    }

}
} // namespace NKikimr::NDDisk
