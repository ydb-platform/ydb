#include <ydb/core/transfer/ut/common/transfer_common.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>


Y_UNIT_TEST_SUITE(TransferMetrics) {

auto DetailedMetricCheck = [](bool withHost, const auto& metricsList) {
    std::pair<TString, ui64> result;
    if (withHost) {
        if (metricsList.size() <= 10) {
            result.first = TStringBuilder() << "Expected over 10 metrics, got " << metricsList.size();
        }
    } else if (metricsList.size() != 10) {
        result.first = TStringBuilder() << "Expected exctly 10 metrics, got " << metricsList.size();
    }
    if (result.first) {
        return result;
    }
    ui64 totalWorkers = 0;
    ui64 totalWorkerValue = 0, totalHostValue = 0;
    for (const auto& metric : metricsList) {
        auto workerIter = metric.Labels.find("worker");
        auto hostIter = metric.Labels.find("host");
        TString host = hostIter.IsEnd() ? "" : hostIter->second;
        if (!withHost && !host.empty()) {
            result.first = TStringBuilder() << "Unexpected host label";
            return result;
        }
        if (!withHost && workerIter.IsEnd()) {
            result.first = TStringBuilder() << "No worker label ";
            return result;
        }
        if (!workerIter.IsEnd() && !host.empty()) {
            result.first = TStringBuilder() << "Both worker and non-empty host labels: " << workerIter->second << " and " << host;
            return result;
        }
        if (!workerIter.IsEnd()) {
            totalWorkers++;
            totalWorkerValue += metric.Value;
        } else {
            totalHostValue += metric.Value;
        }
    }
    if (totalWorkers != 10) {
        result.first = TStringBuilder() << "Expected 10 workers, got " << totalWorkers;
    }

    if (withHost && totalWorkerValue != totalHostValue) {
        result.first = TStringBuilder() << "Different overal value for workers and hosts";
    }
    result.second = totalWorkerValue;
    return result;
};

TMetricsValidator::TValueValidator ObjectNonZeroValueCheck = [](const auto& metricsList) -> TString {
    if (metricsList.size() != 1) {
        return TStringBuilder() << "Expected 1 metric, got " << metricsList.size();
    };
    if (metricsList[0].Value == 0) {
        return TStringBuilder() << "Expected non-zero metric, got 0";
    }
    return TString{};
};

TMetricsValidator::TValueValidator DetailedMetricBasicValueCheck = [] (const auto& metricsList) {
    return DetailedMetricCheck(false, metricsList).first;
};

TMetricsValidator::TValueValidator DetailedMetricWHostValueCheck = [](const auto& metricsList) {
    return DetailedMetricCheck(true, metricsList).first;
};

TMetricsValidator::TValueValidator DetailedMetricNonZeroValueCheck = [](const auto& metricsList) {
    auto checkRes = DetailedMetricCheck(false, metricsList);
    if (checkRes.first) {
        return checkRes.first;
    }
    if (checkRes.second == 0) {
        return TString{"Expected non-zero value"};
    };
    return TString{};
};

TMetricsValidator::TValueValidator DetailedMetricWHostNonZeroValueCheck = [](const auto& metricsList) {
    auto checkRes = DetailedMetricCheck(true, metricsList);
    if (checkRes.first) {
        return checkRes.first;
    }
    if (checkRes.second == 0) {
        return TString{"Expected non-zero value"};
    };
    return TString{};
};

std::pair<TVector<TMessage>, TExpectations> GenerateData(ui64 messageSize, ui64 messageCount) {
    TVector<TMessage> messages;
    TExpectations expectations;
    expectations.resize(messageCount);
    TString data{messageSize, 'a'};
    for (ui64 i = 0; i < messageCount; ++i) {
        TStringBuilder body;
        body << "Message-" << ToString(i) << "-" << data;
        messages.push_back(TMessage{.Message = body, .SeqNo = i + 1});
        expectations[i] = {{
            _C("SeqNo", i + 1),
        }};
    }
    return {messages, expectations};
}

Y_UNIT_TEST(NoMetrics) {
    const std::string tableType = "ROW";
    TMetricsValidator metricsValidator;

    metricsValidator.HasTransferMetrics();
    metricsValidator.AddValidator([](const TTransferMetrics& metrics) {
        if (metrics.DetailedMetrics.size() > 0) {
            return TString{"Detailed metrics are expected to be empty"};
        }
        if (metrics.AggregatedMetrics.size() > 0) {
            return TString{"Aggregated metrics are expected to be empty"};
        }
        return TString{};
    });

    auto testData = GenerateData(10240, 10);

    MainTestCase::CreateTransferSettings settings{};
    settings.MetricsLevel = 1;
    MainTestCase(std::nullopt, tableType).Run({.TableDDL = R"(
                CREATE TABLE `%s` (
                    Offset Uint64 NOT NULL,
                    SeqNo Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Offset, SeqNo)
                )  WITH (
                    STORE = %s
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Offset:$x._offset,
                            SeqNo:$x._seq_no,
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = std::move(testData.first),

            .Expectations = std::move(testData.second),

            .MetricsValidator = metricsValidator},

        settings);
}

Y_UNIT_TEST(OnlyObjectMetrics) {
    const std::string tableType = "ROW";
    TMetricsValidator metricsValidator;

    metricsValidator.HasTransferMetrics();
    metricsValidator.AddValidator([](const TTransferMetrics& metrics) {
        if (metrics.DetailedMetrics.size() > 0) {
            return TString{"Detailed metrics are expected to be empty"};
        }
        return TString{};
    });

    auto testData = GenerateData(10240, 100);

    MainTestCase::CreateTransferSettings settings{};
    settings.MetricsLevel = 2;
    MainTestCase(std::nullopt, tableType).Run({.TableDDL = R"(
                CREATE TABLE `%s` (
                    Offset Uint64 NOT NULL,
                    SeqNo Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Offset, SeqNo)
                )  WITH (
                    STORE = %s
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Offset:$x._offset,
                            SeqNo:$x._seq_no,
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = std::move(testData.first),

            .Expectations = std::move(testData.second),

            .MetricsValidator = metricsValidator},

        settings);
}

Y_UNIT_TEST(HasDetailedMetrics) {

    const std::string tableType = "ROW";
    TMetricsValidator metricsValidator;

    metricsValidator.HasTransferMetrics().HasDetailedMetrics();
    metricsValidator.HasTransferSensor("transfer.worker_restarts");
    metricsValidator.HasTransferSensor("transfer.processing_errors");
    metricsValidator.HasTransferSensor("transfer.write_errors");
    metricsValidator.HasTransferSensor("transfer.read.duration_milliseconds", ObjectNonZeroValueCheck);
    metricsValidator.HasTransferSensor("transfer.write.duration_milliseconds", ObjectNonZeroValueCheck);
    metricsValidator.HasTransferSensor("transfer.write_bytes", ObjectNonZeroValueCheck);
    metricsValidator.HasTransferSensor("transfer.write_rows", ObjectNonZeroValueCheck);
    metricsValidator.HasTransferSensor("transfer.decompress.cpu_elapsed_microseconds", ObjectNonZeroValueCheck);
    metricsValidator.HasTransferSensor("transfer.process.cpu_elapsed_microseconds", ObjectNonZeroValueCheck);
    metricsValidator.HasTransferSensor("transfer.worker_uptime_milliseconds_min", ObjectNonZeroValueCheck);

    metricsValidator.HasDetailedSensor("transfer.worker_restarts", DetailedMetricBasicValueCheck);
    metricsValidator.HasDetailedSensor("transfer.write_errors", DetailedMetricBasicValueCheck);
    metricsValidator.HasDetailedSensor("transfer.worker_uptime_milliseconds", DetailedMetricNonZeroValueCheck);
    metricsValidator.HasDetailedSensor("transfer.read.duration_milliseconds", DetailedMetricNonZeroValueCheck);
    metricsValidator.HasDetailedSensor("transfer.write.duration_milliseconds", DetailedMetricNonZeroValueCheck);
    metricsValidator.HasDetailedSensor("transfer.write_rows", DetailedMetricNonZeroValueCheck);
    metricsValidator.HasDetailedSensor("transfer.write_bytes", DetailedMetricNonZeroValueCheck);

    metricsValidator.HasDetailedSensor("transfer.decompress.cpu_elapsed_microseconds", DetailedMetricWHostNonZeroValueCheck);
    metricsValidator.HasDetailedSensor("transfer.process.cpu_elapsed_microseconds", DetailedMetricWHostNonZeroValueCheck);

    auto testData = GenerateData(2'000'000, 20);
    MainTestCase::CreateTransferSettings settings{};
    settings.MetricsLevel = 3;
    MainTestCase(std::nullopt, tableType).Run({.TableDDL = R"(
                CREATE TABLE `%s` (
                    Offset Uint64 NOT NULL,
                    SeqNo Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Offset, SeqNo)
                )  WITH (
                    STORE = %s
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Offset:$x._offset,
                            SeqNo:$x._seq_no,
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = std::move(testData.first),

            .Expectations = std::move(testData.second),

            .MetricsValidator = metricsValidator},

        settings);
}

Y_UNIT_TEST(TestDescribeTransfer) {
    const std::string tableType = "ROW";

    auto testData = GenerateData(20'000, 200);
    MainTestCase::CreateTransferSettings settings{};
    settings.MetricsLevel = 3;
    auto testCase = MainTestCase(std::nullopt, tableType);
    testCase.MakeTest({.TableDDL = R"(
                CREATE TABLE `%s` (
                    Offset Uint64 NOT NULL,
                    SeqNo Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Offset, SeqNo)
                )  WITH (
                    STORE = %s
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Offset:$x._offset,
                            SeqNo:$x._seq_no,
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = std::move(testData.first),

            .Expectations = std::move(testData.second)},

        settings);

        auto res = testCase.DescribeTransfer(true);
        auto stats = res.GetStats();

        testCase.MakeCleanup();

        auto MultiValueWindowNonZero = [&](const auto& window) {
            UNIT_ASSERT(window.per_minute().seconds());
            UNIT_ASSERT(window.per_hour().seconds());
        };

        auto AddAggregate = [&](const auto& src, auto& dst) {
            dst.mutable_per_minute()->set_seconds(src.per_minute().seconds() + dst.per_minute().seconds());
            dst.mutable_per_hour()->set_seconds(src.per_hour().seconds() + dst.per_hour().seconds());
        };

        UNIT_ASSERT_VALUES_EQUAL(stats.workers_stats_size(), 10);
        UNIT_ASSERT(stats.min_worker_uptime().seconds());
        UNIT_ASSERT(stats.min_worker_uptime().seconds());
        UNIT_ASSERT(stats.stats_collection_start().seconds());
        MultiValueWindowNonZero(stats.read_bytes());
        MultiValueWindowNonZero(stats.read_messages());
        MultiValueWindowNonZero(stats.write_bytes());
        MultiValueWindowNonZero(stats.write_rows());
        MultiValueWindowNonZero(stats.decompression_cpu_time());
        MultiValueWindowNonZero(stats.processing_cpu_time());

        Ydb::Replication::MultipleWindowsStat totalReadBytes;
        Ydb::Replication::MultipleWindowsStat totalReadMessages;
        Ydb::Replication::MultipleWindowsStat totalWriteBytes;
        Ydb::Replication::MultipleWindowsStat totalWriteRows;

        ui64 totalStateVal = 0;
        ui64 totalOffset = 0;
        THashSet<i64> partitions;
        for (const auto& workerStats: stats.workers_stats()) {
            UNIT_ASSERT(workerStats.worker_id());
            UNIT_ASSERT(workerStats.uptime().seconds());
            UNIT_ASSERT(workerStats.last_state_change().seconds());
            totalStateVal += static_cast<ui64>(workerStats.state());
            totalOffset += static_cast<ui64>(workerStats.read_offset());
            Cerr << "For worker: " << workerStats.worker_id() << " got partition: " << workerStats.partition_id() << "\n";
            UNIT_ASSERT(workerStats.partition_id() >= 0 && workerStats.partition_id() < 10);
            partitions.insert(workerStats.partition_id());

            AddAggregate(workerStats.read_bytes(), totalReadBytes);
            AddAggregate(workerStats.read_messages(), totalReadMessages);
            AddAggregate(workerStats.write_bytes(), totalWriteBytes);
            AddAggregate(workerStats.write_rows(), totalWriteRows);
        }

        UNIT_ASSERT(totalStateVal);
        UNIT_ASSERT(totalOffset);
        UNIT_ASSERT(partitions.size() > 1);
        MultiValueWindowNonZero(totalReadBytes);
        MultiValueWindowNonZero(totalReadMessages);
        MultiValueWindowNonZero(totalWriteBytes);
        MultiValueWindowNonZero(totalWriteRows);
}
}
