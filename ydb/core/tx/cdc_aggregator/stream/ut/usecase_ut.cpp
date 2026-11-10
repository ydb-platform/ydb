#include "helpers.h"

#include <ydb/core/tx/cdc_aggregator/stream/activate_usecase.h>
#include <ydb/core/tx/cdc_aggregator/stream/configure_usecase.h>
#include <ydb/core/tx/cdc_aggregator/stream/replace_source_usecase.h>
#include <ydb/core/tx/cdc_aggregator/stream/report_delivered_usecase.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NCdcAggregator {

using namespace NTest;

namespace {

using EReportStatus = TReportDeliveredResult::EStatus;
using ESourceStatus = TReportDeliveredResult::TSourceResult::EStatus;

TReportDeliveredCommand MakeReportCommand(TVector<std::pair<TSourceId, ui64>> reports) {
    TReportDeliveredCommand command{.StreamPathId = StreamPathId};
    for (const auto& [source, step] : reports) {
        command.Reports.push_back({.Source = source, .Delivered = MakeBarrier(step)});
    }
    return command;
}

} // namespace

Y_UNIT_TEST_SUITE(TConfigureStream) {

    Y_UNIT_TEST(ShouldSaveConfiguration_WhenStreamIsNew) {
        // given
        TStream stream;
        TRecordingRepository repository;

        // when
        const auto result = ConfigureStream(stream, repository, {.Config = MakeStreamConfig({3, 1, 2})});

        // then
        UNIT_ASSERT(result.Status == TConfigureResult::EStatus::Success);
        UNIT_ASSERT_VALUES_EQUAL(result.ConfigVersion, 1);
        UNIT_ASSERT_VALUES_EQUAL(repository.Saved.size(), 1);
        UNIT_ASSERT(stream.GetConfig().Sources == TVector<TSourceId>({1, 2, 3}));
    }

    Y_UNIT_TEST(ShouldNotSave_WhenSameConfigurationRepeats) {
        // given
        TStream stream;
        TRecordingRepository repository;
        ConfigureStream(stream, repository, {.Config = MakeStreamConfig({1, 2})});

        // when
        const auto result = ConfigureStream(stream, repository, {.Config = MakeStreamConfig({2, 1})});

        // then
        UNIT_ASSERT(result.Status == TConfigureResult::EStatus::Success);
        UNIT_ASSERT_VALUES_EQUAL(repository.Saved.size(), 1);
    }

    Y_UNIT_TEST(ShouldReturnStoredVersion_WhenConfigurationConflicts) {
        // given
        TStream stream;
        TRecordingRepository repository;
        ConfigureStream(stream, repository, {.Config = MakeStreamConfig({1, 2})});

        // when
        const auto result = ConfigureStream(stream, repository, {.Config = MakeStreamConfig({1, 2, 3}, 2)});

        // then
        UNIT_ASSERT(result.Status == TConfigureResult::EStatus::Conflict);
        UNIT_ASSERT_VALUES_EQUAL(result.ConfigVersion, 1);
        UNIT_ASSERT_VALUES_EQUAL(repository.Saved.size(), 1);
    }

    Y_UNIT_TEST(ShouldRejectConfiguration_WhenItCannotServeStream) {
        // given
        auto noSources = MakeStreamConfig({});
        auto duplicateSource = MakeStreamConfig({1, 1});
        auto noVersion = MakeStreamConfig({1}, 0);
        auto noPathId = MakeStreamConfig({1});
        noPathId.StreamPathId = TPathId();
        auto duplicatePartition = MakeStreamConfig({1});
        duplicatePartition.Partitions.push_back(duplicatePartition.Partitions.front());
        auto noPartitionTablet = MakeStreamConfig({1});
        noPartitionTablet.Partitions.front().TabletId = 0;

        for (auto config : {noSources, duplicateSource, noVersion, noPathId, duplicatePartition, noPartitionTablet}) {
            TStream stream;
            TRecordingRepository repository;

            // when
            const auto result = ConfigureStream(stream, repository, {.Config = config});

            // then
            UNIT_ASSERT(result.Status == TConfigureResult::EStatus::BadRequest);
            UNIT_ASSERT(!result.Error.empty());
            UNIT_ASSERT(!stream.IsConfigured());
            UNIT_ASSERT(repository.Saved.empty());
        }
    }
}

Y_UNIT_TEST_SUITE(TActivateStream) {

    Y_UNIT_TEST(ShouldReportActivationOnce_WhenActivatedTwice) {
        // given
        auto stream = MakeConfiguredStream({1});
        TRecordingRepository repository;

        // when
        const auto first = ActivateStream(stream, repository, {.StreamPathId = StreamPathId, .ConfigVersion = 1});
        const auto second = ActivateStream(stream, repository, {.StreamPathId = StreamPathId, .ConfigVersion = 1});

        // then
        UNIT_ASSERT(first.Status == TActivateResult::EStatus::Success && first.Activated);
        UNIT_ASSERT(second.Status == TActivateResult::EStatus::Success && !second.Activated);
        UNIT_ASSERT_VALUES_EQUAL(repository.Saved.size(), 1);
    }

    Y_UNIT_TEST(ShouldRejectActivation_WhenStreamIsNotConfiguredOrOther) {
        // given
        TStream notConfigured;
        auto stream = MakeConfiguredStream({1});
        TRecordingRepository repository;

        // when
        const auto notReady = ActivateStream(notConfigured, repository, {.StreamPathId = StreamPathId, .ConfigVersion = 1});
        const auto wrongStream = ActivateStream(stream, repository, {.StreamPathId = TPathId(1, 1), .ConfigVersion = 1});
        const auto wrongVersion = ActivateStream(stream, repository, {.StreamPathId = StreamPathId, .ConfigVersion = 2});

        // then
        UNIT_ASSERT(notReady.Status == TActivateResult::EStatus::NotReady);
        UNIT_ASSERT(wrongStream.Status == TActivateResult::EStatus::WrongStream);
        UNIT_ASSERT(wrongVersion.Status == TActivateResult::EStatus::Conflict);
        UNIT_ASSERT(repository.Saved.empty());
        UNIT_ASSERT(stream.GetPhase() == EStreamPhase::Configured);
    }
}

Y_UNIT_TEST_SUITE(TReportDelivered) {

    Y_UNIT_TEST(ShouldAnswerNotReady_WhenStreamIsNotConfigured) {
        // given
        TStream stream;
        TRecordingRepository repository;

        // when
        const auto result = ReportDelivered(stream, repository, MakeReportCommand({{1, 100}}));

        // then
        UNIT_ASSERT(result.Status == EReportStatus::NotReady);
        UNIT_ASSERT(result.Results.empty());
        UNIT_ASSERT(repository.Saved.empty());
    }

    Y_UNIT_TEST(ShouldRejectBatch_WhenStreamDiffers) {
        // given
        auto stream = MakeConfiguredStream({1});
        TRecordingRepository repository;
        auto command = MakeReportCommand({{1, 100}});
        command.StreamPathId = TPathId(1, 1);

        // when
        const auto result = ReportDelivered(stream, repository, command);

        // then
        UNIT_ASSERT(result.Status == EReportStatus::WrongStream);
        UNIT_ASSERT(repository.Saved.empty());
    }

    Y_UNIT_TEST(ShouldConfirmFinalProgress_WhenSourceRepeatsInBatch) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TRecordingRepository repository;

        // when
        const auto result = ReportDelivered(stream, repository, MakeReportCommand({{1, 100}, {1, 120}, {2, 90}}));

        // then
        UNIT_ASSERT(result.Status == EReportStatus::Success);
        UNIT_ASSERT_VALUES_EQUAL(result.Results.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(result.Results[0].PersistedVersion, TRowVersion(120, 0));
        UNIT_ASSERT_VALUES_EQUAL(result.Results[1].PersistedVersion, TRowVersion(120, 0));
        UNIT_ASSERT_VALUES_EQUAL(result.Results[2].PersistedVersion, TRowVersion(90, 0));
        UNIT_ASSERT(result.AdvancedReadyBarrier);
        UNIT_ASSERT_VALUES_EQUAL(result.AdvancedReadyBarrier->Version, TRowVersion(90, 0));
        UNIT_ASSERT_VALUES_EQUAL(repository.Saved.size(), 1);
    }

    Y_UNIT_TEST(ShouldConfirmStoredVersion_WhenReportIsOlder) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TRecordingRepository repository;
        ReportDelivered(stream, repository, MakeReportCommand({{1, 120}}));

        // when
        const auto result = ReportDelivered(stream, repository, MakeReportCommand({{1, 100}}));

        // then
        UNIT_ASSERT(result.Results[0].Status == ESourceStatus::Success);
        UNIT_ASSERT(!result.Results[0].Advanced);
        UNIT_ASSERT_VALUES_EQUAL(result.Results[0].PersistedVersion, TRowVersion(120, 0));
        UNIT_ASSERT(!result.AdvancedReadyBarrier);
        UNIT_ASSERT_VALUES_EQUAL(repository.Saved.size(), 1);
    }

    Y_UNIT_TEST(ShouldRejectOnlyBadReports_WhenBatchIsMixed) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TRecordingRepository repository;
        TStreamChanges setup;
        stream.ReplaceSource({.Parent = 2, .Children = {3}}, setup);
        auto command = MakeReportCommand({{1, 100}, {9, 100}, {2, 100}});
        command.Reports.push_back({.Source = 3, .Delivered = {.Version = TRowVersion::Min(), .Payload = "x"}});
        command.Reports.push_back({.Source = 3, .Delivered = {.Version = TRowVersion(100, 0)}});
        command.Reports.push_back({.Source = 3, .Delivered = {.Version = TRowVersion(100, 0), .Payload = TString(MaxBarrierPayloadBytes + 1, 'x')}});

        // when
        const auto result = ReportDelivered(stream, repository, command);

        // then
        UNIT_ASSERT(result.Status == EReportStatus::Success);
        UNIT_ASSERT(result.Results[0].Status == ESourceStatus::Success);
        UNIT_ASSERT(result.Results[1].Status == ESourceStatus::UnknownSource);
        UNIT_ASSERT(result.Results[2].Status == ESourceStatus::RetiredSource);
        UNIT_ASSERT(result.Results[3].Status == ESourceStatus::BadRequest);
        UNIT_ASSERT(result.Results[4].Status == ESourceStatus::BadRequest);
        UNIT_ASSERT(result.Results[5].Status == ESourceStatus::BadRequest);
        UNIT_ASSERT(!*stream.FindSourceProgress(3));
    }

    Y_UNIT_TEST(ShouldRejectBatch_WhenItIsTooLarge) {
        // given
        auto stream = MakeConfiguredStream({1});
        TRecordingRepository repository;
        TReportDeliveredCommand command{.StreamPathId = StreamPathId};
        command.Reports.resize(MaxReportsPerBatch + 1, {.Source = 1, .Delivered = MakeBarrier(100)});

        // when
        const auto result = ReportDelivered(stream, repository, command);

        // then
        UNIT_ASSERT(result.Status == EReportStatus::BadRequest);
        UNIT_ASSERT(!*stream.FindSourceProgress(1));
    }
}

Y_UNIT_TEST_SUITE(TReplaceStreamSource) {

    Y_UNIT_TEST(ShouldSaveOnce_WhenReplacementRepeats) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TRecordingRepository repository;
        const TReplaceSourceCommand command{.StreamPathId = StreamPathId, .Replacement = {.Parent = 1, .Children = {4, 3}}};

        // when
        const auto first = ReplaceStreamSource(stream, repository, command);
        const auto second = ReplaceStreamSource(stream, repository, command);

        // then
        UNIT_ASSERT(first.Status == TReplaceSourceResult::EStatus::Success);
        UNIT_ASSERT(second.Status == TReplaceSourceResult::EStatus::Success);
        UNIT_ASSERT_VALUES_EQUAL(repository.Saved.size(), 1);
    }

    Y_UNIT_TEST(ShouldRejectReplacement_WhenRequestIsInvalid) {
        // given
        TStream notConfigured;
        auto stream = MakeConfiguredStream({1, 2});
        TRecordingRepository repository;

        // when
        const auto notReady = ReplaceStreamSource(notConfigured, repository,
            {.StreamPathId = StreamPathId, .Replacement = {.Parent = 1, .Children = {3}}});
        const auto wrongStream = ReplaceStreamSource(stream, repository,
            {.StreamPathId = TPathId(1, 1), .Replacement = {.Parent = 1, .Children = {3}}});
        const auto noChildren = ReplaceStreamSource(stream, repository,
            {.StreamPathId = StreamPathId, .Replacement = {.Parent = 1}});
        const auto unknown = ReplaceStreamSource(stream, repository,
            {.StreamPathId = StreamPathId, .Replacement = {.Parent = 9, .Children = {3}}});

        // then
        UNIT_ASSERT(notReady.Status == TReplaceSourceResult::EStatus::NotReady);
        UNIT_ASSERT(wrongStream.Status == TReplaceSourceResult::EStatus::WrongStream);
        UNIT_ASSERT(noChildren.Status == TReplaceSourceResult::EStatus::BadRequest);
        UNIT_ASSERT(unknown.Status == TReplaceSourceResult::EStatus::UnknownSource);
        UNIT_ASSERT(repository.Saved.empty());
    }
}

} // namespace NKikimr::NCdcAggregator
