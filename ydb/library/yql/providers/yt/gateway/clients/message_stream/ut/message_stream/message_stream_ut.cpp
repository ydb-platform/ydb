#include <ydb/library/yql/providers/yt/gateway/clients/message_stream/yql_qyt_message_stream_client.h>
#include <ydb/library/yql/providers/yt/gateway/clients/message_stream/yql_yt_client.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/queue_client/consumer_client.h>
#include <ydb/library/yql/providers/yt/gateway/clients/message_stream/yql_qyt_blocking_queue.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/client/queue_client/queue_rowset.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/transaction_client/helpers.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYql {
namespace {

using namespace testing;
using namespace NFq;
using NYT::NApi::TMockClient;


class TTestMountCache : public NYT::NTabletClient::ITableMountCache {
public:
    NYT::TFuture<NYT::NTabletClient::TTableMountInfoPtr> GetTableInfo(const NYT::NYPath::TYPath& path) override {
        auto info = NYT::New<NYT::NTabletClient::TTableMountInfo>();
        info->Path = path;
        info->Schemas[NYT::NTabletClient::ETableSchemaKind::Primary] = NYT::NQueueClient::GetConsumerSchema();
        return NYT::MakeFuture(info);
    }
    void InvalidateTable(const NYT::NTabletClient::TTableMountInfoPtr&) override {}
    void InvalidateTablet(NYT::NTabletClient::TTabletId) override {}
    TInvalidationResult InvalidateOnError(const NYT::TError&, bool, NYT::NTabletClient::TTabletId) override { return {}; }
    void Clear() override {}
    void Reconfigure(NYT::NTabletClient::TTableMountCacheConfigPtr) override {}
};

void ConfigureEmptyConsumer(TMockClient& yt) {
    yt.SetTableMountCache(NYT::New<TTestMountCache>());
    EXPECT_CALL(yt, GetClusterName(_))
        .WillRepeatedly(Return(NYT::MakeFuture(std::optional<std::string>("test-cluster"))));
    auto names = NYT::New<NYT::NTableClient::TNameTable>();
    names->GetIdOrRegisterName("partition_index");
    names->GetIdOrRegisterName("offset");
    NYT::NApi::TSelectRowsResult result;
    result.Rowset = NYT::NApi::CreateRowset(names,
        NYT::MakeSharedRange(std::vector<NYT::NTableClient::TUnversionedRow>{}));
    EXPECT_CALL(yt, SelectRows(_, _)).WillRepeatedly(Return(NYT::MakeFuture(result)));
}

TEST(TQytMessageStream, DescribeStream) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    EXPECT_CALL(*yt, GetNode("//queues/topic/@tablet_count", _))
        .WillOnce(Return(NYT::MakeFuture(NYT::NYson::TYsonString(TStringBuf("3")))));
    auto client = CreateQytMessageStreamClient("topic", {.Client = yt, .PathPrefix = "//queues"});
    const auto result = client->DescribeStream().GetValueSync();
    ASSERT_TRUE(result.IsSuccess());
    EXPECT_EQ(result.Value.Partitions.size(), 3u);
}

TEST(TQytMessageStream, DescribePartitionOffsets) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    EXPECT_CALL(*yt, GetNode("//queues/topic/@tablet_count", _))
        .WillOnce(Return(NYT::MakeFuture(NYT::NYson::TYsonString(TStringBuf("3")))));
    NYT::NApi::TTabletInfo info;
    info.TrimmedRowCount = 7;
    info.TotalRowCount = 19;
    EXPECT_CALL(*yt, GetTabletInfos("//queues/topic", ElementsAre(2), _))
        .WillOnce(Return(NYT::MakeFuture(std::vector{info})));
    auto client = CreateQytMessageStreamClient("topic", {.Client = yt, .PathPrefix = "//queues"});
    const auto result = client->DescribePartition(TMessageStreamPartitionId{2}).GetValueSync();
    ASSERT_TRUE(result.IsSuccess());
    EXPECT_EQ(result.Value.PartitionId.Value, 2u);
    EXPECT_EQ(result.Value.StartOffset, 7u);
    EXPECT_EQ(result.Value.EndOffset, 19u);
}

TEST(TQytMessageStream, DescribeMissingPartition) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    EXPECT_CALL(*yt, GetNode("//topic/@tablet_count", _))
        .WillOnce(Return(NYT::MakeFuture(NYT::NYson::TYsonString(TStringBuf("1")))));
    const auto result = CreateQytMessageStreamClient("//topic", {.Client = yt})->DescribePartition(TMessageStreamPartitionId{1}).GetValueSync();
    EXPECT_EQ(result.Status, EMessageStreamStatus::NotFound);
    EXPECT_FALSE(result.Issues.Empty());
}

TEST(TQytMessageStream, DescribeFailure) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    EXPECT_CALL(*yt, GetNode("//topic/@tablet_count", _))
        .WillOnce(Return(NYT::MakeFuture<NYT::NYson::TYsonString>(NYT::TError("describe failed"))));
    const auto result = CreateQytMessageStreamClient("//topic", {.Client = yt})->DescribeStream().GetValueSync();
    EXPECT_EQ(result.Status, EMessageStreamStatus::InternalError);
    EXPECT_FALSE(result.Issues.Empty());
}

TEST(TQytMessageStream, ConfirmStartAndEventLimit) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    ConfigureEmptyConsumer(*yt);
    NYT::NApi::TTabletInfo info;
    info.TotalRowCount = 43;
    EXPECT_CALL(*yt, GetTabletInfos("//topic", ElementsAre(2), _))
        .WillOnce(Return(NYT::MakeFuture(std::vector{info})));

    auto client = CreateQytMessageStreamClient("//topic", {.Client = yt, .PollPeriodMs = 1});
    TMessageStreamReadSessionSettings settings;
    settings.PartitionIds = {TMessageStreamPartitionId{0}};
    settings.Consumer = "//consumer";
    settings.PartitionIds = {TMessageStreamPartitionId{2}};
    auto session = client->CreateReadSession(settings);
    ASSERT_TRUE(session->WaitEvent().Wait(TDuration::Seconds(5)));
    EXPECT_TRUE(session->GetEvents({.MaxEventsCount = 0}).empty());
    auto events = session->GetEvents({.MaxEventsCount = 1});
    ASSERT_EQ(events.size(), 1u);
    auto* start = std::get_if<TMessageStreamPartitionStartRequestedEvent>(&events.front());
    ASSERT_NE(start, nullptr);
    EXPECT_EQ(start->PartitionControl->GetPartitionId().Value, 2u);
    EXPECT_EQ(start->EndOffset, 43u);
    EXPECT_CALL(*yt, PullQueueConsumer(_, _, std::optional<i64>(42), 2, _, _))
        .WillOnce(Return(NYT::MakeFuture<NYT::NApi::TPullQueueResult>(NYT::TError("permission denied"))));
    start->PartitionControl->ConfirmStart(42, std::nullopt);
    ASSERT_TRUE(session->WaitEvent().Wait(TDuration::Seconds(5)));
    events = session->GetEvents({.MaxEventsCount = 1});
    ASSERT_EQ(events.size(), 1u);
    const auto* closed = std::get_if<TMessageStreamSessionClosedEvent>(&events.front());
    ASSERT_NE(closed, nullptr);
    EXPECT_EQ(closed->Status, EMessageStreamStatus::InvalidArgument);
    session->Close().GetValueSync();
}

void CheckReadData(bool withWriteTime) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    ConfigureEmptyConsumer(*yt);

    NYT::NApi::TTabletInfo info;
    info.TotalRowCount = 43;
    EXPECT_CALL(*yt, GetTabletInfos("//topic", ElementsAre(2), _))
        .WillOnce(Return(NYT::MakeFuture(std::vector{info})));

    auto names = NYT::New<NYT::NTableClient::TNameTable>();
    const auto dataId = names->GetIdOrRegisterName("data");
    NYT::NTableClient::TUnversionedRowBuilder builder;
    builder.AddValue(NYT::NTableClient::MakeUnversionedStringValue(TStringBuf("payload"), dataId));
    if (withWriteTime) {
        const auto timestampId = names->GetIdOrRegisterName("$timestamp");
        builder.AddValue(NYT::NTableClient::MakeUnversionedUint64Value(
            NYT::NTransactionClient::TimestampFromUnixTime(123).Underlying(), timestampId));
    }
    auto rows = NYT::MakeSharedRange(std::vector<NYT::NTableClient::TUnversionedRow>{builder.GetRow()});
    auto rowset = NYT::NQueueClient::CreateQueueRowset(NYT::NApi::CreateRowset(names, rows), 42);
    EXPECT_CALL(*yt, PullQueueConsumer(_, _, std::optional<i64>(42), 2, _, _))
        .WillOnce(Return(NYT::MakeFuture(NYT::NApi::TPullQueueResult{rowset})));
    EXPECT_CALL(*yt, PullQueueConsumer(_, _, std::optional<i64>(43), 2, _, _))
        .WillOnce(Return(NYT::MakeFuture<NYT::NApi::TPullQueueResult>(NYT::TError("permission denied"))));

    TMessageStreamReadSessionSettings settings;
    settings.PartitionIds = {TMessageStreamPartitionId{0}};
    settings.Consumer = "//consumer";
    settings.RequireWriteTime = withWriteTime;
    settings.PartitionIds = {TMessageStreamPartitionId{2}};
    auto session = CreateQytMessageStreamClient("//topic", {.Client = yt, .PollPeriodMs = 1})->CreateReadSession(settings);
    ASSERT_TRUE(session->WaitEvent().Wait(TDuration::Seconds(5)));
    auto events = session->GetEvents({.MaxEventsCount = 1});
    ASSERT_EQ(events.size(), 1u);
    std::get<TMessageStreamPartitionStartRequestedEvent>(events.front()).PartitionControl->ConfirmStart(42, std::nullopt);
    ASSERT_TRUE(session->WaitEvent().Wait(TDuration::Seconds(5)));
    events = session->GetEvents({.MaxEventsCount = 1});
    ASSERT_EQ(events.size(), 1u);
    const auto* data = std::get_if<TMessageStreamDataEvent>(&events.front());
    ASSERT_NE(data, nullptr);
    ASSERT_EQ(data->Records.size(), 1u);
    EXPECT_EQ(data->PartitionControl->GetPartitionId().Value, 2u);
    EXPECT_EQ(data->Records.front().Data.value(), "payload");
    EXPECT_EQ(data->Records.front().Id.PartitionId.Value, 2u);
    EXPECT_EQ(data->Records.front().Id.Offset, 42u);
    EXPECT_FALSE(data->Records.front().CreateTime);
    EXPECT_FALSE(data->Records.front().SeqNo);
    if (withWriteTime) {
        EXPECT_EQ(data->Records.front().WriteTime, TInstant::Seconds(123));
    } else {
        EXPECT_FALSE(data->Records.front().WriteTime);
    }
    ASSERT_TRUE(session->WaitEvent().Wait(TDuration::Seconds(5)));
    events = session->GetEvents({.MaxEventsCount = 1});
    ASSERT_EQ(events.size(), 1u);
    EXPECT_TRUE(std::holds_alternative<TMessageStreamSessionClosedEvent>(events.front()));
    session->Close().GetValueSync();
}

TEST(TQytMessageStream, ReadDataAndKeepNextEvent) {
    CheckReadData(false);
}

TEST(TQytMessageStream, PreserveBackendWriteTime) {
    CheckReadData(true);
}

TEST(TQytMessageStream, InclusiveMaxOffset) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    ConfigureEmptyConsumer(*yt);
    NYT::NApi::TTabletInfo info;
    info.TotalRowCount = 44;
    EXPECT_CALL(*yt, GetTabletInfos("//topic", ElementsAre(0), _))
        .WillOnce(Return(NYT::MakeFuture(std::vector{info})));

    auto names = NYT::New<NYT::NTableClient::TNameTable>();
    const auto dataId = names->GetIdOrRegisterName("data");
    NYT::NTableClient::TUnversionedRowBuilder builder;
    builder.AddValue(NYT::NTableClient::MakeUnversionedStringValue(TStringBuf("payload"), dataId));
    auto rows = NYT::MakeSharedRange(std::vector<NYT::NTableClient::TUnversionedRow>{builder.GetRow(), builder.GetRow()});
    auto rowset = NYT::NQueueClient::CreateQueueRowset(NYT::NApi::CreateRowset(names, rows), 42);
    EXPECT_CALL(*yt, PullQueueConsumer(_, _, std::optional<i64>(42), 0, _, _))
        .WillOnce(Return(NYT::MakeFuture(NYT::NApi::TPullQueueResult{rowset})));

    TMessageStreamReadSessionSettings settings;
    settings.PartitionIds = {TMessageStreamPartitionId{0}};
    settings.Consumer = "//consumer";
    settings.AutoPartitioningSupport = false;
    auto session = CreateQytMessageStreamClient("//topic", {.Client = yt, .PollPeriodMs = 1})->CreateReadSession(settings);
    ASSERT_TRUE(session->WaitEvent().Wait(TDuration::Seconds(5)));
    auto events = session->GetEvents({.MaxEventsCount = 1});
    ASSERT_EQ(events.size(), 1u);
    std::get<TMessageStreamPartitionStartRequestedEvent>(events.front()).PartitionControl->ConfirmStart(42, 42);
    ASSERT_TRUE(session->WaitEvent().Wait(TDuration::Seconds(5)));
    events = session->GetEvents({.MaxEventsCount = 1});
    ASSERT_EQ(events.size(), 1u);
    const auto* data = std::get_if<TMessageStreamDataEvent>(&events.front());
    ASSERT_NE(data, nullptr);
    ASSERT_EQ(data->Records.size(), 1u);
    EXPECT_EQ(data->Records.front().Id.Offset, 42u);
    session->Close().GetValueSync();
}

TEST(TQytMessageStream, CloseBeforeConfirmStart) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    ConfigureEmptyConsumer(*yt);
    NYT::NApi::TTabletInfo info;
    info.TotalRowCount = 43;
    EXPECT_CALL(*yt, GetTabletInfos("//topic", ElementsAre(0), _))
        .WillOnce(Return(NYT::MakeFuture(std::vector{info})));

    TMessageStreamReadSessionSettings settings;
    settings.PartitionIds = {TMessageStreamPartitionId{0}};
    settings.Consumer = "//consumer";
    auto session = CreateQytMessageStreamClient("//topic", {.Client = yt, .PollPeriodMs = 1})->CreateReadSession(settings);
    ASSERT_TRUE(session->WaitEvent().Wait(TDuration::Seconds(5)));
    session->Close().GetValueSync();
}


TEST(TQytMessageStream, RejectUnsupportedSettingsBeforeBackendAccess) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    auto client = CreateQytMessageStreamClient("//topic", {.Client = yt});
    TMessageStreamReadSessionSettings settings;
    settings.PartitionIds = {TMessageStreamPartitionId{0}};
    try {
        client->CreateReadSession(settings);
        FAIL() << "Expected Unsupported";
    } catch (const TMessageStreamException& error) {
        EXPECT_EQ(error.GetStatus(), EMessageStreamStatus::Unsupported);
    }
    settings.Consumer = "//consumer";
    settings.PartitionIds.push_back(TMessageStreamPartitionId{0});
    try {
        client->CreateReadSession(settings);
        FAIL() << "Expected InvalidArgument";
    } catch (const TMessageStreamException& error) {
        EXPECT_EQ(error.GetStatus(), EMessageStreamStatus::InvalidArgument);
    }
}


TEST(TQytMessageStream, ConsumerLookupFailureIsNotReplacedWithZero) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    EXPECT_CALL(*yt, GetClusterName(_))
        .WillOnce(Return(NYT::MakeFuture<std::optional<std::string>>(NYT::TError("consumer unavailable"))));
    auto client = CreateQytMessageStreamClient("//topic", {.Client = yt});
    TMessageStreamReadSessionSettings settings;
    settings.Consumer = "//consumer";
    settings.PartitionIds = {TMessageStreamPartitionId{0}};
    EXPECT_ANY_THROW(client->CreateReadSession(settings));
}

TEST(TQytMessageStream, StreamIdentityIsImmutableAndNonempty) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    auto client = CreateQytMessageStreamClient("//topic", {.Client = yt});
    EXPECT_EQ(client->GetStream(), "//topic");
    EXPECT_THROW(CreateQytMessageStreamClient("", {.Client = yt}), TMessageStreamException);
}

TEST(TQytMessageStream, MultiplePartitionsHaveIndependentControls) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    ConfigureEmptyConsumer(*yt);
    NYT::NApi::TTabletInfo info;
    info.TotalRowCount = 0;
    EXPECT_CALL(*yt, GetTabletInfos("//topic", ElementsAre(0), _))
        .WillOnce(Return(NYT::MakeFuture(std::vector{info})));
    EXPECT_CALL(*yt, GetTabletInfos("//topic", ElementsAre(1), _))
        .WillOnce(Return(NYT::MakeFuture(std::vector{info})));
    TMessageStreamReadSessionSettings settings;
    settings.Consumer = "//consumer";
    settings.PartitionIds = {TMessageStreamPartitionId{0}, TMessageStreamPartitionId{1}};
    settings.AutoPartitioningSupport = false;
    auto session = CreateQytMessageStreamClient("//topic", {.Client = yt, .PollPeriodMs = 1})->CreateReadSession(settings);
    std::vector<std::shared_ptr<IMessageStreamPartitionControl>> controls;
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(session->WaitEvent().Wait(TDuration::Seconds(5)));
        auto events = session->GetEvents({.MaxEventsCount = 1});
        ASSERT_EQ(events.size(), 1u);
        auto control = std::get<TMessageStreamPartitionStartRequestedEvent>(events.front()).PartitionControl;
        control->ConfirmStart(std::nullopt, std::nullopt);
        controls.push_back(control);
    }
    EXPECT_NE(controls[0]->GetPartitionId(), controls[1]->GetPartitionId());
    session->Close().GetValueSync();
    for (const auto& control : controls) {
        EXPECT_FALSE(control->AcknowledgeRange(0, 1));
        control->RequestStatus();
    }
    EXPECT_TRUE(session->GetEvents({}).empty());
}

TEST(TQytMessageStream, ControlEventDoesNotBlockBehindFullDataBuffer) {
    TBlockingEQueue<TMessageStreamReadEvent> queue(1);
    queue.Push(TMessageStreamDataEvent{}, 1);
    queue.PushControl(TMessageStreamPartitionStatusEvent{});
    queue.PushControl(TMessageStreamSessionClosedEvent{});
    ASSERT_TRUE(std::holds_alternative<TMessageStreamDataEvent>(*queue.Pop(false)));
    ASSERT_TRUE(std::holds_alternative<TMessageStreamPartitionStatusEvent>(*queue.Pop(false)));
    ASSERT_TRUE(std::holds_alternative<TMessageStreamSessionClosedEvent>(*queue.Pop(false)));
    queue.Stop();
    queue.PushControl(TMessageStreamPartitionStatusEvent{});
    EXPECT_FALSE(queue.Pop(false));
}


TEST(TQytMessageStream, NativeClientUsesEdsToken) {
    auto client = CreateYtClient("localhost:9013", "eds-token");
    EXPECT_EQ(client->GetOptions().Token, "eds-token");
}

TEST(TQytMessageStream, DetectQueueAndTableFromAttributes) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    EXPECT_CALL(*yt, GetNode("//queue/@", _)).WillOnce(Return(NYT::MakeFuture(
        NYT::NYson::TYsonString(TStringBuf("{type=table;dynamic=%true;sorted=%false}")))));
    EXPECT_CALL(*yt, GetNode("//table/@", _)).WillOnce(Return(NYT::MakeFuture(
        NYT::NYson::TYsonString(TStringBuf("{type=table;dynamic=%false;sorted=%false}")))));
    EXPECT_CALL(*yt, GetNode("//sorted/@", _)).WillOnce(Return(NYT::MakeFuture(
        NYT::NYson::TYsonString(TStringBuf("{type=table;dynamic=%true;sorted=%true}")))));
    EXPECT_TRUE(IsYtQueue(yt, "//queue").GetValueSync());
    EXPECT_FALSE(IsYtQueue(yt, "//table").GetValueSync());
    EXPECT_FALSE(IsYtQueue(yt, "//sorted").GetValueSync());
}

TEST(TQytMessageStream, ObjectLookupFailureIsNotAQueue) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    EXPECT_CALL(*yt, GetNode("//missing/@", _)).WillOnce(Return(
        NYT::MakeFuture<NYT::NYson::TYsonString>(NYT::TError("not found"))));
    EXPECT_ANY_THROW(IsYtQueue(yt, "//missing").GetValueSync());
}

TEST(TQytMessageStream, RejectTimeSeekBeforeBackendAccess) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    TMessageStreamReadSessionSettings settings;
    settings.Consumer = "//consumer";
    settings.PartitionIds = {TMessageStreamPartitionId{0}};
    settings.ReadFromWriteTime = TInstant::Seconds(123);
    try {
        CreateQytMessageStreamClient("//queue", {.Client = yt})->CreateReadSession(settings);
        FAIL() << "Expected Unsupported";
    } catch (const TMessageStreamException& error) {
        EXPECT_EQ(error.GetStatus(), EMessageStreamStatus::Unsupported);
    }
}

TEST(TQytMessageStream, TrimmedStartAndAcknowledgement) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    ConfigureEmptyConsumer(*yt);
    NYT::NApi::TTabletInfo info;
    info.TrimmedRowCount = 42;
    info.TotalRowCount = 43;
    EXPECT_CALL(*yt, GetTabletInfos("//topic", ElementsAre(0), _))
        .WillOnce(Return(NYT::MakeFuture(std::vector{info})));
    auto names = NYT::New<NYT::NTableClient::TNameTable>();
    const auto dataId = names->GetIdOrRegisterName("data");
    NYT::NTableClient::TUnversionedRowBuilder builder;
    builder.AddValue(NYT::NTableClient::MakeUnversionedStringValue(TStringBuf("payload"), dataId));
    auto rows = NYT::MakeSharedRange(std::vector<NYT::NTableClient::TUnversionedRow>{builder.GetRow()});
    auto rowset = NYT::NQueueClient::CreateQueueRowset(NYT::NApi::CreateRowset(names, rows), 42);
    EXPECT_CALL(*yt, PullQueueConsumer(_, _, std::optional<i64>(42), 0, _, _))
        .WillOnce(Return(NYT::MakeFuture(NYT::NApi::TPullQueueResult{rowset})));
    auto commitStarted = NThreading::NewPromise();
    auto commit = NYT::NewPromise<NYT::NApi::ITransactionPtr>();
    EXPECT_CALL(*yt, StartTransaction(_, _)).WillOnce([&](auto, const auto&) {
        commitStarted.SetValue();
        return commit.ToFuture();
    });
    TMessageStreamReadSessionSettings settings;
    settings.Consumer = "//consumer";
    settings.PartitionIds = {TMessageStreamPartitionId{0}};
    settings.AutoPartitioningSupport = false;
    auto session = CreateQytMessageStreamClient("//topic", {.Client = yt, .PollPeriodMs = 1})->CreateReadSession(settings);
    ASSERT_TRUE(session->WaitEvent().Wait(TDuration::Seconds(5)));
    auto events = session->GetEvents({.MaxEventsCount = 1});
    auto control = std::get<TMessageStreamPartitionStartRequestedEvent>(events.front()).PartitionControl;
    control->ConfirmStart(0, std::nullopt);
    ASSERT_TRUE(session->WaitEvent().Wait(TDuration::Seconds(5)));
    events = session->GetEvents({.MaxEventsCount = 1});
    ASSERT_EQ(std::get<TMessageStreamDataEvent>(events.front()).Records.front().Id.Offset, 42u);
    EXPECT_TRUE(control->AcknowledgeRange(42, 43));
    EXPECT_TRUE(commitStarted.GetFuture().Wait(TDuration::Seconds(5)));
    // An unfinished backend transaction must not block the acknowledgement caller.
    auto close = session->Close();
    EXPECT_FALSE(control->AcknowledgeRange(43, 44));
    commit.Set(NYT::TError("test cancelled"));
    EXPECT_TRUE(close.Wait(TDuration::Seconds(5)));
}

TEST(TQytMessageStream, CloseCancelsOutstandingPull) {
    auto yt = NYT::New<StrictMock<TMockClient>>();
    ConfigureEmptyConsumer(*yt);
    NYT::NApi::TTabletInfo info;
    info.TotalRowCount = 43;
    EXPECT_CALL(*yt, GetTabletInfos("//topic", ElementsAre(0), _))
        .WillOnce(Return(NYT::MakeFuture(std::vector{info})));
    auto pull = NYT::NewPromise<NYT::NApi::TPullQueueResult>();
    auto started = NThreading::NewPromise();
    EXPECT_CALL(*yt, PullQueueConsumer(_, _, _, 0, _, _)).WillOnce([&](const auto&, const auto&, auto, auto, const auto&, const auto&) {
        started.SetValue();
        return pull.ToFuture();
    });
    TMessageStreamReadSessionSettings settings;
    settings.Consumer = "//consumer";
    settings.PartitionIds = {TMessageStreamPartitionId{0}};
    auto session = CreateQytMessageStreamClient("//topic", {.Client = yt, .PollPeriodMs = 1})->CreateReadSession(settings);
    ASSERT_TRUE(session->WaitEvent().Wait(TDuration::Seconds(5)));
    auto events = session->GetEvents({.MaxEventsCount = 1});
    auto control = std::get<TMessageStreamPartitionStartRequestedEvent>(events.front()).PartitionControl;
    control->RequestStatus();
    events = session->GetEvents({.MaxEventsCount = 1});
    EXPECT_FALSE(std::get<TMessageStreamPartitionStatusEvent>(events.front()).EndOffset);
    control->ConfirmStart(std::nullopt, std::nullopt);
    ASSERT_TRUE(started.GetFuture().Wait(TDuration::Seconds(5)));
    auto close = session->Close();
    EXPECT_TRUE(close.Wait(TDuration::Seconds(5)));
    EXPECT_TRUE(session->GetEvents({}).empty());
}

} // namespace
} // namespace NYql
