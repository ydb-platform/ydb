#include "query_statistics.h"

#include <yt/yt_proto/yt/client/query_client/proto/query_statistics.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueryClient {

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TAggregate<T>::operator=(T value)
{
    Total_ = value;
    Max_ = value;
    ArgmaxNode_ = NNet::GetLocalHostName();
}

template <class T>
void TAggregate<T>::Merge(const TAggregate<T>& other)
{
    Total_ += other.Total_;
    if (other.Max_ > Max_) {
        ArgmaxNode_ = other.ArgmaxNode_;
        Max_ = other.Max_;
    }
}

template class TAggregate<i64>;
template class TAggregate<double>;

template <class T>
void Serialize(const TAggregate<T>& counter, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total").Value(counter.GetTotal())
            .Item("max").Value(counter.GetMax())
            .Item("argmax_node").Value(counter.ArgmaxNode())
        .EndMap();
}

template <class T>
void ToProto(NProto::TQueryStatistics::TAggregate* serialized, const TAggregate<T>& original)
{
    if constexpr (std::is_same_v<T, TDuration>) {
        serialized->set_total(original.GetTotal().GetValue());
        serialized->set_max(original.GetMax().GetValue());
    } else {
        serialized->set_total(original.GetTotal());
        serialized->set_max(original.GetMax());
    }
    serialized->set_argmax_node(original.ArgmaxNode());
}

template <class T>
void FromProto(TAggregate<T>* original, const NProto::TQueryStatistics::TAggregate& serialized)
{
    if (serialized.has_total()) {
        if constexpr (std::is_same_v<T, TDuration>) {
            original->SetTotal(TDuration::FromValue(serialized.total()));
        } else {
            original->SetTotal(serialized.total());
        }
    }
    if (serialized.has_max()) {
        if constexpr (std::is_same_v<T, TDuration>) {
            original->SetMax(TDuration::FromValue(serialized.max()));
        } else {
            original->SetMax(serialized.max());
        }
    }
    if (serialized.has_argmax_node()) {
        FromProto(&original->ArgmaxNode(), serialized.argmax_node());
    }
}

////////////////////////////////////////////////////////////////////////////////

TQueryStatistics TQueryStatistics::FromExecutionStatistics(const TExecutionStatistics& statistics)
{
    TQueryStatistics queryStatistics;

    queryStatistics.RowsRead = statistics.RowsRead;
    queryStatistics.DataWeightRead = statistics.DataWeightRead;
    queryStatistics.RowsWritten = statistics.RowsWritten;
    queryStatistics.SyncTime = statistics.SyncTime;
    queryStatistics.AsyncTime = statistics.AsyncTime;
    queryStatistics.ExecuteTime = statistics.ExecuteTime;
    queryStatistics.ReadTime = statistics.ReadTime;
    queryStatistics.WriteTime = statistics.WriteTime;
    queryStatistics.CodegenTime = statistics.CodegenTime;
    queryStatistics.WaitOnReadyEventTime = statistics.WaitOnReadyEventTime;
    queryStatistics.MemoryUsage = statistics.MemoryUsage;
    queryStatistics.GroupedRowCount = statistics.GroupedRowCount;

    queryStatistics.IncompleteInput = statistics.IncompleteInput;
    queryStatistics.IncompleteOutput = statistics.IncompleteOutput;

    return queryStatistics;
}

void TQueryStatistics::AddInnerStatistics(TQueryStatistics statistics)
{
    IncompleteInput |= statistics.IncompleteInput;
    IncompleteOutput |= statistics.IncompleteOutput;
    InnerStatistics.push_back(std::move(statistics));
}

void TQueryStatistics::Merge(const TQueryStatistics& statistics)
{
    RowsRead.Merge(statistics.RowsRead);
    DataWeightRead.Merge(statistics.DataWeightRead);
    RowsWritten.Merge(statistics.RowsWritten);
    SyncTime.Merge(statistics.SyncTime);
    AsyncTime.Merge(statistics.AsyncTime);
    ExecuteTime.Merge(statistics.ExecuteTime);
    ReadTime.Merge(statistics.ReadTime);
    WriteTime.Merge(statistics.WriteTime);
    CodegenTime.Merge(statistics.CodegenTime);
    WaitOnReadyEventTime.Merge(statistics.WaitOnReadyEventTime);
    MemoryUsage.Merge(statistics.MemoryUsage);
    GroupedRowCount.Merge(statistics.GroupedRowCount);

    IncompleteInput |= statistics.IncompleteInput;
    IncompleteOutput |= statistics.IncompleteOutput;
    QueryCount += statistics.QueryCount;

    if (InnerStatistics.empty() && statistics.InnerStatistics.empty()) {
        return;
    }

    TQueryStatistics merged;

    for (const auto& incoming : InnerStatistics) {
        merged.Merge(incoming);
    }

    for (const auto& incoming : statistics.InnerStatistics) {
        merged.Merge(incoming);
    }

    InnerStatistics = {std::move(merged)};
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TQueryStatistics* serialized, const TQueryStatistics& original)
{
    // COMPAT(sabdenovch)
    serialized->set_rows_read(original.RowsRead.GetTotal());
    serialized->set_data_weight_read(original.DataWeightRead.GetTotal());
    serialized->set_rows_written(original.RowsWritten.GetTotal());
    serialized->set_sync_time(ToProto(original.SyncTime.GetTotal()));
    serialized->set_async_time(ToProto(original.AsyncTime.GetTotal()));
    serialized->set_execute_time(ToProto(original.ExecuteTime.GetTotal()));
    serialized->set_read_time(ToProto(original.ReadTime.GetTotal()));
    serialized->set_write_time(ToProto(original.WriteTime.GetTotal()));
    serialized->set_codegen_time(ToProto(original.CodegenTime.GetTotal()));
    serialized->set_wait_on_ready_event_time(ToProto(original.WaitOnReadyEventTime.GetTotal()));
    serialized->set_memory_usage(original.MemoryUsage.GetTotal());
    serialized->set_grouped_row_count(original.GroupedRowCount.GetTotal());

    ToProto(serialized->mutable_rows_read_aggr(), original.RowsRead);
    ToProto(serialized->mutable_data_weight_read_aggr(), original.DataWeightRead);
    ToProto(serialized->mutable_rows_written_aggr(), original.RowsWritten);
    ToProto(serialized->mutable_sync_time_aggr(), original.SyncTime);
    ToProto(serialized->mutable_async_time_aggr(), original.AsyncTime);
    ToProto(serialized->mutable_execute_time_aggr(), original.ExecuteTime);
    ToProto(serialized->mutable_read_time_aggr(), original.ReadTime);
    ToProto(serialized->mutable_write_time_aggr(), original.WriteTime);
    ToProto(serialized->mutable_codegen_time_aggr(), original.CodegenTime);
    ToProto(serialized->mutable_wait_on_ready_event_time_aggr(), original.WaitOnReadyEventTime);
    ToProto(serialized->mutable_memory_usage_aggr(), original.MemoryUsage);
    ToProto(serialized->mutable_grouped_row_count_aggr(), original.GroupedRowCount);

    serialized->set_incomplete_input(original.IncompleteInput);
    serialized->set_incomplete_output(original.IncompleteOutput);
    serialized->set_query_count(original.QueryCount);
    ToProto(serialized->mutable_inner_statistics(), original.InnerStatistics);
}

#define DESERIALIZE_I64_AND_MAYBE_FALLBACK(snakeCaseName, camelCaseName) \
    if (serialized.has_##snakeCaseName##_aggr()) { \
        FromProto(&original->camelCaseName, serialized.snakeCaseName##_aggr()); \
    } else if (serialized.has_##snakeCaseName()) { \
        original->camelCaseName.SetTotal(serialized.snakeCaseName()); \
    }

#define DESERIALIZE_DURATION_AND_MAYBE_FALLBACK(snakeCaseName, camelCaseName) \
    if (serialized.has_##snakeCaseName##_aggr()) { \
        FromProto(&original->camelCaseName, serialized.snakeCaseName##_aggr()); \
    } else if (serialized.has_##snakeCaseName()) { \
        original->camelCaseName.SetTotal(TDuration::FromValue(serialized.snakeCaseName())); \
    }

void FromProto(TQueryStatistics* original, const NProto::TQueryStatistics& serialized)
{
    // COMPAT(sabdenovch)
    DESERIALIZE_I64_AND_MAYBE_FALLBACK(rows_read, RowsRead);
    DESERIALIZE_I64_AND_MAYBE_FALLBACK(data_weight_read, DataWeightRead);
    DESERIALIZE_I64_AND_MAYBE_FALLBACK(rows_written, RowsWritten);
    DESERIALIZE_DURATION_AND_MAYBE_FALLBACK(sync_time, SyncTime);
    DESERIALIZE_DURATION_AND_MAYBE_FALLBACK(async_time, AsyncTime);
    DESERIALIZE_DURATION_AND_MAYBE_FALLBACK(execute_time, ExecuteTime);
    DESERIALIZE_DURATION_AND_MAYBE_FALLBACK(read_time, ReadTime);
    DESERIALIZE_DURATION_AND_MAYBE_FALLBACK(write_time, WriteTime);
    DESERIALIZE_DURATION_AND_MAYBE_FALLBACK(codegen_time, CodegenTime);
    DESERIALIZE_DURATION_AND_MAYBE_FALLBACK(wait_on_ready_event_time, WaitOnReadyEventTime);
    DESERIALIZE_I64_AND_MAYBE_FALLBACK(memory_usage, MemoryUsage);
    DESERIALIZE_I64_AND_MAYBE_FALLBACK(grouped_row_count, GroupedRowCount);
    original->IncompleteInput = serialized.incomplete_input();
    original->IncompleteOutput = serialized.incomplete_output();
    original->QueryCount = serialized.query_count();
    FromProto(&original->InnerStatistics, serialized.inner_statistics());
}

void FormatValue(TStringBuilderBase* builder, const TQueryStatistics& stats, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{"
        "RowsRead: %v, DataWeightRead: %v, RowsWritten: %v, "
        "SyncTime: %v, AsyncTime: %v, ExecuteTime: %v, ReadTime: %v, WriteTime: %v, CodegenTime: %v, "
        "WaitOnReadyEventTime: %v, IncompleteInput: %v, IncompleteOutput: %v, MemoryUsage: %v"
        "}",
        stats.RowsRead.GetTotal(),
        stats.DataWeightRead.GetTotal(),
        stats.RowsWritten.GetTotal(),
        stats.SyncTime.GetTotal(),
        stats.AsyncTime.GetTotal(),
        stats.ExecuteTime.GetTotal(),
        stats.ReadTime.GetTotal(),
        stats.WriteTime.GetTotal(),
        stats.CodegenTime.GetTotal(),
        stats.WaitOnReadyEventTime.GetTotal(),
        stats.IncompleteInput,
        stats.IncompleteOutput,
        stats.MemoryUsage.GetTotal());
}

void Serialize(const TQueryStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonMapFragmentFluently(consumer)
        // COMPAT(sabdenovch) begin {
        .Item("rows_read").Value(statistics.RowsRead.GetTotal())
        .Item("data_weight_read").Value(statistics.DataWeightRead.GetTotal())
        .Item("rows_written").Value(statistics.RowsWritten.GetTotal())
        .Item("sync_time").Value(statistics.SyncTime.GetTotal())
        .Item("async_time").Value(statistics.AsyncTime.GetTotal())
        .Item("execute_time").Value(statistics.ExecuteTime.GetTotal())
        .Item("read_time").Value(statistics.ReadTime.GetTotal())
        .Item("write_time").Value(statistics.WriteTime.GetTotal())
        .Item("codegen_time").Value(statistics.CodegenTime.GetTotal())
        .Item("wait_on_ready_event_time").Value(statistics.WaitOnReadyEventTime.GetTotal())
        .Item("memory_usage").Value(statistics.MemoryUsage.GetTotal())
        .Item("grouped_row_count").Value(statistics.GroupedRowCount.GetTotal())
        // COMPAT(sabdenovch) end }
        .Item("rows_read_aggr").Value(statistics.RowsRead)
        .Item("data_weight_read_aggr").Value(statistics.DataWeightRead)
        .Item("rows_written_aggr").Value(statistics.RowsWritten)
        .Item("sync_time_aggr").Value(statistics.SyncTime)
        .Item("async_time_aggr").Value(statistics.AsyncTime)
        .Item("execute_time_aggr").Value(statistics.ExecuteTime)
        .Item("read_time_aggr").Value(statistics.ReadTime)
        .Item("write_time_aggr").Value(statistics.WriteTime)
        .Item("codegen_time_aggr").Value(statistics.CodegenTime)
        .Item("wait_on_ready_event_time_aggr").Value(statistics.WaitOnReadyEventTime)
        .Item("memory_usage_aggr").Value(statistics.MemoryUsage)
        .Item("grouped_row_count_aggr").Value(statistics.GroupedRowCount)
        .Item("incomplete_input").Value(statistics.IncompleteInput)
        .Item("incomplete_output").Value(statistics.IncompleteOutput)
        .Item("query_count").Value(statistics.QueryCount)
        .DoIf(!statistics.InnerStatistics.empty(), [&] (NYTree::TFluentMap fluent) {
            fluent
                .Item("inner_statistics").DoListFor(statistics.InnerStatistics, [=] (
                    NYTree::TFluentList fluent,
                    const TQueryStatistics& statistics)
                {
                    fluent
                        .Item().DoMap([&] (NYTree::TFluentMap fluent) {
                            Serialize(statistics, fluent.GetConsumer());
                        });
                });
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
