#include "query_statistics.h"

#include <yt/yt_proto/yt/client/query_client/proto/query_statistics.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueryClient {

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void TQueryStatistics::AddInnerStatistics(TQueryStatistics statistics)
{
    IncompleteInput |= statistics.IncompleteInput;
    IncompleteOutput |= statistics.IncompleteOutput;
    MemoryUsage = std::max(MemoryUsage, statistics.MemoryUsage);
    InnerStatistics.push_back(std::move(statistics));
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TQueryStatistics* serialized, const TQueryStatistics& original)
{
    serialized->set_rows_read(original.RowsRead);
    serialized->set_data_weight_read(original.DataWeightRead);
    serialized->set_rows_written(original.RowsWritten);
    serialized->set_sync_time(ToProto<i64>(original.SyncTime));
    serialized->set_async_time(ToProto<i64>(original.AsyncTime));
    serialized->set_execute_time(ToProto<i64>(original.ExecuteTime));
    serialized->set_read_time(ToProto<i64>(original.ReadTime));
    serialized->set_write_time(ToProto<i64>(original.WriteTime));
    serialized->set_codegen_time(ToProto<i64>(original.CodegenTime));
    serialized->set_wait_on_ready_event_time(ToProto<i64>(original.WaitOnReadyEventTime));
    serialized->set_incomplete_input(original.IncompleteInput);
    serialized->set_incomplete_output(original.IncompleteOutput);
    serialized->set_memory_usage(original.MemoryUsage);
    ToProto(serialized->mutable_inner_statistics(), original.InnerStatistics);
}

void FromProto(TQueryStatistics* original, const NProto::TQueryStatistics& serialized)
{
    original->RowsRead = serialized.rows_read();
    original->DataWeightRead = serialized.data_weight_read();
    original->RowsWritten = serialized.rows_written();
    original->SyncTime = FromProto<TDuration>(serialized.sync_time());
    original->AsyncTime = FromProto<TDuration>(serialized.async_time());
    original->ExecuteTime = FromProto<TDuration>(serialized.execute_time());
    original->ReadTime = FromProto<TDuration>(serialized.read_time());
    original->WriteTime = FromProto<TDuration>(serialized.write_time());
    original->CodegenTime = FromProto<TDuration>(serialized.codegen_time());
    original->WaitOnReadyEventTime = FromProto<TDuration>(serialized.wait_on_ready_event_time());
    original->IncompleteInput = serialized.incomplete_input();
    original->IncompleteOutput = serialized.incomplete_output();
    original->MemoryUsage = serialized.memory_usage();
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
        stats.RowsRead,
        stats.DataWeightRead,
        stats.RowsWritten,
        stats.SyncTime,
        stats.AsyncTime,
        stats.ExecuteTime,
        stats.ReadTime,
        stats.WriteTime,
        stats.CodegenTime,
        stats.WaitOnReadyEventTime,
        stats.IncompleteInput,
        stats.IncompleteOutput,
        stats.MemoryUsage);
}

void Serialize(const TQueryStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonMapFragmentFluently(consumer)
        .Item("rows_read").Value(statistics.RowsRead)
        .Item("data_weight_read").Value(statistics.DataWeightRead)
        .Item("rows_written").Value(statistics.RowsWritten)
        .Item("sync_time").Value(statistics.SyncTime.MilliSeconds())
        .Item("async_time").Value(statistics.AsyncTime.MilliSeconds())
        .Item("execute_time").Value(statistics.ExecuteTime.MilliSeconds())
        .Item("read_time").Value(statistics.ReadTime.MilliSeconds())
        .Item("write_time").Value(statistics.WriteTime.MilliSeconds())
        .Item("codegen_time").Value(statistics.CodegenTime.MilliSeconds())
        .Item("wait_on_ready_event_time").Value(statistics.WaitOnReadyEventTime.MilliSeconds())
        .Item("incomplete_input").Value(statistics.IncompleteInput)
        .Item("incomplete_output").Value(statistics.IncompleteOutput)
        .Item("memory_usage").Value(statistics.MemoryUsage)
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
