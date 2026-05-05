#include <yt/cpp/mapreduce/interface/io.h>

#include <yt/yql/providers/yt/codec/yt_codec_io.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>

namespace NYql::NFmr {

void CheckCancelled(std::shared_ptr<std::atomic<bool>> cancelFlag);

void ParseRecords(
    NYT::TRawTableReaderPtr reader,
    NYT::TRawTableWriterPtr writer,
    ui64 blockCount,
    ui64 blockSize,
    std::shared_ptr<std::atomic<bool>> cancelFlag,
    const TMaybe<TMutex>& writeMutex = Nothing());

void StreamBulkToYtDistributed(
    NYT::TRawTableReaderPtr reader,
    IOutputStream& writer,
    ui64 bufferSize,
    std::shared_ptr<std::atomic<bool>> cancelFlag);

void StreamBulkToRawTableWriter(
    NYT::TRawTableReaderPtr reader,
    NYT::TRawTableWriterPtr writer,
    ui64 bufferSize,
    std::shared_ptr<std::atomic<bool>> cancelFlag);

void ParseRecordsPipelined(
    NYT::TRawTableReaderPtr reader,
    NYT::TRawTableWriterPtr writer,
    ui64 blockCount,
    ui64 blockSize,
    const TFmrRawTableQueueSettings& queueSettings,
    std::shared_ptr<std::atomic<bool>> cancelFlag);

} // namespace NYql::NFmr
