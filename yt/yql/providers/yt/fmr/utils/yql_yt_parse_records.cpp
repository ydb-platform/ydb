#include "yql_yt_parse_records.h"
#include <yql/essentials/utils/log/log.h>

using namespace NYql;

namespace NYql::NFmr {

void CheckCancelled(std::shared_ptr<std::atomic<bool>> cancelFlag) {
    if (cancelFlag->load()) {
        ythrow yexception() << " Job was cancelled, aborting";
    }
}

void ParseRecordsImpl(
    NYT::TRawTableReaderPtr reader,
    ui64 blockCount,
    ui64 blockSize,
    std::shared_ptr<std::atomic<bool>> cancelFlag,
    std::function<void(const TVector<char>&)> writeFunc
) {
    YQL_ENSURE(reader);
    auto blockReader = MakeBlockReader(*reader, blockCount, blockSize);
    NCommon::TInputBuf inputBuf(*blockReader, nullptr);
    TVector<char> curYsonRow;
    char cmd;
    while (true) {
        CheckCancelled(cancelFlag);
        if (!inputBuf.TryRead(cmd)) {
            break;
        }
        curYsonRow.clear();
        CopyYson(cmd, inputBuf, curYsonRow);
        bool needBreak = false;
        if (!inputBuf.TryRead(cmd)) {
            needBreak = true;
        } else {
            YQL_ENSURE(cmd == ';');
            curYsonRow.emplace_back(cmd);
        }
        CheckCancelled(cancelFlag);
        writeFunc(curYsonRow);
        if (needBreak) {
            break;
        }
    }
}

void ParseRecords(
    NYT::TRawTableReaderPtr reader,
    NYT::TRawTableWriterPtr writer,
    ui64 blockCount,
    ui64 blockSize,
    std::shared_ptr<std::atomic<bool>> cancelFlag,
    const TMaybe<TMutex>& writeMutex
) {
    YQL_ENSURE(writer);
    ParseRecordsImpl(reader, blockCount, blockSize, cancelFlag, [&writer, &writeMutex](const TVector<char>& curYsonRow) {
        if (writeMutex) {
            with_lock(*writeMutex) {
                writer->Write(curYsonRow.data(), curYsonRow.size());
                writer->NotifyRowEnd();
            }
        } else {
            writer->Write(curYsonRow.data(), curYsonRow.size());
            writer->NotifyRowEnd();
        }
    });
}

void ParseRecordsToYtDistributed(
    NYT::TRawTableReaderPtr reader,
    IOutputStream& writer,
    ui64 blockCount,
    ui64 blockSize,
    std::shared_ptr<std::atomic<bool>> cancelFlag
) {
    ParseRecordsImpl(reader, blockCount, blockSize, cancelFlag, [&writer](const TVector<char>& curYsonRow) {
        writer.Write(curYsonRow.data(), curYsonRow.size());
    });
}

} // namespace NYql::NFmr
