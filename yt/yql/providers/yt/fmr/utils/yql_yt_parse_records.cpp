#include "yql_yt_parse_records.h"

#include <library/cpp/yson/detail.h>
#include <util/system/thread.h>

#include <yql/essentials/utils/log/log.h>

using namespace NYql;
using namespace NYson::NDetail;

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

void StreamBulkToYtDistributed(
    NYT::TRawTableReaderPtr reader,
    IOutputStream& writer,
    ui64 bufferSize,
    std::shared_ptr<std::atomic<bool>> cancelFlag
) {
    YQL_ENSURE(reader);
    YQL_ENSURE(bufferSize > 0);
    TVector<char> buf(bufferSize);
    while (true) {
        CheckCancelled(cancelFlag);
        size_t bytesRead = reader->Read(buf.data(), buf.size());
        if (bytesRead == 0) {
            break;
        }
        writer.Write(buf.data(), bytesRead);
    }
}

void ParseRecordsPipelined(
    NYT::TRawTableReaderPtr reader,
    NYT::TRawTableWriterPtr writer,
    ui64 blockCount,
    ui64 blockSize,
    const TFmrRawTableQueueSettings& queueSettings,
    std::shared_ptr<std::atomic<bool>> cancelFlag
) {
    YQL_ENSURE(reader);
    YQL_ENSURE(writer);

    auto queue = std::make_shared<TFmrRawTableQueue>(1, queueSettings);

    auto producerFunc = [reader, blockCount, blockSize, cancelFlag, queue]() {
        try {
            ParseRecordsImpl(reader, blockCount, blockSize, cancelFlag, [&queue](const TVector<char>& row) {
                TBuffer rowBuffer(row.data(), row.size());
                queue->AddRow(std::move(rowBuffer));
            });
            queue->NotifyInputFinished(1);
        } catch (const std::exception& e) {
            queue->SetException(e.what());
        } catch (...) {
            queue->SetException("Unknown exception");
        }
    };

    TThread producerThread(producerFunc);
    producerThread.Start();

    try {
        while (true) {
            auto row = queue->PopRow();
            if (!row.Defined()) {
                break;
            }

            writer->Write(row->data(), row->size());
            writer->NotifyRowEnd();
        }
    } catch (const std::exception& e) {
        queue->SetException(e.what());
        producerThread.Join();
        throw;
    } catch (...) {
        queue->SetException("Unknown exception");
        producerThread.Join();
        throw;
    }

    producerThread.Join();
}

} // namespace NYql::NFmr
