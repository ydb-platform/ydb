#include "remote_connection_status.h"

#include "key_value_printer.h"

#include <library/cpp/messagebus/monitoring/mon_proto.pb.h>

#include <util/stream/format.h>
#include <util/stream/output.h>
#include <util/system/yassert.h>

using namespace NBus;
using namespace NBus::NPrivate;

template <typename T>
static void Add(T& thiz, const T& that) {
    thiz += that;
}

template <typename T>
static void Max(T& thiz, const T& that) {
    if (that > thiz) {
        thiz = that;
    }
}

template <typename T>
static void AssertZero(T& thiz, const T& that) {
    Y_ASSERT(thiz == T());
    Y_UNUSED(that);
}

TDurationCounter::TDurationCounter()
    : DURATION_COUNTER_MAP(STRUCT_FIELD_INIT_DEFAULT, COMMA)
{
}

TDuration TDurationCounter::AvgDuration() const {
    if (Count == 0) {
        return TDuration::Zero();
    } else {
        return SumDuration / Count;
    }
}

TDurationCounter& TDurationCounter::operator+=(const TDurationCounter& that) {
    DURATION_COUNTER_MAP(STRUCT_FIELD_ADD, )
    return *this;
}

TString TDurationCounter::ToString() const {
    if (Count == 0) {
        return "0";
    } else {
        TStringStream ss;
        ss << "avg: " << AvgDuration() << ", max: " << MaxDuration << ", count: " << Count;
        return ss.Str();
    }
}

TRemoteConnectionStatusBase::TRemoteConnectionStatusBase()
    : REMOTE_CONNECTION_STATUS_BASE_MAP(STRUCT_FIELD_INIT_DEFAULT, COMMA)
{
}

TRemoteConnectionStatusBase& TRemoteConnectionStatusBase ::operator+=(const TRemoteConnectionStatusBase& that) {
    REMOTE_CONNECTION_STATUS_BASE_MAP(STRUCT_FIELD_ADD, )
    return *this;
}

TRemoteConnectionIncrementalStatusBase::TRemoteConnectionIncrementalStatusBase()
    : REMOTE_CONNECTION_INCREMENTAL_STATUS_BASE_MAP(STRUCT_FIELD_INIT_DEFAULT, COMMA)
{
}

TRemoteConnectionIncrementalStatusBase& TRemoteConnectionIncrementalStatusBase::operator+=(
    const TRemoteConnectionIncrementalStatusBase& that) {
    REMOTE_CONNECTION_INCREMENTAL_STATUS_BASE_MAP(STRUCT_FIELD_ADD, )
    return *this;
}

TRemoteConnectionReaderIncrementalStatus::TRemoteConnectionReaderIncrementalStatus()
    : REMOTE_CONNECTION_READER_INCREMENTAL_STATUS_MAP(STRUCT_FIELD_INIT_DEFAULT, COMMA)
{
}

TRemoteConnectionReaderIncrementalStatus& TRemoteConnectionReaderIncrementalStatus::operator+=(
    const TRemoteConnectionReaderIncrementalStatus& that) {
    TRemoteConnectionIncrementalStatusBase::operator+=(that);
    REMOTE_CONNECTION_READER_INCREMENTAL_STATUS_MAP(STRUCT_FIELD_ADD, )
    return *this;
}

TRemoteConnectionReaderStatus::TRemoteConnectionReaderStatus()
    : REMOTE_CONNECTION_READER_STATUS_MAP(STRUCT_FIELD_INIT_DEFAULT, COMMA)
{
}

TRemoteConnectionReaderStatus& TRemoteConnectionReaderStatus::operator+=(const TRemoteConnectionReaderStatus& that) {
    TRemoteConnectionStatusBase::operator+=(that);
    REMOTE_CONNECTION_READER_STATUS_MAP(STRUCT_FIELD_ADD, )
    return *this;
}

TRemoteConnectionWriterIncrementalStatus::TRemoteConnectionWriterIncrementalStatus()
    : REMOTE_CONNECTION_WRITER_INCREMENTAL_STATUS(STRUCT_FIELD_INIT_DEFAULT, COMMA)
{
}

TRemoteConnectionWriterIncrementalStatus& TRemoteConnectionWriterIncrementalStatus::operator+=(
    const TRemoteConnectionWriterIncrementalStatus& that) {
    TRemoteConnectionIncrementalStatusBase::operator+=(that);
    REMOTE_CONNECTION_WRITER_INCREMENTAL_STATUS(STRUCT_FIELD_ADD, )
    return *this;
}

TRemoteConnectionWriterStatus::TRemoteConnectionWriterStatus()
    : REMOTE_CONNECTION_WRITER_STATUS(STRUCT_FIELD_INIT_DEFAULT, COMMA)
{
}

TRemoteConnectionWriterStatus& TRemoteConnectionWriterStatus::operator+=(const TRemoteConnectionWriterStatus& that) {
    TRemoteConnectionStatusBase::operator+=(that);
    REMOTE_CONNECTION_WRITER_STATUS(STRUCT_FIELD_ADD, )
    return *this;
}

size_t TRemoteConnectionWriterStatus::GetInFlight() const {
    return SendQueueSize + AckMessagesSize;
}

TConnectionStatusMonRecord TRemoteConnectionStatus::GetStatusProtobuf() const {
    TConnectionStatusMonRecord status;

    // TODO: fill unfilled fields
    status.SetSendQueueSize(WriterStatus.SendQueueSize);
    status.SetAckMessagesSize(WriterStatus.AckMessagesSize);
    // status.SetErrorCount();
    // status.SetWriteBytes();
    // status.SetWriteBytesCompressed();
    // status.SetWriteMessages();
    status.SetWriteSyscalls(WriterStatus.Incremental.NetworkOps);
    status.SetWriteActs(WriterStatus.Acts);
    // status.SetReadBytes();
    // status.SetReadBytesCompressed();
    // status.SetReadMessages();
    status.SetReadSyscalls(ReaderStatus.Incremental.NetworkOps);
    status.SetReadActs(ReaderStatus.Acts);

    TMessageStatusCounter sumStatusCounter;
    sumStatusCounter += WriterStatus.Incremental.StatusCounter;
    sumStatusCounter += ReaderStatus.Incremental.StatusCounter;
    sumStatusCounter.FillErrorsProtobuf(&status);

    return status;
}

TString TRemoteConnectionStatus::PrintToString() const {
    TStringStream ss;

    TKeyValuePrinter p;

    if (!Summary) {
        // TODO: print MyAddr too, but only if it is set
        ss << WriterStatus.PeerAddr << " (" << WriterStatus.ConnectionId << ")"
           << ", writefd=" << WriterStatus.Fd
           << ", readfd=" << ReaderStatus.Fd
           << Endl;
        if (WriterStatus.Connected) {
            p.AddRow("connect time", WriterStatus.ConnectTime.ToString());
            p.AddRow("writer state", ToCString(WriterStatus.State));
        } else {
            ss << "not connected";
            if (WriterStatus.ConnectError != 0) {
                ss << ", last connect error: " << LastSystemErrorText(WriterStatus.ConnectError);
            }
            ss << Endl;
        }
    }
    if (!Server) {
        p.AddRow("connect syscalls", WriterStatus.ConnectSyscalls);
    }

    p.AddRow("send queue", LeftPad(WriterStatus.SendQueueSize, 6));

    if (Server) {
        p.AddRow("quota msg", LeftPad(ReaderStatus.QuotaMsg, 6));
        p.AddRow("quota bytes", LeftPad(ReaderStatus.QuotaBytes, 6));
        p.AddRow("quota exhausted", LeftPad(ReaderStatus.QuotaExhausted, 6));
        p.AddRow("reader wakeups", LeftPad(WriterStatus.ReaderWakeups, 6));
    } else {
        p.AddRow("ack messages", LeftPad(WriterStatus.AckMessagesSize, 6));
    }

    p.AddRow("written", WriterStatus.Incremental.MessageCounter.ToString(false));
    p.AddRow("read", ReaderStatus.Incremental.MessageCounter.ToString(true));

    p.AddRow("write syscalls", LeftPad(WriterStatus.Incremental.NetworkOps, 12));
    p.AddRow("read syscalls", LeftPad(ReaderStatus.Incremental.NetworkOps, 12));

    p.AddRow("write acts", LeftPad(WriterStatus.Acts, 12));
    p.AddRow("read acts", LeftPad(ReaderStatus.Acts, 12));

    p.AddRow("write buffer cap", LeftPad(WriterStatus.BufferSize, 12));
    p.AddRow("read buffer cap", LeftPad(ReaderStatus.BufferSize, 12));

    p.AddRow("write buffer drops", LeftPad(WriterStatus.Incremental.BufferDrops, 10));
    p.AddRow("read buffer drops", LeftPad(ReaderStatus.Incremental.BufferDrops, 10));

    if (Server) {
        p.AddRow("process dur", WriterStatus.DurationCounterPrev.ToString());
    }

    ss << p.PrintToString();

    if (false && Server) {
        ss << "time histogram:\n";
        ss << WriterStatus.Incremental.ProcessDurationHistogram.PrintToString();
    }

    TMessageStatusCounter sumStatusCounter;
    sumStatusCounter += WriterStatus.Incremental.StatusCounter;
    sumStatusCounter += ReaderStatus.Incremental.StatusCounter;

    ss << sumStatusCounter.PrintToString();

    return ss.Str();
}

TRemoteConnectionStatus::TRemoteConnectionStatus()
    : REMOTE_CONNECTION_STATUS_MAP(STRUCT_FIELD_INIT_DEFAULT, COMMA)
{
}

TString TSessionDumpStatus::PrintToString() const {
    if (Shutdown) {
        return "shutdown";
    }

    TStringStream ss;
    ss << Head;
    if (ConnectionStatusSummary.Server) {
        ss << "\n";
        ss << Acceptors;
    }
    ss << "\n";
    ss << "connections summary:" << Endl;
    ss << ConnectionsSummary;
    if (!!Connections) {
        ss << "\n";
        ss << Connections;
    }
    ss << "\n";
    ss << Config.PrintToString();
    return ss.Str();
}

TString TBusMessageQueueStatus::PrintToString() const {
    TStringStream ss;
    ss << "work queue:\n";
    ss << ExecutorStatus.Status;
    ss << "\n";
    ss << "queue config:\n";
    ss << Config.PrintToString();
    return ss.Str();
}
