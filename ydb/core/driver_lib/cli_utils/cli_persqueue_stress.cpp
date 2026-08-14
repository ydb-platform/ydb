#include "cli.h"
#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>

namespace NKikimr {
namespace NDriverClient {

struct TCmdPersQueueConfig : public TCliCmdConfig {
    bool IsWrite;
    TString Topic;
    ui32 Partition;
    //write options
    TString SourceId;
    ui32 OneCmdMinSize;
    ui32 OneCmdMaxSize;
    //read options
    ui64 StartOffset;
    ui32 TimeRead;
    ui32 TimeSleep;
    //both commands options
    ui32 BatchSize;
    ui32 Speed;
    TCmdPersQueueConfig();

    void Parse(int argc, char **argv);
};

int PersQueueStress(TCommandConfig &cmdConf, int argc, char** argv) {
    Y_UNUSED(cmdConf);

    TCmdPersQueueConfig config;
    config.Parse(argc, argv);

    if (config.IsWrite) {
        ui64 DoneBytes = 0;
        TInstant timestamp = TInstant::Now();

        TVector<ui32> buckets(3000);

        TString cookie;
        {
        TAutoPtr<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto pr = request->Record.MutablePartitionRequest();
        pr->SetTopic(config.Topic);
        pr->SetPartition(config.Partition);
        pr->MutableCmdGetOwnership();
        TAutoPtr<NBus::TBusMessage> reply;
        NBus::EMessageStatus status = config.SyncCall(request, reply);
        Y_ABORT_UNLESS(status == NBus::MESSAGE_OK);
        const auto& result = static_cast<NMsgBusProxy::TBusResponse *>(reply.Get())->Record;
        Cerr << result.DebugString() << "\n";
        Y_ABORT_UNLESS(result.GetStatus() == NMsgBusProxy::MSTATUS_OK);
        Y_ABORT_UNLESS(result.HasPartitionResponse());
        cookie = result.GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
        Y_ABORT_UNLESS(!cookie.empty());
        }
        ui64 messageNo = 0;

        TAutoPtr<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto pr = request->Record.MutablePartitionRequest();
        pr->SetTopic(config.Topic);
        pr->SetPartition(config.Partition);
        pr->MutableCmdGetMaxSeqNo()->AddSourceId(config.SourceId);
        TAutoPtr<NBus::TBusMessage> reply;
        NBus::EMessageStatus status = config.SyncCall(request, reply);
        Y_ABORT_UNLESS(status == NBus::MESSAGE_OK);
        const auto& result = static_cast<NMsgBusProxy::TBusResponse *>(reply.Get())->Record;
        Cerr << result.DebugString() << "\n";
        Y_ABORT_UNLESS(result.GetStatus() == NMsgBusProxy::MSTATUS_OK);
        Y_ABORT_UNLESS(result.HasPartitionResponse());
        ui64 seqNo = result.GetPartitionResponse().GetCmdGetMaxSeqNoResult().GetSourceIdInfo(0).HasSeqNo() ? result.GetPartitionResponse().GetCmdGetMaxSeqNoResult().GetSourceIdInfo(0).GetSeqNo() : 0;
        while (true) {
            request.Reset(new NMsgBusProxy::TBusPersQueue);
            auto pr = request->Record.MutablePartitionRequest();
            pr->SetTopic(config.Topic);
            pr->SetPartition(config.Partition);
            pr->SetOwnerCookie(cookie);
            pr->SetMessageNo(messageNo++);

            ui32 totalSize = 0;
            for (ui32 i = 0; i < config.BatchSize; ++i) {
                auto write = pr->AddCmdWrite();
                write->SetSourceId(config.SourceId);
                write->SetSeqNo(++seqNo);
                ui32 size = config.OneCmdMinSize + rand() % (config.OneCmdMaxSize - config.OneCmdMinSize + 1);
                write->SetData(TString(size, 'a'));
                totalSize += size;
            }
            TInstant tt = TInstant::Now();
            status = config.SyncCall(request, reply);
            if (status != NBus::MESSAGE_OK)
                Cerr << status << "\n";
            Y_ABORT_UNLESS(status == NBus::MESSAGE_OK);
            const auto& result = static_cast<NMsgBusProxy::TBusResponse *>(reply.Get())->Record;
            DoneBytes +=  totalSize;
            ui32 i = (TInstant::Now() - tt).MilliSeconds() / 10;
            if (i >= buckets.size()) i = buckets.size() - 1;
            ++buckets[i];
            Cerr << "done upto " << seqNo << " " << DoneBytes/1024.0/1024.0 << " Mb speed " << DoneBytes / ((TInstant::Now() - timestamp).MilliSeconds() + 0.0) * 1000.0 /1024.0 /1024.0 << " Mb/s "
                 << (TInstant::Now() - tt).MilliSeconds() / 1000.0 << " seconds\n";
            if (seqNo % 100 == 0) {
                for (i = 0; i < buckets.size(); ++i) {
                    Cout << buckets[i] << " ";
                }
                Cout << "\n";
            }
            Cout.Flush();
            if (result.GetStatus() != NMsgBusProxy::MSTATUS_OK || result.GetErrorCode() !=  NPersQueue::NErrorCode::OK) {
                Cerr << result.DebugString()  << "\n";
            }
            Y_ABORT_UNLESS(result.GetStatus() == NMsgBusProxy::MSTATUS_OK);
            Y_ABORT_UNLESS(result.GetErrorCode() == NPersQueue::NErrorCode::OK);
            if (config.Speed) {
                TInstant T2 = timestamp + TDuration::MilliSeconds(DoneBytes * 1000 / 1024.0 / config.Speed);
                Sleep(T2 - TInstant::Now());
            }
        }
    } else {
        ui64 offset = config.StartOffset;
        while (true) {
            Sleep(TDuration::MilliSeconds(config.TimeSleep));
            TInstant timestamp = TInstant::Now();
            ui64 bytes = 0;
            ui32 count = 0;
            while((TInstant::Now() - timestamp).MilliSeconds() < config.TimeRead) {
                TAutoPtr<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
                auto pr = request->Record.MutablePartitionRequest();
                pr->SetTopic(config.Topic);
                pr->SetPartition(config.Partition);
                auto read = pr->MutableCmdRead();
                read->SetOffset(offset);
                read->SetCount(config.BatchSize);
                read->SetClientId("user");
                TAutoPtr<NBus::TBusMessage> reply;
                NBus::EMessageStatus status = config.SyncCall(request, reply);
                Y_ABORT_UNLESS(status == NBus::MESSAGE_OK);
                const auto& result = static_cast<NMsgBusProxy::TBusResponse *>(reply.Get())->Record;
                if (result.GetStatus() != NMsgBusProxy::MSTATUS_OK || result.GetErrorCode() != NPersQueue::NErrorCode::OK) {
                    Cerr << result.DebugString() << "\n";
                }
                Y_ABORT_UNLESS(result.GetStatus() == NMsgBusProxy::MSTATUS_OK);
                Y_ABORT_UNLESS(result.GetErrorCode() == NPersQueue::NErrorCode::OK);
                const auto& rd = result.GetPartitionResponse().GetCmdReadResult();
                for (ui32 i = 0; i < rd.ResultSize(); ++i) {
                    bytes += rd.GetResult(i).ByteSize();
                    count++;
                    offset = rd.GetResult(i).GetOffset() + 1;
                }
                request.Reset(new NMsgBusProxy::TBusPersQueue);
                pr = request->Record.MutablePartitionRequest();
                pr->SetTopic(config.Topic);
                pr->SetPartition(config.Partition);
                auto set = pr->MutableCmdSetClientOffset();
                set->SetOffset(offset);
                set->SetClientId("user");

                status = config.SyncCall(request, reply);
                Y_ABORT_UNLESS(status == NBus::MESSAGE_OK);
                if (config.Speed) {
                    TInstant T2 = timestamp + TDuration::MilliSeconds(bytes * 1000 / 1024.0 / config.Speed);
                    Sleep(T2 - TInstant::Now());
                }
                Cerr << "offset " << offset << " from cache " << rd.GetBlobsFromCache() << " from disk " << rd.GetBlobsFromDisk() << " read done " << count << " size " << bytes << " speed " << bytes * 1000.0 / ((TInstant::Now() - timestamp).MilliSeconds() + 0.0) / 1024.0/1024.0 << " Mb/s\n";
            }
        }

    }
    return 0;
}

TCmdPersQueueConfig::TCmdPersQueueConfig()
{}

void TCmdPersQueueConfig::Parse(int argc, char **argv) {
    using namespace NLastGetopt;

    TOpts opts = TOpts::Default();

    TString command;
    opts.AddLongOption('c', "command", "type of action").Required().RequiredArgument("[write|read]").StoreResult(&command);
    opts.AddLongOption("topic", "topic").Required().RequiredArgument("STR").StoreResult(&Topic);
    opts.AddLongOption("partition", "partition").Required().RequiredArgument("NUM").StoreResult(&Partition);

    opts.AddLongOption("sourceid", "sourceId").Optional().RequiredArgument("STR").StoreResult(&SourceId);
    opts.AddLongOption('b', "batchsize", "batch size").Optional().RequiredArgument("NUM").StoreResult(&BatchSize);
    opts.AddLongOption('m', "minsize", "msg min size").Optional().RequiredArgument("NUM").StoreResult(&OneCmdMinSize);
    opts.AddLongOption('M', "maxsize", "msg max size").Optional().RequiredArgument("NUM").StoreResult(&OneCmdMaxSize);

    opts.AddLongOption('o', "offset", "StartOffset").Optional().RequiredArgument("NUM").DefaultValue("0").StoreResult(&StartOffset);
    opts.AddLongOption('R', "readtime", "read time, ms").Optional().RequiredArgument("NUM").DefaultValue("1000").StoreResult(&TimeRead);
    opts.AddLongOption('S', "sleetime", "sleep time before reads, ms").Optional().RequiredArgument("NUM").DefaultValue("0").StoreResult(&TimeSleep);

    opts.AddLongOption('B', "speed", "speed of read/write, Kb/s").Optional().RequiredArgument("NUM").DefaultValue("0").StoreResult(&Speed);


    ConfigureBaseLastGetopt(opts);

    TOptsParseResult res(&opts, argc, argv);
    IsWrite = command == "write";
    ConfigureMsgBusLastGetopt(res, argc, argv);
}

}
}
