#include "cli.h"
#include <google/protobuf/text_format.h>
#include <util/stream/file.h>
#include <util/string/printf.h>
#include <ydb/core/protos/base.pb.h>

namespace NKikimr {
namespace NDriverClient {


struct TCmdKeyValueConfig : public TCliCmdConfig {
    bool IsReadToFile;
    bool IsWriteFromFile;
    TString ReadToPath;
    TString WriteFromPath;
    TString Proto;

    TCmdKeyValueConfig();

    void Parse(int argc, char **argv);
};

template<typename TSuccessOp>
int ClientSyncCall(TAutoPtr<NBus::TBusMessage> request, const TCliCmdConfig &cliConfig, TSuccessOp successOp) {
    TAutoPtr<NBus::TBusMessage> reply;
    NBus::EMessageStatus status = cliConfig.SyncCall(request, reply);

    switch (status) {
    case NBus::MESSAGE_OK:
        {
            const NKikimrClient::TResponse &response = static_cast<NMsgBusProxy::TBusResponse *>(reply.Get())->Record;
            successOp(response);
            return response.GetStatus() == NMsgBusProxy::MSTATUS_OK ? 0 : 1;
        }
    default:
        {
            const char *description = NBus::MessageStatusDescription(status);
            Cerr << description << Endl;
        }
        return 1;
    }
}


int KeyValueRequest(TCommandConfig &cmdConf, int argc, char **argv) {
    Y_UNUSED(cmdConf);

#ifdef _win32_
    WSADATA dummy;
    WSAStartup(MAKEWORD(2, 2), &dummy);
#endif

    TCmdKeyValueConfig requestConfig;
    requestConfig.Parse(argc, argv);

    TVector<NKikimrClient::TKeyValueRequest> records;
    NKikimrClient::TKeyValueRequest& record = *records.emplace(records.end());

    const bool isOk = ::google::protobuf::TextFormat::ParseFromString(requestConfig.Proto, &record);
    if (!isOk) {
        ythrow TWithBackTrace<yexception>() << "Error parsing protobuf: \'" << requestConfig.Proto << "\'";
    }

    const ui32 maxReadBlockSize = 20 << 20;
    const ui32 maxWriteBlockSize = 16 << 20;

    TString readBuffer;
    if (requestConfig.IsReadToFile) {
        Y_ABORT_UNLESS(record.CmdReadSize() == 1);
        auto& cmdRead = *record.MutableCmdRead(0);
        cmdRead.SetSize(maxReadBlockSize);
    }

    if (requestConfig.IsWriteFromFile) {
        Y_ABORT_UNLESS(record.CmdWriteSize() == 1);
        Y_ABORT_UNLESS(record.GetCmdWrite(0).HasKey());
        Y_ABORT_UNLESS(!record.GetCmdWrite(0).HasValue());
        TString data = TUnbufferedFileInput(requestConfig.WriteFromPath).ReadAll();
        if (data.size() <= maxWriteBlockSize) {
            record.MutableCmdWrite(0)->SetValue(data);
        } else {
            const auto& originalCmdWrite = record.GetCmdWrite(0);
            TString key = originalCmdWrite.GetKey();
            TVector<TString> parts;

            ui64 tabletId = record.GetTabletId();

            TMaybe<NKikimrClient::TKeyValueRequest::EStorageChannel> storageChannel;
            if (originalCmdWrite.HasStorageChannel()) {
                storageChannel = originalCmdWrite.GetStorageChannel();
            }

            TMaybe<NKikimrClient::TKeyValueRequest::EPriority> priority;
            if (originalCmdWrite.HasPriority()) {
                priority = originalCmdWrite.GetPriority();
            }

            records.clear();

            for (ui32 offset = 0, partId = 0; offset < data.size(); ++partId) {
                const ui32 size = Min<ui32>(maxWriteBlockSize, data.size() - offset);
                NKikimrClient::TKeyValueRequest& last = *records.emplace(records.end());
                last.SetTabletId(tabletId);
                auto& cmdWrite = *last.AddCmdWrite();
                TString partKey = Sprintf("%s@PartId#%09" PRIu32, key.data(), partId);
                cmdWrite.SetKey(partKey);
                cmdWrite.SetValue(data.substr(offset, size));
                if (storageChannel) {
                    cmdWrite.SetStorageChannel(*storageChannel);
                }
                if (priority) {
                    cmdWrite.SetPriority(*priority);
                }
                parts.push_back(partKey);
                offset += size;
            }

            NKikimrClient::TKeyValueRequest& last = *records.emplace(records.end());
            last.SetTabletId(tabletId);
            auto& cmdConcat = *last.AddCmdConcat();
            for (const TString& part : parts) {
                cmdConcat.AddInputKeys(part);
            }
            cmdConcat.SetOutputKey(key);
            cmdConcat.SetKeepInputs(false);
        }
    }

    auto successOp = [&](const NKikimrClient::TResponse &response) {
        const ui32 status = response.GetStatus();
        Cout << "status: " << status << Endl;
        Cout << "status transcript: " << static_cast<NMsgBusProxy::EResponseStatus>(response.GetStatus()) << Endl;
        if (status != NMsgBusProxy::MSTATUS_OK) {
            Cout << "error reason: " << response.GetErrorReason() << Endl;
        }

        if (requestConfig.IsReadToFile) {
            Y_ABORT_UNLESS(status == NMsgBusProxy::MSTATUS_OK);
            Y_ABORT_UNLESS(response.ReadResultSize() == 1);
            Y_ABORT_UNLESS(response.GetReadResult(0).HasStatus());
            Y_ABORT_UNLESS(response.GetReadResult(0).GetStatus() == NKikimrProto::OK);
            Y_ABORT_UNLESS(response.GetReadResult(0).HasValue());
            TString data = response.GetReadResult(0).GetValue();
            readBuffer += data;

            if (data.size() == maxReadBlockSize) {
                auto& last = *records.emplace(records.end());
                last = records[0];
                last.MutableCmdRead(0)->SetOffset(readBuffer.size());
            }
        } else if (requestConfig.IsWriteFromFile) {
            Y_ABORT_UNLESS(status == NMsgBusProxy::MSTATUS_OK);
            if (response.WriteResultSize() == 1) {
                Y_ABORT_UNLESS(response.GetWriteResult(0).HasStatus());
                Y_ABORT_UNLESS(response.GetWriteResult(0).GetStatus() == NKikimrProto::OK);
            } else if (response.ConcatResultSize() == 1) {
                Y_ABORT_UNLESS(response.GetConcatResult(0).HasStatus());
                Y_ABORT_UNLESS(response.GetConcatResult(0).GetStatus() == NKikimrProto::OK);
            } else {
                Y_ABORT("unexpected case");
            }
        } else {
            TString str;
            const bool isOk = ::google::protobuf::TextFormat::PrintToString(response, &str);
            if (!isOk) {
                ythrow TWithBackTrace<yexception>() << "Error printing response to string!";
            }
            Cout << "response: " << str << Endl;
        }
    };

    int status = 0;
    for (size_t i = 0; i < records.size(); ++i) {
        auto request = MakeHolder<NMsgBusProxy::TBusKeyValue>();
        request->Record = records[i];
        status = ClientSyncCall(request.Release(), requestConfig, successOp);
        if (status) {
            break;
        }
    }

    if (requestConfig.IsReadToFile) {
        TFile file(requestConfig.ReadToPath, CreateNew | WrOnly);
        file.Write(readBuffer.data(), readBuffer.size());
        file.Close();
    }

    return status;
}


TCmdKeyValueConfig::TCmdKeyValueConfig()
    : IsReadToFile(false)
    , IsWriteFromFile(false)
{}


void TCmdKeyValueConfig::Parse(int argc, char **argv) {
    using namespace NLastGetopt;

    TOpts opts = TOpts::Default();
    opts.AddLongOption("protobuf", "string representation of the keyvalue request protobuf").Required()
        .RequiredArgument("PROTOBUF").StoreResult(&Proto);
    opts.AddLongOption("read-to-file", "output file path, protobuf must contain single read command!").Optional()
        .RequiredArgument("PATH").StoreResult(&ReadToPath);
    opts.AddLongOption("write-from-file", "input file path, protobuf must contain single write command!").Optional()
        .RequiredArgument("PATH").StoreResult(&WriteFromPath);

    ConfigureBaseLastGetopt(opts);
    TOptsParseResult res(&opts, argc, argv);
    ConfigureMsgBusLastGetopt(res, argc, argv);

    if (ReadToPath.size() != 0) {
        IsReadToFile = true;
    }
    if (WriteFromPath.size() != 0) {
        IsWriteFromFile = true;
    }
    if (IsReadToFile && IsWriteFromFile) {
        ythrow TWithBackTrace<yexception>() << "Use either --read-to-file or --write-from-file!";
    }
}


}
}
