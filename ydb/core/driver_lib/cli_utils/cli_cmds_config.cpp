#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include "cli.h"
#include "cli_cmds.h"
#include "proto_common.h"

namespace NKikimr {
namespace NDriverClient {

class TInit : public TClientCommand {
    TString YamlFile;
    bool DryRun = false;

public:
    TInit()
        : TClientCommand("init", {}, "Initialize and manage blobstorage config using yaml description")
    {}

    void Config(TConfig& config) override {
        TClientCommand::Config(config);

        config.Opts->AddLongOption("yaml-file", "read blobstorage config from yaml file")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&YamlFile);

        config.Opts->AddLongOption('n', "dry-run", "do not apply updates")
            .Optional()
            .NoArgument()
            .SetFlag(&DryRun);
    }

    int Run(TConfig& config) override {
        TString data;

        try {
            data = TUnbufferedFileInput(YamlFile).ReadAll();
        } catch (const yexception& ex) {
            Cerr << "failed to read config from file: " << ex.what() << Endl;
            return EXIT_FAILURE;
        }

        TAutoPtr<NMsgBusProxy::TBusBlobStorageConfigRequest> msg(new NMsgBusProxy::TBusBlobStorageConfigRequest);

        NKikimrClient::TBlobStorageConfigRequest& request = msg->Record;
        if (config.SecurityToken) {
            request.SetSecurityToken(config.SecurityToken);
        }

        try {
            request.MutableRequest()->CopyFrom(NKikimr::NYaml::BuildInitDistributedStorageCommand(data));
        } catch (const yexception& ex) {
            Cerr << "failed to parse config from file: " << ex.what() << Endl;
            return EXIT_FAILURE;
        }

        if (DryRun) {
            request.MutableRequest()->SetRollback(true);
        }

        auto callback = [](const NMsgBusProxy::TBusResponse& response) {
            const auto& record = response.Record;
            if (record.HasBlobStorageConfigResponse()) {
                TString data;
                const auto& response = record.GetBlobStorageConfigResponse();
                if (google::protobuf::TextFormat::PrintToString(response, &data)) {
                    Cout << data;
                } else {
                    Cerr << "failed to print protobuf" << Endl;
                    return EXIT_FAILURE;
                }
                return response.GetSuccess() ? EXIT_SUCCESS : 2;
            }
            return record.GetStatus() == NMsgBusProxy::MSTATUS_OK ? EXIT_SUCCESS : EXIT_FAILURE;
        };

        return MessageBusCall<NMsgBusProxy::TBusBlobStorageConfigRequest, NMsgBusProxy::TBusResponse>(config, msg, callback);
    }
};


class TInvoke : public TClientCommand {
    TString ProtoFile;
    TString Protobuf;
    bool DryRun = false;

public:
    TInvoke()
        : TClientCommand("invoke", {}, "Query or update blob storage configuration")
    {}

    void Config(TConfig& config) override {
        TClientCommand::Config(config);

        config.Opts->AddLongOption("proto-file", "read protobuf query from file")
            .Optional()
            .RequiredArgument("PATH")
            .StoreResult(&ProtoFile);

        config.Opts->AddLongOption("proto", "query protobuf")
            .Optional()
            .RequiredArgument("PROTOBUF")
            .StoreResult(&Protobuf);

        config.Opts->AddLongOption('n', "dry-run", "do not apply updates")
            .Optional()
            .NoArgument()
            .SetFlag(&DryRun);
    }

    int Run(TConfig& config) override {
        TString data;

        if (ProtoFile) {
            try {
                data = TUnbufferedFileInput(ProtoFile).ReadAll();
            } catch (const yexception& ex) {
                Cerr << "failed to ready query from file: " << ex.what() << Endl;
                return EXIT_FAILURE;
            }
        } else if (Protobuf) {
            data = std::move(Protobuf);
        } else {
            Cerr << "either --proto-file or --proto must be provided" << Endl;
            return EXIT_FAILURE;
        }

        TAutoPtr<NMsgBusProxy::TBusBlobStorageConfigRequest> msg(new NMsgBusProxy::TBusBlobStorageConfigRequest);

        NKikimrClient::TBlobStorageConfigRequest& request = msg->Record;
        if (config.SecurityToken) {
            request.SetSecurityToken(config.SecurityToken);
        }
        bool success = google::protobuf::TextFormat::ParseFromString(data, request.MutableRequest());
        if (!success) {
            Cerr << "failed to parse input protobuf" << Endl;
            return EXIT_FAILURE;
        }

        if (DryRun) {
            request.MutableRequest()->SetRollback(true);
        }

        auto callback = [](const NMsgBusProxy::TBusResponse& response) {
            const auto& record = response.Record;
            if (record.HasBlobStorageConfigResponse()) {
                TString data;
                const auto& response = record.GetBlobStorageConfigResponse();
                if (google::protobuf::TextFormat::PrintToString(response, &data)) {
                    Cout << data;
                } else {
                    Cerr << "failed to print protobuf" << Endl;
                    return EXIT_FAILURE;
                }
                return response.GetSuccess() ? EXIT_SUCCESS : 2;
            }
            return record.GetStatus() == NMsgBusProxy::MSTATUS_OK ? EXIT_SUCCESS : EXIT_FAILURE;
        };

        return MessageBusCall<NMsgBusProxy::TBusBlobStorageConfigRequest, NMsgBusProxy::TBusResponse>(config, msg, callback);
    }
};

class TClientCommandBsConfig : public TClientCommandTree {
public:
    TClientCommandBsConfig()
        : TClientCommandTree("config", {}, "Configuration management")
    {
        AddCommand(std::make_unique<TInvoke>());
        AddCommand(std::make_unique<TInit>());
    }
};

std::unique_ptr<TClientCommand> CreateClientCommandBsConfig() {
    return std::make_unique<TClientCommandBsConfig>();
}

} // NDriverClient
} // NKikimr
