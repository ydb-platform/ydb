#include "ydb_service_monitoring.h"

#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NYdb {
namespace NConsoleClient {

TCommandMonitoring::TCommandMonitoring()
    : TClientCommandTree("monitoring", {"mon"}, "Monitoring service operations")
{
    AddCommand(std::make_unique<TCommandSelfCheck>());
}

TCommandSelfCheck::TCommandSelfCheck()
    : TYdbSimpleCommand("healthcheck", {"selfcheck"}, "Self check. Returns status of the database")
{}

void TCommandSelfCheck::Config(TConfig& config) {
    TYdbSimpleCommand::Config(config);
    config.SetFreeArgsNum(0);

    AddFormats(config, { EOutputFormat::Pretty, EOutputFormat::Json });

    config.Opts->AddLongOption('v', "verbose", "Return detailed info about components checked with their statuses.")
        .StoreTrue(&Verbose);
}

void TCommandSelfCheck::Parse(TConfig& config) {
    TYdbSimpleCommand::Parse(config);
    ParseFormats();
}

int TCommandSelfCheck::Run(TConfig& config) {
    NMonitoring::TMonitoringClient client(CreateDriver(config));
    NMonitoring::TSelfCheckSettings settings;

    if (Verbose) {
        settings.ReturnVerboseStatus(true);
    }

    NMonitoring::TSelfCheckResult result = client.SelfCheck(
        FillSettings(settings)
    ).GetValueSync();
    ThrowOnError(result);
    return PrintResponse(result);
}

int TCommandSelfCheck::PrintResponse(NMonitoring::TSelfCheckResult& result) {
    const auto& proto = NYdb::TProtoAccessor::GetProto(result);
    switch (OutputFormat) {
        case EOutputFormat::Default:
        case EOutputFormat::Pretty:
        {
            NColorizer::TColors colors = NColorizer::AutoColors(Cout);
            TStringBuf statusColor;
            auto hcResultString = SelfCheck_Result_Name(proto.Getself_check_result());
            switch (proto.Getself_check_result()) {
                case Ydb::Monitoring::SelfCheck::GOOD:
                    statusColor = colors.GreenColor();
                    break;
                case Ydb::Monitoring::SelfCheck::DEGRADED:
                    statusColor = colors.YellowColor();
                    break;
                case Ydb::Monitoring::SelfCheck::MAINTENANCE_REQUIRED:
                    // Orange-ish
                    statusColor = colors.IsTTY() ? "\033[88;91m" : "";
                    break;
                case Ydb::Monitoring::SelfCheck::EMERGENCY:
                    statusColor = colors.RedColor();
                    break;
                case Ydb::Monitoring::SelfCheck::UNSPECIFIED:
                    statusColor = colors.OldColor();
                    break;
                default:
                    Cout << "Unknown healthcheck status: " << hcResultString << Endl;
                    return EXIT_FAILURE;
            }

            Cout << "Healthcheck status: " << statusColor << hcResultString << colors.OldColor() << Endl;
            if (proto.Getself_check_result() != Ydb::Monitoring::SelfCheck::GOOD) {
                Cerr << "For more info use \"--format json\" option" << Endl;
            }
            break;
        }
        case EOutputFormat::Json:
        {
            TString json;
            google::protobuf::util::JsonPrintOptions jsonOpts;
            jsonOpts.preserve_proto_field_names = true;
            jsonOpts.add_whitespace = true;
            auto convertStatus = google::protobuf::util::MessageToJsonString(proto, &json, jsonOpts);
            if (convertStatus.ok()) {
                Cout << json;
            } else {
                Cerr << "Error occurred while converting result proto to json" << Endl;
                return EXIT_FAILURE;
            }
            break;
        }
        default:
            throw TMisuseException() << "This command doesn't support " << OutputFormat << " output format";
    }
    return EXIT_SUCCESS;
}

}
}
