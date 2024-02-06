#include "cli.h"
#include "cli_cmds.h"

#include <ydb/library/aclib/aclib.h>

#include <util/string/split.h>
#include <util/string/join.h>
#include <util/string/printf.h>

namespace NKikimr {
namespace NDriverClient {

class TCmsClientCommand : public TClientCommandConfig {
public:
    TString Domain;
    NKikimrClient::TCmsRequest Request;

    TCmsClientCommand(const TString &name,
                      const std::initializer_list<TString> &aliases,
                      const TString &description)
        : TClientCommandConfig(name, aliases, description)
    {
    }

    void Config(TConfig &config) override
    {
        TClientCommand::Config(config);

        Domain = "";

        config.Opts->AddLongOption("domain", "Set target domain (required for clusters with multiple domains)")
            .RequiredArgument("NAME").StoreResult(&Domain);
    }

    void Parse(TConfig& config) override
    {
        TClientCommand::Parse(config);

        if (Domain)
            Request.SetDomainName(Domain);
    }

    int Run(TConfig &config) override
    {
        TAutoPtr<NMsgBusProxy::TBusCmsRequest> request(new NMsgBusProxy::TBusCmsRequest);
        request->Record.CopyFrom(Request);

        auto handler = [this](const NMsgBusProxy::TBusCmsResponse &response) -> int {
            auto &rec = response.Record;
            switch (rec.GetStatus().GetCode()) {
            case NKikimrCms::TStatus::OK:
            case NKikimrCms::TStatus::ALLOW:
            case NKikimrCms::TStatus::ALLOW_PARTIAL:
                PrintOkResponse(rec);
                return 0;
            default:
                PrintErrorResponse(rec);
                return 1;
            }
        };

        int result = MessageBusCall<NMsgBusProxy::TBusCmsRequest,
                                    NMsgBusProxy::TBusCmsResponse>(config, request, handler);
        return result;
    }

    virtual void PrintErrorResponse(const NKikimrClient::TCmsResponse &response)
    {
        PrintResponse(response);
    }

    virtual void PrintOkResponse(const NKikimrClient::TCmsResponse &response)
    {
        PrintResponse(response);
    }

    void PrintResponse(const NKikimrClient::TCmsResponse &response)
    {
        Cout << response.GetStatus().GetCode() << Endl
             << response.DebugString();
        if (const auto& issues = response.GetStatus().GetReason()) {
            Cerr << issues << Endl;
        }
    }
};

class TClientCommandState : public TCmsClientCommand {
public:
    TVector<TString> Hosts;

    TClientCommandState()
        : TCmsClientCommand("state", {}, "Get cluster state")
    {
    }

    void Config(TConfig &config) override
    {
        TCmsClientCommand::Config(config);

        config.Opts->AddLongOption("host", "Get state for specified host(s)")
            .RequiredArgument("NAME").AppendTo(&Hosts);
        config.SetFreeArgsNum(0);
    }

    void Parse(TConfig& config) override
    {
        TCmsClientCommand::Parse(config);

        auto &rec = *Request.MutableClusterStateRequest();
        for (auto &host : Hosts)
            *rec.AddHosts() = host;
    }

    void PrintOkResponse(const NKikimrClient::TCmsResponse &response) override
    {
        auto &state = response.GetClusterStateResponse().GetState();

        TInstant timestamp = TInstant::MicroSeconds(state.GetTimestamp());
        Cout << "Cluster state at " << timestamp.ToStringLocalUpToSeconds() << Endl;

        TMultiMap<TString, const NKikimrCms::THostState*> hosts;
        for (auto &host : state.GetHosts())
            hosts.emplace(host.GetName(), &host);

        for (auto &pr : hosts)
            PrintHost(*pr.second);
    }

    void PrintHost(const NKikimrCms::THostState &host)
    {
        Cout << "Host " << host.GetName() << ":" << host.GetInterconnectPort()
             << " (" << host.GetNodeId() << ")"
             << " is " << host.GetState() << Endl;

        if (host.MarkersSize()) {
            TVector<NKikimrCms::EMarker> markers;
            for (size_t i = 0; i < host.MarkersSize(); ++i)
                markers.push_back(host.GetMarkers(i));
            Cout << "  Markers: " << JoinSeq(", ", markers) << Endl;
        }

        TMultiMap<TString, const NKikimrCms::TServiceState*> services;
        for (auto &service : host.GetServices())
            services.emplace(service.GetName(), &service);

        for (auto &pr : services)
            PrintService(*pr.second);

        TMultiMap<TString, const NKikimrCms::TDeviceState*> devices;
        for (auto &device : host.GetDevices())
            devices.emplace(device.GetName(), &device);

        for (auto &pr : devices)
            PrintDevice(*pr.second);
    }

    void PrintDevice(const NKikimrCms::TDeviceState &device)
    {
        Cout << "  Device " << device.GetName() << " is " << device.GetState() << Endl;
        if (device.MarkersSize()) {
            TVector<NKikimrCms::EMarker> markers;
            for (size_t i = 0; i < device.MarkersSize(); ++i)
                markers.push_back(device.GetMarkers(i));
            Cout << "    Markers: " << JoinSeq(", ", markers) << Endl;
        }
    }

    void PrintService(const NKikimrCms::TServiceState &service)
    {
        Cout << "  Service " << service.GetName() << " is " << service.GetState() << Endl;
    }
};

class TClientCommandManageRequest : public TCmsClientCommand
{
public:
    NKikimrCms::TManageRequestRequest::ECommand Command;
    TString User;
    bool DryRun;
    bool HasId;

    TClientCommandManageRequest(const TString &name,
                                const std::initializer_list<TString> &aliases,
                                const TString &description,
                                NKikimrCms::TManageRequestRequest::ECommand cmd,
                                bool hasId)
        : TCmsClientCommand(name, aliases, description)
        , Command(cmd)
        , HasId(hasId)
    {
    }

    void Config(TConfig &config) override
    {
        TCmsClientCommand::Config(config);

        User = "";
        DryRun = false;

        config.Opts->AddLongOption("user", "User name").Required()
            .RequiredArgument("NAME").StoreResult(&User);
        if (HasId) {
            config.SetFreeArgsNum(1);
            SetFreeArgTitle(0, "<ID>", "Request ID");
        } else {
            config.SetFreeArgsNum(0);
        }
        config.Opts->AddLongOption("dry", "Dry run").NoArgument().SetFlag(&DryRun);
    }

    void Parse(TConfig& config) override
    {
        TCmsClientCommand::Parse(config);

        auto &rec = *Request.MutableManageRequestRequest();
        rec.SetUser(User);
        rec.SetCommand(Command);
        if (HasId)
            rec.SetRequestId(config.ParseResult->GetFreeArgs()[0]);
        if (DryRun)
            rec.SetDryRun(DryRun);
    }
};

class TClientCommandGetRequest : public TClientCommandManageRequest
{
public:
    TClientCommandGetRequest()
        : TClientCommandManageRequest("get", {}, "Get scheduled request",
                                      NKikimrCms::TManageRequestRequest::GET, true)
    {
    }
};

class TClientCommandListRequest : public TClientCommandManageRequest
{
public:
    TClientCommandListRequest()
        : TClientCommandManageRequest("list", {}, "List scheduled requests",
                                      NKikimrCms::TManageRequestRequest::LIST, false)
    {
    }
};

class TClientCommandRejectRequest : public TClientCommandManageRequest
{
public:
    TClientCommandRejectRequest()
        : TClientCommandManageRequest("reject", {}, "Reject scheduled request",
                                      NKikimrCms::TManageRequestRequest::REJECT, true)
    {
    }
};

class TClientCommandCheckRequest : public TCmsClientCommand
{
public:
    TString User;
    TString Id;
    bool DryRun;
    bool HasId;

    TClientCommandCheckRequest()
        : TCmsClientCommand("check", {}, "check scheduled request")
    {
    }

    void Config(TConfig &config) override
    {
        TCmsClientCommand::Config(config);

        User = "";
        DryRun = false;

        config.Opts->AddLongOption("user", "User name").Required()
            .RequiredArgument("NAME").StoreResult(&User);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<ID>", "Request ID");
        config.Opts->AddLongOption("dry", "Dry run").NoArgument().SetFlag(&DryRun);
    }

    void Parse(TConfig& config) override
    {
        TCmsClientCommand::Parse(config);

        auto &rec = *Request.MutableCheckRequest();
        rec.SetUser(User);
        rec.SetRequestId(config.ParseResult->GetFreeArgs()[0]);
        if (DryRun)
            rec.SetDryRun(DryRun);
    }
};

class TClientCommandWithAction : public TCmsClientCommand {
public:
    enum EFreeField {
        FF_TENANT,
        FF_HOST,
        FF_SERVICE,
        FF_DEVICE
    };

    TVector<NKikimrCms::TAction> Actions;
    NKikimrCms::TAction::EType Type;
    EFreeField FreeArgField;
    bool HasDuration;
    TString Host;
    ui32 Duration;

    TClientCommandWithAction(const TString &description,
                             NKikimrCms::TAction::EType type,
                             EFreeField freeArgField,
                             bool hasDuration)
        : TCmsClientCommand(CommandName(freeArgField), {}, description)
        , Type(type)
        , FreeArgField(freeArgField)
        , HasDuration(hasDuration)
    {
    }

    void Config(TConfig &config) override
    {
        TCmsClientCommand::Config(config);

        Host = "";
        Duration = 0;

        if (FreeArgField == FF_SERVICE || FreeArgField == FF_DEVICE)
            config.Opts->AddLongOption("host", "Host name").Required()
                .RequiredArgument("NAME").StoreResult(&Host);
        if (HasDuration)
            config.Opts->AddLongOption("duration", "Action duration in minutes")
                .Required().RequiredArgument("NUM").StoreResult(&Duration);
        config.SetFreeArgsMin(1);
        config.Opts->SetFreeArgDefaultTitle("<NAME>", FreeArgDescr(FreeArgField));
    }

    void Parse(TConfig& config) override
    {
        TCmsClientCommand::Parse(config);

        for (auto &param : config.ParseResult->GetFreeArgs()) {
            Actions.emplace_back();
            auto &action = Actions.back();
            action.SetType(Type);
            if (Host)
                action.SetHost(Host);
            if (param) {
                switch (FreeArgField) {
                case FF_TENANT:
                    action.SetTenant(param);
                    break;
                case FF_HOST:
                    action.SetHost(param);
                    break;
                case FF_SERVICE:
                    *action.AddServices() = param;
                    break;
                case FF_DEVICE:
                    *action.AddDevices() = param;
                    break;
                default:
                    Y_ABORT("Unknown free arg field");
                }
            }
            if (Duration)
                action.SetDuration(TDuration::Minutes(Duration).GetValue());
        }
    }

private:
    static TString CommandName(EFreeField freeArgField) {
        switch (freeArgField) {
        case FF_TENANT:
            return "tenant";
        case FF_HOST:
            return "host";
        case FF_SERVICE:
            return "service";
        case FF_DEVICE:
            return "device";
        default:
            Y_ABORT("Unknown free arg field");
        }
    }

    static TString FreeArgDescr(EFreeField freeArgField) {
        switch (freeArgField) {
        case FF_TENANT:
            return "Tenant name";
        case FF_HOST:
            return "Host name";
        case FF_SERVICE:
            return "Service name";
        case FF_DEVICE:
            return "Device name";
        default:
            Y_ABORT("Unknown free arg field");
        }
    }
};

class TClientCommandMakeRequest : public TClientCommandWithAction {
public:
    TString User;
    TString Reason;
    bool DryRun;
    bool Schedule;
    bool AllowPartial;
    bool EvictVDisks;
    ui32 Hours;
    ui32 Minutes;
    TString TenantPolicy;
    TString AvailabilityMode;
    i32 Priority;

    TClientCommandMakeRequest(const TString &description,
                             NKikimrCms::TAction::EType type,
                             EFreeField freeArgField,
                             bool hasDuration)
        : TClientCommandWithAction(description, type, freeArgField, hasDuration)
    {
    }

    void Config(TConfig &config) override
    {
        TClientCommandWithAction::Config(config);

        User = "";
        Reason = "";
        DryRun = false;
        Schedule = false;
        AllowPartial = false;
        EvictVDisks = false;
        Hours = 0;
        Minutes = 0;
        Priority = 0;

        config.Opts->AddLongOption("user", "User name").Required()
            .RequiredArgument("NAME").StoreResult(&User);
        config.Opts->AddLongOption("dry", "Dry run").NoArgument().SetFlag(&DryRun);
        config.Opts->AddLongOption("reason", "Informational request description")
            .RequiredArgument("STRING").StoreResult(&Reason);
        config.Opts->AddLongOption("schedule", "Schedule action in CMS if it's disallowed right now")
            .NoArgument().SetFlag(&Schedule);
        config.Opts->AddLongOption("hours", "Permission duration")
            .RequiredArgument("NUM").StoreResult(&Hours);
        config.Opts->AddLongOption("minutes", "Permission duration")
            .RequiredArgument("NUM").StoreResult(&Minutes);
        config.Opts->AddLongOption("tenant-policy", "Policy for computation node restart")
            .RequiredArgument("none|default").StoreResult(&TenantPolicy);
        config.Opts->AddLongOption("allow-partial", "Allow partial permission")
            .NoArgument().SetFlag(&AllowPartial);
        config.Opts->AddLongOption("availability-mode", "Availability mode")
            .RequiredArgument("max|keep|force").DefaultValue("max").StoreResult(&AvailabilityMode);
        config.Opts->AddLongOption("evict-vdisks", "Evict vdisks before granting permission(s)")
            .NoArgument().SetFlag(&EvictVDisks);
        config.Opts->AddLongOption("priority", "Request priority")
            .RequiredArgument("NUM").StoreResult(&Priority);
            
    }

    void Parse(TConfig& config) override
    {
        TClientCommandWithAction::Parse(config);

        auto &rec = *Request.MutablePermissionRequest();
        for (auto &action : Actions)
            rec.AddActions()->CopyFrom(action);
        rec.SetUser(User);
        if (DryRun)
            rec.SetDryRun(DryRun);
        if (Reason)
            rec.SetReason(Reason);
        if (Schedule)
            rec.SetSchedule(Schedule);
        if (AllowPartial)
            rec.SetPartialPermissionAllowed(AllowPartial);
        if (EvictVDisks)
            rec.SetEvictVDisks(EvictVDisks);
        if (TenantPolicy) {
            if (TenantPolicy == "none")
                rec.SetTenantPolicy(NKikimrCms::NONE);
            else if (TenantPolicy == "default")
                rec.SetTenantPolicy(NKikimrCms::DEFAULT);
            else
                ythrow yexception() << "unknown tenant policy '" << TenantPolicy << "'";
        }
        if (AvailabilityMode) {
            if (AvailabilityMode == "max")
                rec.SetAvailabilityMode(NKikimrCms::MODE_MAX_AVAILABILITY);
            else if (AvailabilityMode == "keep")
                rec.SetAvailabilityMode(NKikimrCms::MODE_KEEP_AVAILABLE);
            else if (AvailabilityMode == "force")
                rec.SetAvailabilityMode(NKikimrCms::MODE_FORCE_RESTART);
            else
                ythrow yexception() << "unknown availability mode '" << AvailabilityMode << "'";
        }
        if (Hours || Minutes) {
            auto duration = TDuration::Minutes(Minutes) + TDuration::Hours(Hours);
            rec.SetDuration(duration.GetValue());
        }
        if (Priority) {
            rec.SetPriority(Priority);
        }
    }
};

class TClientCommandSingleRequest : public TClientCommandTree {
public:
    TClientCommandSingleRequest(const TString &name,
                                const TString &description,
                                const TString &requestDescription,
                                NKikimrCms::TAction::EType type,
                                TClientCommandWithAction::EFreeField freeArgField,
                                bool hasDuration)
        : TClientCommandTree(name, {}, description)
    {
        AddCommand(std::make_unique<TClientCommandMakeRequest>(requestDescription, type, freeArgField, hasDuration));
    }
};

class TClientCommandAddRequest : public TClientCommandTree {
public:
    TClientCommandAddRequest()
        : TClientCommandTree("add", {}, "")
    {
        AddCommand(std::make_unique<TClientCommandMakeRequest>("Ask for permission to add host",
                                                 NKikimrCms::TAction::ADD_HOST,
                                                 TClientCommandWithAction::FF_HOST,
                                                 false));
        AddCommand(std::make_unique<TClientCommandMakeRequest>("Ask for permission to add device",
                                                 NKikimrCms::TAction::ADD_DEVICES,
                                                 TClientCommandWithAction::FF_DEVICE,
                                                 false));
    }
};

class TClientCommandRestartRequest : public TClientCommandTree {
public:
    TClientCommandRestartRequest()
        : TClientCommandTree("restart", {}, "")
    {
        AddCommand(std::make_unique<TClientCommandMakeRequest>("Ask for permission to restart service",
                                                 NKikimrCms::TAction::RESTART_SERVICES,
                                                 TClientCommandWithAction::FF_SERVICE,
                                                 true));
        AddCommand(std::make_unique<TClientCommandMakeRequest>("Ask for permission to restart host",
                                                 NKikimrCms::TAction::SHUTDOWN_HOST,
                                                 TClientCommandWithAction::FF_HOST,
                                                 true));
        AddCommand(std::make_unique<TClientCommandMakeRequest>("Ask for permission to restart tenant's hosts",
                                                 NKikimrCms::TAction::SHUTDOWN_HOST,
                                                 TClientCommandWithAction::FF_TENANT,
                                                 true));
    }
};

class TClientCommandRequest : public TClientCommandTree
{
public:
    TClientCommandRequest()
        : TClientCommandTree("request", {}, "Make and manage permission requests")
    {
        //AddCommand(std::make_unique<TClientCommandSingleRequest>("start", "", "Ask for permission to start service",
        //                                           NKikimrCms::TAction::START_SERVICES,
        //                                           TClientCommandWithAction::FF_SERVICE,
        //                                           false));
        //AddCommand(std::make_unique<TClientCommandSingleRequest>("stop", "", "Ask for permission to stop service",
        //                                           NKikimrCms::TAction::STOP_SERVICES,
        //                                           TClientCommandWithAction::FF_SERVICE,
        //                                           false));
        //AddCommand(std::make_unique<TClientCommandSingleRequest>("decommission", "", "Ask for permission to decommission host",
        //                                           NKikimrCms::TAction::DECOMMISSION_HOST,
        //                                           TClientCommandWithAction::FF_HOST,
        //                                           false));
        AddCommand(std::make_unique<TClientCommandSingleRequest>("replace", "", "Ask for permission to replace device",
                                                   NKikimrCms::TAction::REPLACE_DEVICES,
                                                   TClientCommandWithAction::FF_DEVICE,
                                                   true));
        //AddCommand(std::make_unique<TClientCommandSingleRequest>("remove", "", "Ask for permission to remove device",
        //                                           NKikimrCms::TAction::REMOVE_DEVICES,
        //                                           TClientCommandWithAction::FF_DEVICE,
        //                                           false));
        //AddCommand(std::make_unique<TClientCommandAddRequest>());
        AddCommand(std::make_unique<TClientCommandRestartRequest>());
        AddCommand(std::make_unique<TClientCommandGetRequest>());
        AddCommand(std::make_unique<TClientCommandListRequest>());
        AddCommand(std::make_unique<TClientCommandRejectRequest>());
        AddCommand(std::make_unique<TClientCommandCheckRequest>());
    }
};

class TClientCommandSendNotification : public TClientCommandWithAction {
public:
    TString User;
    TString Reason;
    ui32 Delay;

    TClientCommandSendNotification(const TString &description,
                                   NKikimrCms::TAction::EType type,
                                   EFreeField freeArgField,
                                   bool hasDuration)
        : TClientCommandWithAction(description, type, freeArgField, hasDuration)
    {
    }

    void Config(TConfig &config) override
    {
        TClientCommandWithAction::Config(config);

        User = "";
        Reason = "";
        Delay = 0;

        config.Opts->AddLongOption("user", "User name").Required()
            .RequiredArgument("NAME").StoreResult(&User);
        config.Opts->AddLongOption("reason", "Informational notification description")
            .RequiredArgument("STRING").StoreResult(&Reason);
        config.Opts->AddLongOption("delay", "Minutes until action happens")
            .RequiredArgument("NUM").StoreResult(&Delay);
    }

    void Parse(TConfig& config) override
    {
        TClientCommandWithAction::Parse(config);

        auto &rec = *Request.MutableNotification();
        for (auto &action : Actions)
            rec.AddActions()->CopyFrom(action);
        rec.SetUser(User);
        if (Reason)
            rec.SetReason(Reason);
        auto time = Now() + TDuration::Minutes(Delay);
        rec.SetTime(time.GetValue());
    }
};

class TClientCommandSingleNotify : public TClientCommandTree {
public:
    TClientCommandSingleNotify(const TString &name,
                               const TString &description,
                               const TString &requestDescription,
                               NKikimrCms::TAction::EType type,
                               TClientCommandWithAction::EFreeField freeArgField,
                               bool hasDuration)
        : TClientCommandTree(name, {}, description)
    {
        AddCommand(std::make_unique<TClientCommandSendNotification>(requestDescription, type, freeArgField, hasDuration));
    }
};

class TClientCommandNotifyAdd : public TClientCommandTree {
public:
    TClientCommandNotifyAdd()
        : TClientCommandTree("add", {}, "")
    {
        AddCommand(std::make_unique<TClientCommandSendNotification>("Notify about new host",
                                                      NKikimrCms::TAction::ADD_HOST,
                                                      TClientCommandWithAction::FF_HOST,
                                                      false));
        AddCommand(std::make_unique<TClientCommandSendNotification>("Notify about new device",
                                                      NKikimrCms::TAction::ADD_DEVICES,
                                                      TClientCommandWithAction::FF_DEVICE,
                                                      false));
    }
};

class TClientCommandNotifyRestart : public TClientCommandTree {
public:
    TClientCommandNotifyRestart()
        : TClientCommandTree("restart", {}, "")
    {
        AddCommand(std::make_unique<TClientCommandSendNotification>("Notify about service restart",
                                                      NKikimrCms::TAction::RESTART_SERVICES,
                                                      TClientCommandWithAction::FF_SERVICE,
                                                      true));
        AddCommand(std::make_unique<TClientCommandSendNotification>("Notify about host restart",
                                                      NKikimrCms::TAction::SHUTDOWN_HOST,
                                                      TClientCommandWithAction::FF_HOST,
                                                      true));
    }
};

class TClientCommandManageNotification : public TCmsClientCommand
{
public:
    NKikimrCms::TManageNotificationRequest::ECommand Command;
    TString User;
    bool DryRun;
    bool HasId;

    TClientCommandManageNotification(const TString &name,
                                     const std::initializer_list<TString> &aliases,
                                     const TString &description,
                                     NKikimrCms::TManageNotificationRequest::ECommand cmd,
                                     bool hasId)
        : TCmsClientCommand(name, aliases, description)
        , Command(cmd)
        , HasId(hasId)
    {
    }

    void Config(TConfig &config) override
    {
        TCmsClientCommand::Config(config);

        User = "";
        DryRun = false;

        config.Opts->AddLongOption("user", "User name").Required()
            .RequiredArgument("NAME").StoreResult(&User);
        if (HasId) {
            config.SetFreeArgsNum(1);
            SetFreeArgTitle(0, "<ID>", "Notification ID");
        } else {
            config.SetFreeArgsNum(0);
        }
        config.Opts->AddLongOption("dry", "Dry run").NoArgument().SetFlag(&DryRun);
    }

    void Parse(TConfig& config) override
    {
        TCmsClientCommand::Parse(config);

        auto &rec = *Request.MutableManageNotificationRequest();
        rec.SetUser(User);
        rec.SetCommand(Command);
        if (HasId)
            rec.SetNotificationId(config.ParseResult->GetFreeArgs()[0]);
        if (DryRun)
            rec.SetDryRun(DryRun);
    }
};

class TClientCommandGetNotification : public TClientCommandManageNotification
{
public:
    TClientCommandGetNotification()
        : TClientCommandManageNotification("get", {}, "Get stored notification",
                                           NKikimrCms::TManageNotificationRequest::GET, true)
    {
    }
};

class TClientCommandListNotification : public TClientCommandManageNotification
{
public:
    TClientCommandListNotification()
        : TClientCommandManageNotification("list", {}, "List stored notifications",
                                           NKikimrCms::TManageNotificationRequest::LIST, false)
    {
    }
};

class TClientCommandRejectNotification : public TClientCommandManageNotification
{
public:
    TClientCommandRejectNotification()
        : TClientCommandManageNotification("reject", {}, "Reject stored notification",
                                           NKikimrCms::TManageNotificationRequest::REJECT, true)
    {
    }
};

class TClientCommandNotify : public TClientCommandTree
{
public:
    TClientCommandNotify()
        : TClientCommandTree("notify", {}, "Send and manage notifications")
    {
        //AddCommand(std::make_unique<TClientCommandSingleNotify>("start", "", "Notify about service start",
        //                                          NKikimrCms::TAction::START_SERVICES,
        //                                          TClientCommandWithAction::FF_SERVICE,
        //                                          false));
        //AddCommand(std::make_unique<TClientCommandSingleNotify>("stop", "", "Notify about service stop",
        //                                          NKikimrCms::TAction::STOP_SERVICES,
        //                                          TClientCommandWithAction::FF_SERVICE,
        //                                          false));
        //AddCommand(std::make_unique<TClientCommandSingleNotify>("decommission", "", "Notify about host decomission",
        //                                          NKikimrCms::TAction::DECOMMISSION_HOST,
        //                                          TClientCommandWithAction::FF_HOST,
        //                                          false));
        AddCommand(std::make_unique<TClientCommandSingleNotify>("replace", "", "Notify about device replacement",
                                                  NKikimrCms::TAction::REPLACE_DEVICES,
                                                  TClientCommandWithAction::FF_DEVICE,
                                                  true));
        //AddCommand(std::make_unique<TClientCommandSingleNotify>("remove", "", "Notify about device removal",
        //                                          NKikimrCms::TAction::REMOVE_DEVICES,
        //                                          TClientCommandWithAction::FF_DEVICE,
        //                                          false));
        //AddCommand(std::make_unique<TClientCommandNotifyAdd>());
        AddCommand(std::make_unique<TClientCommandNotifyRestart>());
        AddCommand(std::make_unique<TClientCommandGetNotification>());
        AddCommand(std::make_unique<TClientCommandListNotification>());
        AddCommand(std::make_unique<TClientCommandRejectNotification>());
    }
};

class TClientCommandManagePermission : public TCmsClientCommand
{
public:
    NKikimrCms::TManagePermissionRequest::ECommand Command;
    TString User;
    ui32 Hours;
    ui32 Minutes;
    bool DryRun;
    bool HasId;
    bool HasDeadline;

    TClientCommandManagePermission(const TString &name,
                                   const std::initializer_list<TString> &aliases,
                                   const TString &description,
                                   NKikimrCms::TManagePermissionRequest::ECommand cmd,
                                   bool hasId,
                                   bool hasDeadline)
        : TCmsClientCommand(name, aliases, description)
        , Command(cmd)
        , HasId(hasId)
        , HasDeadline(hasDeadline)
    {
    }

    void Config(TConfig &config) override
    {
        TCmsClientCommand::Config(config);

        User = "";
        Hours = 0;
        Minutes = 0;
        DryRun = false;

        config.Opts->AddLongOption("user", "User name").Required()
            .RequiredArgument("NAME").StoreResult(&User);
        if (HasId) {
            config.SetFreeArgsNum(1);
            SetFreeArgTitle(0, "<ID>", "Permission ID");
        } else {
            config.SetFreeArgsNum(0);
        }
        if (HasDeadline) {
            config.Opts->AddLongOption("hours", "New permission duration")
                .RequiredArgument("NUM").StoreResult(&Hours);
            config.Opts->AddLongOption("minutes", "New permission duration")
                .RequiredArgument("NUM").StoreResult(&Minutes);
        }
        config.Opts->AddLongOption("dry", "Dry run").NoArgument().SetFlag(&DryRun);
    }

    void Parse(TConfig& config) override
    {
        TCmsClientCommand::Parse(config);

        auto &rec = *Request.MutableManagePermissionRequest();
        rec.SetUser(User);
        rec.SetCommand(Command);
        if (HasId)
            *rec.AddPermissions() = config.ParseResult->GetFreeArgs()[0];
        if (Hours || Minutes) {
            auto deadline = Now() + TDuration::Minutes(Minutes) + TDuration::Hours(Hours);
            rec.SetDeadline(deadline.GetValue());
        }
        if (DryRun)
            rec.SetDryRun(DryRun);
    }
};

class TClientCommandGetPermission : public TClientCommandManagePermission
{
public:
    TClientCommandGetPermission()
        : TClientCommandManagePermission("get", {}, "Get permission",
                                         NKikimrCms::TManagePermissionRequest::GET,
                                         true, false)
    {
    }
};

class TClientCommandListPermission : public TClientCommandManagePermission
{
public:
    TClientCommandListPermission()
        : TClientCommandManagePermission("list", {}, "List permissions",
                                         NKikimrCms::TManagePermissionRequest::LIST,
                                         false, false)
    {
    }
};

class TClientCommandDonePermission : public TClientCommandManagePermission
{
public:
    TClientCommandDonePermission()
        : TClientCommandManagePermission("done", {}, "Done with permission",
                                         NKikimrCms::TManagePermissionRequest::DONE,
                                         true, false)
    {
    }
};

class TClientCommandExtendPermission : public TClientCommandManagePermission
{
public:
    TClientCommandExtendPermission()
        : TClientCommandManagePermission("extend", {}, "Extend permission",
                                         NKikimrCms::TManagePermissionRequest::EXTEND,
                                         true, true)
    {
    }
};

class TClientCommandRejectPermission : public TClientCommandManagePermission
{
public:
    TClientCommandRejectPermission()
        : TClientCommandManagePermission("reject", {}, "Decline permission",
                                         NKikimrCms::TManagePermissionRequest::REJECT,
                                         true, false)
    {
    }
};

class TClientCommandPermission : public TClientCommandTree
{
public:
    TClientCommandPermission()
        : TClientCommandTree("permission", {}, "Manage issued permissions")
    {
        AddCommand(std::make_unique<TClientCommandGetPermission>());
        AddCommand(std::make_unique<TClientCommandListPermission>());
        AddCommand(std::make_unique<TClientCommandDonePermission>());
        //AddCommand(std::make_unique<TClientCommandExtendPermission>());
        AddCommand(std::make_unique<TClientCommandRejectPermission>());
    }
};

class TClientCommandGetConfig : public TCmsClientCommand {
public:
    TClientCommandGetConfig()
        : TCmsClientCommand("get", {}, "Get CMS config")
    {
    }

    void Parse(TConfig& config) override
    {
        TCmsClientCommand::Parse(config);

        Request.MutableGetConfigRequest();
    }
};

class TClientCommandSetConfig : public TCmsClientCommand {
public:
    TClientCommandSetConfig()
        : TCmsClientCommand("set", {}, "Set CMS config")
    {
    }

    virtual void Config(TConfig& config) override {
        TCmsClientCommand::Config(config);

        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<CONFIG>", "Config protobuf or file with config protobuf");
    }

    void Parse(TConfig& config) override
    {
        TCmsClientCommand::Parse(config);

        auto proto = config.ParseResult->GetFreeArgs().at(0);
        auto request = GetProtobuf<NKikimrCms::TCmsConfig>(proto);
        Request.MutableSetConfigRequest()->MutableConfig()->CopyFrom(*request);
    }
};

class TClientCommandManageConfig : public TClientCommandTree
{
public:
    TClientCommandManageConfig()
        : TClientCommandTree("config", {}, "Manage CMS config")
    {
        AddCommand(std::make_unique<TClientCommandGetConfig>());
        AddCommand(std::make_unique<TClientCommandSetConfig>());
    }
};

class TClientCommandManageMarker : public TCmsClientCommand {
private:
    TVector<TString> Hosts;
    TVector<ui32> Nodes;
    TVector<TString> PDisks;

protected:
    NKikimrCms::EMarker Marker;
    NKikimrCms::TItems Items;

public:
    TClientCommandManageMarker(const TString &name,
                               const std::initializer_list<TString> &aliases,
                               const TString &description)
        : TCmsClientCommand(name, aliases, description)
    {
    }

    virtual void Config(TConfig& config) override {
        TCmsClientCommand::Config(config);

        TVector<NKikimrCms::EMarker> allMarkers;
        for (ui32 i = NKikimrCms::EMarker_MIN + 1; i <= NKikimrCms::EMarker_MAX; ++i)
            if (NKikimrCms::EMarker_IsValid(i))
                allMarkers.push_back(static_cast<NKikimrCms::EMarker>(i));
        TString desc = Sprintf("Marker name (%s)", JoinSeq(", ", allMarkers).data());

        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<MARKER>", desc);
        config.Opts->AddLongOption("host", "Host(s) to mark")
            .RequiredArgument("NAME").AppendTo(&Hosts);
        config.Opts->AddLongOption("node", "Node(s) to mark")
            .RequiredArgument("ID").AppendTo(&Nodes);
        config.Opts->AddLongOption("pdisk", "PDisk(s) to mark")
            .RequiredArgument("<NODE>:<ID>").AppendTo(&PDisks);
    }

    void Parse(TConfig& config) override
    {
        TCmsClientCommand::Parse(config);

        if (!NKikimrCms::EMarker_Parse(config.ParseResult->GetFreeArgs().at(0), &Marker))
            ythrow yexception() << "invalid marker name " << config.ParseResult->GetFreeArgs().at(0);

        for (auto &host : Hosts)
            Items.AddHosts(host);
        for (auto &nodeId : Nodes)
            Items.AddNodes(nodeId);
        for (auto &pdiskId : PDisks) {
            TVector<TString> parts = StringSplitter(pdiskId).Split(':').ToList<TString>();
            if (parts.size() != 2)
                ythrow yexception() << "bad PDisk ID format in '" + pdiskId + "'";
            ui32 nodeId;
            ui32 diskId;
            if (!TryFromString<ui32>(parts[0], nodeId)
                || !TryFromString<ui32>(parts[1], diskId))
                ythrow yexception() << "bad PDisk ID format in '" + pdiskId + "'";
            auto &pdisk = *Items.AddPDisks();
            pdisk.SetNodeId(nodeId);
            pdisk.SetDiskId(diskId);
        }
    }
};

class TClientCommandSetMarker : public TClientCommandManageMarker {
public:
    TClientCommandSetMarker()
        : TClientCommandManageMarker("set", {}, "Set marker")
    {
    }

    void Parse(TConfig& config) override
    {
        TClientCommandManageMarker::Parse(config);

        auto &req = *Request.MutableSetMarkerRequest();
        req.SetMarker(Marker);
        req.MutableItems()->CopyFrom(Items);
    }
};

class TClientCommandResetMarker : public TClientCommandManageMarker {
public:
    TClientCommandResetMarker()
        : TClientCommandManageMarker("reset", {}, "Reset marker")
    {
    }

    void Parse(TConfig& config) override
    {
        TClientCommandManageMarker::Parse(config);

        auto &req = *Request.MutableResetMarkerRequest();
        req.SetMarker(Marker);
        req.MutableItems()->CopyFrom(Items);
    }
};

class TClientCommandMarker : public TClientCommandTree
{
public:
    TClientCommandMarker()
        : TClientCommandTree("marker", {}, "Manage markers")
    {
        AddCommand(std::make_unique<TClientCommandSetMarker>());
        AddCommand(std::make_unique<TClientCommandResetMarker>());
    }
};

class TClientCommandLogTail : public TCmsClientCommand {
private:
    ui64 Offset = 0;
    ui64 Limit = 50;
    NKikimrCms::TLogRecordData::EType RecType = NKikimrCms::TLogRecordData::UNKNOWN;
    NKikimrCms::ETextFormat Format = NKikimrCms::TEXT_FORMAT_SHORT;

public:
    TClientCommandLogTail()
        : TCmsClientCommand("tail", {}, "Print log tail")
    {
    }

    virtual void Config(TConfig& config) override {
        TCmsClientCommand::Config(config);

        TVector<NKikimrCms::TLogRecordData::EType> allTypes;
        for (ui32 i = NKikimrCms::TLogRecordData::EType_MIN + 1;
             i <= NKikimrCms::TLogRecordData::EType_MAX;
             ++i)
            if (NKikimrCms::TLogRecordData::EType_IsValid(i))
                allTypes.push_back(static_cast<NKikimrCms::TLogRecordData::EType>(i));
        TString desc = Sprintf("Filter log records by type (%s)", JoinSeq(", ", allTypes).data());

        config.Opts->AddLongOption("detailed", "Show more detailed log messages")
            .NoArgument();
        config.Opts->AddLongOption("page", "Show Nth log page from the bottom")
            .RequiredArgument("N").StoreResult(&Offset);
        config.Opts->AddLongOption("limit", "Limit number of fetched entries (50 by default)")
            .RequiredArgument("COUNT").StoreResult(&Limit);
        config.Opts->AddLongOption("type", desc)
            .RequiredArgument("NAME");
    }

    void Parse(TConfig& config) override
    {
        TCmsClientCommand::Parse(config);

        if (config.ParseResult->Has("type")) {
            TString recType = config.ParseResult->Get("type");
            if (!NKikimrCms::TLogRecordData::EType_Parse(recType, &RecType))
                ythrow yexception() << "invalid record type name " << recType;
        }

        if (!Limit)
            ythrow yexception() << "log size limit shouldn't be zero";

        Offset *= Limit;

        if (config.ParseResult->Has("detailed"))
            Format = NKikimrCms::TEXT_FORMAT_DETAILED;

        auto &req = *Request.MutableGetLogTailRequest();
        req.SetTextFormat(Format);
        req.MutableLogFilter()->SetRecordType(RecType);
        req.MutableLogFilter()->SetLimit(Limit);
        req.MutableLogFilter()->SetOffset(Offset);
    }

    void PrintOkResponse(const NKikimrClient::TCmsResponse &response) override
    {
        if (!response.GetGetLogTailResponse().LogRecordsSize())
            Cout << "<empty>" << Endl;

        for (auto &rec : response.GetGetLogTailResponse().GetLogRecords()) {
            TInstant timestamp = TInstant::FromValue(rec.GetTimestamp());
            Cout << timestamp << " "
                 << static_cast<NKikimrCms::TLogRecordData::EType>(rec.GetRecordType())
                 << " " << rec.GetMessage() << Endl;
        }
    }
};

class TClientCommandLog : public TClientCommandTree
{
public:
    TClientCommandLog()
        : TClientCommandTree("log", {}, "Inspect log records")
    {
        AddCommand(std::make_unique<TClientCommandLogTail>());
    }
};

TClientCommandCms::TClientCommandCms()
    : TClientCommandTree("cms", {}, "CMS requests")
{
    AddCommand(std::make_unique<TClientCommandState>());
    AddCommand(std::make_unique<TClientCommandRequest>());
    AddCommand(std::make_unique<TClientCommandNotify>());
    AddCommand(std::make_unique<TClientCommandPermission>());
    AddCommand(std::make_unique<TClientCommandManageConfig>());
    AddCommand(std::make_unique<TClientCommandMarker>());
    AddCommand(std::make_unique<TClientCommandLog>());
}

} // namespace NDriverClient
} // namespace NKikimr
