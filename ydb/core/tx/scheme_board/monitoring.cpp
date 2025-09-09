#include "helpers.h"
#include "mon_events.h"
#include "monitoring.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/tx/scheme_board/populator.h>
#include <ydb/core/tx/scheme_board/two_part_description.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>

#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/list.h>
#include <util/generic/queue.h>
#include <util/generic/variant.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>

#include <google/protobuf/json/json.h>

#include <ranges>

// additional html elements
namespace NMonitoring {
    const char NavTag[] = "nav";
    using TNav = TTag<NavTag>;
}

namespace NKikimr {
namespace NSchemeBoard {

using namespace NJson;

#define SBB_LOG_T(stream) SB_LOG_T(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_D(stream) SB_LOG_D(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_I(stream) SB_LOG_I(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_N(stream) SB_LOG_N(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_W(stream) SB_LOG_W(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_E(stream) SB_LOG_E(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)

struct TBackupLimits {
    ui32 DefaultInFlight = 1'000;
    ui32 MinInFlight = 1;
    ui32 MaxInFlight = 10'000;
};

struct TBackupProgress {
    enum class EStatus {
        Idle,
        Starting,
        Running,
        Completed,
        Error,
    };

    ui32 TotalPaths = 0;
    ui32 CompletedPaths = 0;
    double Progress = 0.;
    EStatus Status = EStatus::Idle;
    TString ErrorMessage;

    TBackupProgress() = default;

    TBackupProgress(const TSchemeBoardMonEvents::TEvBackupProgress& ev)
        : TotalPaths(ev.TotalPaths)
        , CompletedPaths(ev.CompletedPaths)
        , Progress(TotalPaths > 0 ? (100. * CompletedPaths / TotalPaths) : 0.)
        , Status(EStatus::Running)
    {
    }

    bool IsRunning() const {
        return Status == EStatus::Starting || Status == EStatus::Running;
    }

    TString StatusToString() const {
        switch (Status) {
            case EStatus::Idle: return "idle";
            case EStatus::Starting: return "starting";
            case EStatus::Running: return "running";
            case EStatus::Completed: return "completed";
            case EStatus::Error: return TStringBuilder() << "error: " << ErrorMessage;
        }
    }

    TString ToJson() const {
        TJsonValue json;
        json["completed"] = CompletedPaths;
        json["total"] = TotalPaths;
        json["progress"] = Progress;
        json["status"] = StatusToString();

        return WriteJson(json);
    }
};

class TBackupProxyActor: public TActorBootstrapped<TBackupProxyActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::SCHEME_BOARD_BACKUP_PROXY_ACTOR;
    }
    static constexpr TStringBuf LogPrefix() {
        return "proxy"sv;
    }

    TBackupProxyActor(const TString& path, const TActorId& parent)
        : Path(path)
        , Parent(parent)
    {
    }

    void Bootstrap() {
        Send(NKikimr::MakeStateStorageProxyID(), new TEvStateStorage::TEvResolveSchemeBoard(Path));
        Become(&TBackupProxyActor::StateResolve, DefaultTimeout, new TEvents::TEvWakeup());
    }

private:
    void Handle(TEvStateStorage::TEvResolveReplicasList::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& replicas = msg.GetPlainReplicas();
        SBB_LOG_D("Handle " << ev->Get()->ToString());

        if (replicas.empty()) {
            return ReplyError();
        }

        Send(SelectReplica(replicas), new TSchemeBoardMonEvents::TEvDescribeRequest(Path));
        Become(&TBackupProxyActor::StateDescribe, DefaultTimeout, new TEvents::TEvWakeup());
    }

    void ReplyError() {
        Send(Parent, new TSchemeBoardMonEvents::TEvDescribeResponse());
        PassAway();
    }

    static TActorId SelectReplica(const TVector<TActorId>& replicas) {
        Y_ABORT_UNLESS(!replicas.empty());
        return replicas[RandomNumber<size_t>(replicas.size())];
    }

    void Handle(TSchemeBoardMonEvents::TEvDescribeResponse::TPtr& ev) {
        SBB_LOG_D("Handle " << ev->Get()->ToString());
        Send(Parent, ev->Release().Release());
        PassAway();
    }

    void Timeout() {
        SBB_LOG_D("Timeout");
        ReplyError();
    }

    void Unavailable() {
        SBB_LOG_D("Unavailable");
        ReplyError();
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvResolveReplicasList, Handle);
            cFunc(TEvents::TEvWakeup::EventType, Timeout);
            cFunc(TEvents::TEvUndelivered::EventType, Unavailable);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardMonEvents::TEvDescribeResponse, Handle);
            cFunc(TEvents::TEvWakeup::EventType, Timeout);
            cFunc(TEvents::TEvUndelivered::EventType, Unavailable);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    static constexpr TDuration DefaultTimeout = TDuration::Seconds(10);
    TString Path;
    TActorId Parent;
};

class TBackupActor: public TActorBootstrapped<TBackupActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::SCHEME_BOARD_BACKUP_ACTOR;
    }
    static constexpr TStringBuf LogPrefix() {
        return "main"sv;
    }

    TBackupActor(const TString& filePath, ui32 inFlightLimit, const TActorId& monitoringActor)
        : FilePath(filePath)
        , InFlightLimit(inFlightLimit)
        , Parent(monitoringActor)
    {
    }

    void Bootstrap() {
        try {
            OutputFile.ConstructInPlace(FilePath);
        } catch (...) {
            return ReplyError("Failed to create output file");
        }

        PendingPaths.emplace(GetClusterRootPath());
        TotalPaths = 1;

        ProcessPaths();
        Become(&TBackupActor::StateWork);
    }

private:
    TString GetClusterRootPath() const {
        if (auto* domainsInfo = AppData(TlsActivationContext->AsActorContext())->DomainsInfo.Get()) {
            if (auto* domain = domainsInfo->GetDomain()) {
                return TStringBuilder() << "/" << domain->Name;
            }
        }
        return "/";
    }

    void ProcessPaths() {
        while (!PendingPaths.empty() && InProgressPaths < InFlightLimit) {
            TString path = PendingPaths.front();
            PendingPaths.pop();

            TActorId proxyActor = Register(new TBackupProxyActor(path, SelfId()));
            ++InProgressPaths;
            ActorToPath[proxyActor] = path;

            SBB_LOG_D("ProcessPaths"
                << ", path: " << path
                << ", proxy actor: " << proxyActor
                << ", paths in progress: " << InProgressPaths
            );
        }

        SendProgressUpdate();
    }

    void Handle(TSchemeBoardMonEvents::TEvDescribeResponse::TPtr& ev) {
        auto it = ActorToPath.find(ev->Sender);
        if (it == ActorToPath.end()) {
            return;
        }

        const TString path = it->second;
        const TString& jsonDescription = ev->Get()->Record.GetJson();

        --InProgressPaths;
        ActorToPath.erase(it);

        if (!jsonDescription.empty()) {
            (*OutputFile) << jsonDescription << "\n";
            ++CompletedPaths;

            // parse children and add to pending queue
            try {
                TJsonValue parsedJson;
                if (NJson::ReadJsonFastTree(jsonDescription, &parsedJson)) {
                    if (parsedJson.Has("PathDescription") && parsedJson["PathDescription"].Has("Children")) {
                        const auto& children = parsedJson["PathDescription"]["Children"];
                        SBB_LOG_T("Queue children: " << WriteJson(&children, false));
                        for (const auto& child : children.GetArraySafe()) {
                            if (child.Has("Name")) {
                                TString childPath = TStringBuilder() << path << "/" << child["Name"].GetStringSafe();
                                PendingPaths.emplace(std::move(childPath));
                                ++TotalPaths;
                            }
                        }
                    }
                }
            } catch (...) {
                // ignore parsing errors
            }
        }

        ProcessPaths();

        if (InProgressPaths == 0 && PendingPaths.empty()) {
            ReplySuccess();
        }
    }

    void SendProgressUpdate() {
        SBB_LOG_D("SendProgressUpdate"
            << ", paths in progress: " << InProgressPaths
            << ", completed paths: " << CompletedPaths
            << ", total paths: " << TotalPaths
            << ", pending paths: " << PendingPaths.size()
        );
        Send(Parent, new TSchemeBoardMonEvents::TEvBackupProgress(TotalPaths, CompletedPaths));
    }

    void ReplyError(const TString& error) {
        Send(Parent, new TSchemeBoardMonEvents::TEvBackupResult(error));
        PassAway();
    }

    void ReplySuccess() {
        Send(Parent, new TSchemeBoardMonEvents::TEvBackupResult());
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardMonEvents::TEvDescribeResponse, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    TString FilePath;
    ui32 InFlightLimit;
    TActorId Parent;
    TQueue<TString> PendingPaths;
    THashMap<TActorId, TString> ActorToPath;
    TMaybe<TFileOutput> OutputFile;
    ui32 InProgressPaths = 0;
    ui32 CompletedPaths = 0;
    ui32 TotalPaths = 0;
};

struct TRestoreProgress {
    enum class EStatus {
        Idle,
        Starting,
        Running,
        Completed,
        Error
    };

    ui32 TotalPaths = 0;
    ui32 ProcessedPaths = 0;
    double Progress = 0.;
    EStatus Status = EStatus::Idle;
    TString ErrorMessage;

    TRestoreProgress() = default;

    TRestoreProgress(const TSchemeBoardMonEvents::TEvRestoreProgress& ev)
        : TotalPaths(ev.TotalPaths)
        , ProcessedPaths(ev.ProcessedPaths)
        , Progress(TotalPaths > 0 ? (100. * ProcessedPaths / TotalPaths) : 0.)
        , Status(EStatus::Running)
    {
    }

    bool IsRunning() const {
        return Status == EStatus::Starting || Status == EStatus::Running;
    }

    TString StatusToString() const {
        switch (Status) {
            case EStatus::Idle: return "idle";
            case EStatus::Starting: return "starting";
            case EStatus::Running: return "running";
            case EStatus::Completed: return "completed";
            case EStatus::Error: return TStringBuilder() << "error: " << ErrorMessage;
        }
    }

    TString ToJson() const {
        TJsonValue json;
        json["processed"] = ProcessedPaths;
        json["total"] = TotalPaths;
        json["progress"] = Progress;
        json["status"] = StatusToString();
        return WriteJson(json);
    }
};

class TRestoreActor : public TActorBootstrapped<TRestoreActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::SCHEME_BOARD_RESTORE_ACTOR;
    }
    static constexpr TStringBuf LogPrefix() {
        return "restore"sv;
    }

    TRestoreActor(const TString& filePath, ui64 schemeShardId, ui64 generation, const TActorId& monitoringActor)
        : FilePath(filePath)
        , SchemeShardId(schemeShardId)
        , Generation(generation)
        , Parent(monitoringActor)
    {
    }

    void Bootstrap() {
        try {
            InputFile.ConstructInPlace(FilePath);
        } catch (...) {
            return ReplyError("Failed to open input file");
        }
        ReadAndProcessFile();
        Become(&TRestoreActor::StateWork);
    }

private:
    void ReadAndProcessFile() {
        TString line;
        std::vector<std::pair<TPathId, TTwoPartDescription>> descriptions;

        while (InputFile->ReadLine(line)) {
            TTwoPartDescription description;
            auto status = google::protobuf::json::JsonStringToMessage(line, &description.Record);
            if (!status.ok()) {
                return ReplyError(TStringBuilder() << "Failed to parse JSON line: " << TStringBuf(line, 0, 100) << ", status: " << status.ToString());
            }
            TPathId pathId(description.Record.GetPathOwnerId(), description.Record.GetPathId());
            if (pathId.OwnerId != SchemeShardId) {
                continue;
            }

            TotalPaths++;
            descriptions.emplace_back(pathId, std::move(description));
        }

        if (!descriptions.empty()) {
            Populate(std::move(descriptions));
        }
    }

    void Populate(std::vector<std::pair<TPathId, NSchemeBoard::TTwoPartDescription>>&& descriptions) {
        SBB_LOG_D("Populate"
            << ", total paths: " << TotalPaths
        );
        std::sort(descriptions.begin(), descriptions.end());
        auto paths = descriptions | std::views::keys;
        PathsToProcess = TVector<TPathId>(paths.begin(), paths.end());

        Populator = Register(CreateSchemeBoardPopulator(
            SchemeShardId,
            Generation,
            std::move(descriptions),
            PathsToProcess.back().LocalPathId
        ));

        SendProgressUpdate();
        TActivationContext::Schedule(ProgressPollingInterval, new IEventHandle(Populator, SelfId(), new TSchemeBoardMonEvents::TEvInfoRequest(1)));
    }

    void Handle(TSchemeBoardMonEvents::TEvInfoResponse::TPtr& ev) {
        SBB_LOG_D("Handle " << ev->Get()->ToString());
        if (ev->Sender != Populator || !ev->Get()->Record.HasPopulatorResponse()) {
            SBB_LOG_N("Unexpected info response");
        }
        const auto& info = ev->Get()->Record.GetPopulatorResponse();
        TPathId maxRequestedPathId(info.GetMaxRequestedPathId().GetOwnerId(), info.GetMaxRequestedPathId().GetLocalPathId());
        const auto position = LowerBound(PathsToProcess.begin(), PathsToProcess.end(), maxRequestedPathId);
        ProcessedPaths = position - PathsToProcess.begin();
        SendProgressUpdate();
        if (ProcessedPaths == TotalPaths) {
            return ReplySuccess();
        }
        TActivationContext::Schedule(ProgressPollingInterval, new IEventHandle(Populator, SelfId(), new TSchemeBoardMonEvents::TEvInfoRequest(1)));
    }

    void SendProgressUpdate() {
        Send(Parent, new TSchemeBoardMonEvents::TEvRestoreProgress(TotalPaths, ProcessedPaths));
    }

    void ReplyError(const TString& error) {
        Send(Parent, new TSchemeBoardMonEvents::TEvRestoreResult(error));
        PassAway();
    }

    void ReplySuccess() {
        Send(Parent, new TSchemeBoardMonEvents::TEvRestoreResult());
        PassAway();
    }

    void PassAway() override {
        Send(Populator, new TEvents::TEvPoisonPill());
        IActor::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardMonEvents::TEvInfoResponse, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    static constexpr TDuration ProgressPollingInterval = TDuration::Seconds(1);

    TString FilePath;
    ui64 SchemeShardId;
    ui64 Generation;
    TActorId Parent;
    TMaybe<TFileInput> InputFile;
    TActorId Populator;
    ui32 TotalPaths = 0;
    ui32 ProcessedPaths = 0;
    TVector<TPathId> PathsToProcess;
};

class TMonitoring: public TActorBootstrapped<TMonitoring> {
    static constexpr char ROOT[] = "scheme_board";
    static constexpr TBackupLimits BackupLimits = TBackupLimits();
    static constexpr TStringBuf LogPrefix() {
        return "monitoring"sv;
    }

    using TActivity = NKikimrServices::TActivity;
    using EActivityType = TActivity::EType;
    using EContentType = NMon::IEvHttpInfoRes::EContentType;

    enum class ERequestType {
        Unknown,
        Index,
        Populator,
        ReplicaPopulator,
        Replica,
        Subscriber,
        SubscriberProxy,
        ReplicaSubscriber,
        Cache,
        Describe,
        Resolver,
        Resolve,
        Backup,
        Restore,
    };

    enum class EAttributeType {
        Unknown,
        Pod,
        String,
        ActorId,
    };

    struct TActorInfo {
        EActivityType ActivityType;
        TJsonMap Attributes;

        TActorInfo(EActivityType activityType, const TJsonMap& attributes)
            : ActivityType(activityType)
            , Attributes(attributes)
        {
        }

        TActorInfo(const TSchemeBoardMonEvents::TEvRegister& ev)
            : TActorInfo(ev.ActivityType, ev.Attributes)
        {
        }
    };

    void Handle(TSchemeBoardMonEvents::TEvRegister::TPtr& ev) {
        const auto& msg = *ev->Get();

        if (RegisteredActors.emplace(ev->Sender, msg).second) {
            ByActivityType[msg.ActivityType].emplace(ev->Sender);
        }
    }

    void Handle(TSchemeBoardMonEvents::TEvUnregister::TPtr& ev) {
        auto registered = RegisteredActors.find(ev->Sender);
        if (registered == RegisteredActors.end()) {
            return;
        }

        auto byActivity = ByActivityType.find(registered->second.ActivityType);
        Y_ABORT_UNLESS(byActivity != ByActivityType.end());

        byActivity->second.erase(ev->Sender);
        if (byActivity->second.empty()) {
            ByActivityType.erase(byActivity);
        }

        RegisteredActors.erase(registered);
    }

    static ERequestType ParseRequestType(const TStringBuf relPath) {
        if (!relPath || relPath == "/") {
            return ERequestType::Index;
        } else if (relPath.StartsWith("/populator")) {
            return ERequestType::Populator;
        } else if (relPath.StartsWith("/replica_populator")) {
            return ERequestType::ReplicaPopulator;
        } else if (relPath.StartsWith("/replica")) {
            return ERequestType::Replica;
        } else if (relPath.StartsWith("/subscriber")) {
            return ERequestType::Subscriber;
        } else if (relPath.StartsWith("/subscriber_proxy")) {
            return ERequestType::SubscriberProxy;
        } else if (relPath.StartsWith("/replica_subscriber")) {
            return ERequestType::ReplicaSubscriber;
        } else if (relPath.StartsWith("/cache")) {
            return ERequestType::Cache;
        } else if (relPath.StartsWith("/describe")) {
            return ERequestType::Describe;
        } else if (relPath.StartsWith("/resolver")) {
            return ERequestType::Resolver;
        } else if (relPath.StartsWith("/resolve")) {
            return ERequestType::Resolve;
        } else if (relPath.StartsWith("/backup")) {
            return ERequestType::Backup;
        } else if (relPath.StartsWith("/restore")) {
            return ERequestType::Restore;
        } else {
            return ERequestType::Unknown;
        }
    }

    static TString MakeLink(ERequestType requestType, const TStringBuf prefix) {
        TStringBuilder str;

        if (prefix) {
            str << prefix << "/";
        }

        switch (requestType) {
        case ERequestType::Index:
            return str << ROOT;
        case ERequestType::Populator:
            return str << "populator";
        case ERequestType::ReplicaPopulator:
            return str << "replica_populator";
        case ERequestType::Replica:
            return str << "replica";
        case ERequestType::Subscriber:
            return str << "subscriber";
        case ERequestType::SubscriberProxy:
            return str << "subscriber_proxy";
        case ERequestType::ReplicaSubscriber:
            return str << "replica_subscriber";
        case ERequestType::Cache:
            return str << "cache";
        case ERequestType::Describe:
            return str << "describe";
        case ERequestType::Resolver:
            return str << "resolver";
        case ERequestType::Resolve:
            return str << "resolve";
        case ERequestType::Backup:
            return str << "backup";
        case ERequestType::Restore:
            return str << "restore";
        case ERequestType::Unknown:
            return str;
        }
    }

    static EAttributeType ParseAttributeType(const TJsonValue& value) {
        switch (value.GetType()) {
        case JSON_BOOLEAN:
        case JSON_INTEGER:
        case JSON_DOUBLE:
        case JSON_UINTEGER:
            return EAttributeType::Pod;

        case JSON_STRING:
            return EAttributeType::String;

        case JSON_MAP:
        {
            const auto* type = value.GetMapSafe().FindPtr("@type");

            if (!type || type->GetType() != JSON_STRING) {
                return EAttributeType::Unknown;
            }

            if (type->GetStringSafe() == "ACTOR_ID") {
                return EAttributeType::ActorId;
            }
            // can not determine map type, fallback to unknown
            [[fallthrough]];
        }

        default:
            return EAttributeType::Unknown;
        }
    }

    template <typename P, typename D>
    static bool TryGetNext(TStringBuf& s, D delim, P& param) {
        TMaybe<TStringBuf> buf;
        GetNext(s, delim, buf);
        if (!buf) {
            return false;
        }

        return TryFromString(*buf, param);
    }

    static TActorId ParseActorId(TStringBuf str) {
        ui64 x1;
        ui64 x2;

        if (!TryGetNext(str, ':', x1) || !TryGetNext(str, ':', x2)) {
            return {};
        }

        return TActorId(x1, x2);
    }

    static TActorId ParseActorIdFromPath(TStringBuf relPath) {
        if (relPath.EndsWith('/')) {
            relPath.Chop(1);
        }

        auto lastPart = relPath.RNextTok('/');
        if (!lastPart) {
            return {};
        }

        return ParseActorId(lastPart);
    }

    static TActorId ParseActorId(const TJsonValue& value) {
        if (value.GetType() != JSON_STRING) {
            return {};
        }

        return ParseActorId(TStringBuf(value.GetStringSafe()));
    }

    static EActivityType ParseActivityType(const TJsonValue& value) {
        if (value.GetType() != JSON_STRING) {
            return TActivity::OTHER;
        }

        EActivityType result;
        if (!TActivity::EType_Parse(value.GetStringSafe(), &result)) {
            return TActivity::OTHER;
        }

        return result;
    }

    static std::pair<EActivityType, TActorId> ParseActorId(const TJsonValue::TMapType& map) {
        const auto* activityType = map.FindPtr("ActivityType");
        const auto* actorId = map.FindPtr("ActorId");

        if (!activityType || !actorId) {
            return {};
        }

        return std::make_pair(ParseActivityType(*activityType), ParseActorId(*actorId));
    }

    static TPathId PathIdFromProto(const NKikimrSchemeBoardMon::TPathId& proto) {
        return TPathId(proto.GetOwnerId(), proto.GetLocalPathId());
    }

    template <typename T>
    static TString GetPath(const T& proto) {
        if (proto.HasPath()) {
            return proto.GetPath();
        } else if (proto.HasPathId()) {
            return ToString(PathIdFromProto(proto.GetPathId()));
        }

        return {};
    }

    const TJsonValue::TMapType& GetAttrs(const TActorId& actorId) const {
        auto it = RegisteredActors.find(actorId);
        Y_ABORT_UNLESS(it != RegisteredActors.end());

        return it->second.Attributes.GetMapSafe();
    }

    using TRenderer = std::function<void(IOutputStream&)>;

    template <typename T, typename U>
    static void Header(IOutputStream& str, const T& title, const U& subTitile) {
        HTML(str) {
            DIV_CLASS("page-header") {
                TAG(TH3) {
                    str << title;
                    if (subTitile) {
                        SMALL() { str << " " << subTitile; }
                    }
                }
            }
        }
    }

    template <>
    void Header(IOutputStream& str, const TString& activityType, const NActorsProto::TActorId& actorId) {
        Header(str, activityType, ActorIdFromProto(actorId));
    }

    static void Alert(IOutputStream& str, const TStringBuf text) {
        HTML(str) {
            DIV_CLASS("alert alert-warning") {
                STRONG() {
                    str << "Warning:";
                }
                str << " " << text << ".";
            }
        }
    }

    static void Panel(IOutputStream& str, TRenderer title, TRenderer body) {
        HTML(str) {
            DIV_CLASS("panel panel-default") {
                DIV_CLASS("panel-heading") {
                    H4_CLASS("panel-title") {
                        title(str);
                    }
                }
                body(str);
            }
        }
    }

    static void SimplePanel(IOutputStream& str, const TStringBuf title, TRenderer body) {
        auto titleRenderer = [&title](IOutputStream& str) {
            HTML(str) {
                str << title;
            }
        };

        auto bodyRenderer = [body = std::move(body)](IOutputStream& str) {
            HTML(str) {
                DIV_CLASS("panel-body") {
                    body(str);
                }
            }
        };

        Panel(str, titleRenderer, bodyRenderer);
    }

    static void CollapsedPanel(IOutputStream& str, const TStringBuf title, const TStringBuf targetId, TRenderer body) {
        auto titleRenderer = [&title, &targetId](IOutputStream& str) {
            HTML(str) {
                str << "<a data-toggle='collapse' href='#" << targetId << "'>"
                    << title
                << "</a>";
            }
        };

        auto bodyRenderer = [&targetId, body = std::move(body)](IOutputStream& str) {
            HTML(str) {
                str << "<div id='" << targetId << "' class='collapse'>";
                DIV_CLASS("panel-body") {
                    body(str);
                }
                str << "</div>";
            }
        };

        Panel(str, titleRenderer, bodyRenderer);
    }

    static ERequestType ConvertActivityType(EActivityType activityType) {
        static THashMap<EActivityType, ERequestType> activityToRequest = {
            {TActivity::SCHEME_BOARD_POPULATOR_ACTOR, ERequestType::Populator},
            {TActivity::SCHEME_BOARD_REPLICA_POPULATOR_ACTOR, ERequestType::ReplicaPopulator},
            {TActivity::SCHEME_BOARD_REPLICA_ACTOR, ERequestType::Replica},
            {TActivity::SCHEME_BOARD_SUBSCRIBER_ACTOR, ERequestType::Subscriber},
            {TActivity::SCHEME_BOARD_SUBSCRIBER_PROXY_ACTOR, ERequestType::SubscriberProxy},
            {TActivity::SCHEME_BOARD_REPLICA_SUBSCRIBER_ACTOR, ERequestType::ReplicaSubscriber},
            {TActivity::PROXY_SCHEME_CACHE, ERequestType::Cache},
        };

        return activityToRequest.Value(activityType, ERequestType::Unknown);
    }

    static TString ActorIdToStringSafe(const TActorId& actorId) {
        return TStringBuilder() << actorId.RawX1() << ":" << actorId.RawX2();
    }

    template <typename T>
    static void Link(IOutputStream& str, const TStringBuf path, const T& title) {
        HTML(str) {
            HREF(path) {
                str << title;
            }
        }
    }

    static void Link(IOutputStream& str, ERequestType requestType, const TStringBuf title, const TStringBuf prefix = "..") {
        Link(str, MakeLink(requestType, prefix), title);
    }

    static void Link(IOutputStream& str, EActivityType activityType, const TActorId& actorId, const TStringBuf prefix = "..") {
        const TString path = TStringBuilder()
            << MakeLink(ConvertActivityType(activityType), prefix)
            << "/" << ActorIdToStringSafe(actorId);
        Link(str, path, actorId);
    }

    static void Link(IOutputStream& str, EActivityType activityType, const NActorsProto::TActorId& actorId, const TStringBuf prefix = "..") {
        Link(str, activityType, ActorIdFromProto(actorId), prefix);
    }

    template <typename T>
    static void TermDesc(IOutputStream& str, const TStringBuf term, const T& desc) {
        HTML(str) {
            DT() { str << term; }
            DD() { str << desc; }
        }
    }

    template <typename T>
    static void TermDescLink(IOutputStream& str, const TStringBuf term, EActivityType activityType, const T& actorId) {
        HTML(str) {
            DT() { str << term; }
            DD() { Link(str, activityType, actorId); }
        }
    }

    enum EFormType : ui8 {
        ByPath = 1 << 0,
        ByPathId = 1 << 1,
        Both = ByPath | ByPathId,
    };

    static void Form(IOutputStream& str, EFormType formType, ERequestType linkType, const TStringBuf linkPrefix, const TActorId& actorId) {
        HTML(str) {
            FORM_CLASS("form-horizontal") {
                const auto action = MakeLink(linkType, linkPrefix);

                if (formType & EFormType::ByPath) {
                    DIV_CLASS("form-group") {
                        LABEL_CLASS_FOR("col-sm-2 control-label", "path") {
                            str << "Path";
                        }
                        DIV_CLASS("col-sm-8") {
                            str << "<input type='text' id='path' name='path' class='form-control' placeholder='/full/path'>";
                        }
                        DIV_CLASS("col-sm-2") {
                            str << "<button type='submit' name='byPath' formaction='" << action << "' class='btn btn-primary'>"
                                << "Find by path"
                            << "</button>";
                        }
                    }
                }

                if (formType & EFormType::ByPathId) {
                    DIV_CLASS("form-group") {
                        LABEL_CLASS_FOR("col-sm-2 control-label", "pathId") {
                            str << "PathId";
                        }
                        DIV_CLASS("col-sm-4") {
                            str << "<input type='number' id='ownerId' name='ownerId' class='form-control' placeholder='owner id'>";
                        }
                        DIV_CLASS("col-sm-4") {
                            str << "<input type='number' id='pathId' name='pathId' class='form-control' placeholder='local path id'>";
                        }
                        DIV_CLASS("col-sm-2") {
                            str << "<button type='submit' name='byPathId' formaction='" << action << "' class='btn btn-info'>"
                                << "Find by path id"
                            << "</button>";
                        }
                    }
                }

                if (actorId) {
                    str << "<input type='hidden' name='actorId' value='" << ActorIdToStringSafe(actorId) << "'>";
                }

                str << "<pre id='description' class='hidden'/>";

                str << R"(<script>
                $(document).ready(function() {
                    $('button').click(function(e) {
                        e.preventDefault();

                        var btn = this;
                        var form = $('form');

                        $.ajax({
                            type: "GET",
                            url: btn.formAction,
                            data: Object.assign({[btn.name]: 1}, form.serializeArray().reduce(function(obj, cur, _) {
                                obj[cur.name] = cur.value;
                                return obj;
                            }, {})),
                            success: function(data) {
                                $('#description').text(JSON.stringify(data, null, ' ')).removeClass('hidden');
                            },
                            error: function (data) {
                                $('#description').text("Error: " + data).removeClass('hidden');
                            },
                        });
                    });
                });
                </script>)";
            }
        }
    }

    static void ResolveForm(IOutputStream& str) {
        Form(str, EFormType::Both, ERequestType::Resolve, "", {});
    }

    static void DescribeForm(IOutputStream& str, const TActorId& actorId, EFormType type = EFormType::Both) {
        Form(str, type, ERequestType::Describe, "..", actorId);
    }

    static void Navbar(IOutputStream& str, ERequestType originRequestType) {
        static const TVector<std::pair<ERequestType, TStringBuf>> requestTypeToTitle = {
            {ERequestType::Index, "Main"},
            {ERequestType::Resolver, "Resolver"},
            {ERequestType::Backup, "Backup"},
            {ERequestType::Restore, "Restore"},
        };

        const bool isIndex = originRequestType == ERequestType::Index;

        HTML(str) {
            str << "<style>"
                << ".backup-tab, .restore-tab { color: red !important; }"
                << ".backup-tab:hover, .restore-tab:hover { background-color: red !important; color: white !important; }"
                << ".nav-pills > li.active > a.backup-tab, .nav-pills > li.active > a.restore-tab { background-color: red !important; color: white !important; }"
                << ".nav-pills > li.active > a.backup-tab:hover, .nav-pills > li.active > a.restore-tab:hover { background-color: darkred !important; color: white !important; }"
                << "</style>";

            TAG_CLASS(TNav, "navbar") {
                UL_CLASS("nav nav-pills") {
                    for (const auto& [rt, title] : requestTypeToTitle) {
                        const TStringBuf linkPrefix = isIndex
                            ? ROOT
                            : (rt == ERequestType::Index ? ".." : "");

                        TString cssClass;
                        if (rt == ERequestType::Backup || rt == ERequestType::Restore) {
                            cssClass = TStringBuilder() << (rt == ERequestType::Backup ? "backup-tab" : "restore-tab");
                        }

                        if (rt == originRequestType) {
                            LI_CLASS("active") {
                                if (!cssClass.empty()) {
                                    str << "<a href='#' class='" << cssClass << "'>" << title << "</a>";
                                } else {
                                    Link(str, "#", title);
                                }
                            }
                        } else {
                            LI() {
                                if (!cssClass.empty()) {
                                    str << "<a href='" << MakeLink(rt, linkPrefix)
                                        << "' class='" << cssClass << "'>" << title << "</a>";
                                } else {
                                    Link(str, rt, title, linkPrefix);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    TString RenderIndex() const {
        TStringStream str;

        HTML(str) {
            Navbar(str, ERequestType::Index);

            for (const auto& kv : ByActivityType) {
                const auto& activityType = kv.first;
                const auto& actorIds = kv.second;
                const auto activityTypeStr = TActivity::EType_Name(activityType);

                CollapsedPanel(str, activityTypeStr, activityTypeStr, [&](IOutputStream& str) {
                    HTML(str) {
                        TABLE_CLASS("table table-hover") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { str << "#"; }
                                    TABLEH() { str << "Actor"; }

                                    Y_ABORT_UNLESS(!actorIds.empty());
                                    for (const auto& [key, _] : GetAttrs(*actorIds.begin())) {
                                        TABLEH() { str << key; }
                                    }
                                }
                            }
                            TABLEBODY() {
                                int i = 1;
                                for (const auto& actorId : actorIds) {
                                    TABLER() {
                                        TABLED() { str << i++; }
                                        TABLED() { Link(str, activityType, actorId, ROOT); }

                                        for (const auto& [_, value] : GetAttrs(actorId)) {
                                            switch (ParseAttributeType(value)) {
                                            case EAttributeType::Pod:
                                                TABLED() { str << value; }
                                                break;

                                            case EAttributeType::String:
                                                TABLED() { str << value.GetStringSafe(); }
                                                break;

                                            case EAttributeType::ActorId: {
                                                const auto kv = ParseActorId(value.GetMapSafe());
                                                TABLED() { Link(str, kv.first, kv.second, ROOT); }
                                                break;
                                            }

                                            case EAttributeType::Unknown:
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }

        return str.Str();
    }

    static TString RenderResolver(const TList<TActorId>& replicas) {
        TStringStream str;

        HTML(str) {
            Navbar(str, ERequestType::Resolver);
            Header(str, "Replica resolver", "");

            SimplePanel(str, "Resolver", [](IOutputStream& str) {
                ResolveForm(str);
            });

            CollapsedPanel(str, "All replicas", "allReplicas", [&replicas](IOutputStream& str) {
                HTML(str) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "#"; }
                                TABLEH() { str << "Actor"; }
                            }
                        }
                        TABLEBODY() {
                            ui32 i = 0;
                            for (const auto& replica : replicas) {
                                TABLER() {
                                    TABLED() { str << ++i; }
                                    TABLED() { Link(str, TActivity::SCHEME_BOARD_REPLICA_ACTOR, replica, ""); }
                                }
                            }
                        }
                    }
                }
            });
        }

        return str.Str();
    }

    static TString RenderReplica(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kReplicaResponse);
        const auto& response = record.GetReplicaResponse();

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());
            if (record.GetTruncated()) {
                Alert(str, "some lists has been truncated");
            }

            SimplePanel(str, "Descriptions", [&record](IOutputStream& str) {
                const auto& response = record.GetReplicaResponse();

                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(str, "TotalCount", response.GetDescriptions().GetTotalCount());
                        TermDesc(str, "ByPathCount", response.GetDescriptions().GetByPathCount());
                        TermDesc(str, "ByPathIdCount", response.GetDescriptions().GetByPathIdCount());
                    }

                    DescribeForm(str, ActorIdFromProto(record.GetSelf()));
                }
            });

            CollapsedPanel(str, "Populators", "populators", [&response](IOutputStream& str) {
                HTML(str) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "#"; }
                                TABLEH() { str << "Actor"; }
                                TABLEH() { str << "Owner"; }
                                TABLEH() { str << "Generation"; }
                                TABLEH() { str << "PendingGeneration"; }
                            }
                        }
                        TABLEBODY() {
                            for (ui32 i = 0; i < response.PopulatorsSize(); ++i) {
                                const auto& populator = response.GetPopulators(i);

                                TABLER() {
                                    TABLED() { str << (i + 1); }
                                    TABLED() { Link(str, TActivity::SCHEME_BOARD_REPLICA_POPULATOR_ACTOR, populator.GetActorId()); }
                                    TABLED() { str << populator.GetOwner(); }
                                    TABLED() { str << populator.GetGeneration(); }
                                    TABLED() { str << populator.GetPendingGeneration(); }
                                }
                            }
                        }
                    }
                }
            });

            CollapsedPanel(str, "Subscribers", "subscribers", [&response](IOutputStream& str) {
                HTML(str) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "#"; }
                                TABLEH() { str << "Actor"; }
                                TABLEH() { str << "Path"; }
                            }
                        }
                        TABLEBODY() {
                            for (ui32 i = 0; i < response.SubscribersSize(); ++i) {
                                const auto& subscriber = response.GetSubscribers(i);

                                TABLER() {
                                    TABLED() { str << (i + 1); }
                                    TABLED() { Link(str, TActivity::SCHEME_BOARD_REPLICA_SUBSCRIBER_ACTOR, subscriber.GetActorId()); }
                                    TABLED() { str << GetPath(subscriber); }
                                }
                            }
                        }
                    }
                }
            });
        }

        return str.Str();
    }

    static TString RenderPopulator(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kPopulatorResponse);
        const auto& response = record.GetPopulatorResponse();

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());
            if (record.GetTruncated()) {
                Alert(str, "some lists has been truncated");
            }

            SimplePanel(str, "Info", [&record](IOutputStream& str) {
                const auto& response = record.GetPopulatorResponse();

                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(str, "Owner", response.GetOwner());
                        TermDesc(str, "Generation", response.GetGeneration());
                        TermDesc(str, "DescriptionsCount", response.GetDescriptionsCount());
                        TermDesc(str, "MaxPathId", PathIdFromProto(response.GetMaxPathId()));
                        TermDesc(str, "DelayedUpdatesCount", response.GetDelayedUpdatesCount());
                    }

                    DescribeForm(str, ActorIdFromProto(record.GetSelf()), EFormType::ByPathId);
                }
            });

            CollapsedPanel(str, "ReplicaPopulators", "replicaPopulators", [&response](IOutputStream& str) {
                HTML(str) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "#"; }
                                TABLEH() { str << "Actor"; }
                            }
                        }
                        TABLEBODY() {
                            for (ui32 i = 0; i < response.ReplicaPopulatorsSize(); ++i) {
                                const auto& replicaPopulator = response.GetReplicaPopulators(i);

                                TABLER() {
                                    TABLED() { str << (i + 1); }
                                    TABLED() { Link(str, TActivity::SCHEME_BOARD_REPLICA_POPULATOR_ACTOR, replicaPopulator); }
                                }
                            }
                        }
                    }
                }
            });

            CollapsedPanel(str, "UpdateAcks", "updateAcks", [&response](IOutputStream& str) {
                HTML(str) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "#"; }
                                TABLEH() { str << "Cookie"; }
                                TABLEH() { str << "AckTo"; }
                                TABLEH() { str << "PathId / Version / AcksCount"; }
                            }
                        }
                        TABLEBODY() {
                            for (ui32 i = 0; i < response.UpdateAcksSize(); ++i) {
                                const auto& updateAck = response.GetUpdateAcks(i);

                                TABLER() {
                                    TABLED() { str << (i + 1); }
                                    TABLED() { str << updateAck.GetCookie(); }
                                    TABLED() { str << ActorIdFromProto(updateAck.GetAckTo()); }
                                    TABLED() {
                                        TABLE_CLASS("table table-condensed") {
                                            TABLEBODY() {
                                                for (ui32 i = 0; i < updateAck.PathAcksSize(); ++i) {
                                                    const auto& pathAck = updateAck.GetPathAcks(i);

                                                    TABLER() {
                                                        TABLED() { str << PathIdFromProto(pathAck.GetPathId()); }
                                                        TABLED() { str << pathAck.GetVersion(); }
                                                        TABLED() { str << pathAck.GetAcksCount(); }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }

        return str.Str();
    }

    static TString RenderReplicaPopulator(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kReplicaPopulatorResponse);
        const auto& response = record.GetReplicaPopulatorResponse();

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());
            if (record.GetTruncated()) {
                Alert(str, "some lists has been truncated");
            }

            SimplePanel(str, "Info", [&response](IOutputStream& str) {
                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDescLink(str, "Parent", TActivity::SCHEME_BOARD_POPULATOR_ACTOR, response.GetParent());
                        TermDescLink(str, "Replica", TActivity::SCHEME_BOARD_REPLICA_ACTOR, response.GetReplica());
                        TermDesc(str, "Owner", response.GetOwner());
                        TermDesc(str, "Generation", response.GetGeneration());
                        TermDesc(str, "CurPathId", PathIdFromProto(response.GetCurPathId()));
                        TermDesc(str, "LastAckedPathId", PathIdFromProto(response.GetLastAckedPathId()));
                        TermDesc(str, "BatchSize", response.GetBatchSize());
                        TermDesc(str, "BatchSizeLimit", response.GetBatchSizeLimit());
                    }
                }
            });

            auto renderUpdates = [&str](const TStringBuf title, const TStringBuf targetId, const auto& updates) {
                CollapsedPanel(str, title, targetId, [&updates](IOutputStream& str) {
                    HTML(str) {
                        TABLE_CLASS("table table-hover") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { str << "#"; }
                                    TABLEH() { str << "PathId"; }
                                    TABLEH() { str << "Version / TxIds"; }
                                }
                            }
                            TABLEBODY() {
                                for (int i = 0; i < updates.size(); ++i) {
                                    const auto& update = updates[i];

                                    TABLER() {
                                        TABLED() { str << (i + 1); }
                                        TABLED() { str << PathIdFromProto(update.GetPathId()); }
                                        TABLED() {
                                            TABLE_CLASS("table table-condensed") {
                                                TABLEBODY() {
                                                    for (ui32 i = 0; i < update.VersionsSize(); ++i) {
                                                        const auto& version = update.GetVersions(i);

                                                        TABLER() {
                                                            TABLED() { str << version.GetVersion(); }
                                                            TABLED() { str << JoinSeq(", ", version.GetTxIds()); }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            };

            renderUpdates("Updates", "updates", response.GetUpdates());
            renderUpdates("UpdatesRequested", "updatesRequested", response.GetUpdatesRequested());
            renderUpdates("UpdatesInFlight", "updatesInFlight", response.GetUpdatesInFlight());
        }

        return str.Str();
    }

    static TString RenderSubscriber(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kSubscriberResponse);
        const auto& response = record.GetSubscriberResponse();

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());

            SimplePanel(str, "Info", [&response](IOutputStream& str) {
                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(str, "Owner", ActorIdFromProto(response.GetOwner()));
                        TermDesc(str, "DomainOwnerId", response.GetDomainOwnerId());
                        TermDesc(str, "DelayedSyncRequest", response.GetDelayedSyncRequest());
                        TermDesc(str, "CurrentSyncRequest", response.GetCurrentSyncRequest());
                        TermDesc(str, "Path", GetPath(response));
                    }
                }
            });

            CollapsedPanel(str, "State", "state", [&response](IOutputStream& str) {
                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(str, "Deleted", response.GetState().GetDeleted());
                        TermDesc(str, "Strong", response.GetState().GetStrong());
                        TermDesc(str, "PathId", PathIdFromProto(response.GetState().GetPathId()));
                        TermDesc(str, "Version", response.GetState().GetVersion());
                        TermDesc(str, "DomainId", PathIdFromProto(response.GetState().GetDomainId()));
                        TermDesc(str, "AbandonedSchemeShards", JoinSeq(", ", response.GetState().GetAbandonedSchemeShards()));
                    }
                }
            });

            CollapsedPanel(str, "ProxyStates", "proxyStates", [&response](IOutputStream& str) {
                HTML(str) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "#"; }
                                TABLEH() { str << "Actor"; }
                                TABLEH() { str << "Deleted"; }
                                TABLEH() { str << "Strong"; }
                                TABLEH() { str << "PathId"; }
                                TABLEH() { str << "Version"; }
                                TABLEH() { str << "DomainId"; }
                                TABLEH() { str << "AbandonedSchemeShards"; }
                            }
                        }
                        TABLEBODY() {
                            for (ui32 i = 0; i < response.ProxyStatesSize(); ++i) {
                                const auto& proxy = response.GetProxyStates(i);
                                const auto& state = proxy.GetState();

                                TABLER() {
                                    TABLED() { str << (i + 1); }
                                    TABLED() { Link(str, TActivity::SCHEME_BOARD_SUBSCRIBER_PROXY_ACTOR, proxy.GetProxy()); }
                                    TABLED() { str << state.GetDeleted(); }
                                    TABLED() { str << state.GetStrong(); }
                                    TABLED() { str << PathIdFromProto(state.GetPathId()); }
                                    TABLED() { str << state.GetVersion(); }
                                    TABLED() { str << PathIdFromProto(state.GetDomainId()); }
                                    TABLED() { str << JoinSeq(", ", state.GetAbandonedSchemeShards()); }
                                }
                            }
                        }
                    }
                }
            });
        }

        return str.Str();
    }

    static TString RenderSubscriberProxy(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kSubscriberProxyResponse);
        const auto& response = record.GetSubscriberProxyResponse();

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());

            SimplePanel(str, "Info", [&response](IOutputStream& str) {
                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDescLink(str, "Parent", TActivity::SCHEME_BOARD_SUBSCRIBER_ACTOR, response.GetParent());
                        TermDescLink(str, "Replica", TActivity::SCHEME_BOARD_REPLICA_ACTOR, response.GetReplica());
                        TermDescLink(str, "ReplicaSubscriber", TActivity::SCHEME_BOARD_REPLICA_SUBSCRIBER_ACTOR, response.GetReplicaSubscriber());
                        TermDesc(str, "DomainOwnerId", response.GetDomainOwnerId());
                        TermDesc(str, "CurrentSyncRequest", response.GetCurrentSyncRequest());
                        TermDesc(str, "Path", GetPath(response));
                    }
                }
            });
        }

        return str.Str();
    }

    static TString RenderReplicaSubscriber(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kReplicaSubscriberResponse);
        const auto& response = record.GetReplicaSubscriberResponse();

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());

            SimplePanel(str, "Info", [&response](IOutputStream& str) {
                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDescLink(str, "Parent", TActivity::SCHEME_BOARD_SUBSCRIBER_PROXY_ACTOR, response.GetParent());
                        TermDescLink(str, "Replica", TActivity::SCHEME_BOARD_REPLICA_ACTOR, response.GetReplica());
                        TermDesc(str, "DomainOwnerId", response.GetDomainOwnerId());
                        TermDesc(str, "CurrentSyncRequest", response.GetCurrentSyncRequest());
                        TermDesc(str, "Path", GetPath(response));
                    }
                }
            });
        }

        return str.Str();
    }

    static TString RenderCache(const NKikimrSchemeBoardMon::TEvInfoResponse& record) {
        Y_ABORT_UNLESS(record.GetResponseCase() == NKikimrSchemeBoardMon::TEvInfoResponse::kCacheResponse);

        TStringStream str;

        HTML(str) {
            Header(str, record.GetActivityType(), record.GetSelf());

            SimplePanel(str, "Descriptions", [&record](IOutputStream& str) {
                const auto& response = record.GetCacheResponse();

                HTML(str) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(str, "TotalCount", response.GetItemsTotalCount());
                        TermDesc(str, "ByPathCount", response.GetItemsByPathCount());
                        TermDesc(str, "ByPathIdCount", response.GetItemsByPathIdCount());
                    }

                    DescribeForm(str, ActorIdFromProto(record.GetSelf()));
                }
            });
        }

        return str.Str();
    }

    static TString RenderBackup(
        const TBackupProgress& backupProgress,
        bool backupStarted = false,
        const TString& errorMessage = ""
    ) {
        Y_UNUSED(backupProgress, backupStarted, errorMessage);
        TStringStream str;

        HTML(str) {
            Navbar(str, ERequestType::Backup);
            Header(str, "Backup", "Backup path descriptions locally, see <a href='https://ydb.tech/docs' target='_blank'>docs</a>");

            DIV_CLASS("alert alert-info") {
                str << "Backup functionality is not implemented yet";
            }
        }

        return str.Str();
    }

    static TString RenderRestore(
        const TRestoreProgress& restoreProgress,
        bool restoreStarted = false,
        const TString& errorMessage = ""
    ) {
        Y_UNUSED(restoreProgress, restoreStarted, errorMessage);
        TStringStream str;

        HTML(str) {
            Navbar(str, ERequestType::Restore);
            Header(str, "Restore", "Restore path descriptions saved locally, see <a href='https://ydb.tech/docs' target='_blank'>docs</a>");

            DIV_CLASS("alert alert-info") {
                str << "Restore functionality is not implemented yet";
            }
        }

        return str.Str();
    }

    template <typename TDerived, typename TEvResponse>
    class TBaseRequester: public TActorBootstrapped<TDerived> {
        static constexpr char HTTPBADGATEWAY[] = "HTTP/1.1 502 Bad Gateway\r\nConnection: Close\r\n\r\nBad Gateway\r\n";
        static constexpr char HTTPUNAVAILABLE[] = "HTTP/1.1 503 Service Unavailable\r\nConnection: Close\r\n\r\nService Unavailable\r\n";
        static constexpr char HTTPTIMEOUT[] = "HTTP/1.1 504 Gateway Timeout\r\nConnection: Close\r\n\r\nGateway Timeout\r\n";

    protected:
        virtual IEventBase* MakeRequest() const = 0;
        virtual void ProcessResponse(typename TEvResponse::TPtr& ev) = 0;

        void Handle(typename TEvResponse::TPtr& ev) {
            ProcessResponse(ev);
        }

        void Reply(const TString& content, EContentType type = EContentType::Html) {
            this->Send(ReplyTo, new NMon::TEvHttpInfoRes(content, 0, type));
            this->PassAway();
        }

        void BadGateway() {
            Reply(HTTPBADGATEWAY, EContentType::Custom);
        }

        void Timeout() {
            Reply(HTTPTIMEOUT, EContentType::Custom);
        }

        void Unavailable() {
            Reply(HTTPUNAVAILABLE, EContentType::Custom);
        }

    public:
        static constexpr auto ActorActivityType() {
            return TActivity::SCHEME_BOARD_INFO_REQUESTER_ACTOR;
        }

        explicit TBaseRequester(const TActorId& requestFrom, const TActorId& replyTo)
            : RequestFrom(requestFrom)
            , ReplyTo(replyTo)
        {
        }

        void Bootstrap() {
            this->Send(RequestFrom, MakeRequest(), IEventHandle::FlagTrackDelivery);
            this->Become(&TDerived::StateWork, TDuration::Seconds(10), new TEvents::TEvWakeup());
        }

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvResponse, Handle);
                cFunc(TEvents::TEvWakeup::EventType, Timeout);
                cFunc(TEvents::TEvUndelivered::EventType, Unavailable);
            }
        }

        using TBase = TBaseRequester<TDerived, TEvResponse>;

    private:
        const TActorId RequestFrom;
        const TActorId ReplyTo;

    }; // TBaseRequester

    class TReplicaEnumerator: public TBaseRequester<TReplicaEnumerator, TEvStateStorage::TEvListSchemeBoardResult> {
        IEventBase* MakeRequest() const override {
            return new TEvStateStorage::TEvListSchemeBoard(false);
        }

        void ProcessResponse(TEvStateStorage::TEvListSchemeBoardResult::TPtr& ev) override {
            const auto& info = ev->Get()->Info;
            if (!info) {
                return BadGateway();
            }

            return Reply(RenderResolver(info->SelectAllReplicas()));
        }

    public:
        explicit TReplicaEnumerator(const TActorId& replyTo)
            : TBase(NKikimr::MakeStateStorageProxyID(), replyTo)
        {
        }

    }; // TReplicaEnumerator

    class TReplicaResolver: public TBaseRequester<TReplicaResolver, TEvStateStorage::TEvResolveReplicasList> {
        IEventBase* MakeRequest() const override {
            switch (Path.index()) {
            case 0:
                return new TEvStateStorage::TEvResolveSchemeBoard(std::get<TString>(Path));
            case 1:
                return new TEvStateStorage::TEvResolveSchemeBoard(std::get<TPathId>(Path));
            default:
                Y_ABORT("unreachable");
            }
        }

        void ProcessResponse(TEvStateStorage::TEvResolveReplicasList::TPtr& ev) override {
            TJsonValue json;

            auto& replicas = json["replicas"];
            replicas.SetType(JSON_ARRAY);

            for (const auto& replica : ev->Get()->GetPlainReplicas()) {
                replicas.AppendValue(ToString(replica));
            }

            Reply(TStringBuilder() << NMonitoring::HTTPOKJSON << WriteJson(json), EContentType::Custom);
        }

    public:
        template <typename T>
        explicit TReplicaResolver(const TActorId& requestFrom, const TActorId& replyTo, const T& path)
            : TBase(requestFrom, replyTo)
            , Path(path)
        {
        }

    private:
        std::variant<TString, TPathId> Path;

    }; // TReplicaResolver

    class TInfoRequester: public TBaseRequester<TInfoRequester, TSchemeBoardMonEvents::TEvInfoResponse> {
        IEventBase* MakeRequest() const override {
            return new TSchemeBoardMonEvents::TEvInfoRequest();
        }

        void ProcessResponse(TSchemeBoardMonEvents::TEvInfoResponse::TPtr& ev) override {
            const auto& record = ev->Get()->Record;

            switch (record.GetResponseCase()) {
            case NKikimrSchemeBoardMon::TEvInfoResponse::kReplicaResponse:
                return Reply(RenderReplica(record));
            case NKikimrSchemeBoardMon::TEvInfoResponse::kPopulatorResponse:
                return Reply(RenderPopulator(record));
            case NKikimrSchemeBoardMon::TEvInfoResponse::kReplicaPopulatorResponse:
                return Reply(RenderReplicaPopulator(record));
            case NKikimrSchemeBoardMon::TEvInfoResponse::kSubscriberResponse:
                return Reply(RenderSubscriber(record));
            case NKikimrSchemeBoardMon::TEvInfoResponse::kSubscriberProxyResponse:
                return Reply(RenderSubscriberProxy(record));
            case NKikimrSchemeBoardMon::TEvInfoResponse::kReplicaSubscriberResponse:
                return Reply(RenderReplicaSubscriber(record));
            case NKikimrSchemeBoardMon::TEvInfoResponse::kCacheResponse:
                return Reply(RenderCache(record));
            default:
                return BadGateway();
            }
        }

    public:
        using TBase::TBase;

    }; // TInfoRequester

    class TDescriber: public TBaseRequester<TDescriber, TSchemeBoardMonEvents::TEvDescribeResponse> {
        IEventBase* MakeRequest() const override {
            switch (Path.index()) {
            case 0:
                return new TSchemeBoardMonEvents::TEvDescribeRequest(std::get<TString>(Path));
            case 1:
                return new TSchemeBoardMonEvents::TEvDescribeRequest(std::get<TPathId>(Path));
            default:
                Y_ABORT("unreachable");
            }
        }

        void ProcessResponse(TSchemeBoardMonEvents::TEvDescribeResponse::TPtr& ev) override {
            const auto& record = ev->Get()->Record;
            Reply(TStringBuilder() << NMonitoring::HTTPOKJSON << record.GetJson(), EContentType::Custom);
        }

    public:
        template <typename T>
        explicit TDescriber(const TActorId& requestFrom, const TActorId& replyTo, const T& path)
            : TBase(requestFrom, replyTo)
            , Path(path)
        {
        }

    private:
        std::variant<TString, TPathId> Path;

    }; // TDescriber

    template <typename TActionActor>
    bool RunFormAction(const TActorId& requestFrom, const TActorId& replyTo, const TCgiParameters& params) {
        if (params.Has("byPath")) {
            Register(new TActionActor(requestFrom, replyTo, params.Get("path")));
            return true;
        } else if (params.Has("byPathId")) {
            ui64 oid;
            ui64 lpid;
            if (TryFromString(params.Get("ownerId"), oid) && TryFromString(params.Get("pathId"), lpid)) {
                Register(new TActionActor(requestFrom, replyTo, TPathId(oid, lpid)));
                return true;
            }
        }

        return false;
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        const auto& request = ev->Get()->Request;
        const auto& params = request.GetParams();

        switch (ParseRequestType(request.GetPathInfo())) {
        case ERequestType::Index:
            return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderIndex()));

        case ERequestType::Populator:
        case ERequestType::ReplicaPopulator:
        case ERequestType::Replica:
        case ERequestType::Subscriber:
        case ERequestType::SubscriberProxy:
        case ERequestType::ReplicaSubscriber:
        case ERequestType::Cache:
            if (const auto actorId = ParseActorIdFromPath(request.GetPathInfo())) {
                return (void)Register(new TInfoRequester(actorId, ev->Sender));
            }
            break;

        case ERequestType::Describe:
            if (const auto actorId = ParseActorId(TStringBuf(params.Get("actorId")))) {
                if (RunFormAction<TDescriber>(actorId, ev->Sender, params)) {
                    return;
                }
            }
            break;

        case ERequestType::Resolver:
            return (void)Register(new TReplicaEnumerator(ev->Sender));

        case ERequestType::Resolve:
            if (RunFormAction<TReplicaResolver>(NKikimr::MakeStateStorageProxyID(), ev->Sender, params)) {
                return;
            }
            break;

        case ERequestType::Backup:
            if (params.Has("startBackup")) {
                if (BackupProgress.IsRunning()) {
                    return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        RenderBackup(BackupProgress, false, "Backup is already running")
                    ));
                }

                TString filePath = params.Get("backupPath");
                if (filePath.empty()) {
                    return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        RenderBackup(BackupProgress, false, "Backup file path is required")
                    ));
                }

                ui32 inFlightLimit = BackupLimits.DefaultInFlight;
                if (params.Has("inFlightLimit")) {
                    TString inFlightLimitStr = params.Get("inFlightLimit");
                    if (!TryFromString(inFlightLimitStr, inFlightLimit)) {
                        return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                            RenderBackup(BackupProgress, false, "Invalid in-flight limit value")
                        ));
                    }
                }

                BackupProgress = TBackupProgress();
                BackupProgress.Status = TBackupProgress::EStatus::Starting;

                SBB_LOG_I("Starting backup to " << filePath << " with in-flight limit " << inFlightLimit);
                Register(new TBackupActor(filePath, inFlightLimit, SelfId()));

                return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                    RenderBackup(BackupProgress, true)
                ));
            }

            if (params.Has("backupProgress")) {
                return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                    TStringBuilder() << NMonitoring::HTTPOKJSON << BackupProgress.ToJson(),
                    0, EContentType::Custom
                ));
            }

            return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderBackup(BackupProgress)));

        case ERequestType::Restore:
            if (params.Has("startRestore")) {
                if (RestoreProgress.IsRunning()) {
                    return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        RenderRestore(RestoreProgress, false, "Restore is already running")
                    ));
                }

                TString filePath = params.Get("restorePath");
                if (filePath.empty()) {
                    return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        RenderRestore(RestoreProgress, false, "Restore file path is required")
                    ));
                }

                ui64 schemeShardId = 0;
                if (!TryFromString(params.Get("schemeShardId"), schemeShardId)) {
                    return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                        RenderRestore(RestoreProgress, false, "Invalid Scheme Shard ID")
                    ));
                }

                ui64 generation = 1;
                if (params.Has("generation")) {
                    TryFromString(params.Get("generation"), generation);
                }

                RestoreProgress = TRestoreProgress();
                RestoreProgress.Status = TRestoreProgress::EStatus::Starting;

                SBB_LOG_I("Starting restore from " << filePath
                    << " for SchemeShard ID: " << schemeShardId
                    << " of generation: " << generation
                );

                Register(new TRestoreActor(filePath, schemeShardId, generation, SelfId()));

                return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                    RenderRestore(RestoreProgress, true)
                ));
            }

            if (params.Has("restoreProgress")) {
                return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(
                    TStringBuilder() << NMonitoring::HTTPOKJSON << RestoreProgress.ToJson(),
                    0, EContentType::Custom
                ));
            }

            return (void)Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderRestore(RestoreProgress)));

        case ERequestType::Unknown:
            break;
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(NMonitoring::HTTPNOTFOUND, 0, EContentType::Custom));
    }

    void Handle(TSchemeBoardMonEvents::TEvBackupProgress::TPtr& ev) {
        const auto& msg = *ev->Get();
        SBB_LOG_D("Handle " << ev->Get()->ToString());
        BackupProgress = TBackupProgress(msg);
    }

    void Handle(TSchemeBoardMonEvents::TEvBackupResult::TPtr& ev) {
        const auto& msg = *ev->Get();
        SBB_LOG_I("Handle " << ev->Get()->ToString());
        BackupProgress = TBackupProgress();
        if (msg.Error) {
            BackupProgress.Status = TBackupProgress::EStatus::Error;
            BackupProgress.ErrorMessage = *msg.Error;
        } else {
            BackupProgress.Status = TBackupProgress::EStatus::Completed;
        }
    }

    void Handle(TSchemeBoardMonEvents::TEvRestoreProgress::TPtr& ev) {
        const auto& msg = *ev->Get();
        SBB_LOG_D("Handle " << ev->Get()->ToString());
        RestoreProgress = TRestoreProgress(msg);
    }

    void Handle(TSchemeBoardMonEvents::TEvRestoreResult::TPtr& ev) {
        const auto& msg = *ev->Get();
        SBB_LOG_D("Handle " << ev->Get()->ToString());
        RestoreProgress = TRestoreProgress();
        if (msg.Error) {
            RestoreProgress.Status = TRestoreProgress::EStatus::Error;
            RestoreProgress.ErrorMessage = *msg.Error;
        } else {
            RestoreProgress.Status = TRestoreProgress::EStatus::Completed;
        }
    }

public:
    static constexpr auto ActorActivityType() {
        return TActivity::SCHEME_BOARD_MONITORING_ACTOR;
    }

    void Bootstrap() {
        if (auto* mon = AppData()->Mon) {
            auto* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, ROOT, "Scheme Board",
                false, TlsActivationContext->ActorSystem(), SelfId()
            );
        }

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardMonEvents::TEvRegister, Handle);
            hFunc(TSchemeBoardMonEvents::TEvUnregister, Handle);
            hFunc(NMon::TEvHttpInfo, Handle);

            hFunc(TSchemeBoardMonEvents::TEvBackupProgress, Handle);
            hFunc(TSchemeBoardMonEvents::TEvBackupResult, Handle);
            hFunc(TSchemeBoardMonEvents::TEvRestoreProgress, Handle);
            hFunc(TSchemeBoardMonEvents::TEvRestoreResult, Handle);

            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    THashMap<TActorId, TActorInfo> RegisteredActors;
    THashMap<EActivityType, THashSet<TActorId>> ByActivityType;
    TBackupProgress BackupProgress;
    TRestoreProgress RestoreProgress;

}; // TMonitoring

} // NSchemeBoard

IActor* CreateSchemeBoardMonitoring() {
    return new NSchemeBoard::TMonitoring();
}

} // NKikimr
