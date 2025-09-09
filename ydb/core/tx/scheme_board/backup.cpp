#include "backup.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/tx/scheme_board/populator.h>
#include <ydb/core/tx/scheme_board/two_part_description.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/queue.h>

#include <google/protobuf/json/json.h>

#include <ranges>

namespace NKikimr::NSchemeBoard {

using namespace NJson;

TString TBackupProgress::StatusToString() const {
    switch (Status) {
        case EStatus::Idle: return "idle";
        case EStatus::Starting: return "starting";
        case EStatus::Running: return "running";
        case EStatus::Completed: return "completed";
        case EStatus::Error: return TStringBuilder() << "error: " << ErrorMessage;
    }
}

TString TBackupProgress::ToJson() const {
    TJsonValue json;
    json["completed"] = CompletedPaths;
    json["total"] = TotalPaths;
    json["progress"] = GetProgress();
    json["status"] = StatusToString();

    return WriteJson(json);
}

TString TRestoreProgress::StatusToString() const {
    switch (Status) {
        case EStatus::Idle: return "idle";
        case EStatus::Starting: return "starting";
        case EStatus::Running: return "running";
        case EStatus::Completed: return "completed";
        case EStatus::Error: return TStringBuilder() << "error: " << ErrorMessage;
    }
}

TString TRestoreProgress::ToJson() const {
    TJsonValue json;
    json["processed"] = ProcessedPaths;
    json["total"] = TotalPaths;
    json["progress"] = GetProgress();
    json["status"] = StatusToString();
    return WriteJson(json);
}

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
        Send(MakeStateStorageProxyID(), new TEvStateStorage::TEvResolveSchemeBoard(Path));
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

    const TString Path;
    const TActorId Parent;
};

class TBackupActor: public TActorBootstrapped<TBackupActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::SCHEME_BOARD_BACKUP_ACTOR;
    }

    static constexpr TStringBuf LogPrefix() {
        return "main"sv;
    }

    TBackupActor(const TString& filePath, ui32 inFlightLimit, const TActorId& parent)
        : FilePath(filePath)
        , InFlightLimit(inFlightLimit)
        , Parent(parent)
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
            const TString path = PendingPaths.front();
            PendingPaths.pop();

            const TActorId proxyActor = Register(new TBackupProxyActor(path, SelfId()));
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
    const TString FilePath;
    const ui32 InFlightLimit;
    const TActorId Parent;
    TQueue<TString> PendingPaths;
    THashMap<TActorId, TString> ActorToPath;
    TMaybe<TFileOutput> OutputFile;
    ui32 InProgressPaths = 0;
    ui32 CompletedPaths = 0;
    ui32 TotalPaths = 0;
};

class TRestoreActor: public TActorBootstrapped<TRestoreActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::SCHEME_BOARD_RESTORE_ACTOR;
    }

    static constexpr TStringBuf LogPrefix() {
        return "restore"sv;
    }

    TRestoreActor(const TString& filePath, ui64 schemeShardId, ui64 generation, const TActorId& parent)
        : FilePath(filePath)
        , SchemeShardId(schemeShardId)
        , Generation(generation)
        , Parent(parent)
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

    const TString FilePath;
    const ui64 SchemeShardId;
    const ui64 Generation;
    const TActorId Parent;
    TMaybe<TFileInput> InputFile;
    TActorId Populator;
    ui32 TotalPaths = 0;
    ui32 ProcessedPaths = 0;
    TVector<TPathId> PathsToProcess;
};

IActor* CreateSchemeBoardBackuper(const TString& filePath, ui32 inFlightLimit, const TActorId& parent) {
    return new TBackupActor(filePath, inFlightLimit, parent);
}

IActor* CreateSchemeBoardRestorer(const TString& filePath, ui64 schemeShardId, ui64 generation, const TActorId& parent) {
    return new TRestoreActor(filePath, schemeShardId, generation, parent);
}

}
