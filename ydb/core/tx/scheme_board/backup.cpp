#include "backup.h"
#include "mon_events.h"

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

TCommonProgress::TCommonProgress(const TEvCommonProgress& ev)
    : TotalPaths(ev.TotalPaths)
    , ProcessedPaths(ev.ProcessedPaths)
    , Status(EStatus::Running)
{
}

TCommonProgress::TCommonProgress(const TEvCommonResult& ev)
    : TotalPaths(0)
    , ProcessedPaths(0)
    , Status(ev.Error ? EStatus::Error : EStatus::Completed)
    , Error(ev.Error.GetOrElse(""))
    , Warning(ev.Warning.GetOrElse(""))
{
}

TString TCommonProgress::StatusToString() const {
    switch (Status) {
        case EStatus::Idle: return "idle";
        case EStatus::Starting: return "starting";
        case EStatus::Running: return "running";
        case EStatus::Completed: return "completed";
        case EStatus::Error: return TStringBuilder() << "error: " << Error;
    }
}

TString TCommonProgress::ToJson() const {
    TJsonValue json;
    json["processed"] = ProcessedPaths;
    json["total"] = TotalPaths;
    json["progress"] = GetProgress();
    json["status"] = StatusToString();
    json["warning"] = Warning;

    return WriteJson(json);
}

class TBackupActor: public TActorBootstrapped<TBackupActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::SCHEME_BOARD_BACKUP_ACTOR;
    }

    static constexpr TStringBuf LogPrefix() {
        return "backup"sv;
    }

    struct TPathDescriptionAggregate {
        struct TDescriptionCounts {
            const ui32 Required;
            ui32 Received = 0;

            explicit TDescriptionCounts(ui32 required)
                : Required(required)
            {}
        };

        TVector<TDescriptionCounts> DescriptionsByReplicaGroup;
        ui64 MostRecentVersion = 0;
        TString MostRecentDescription;

        explicit TPathDescriptionAggregate(const TVector<ui32>& requiredDescriptions) {
            DescriptionsByReplicaGroup.reserve(requiredDescriptions.size());
            for (const auto& required : requiredDescriptions) {
                DescriptionsByReplicaGroup.emplace_back(required);
            }
        }

        bool AddDescription(TString&& description, ui64 version, size_t replicaGroupIndex) {
            if (DescriptionsByReplicaGroup.size() <= replicaGroupIndex) {
                return false;
            }
            ++DescriptionsByReplicaGroup[replicaGroupIndex].Received;
            if (version > MostRecentVersion) {
                MostRecentVersion = version;
                MostRecentDescription = std::move(description);
            }
            return true;
        }

        bool IsMajorityReached() const {
            for (const auto& counts : DescriptionsByReplicaGroup) {
                if (counts.Received < counts.Required) {
                    return false;
                }
            }
            return true;
        }
    };

    TBackupActor(const TString& filePath, ui32 inFlightLimit, bool requireMajority, const TActorId& parent)
        : FilePath(filePath)
        , InFlightLimit(inFlightLimit)
        , RequireMajority(requireMajority)
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
        while (!PendingPaths.empty() && PathByCookie.size() < InFlightLimit) {
            const TString path = PendingPaths.front();
            PendingPaths.pop();

            const ui64 cookie = ++NextCookie;
            PathByCookie[cookie] = path;

            Send(MakeStateStorageProxyID(), new TEvStateStorage::TEvResolveSchemeBoard(path), 0, cookie);
            Schedule(DefaultTimeout, new TEvents::TEvWakeup(cookie));

            SBB_LOG_D("ProcessPaths"
                << ", path: " << path
                << ", cookie: " << cookie
                << ", paths in progress: " << PathByCookie.size()
            );
        }

        SendProgressUpdate();
    }

    // first 10 bits in the cookie are used to store the index of the replica group
    static ui64 AddReplicaGroupIndex(ui64 cookie, size_t index) {
        Y_ABORT_UNLESS(cookie < 1ull << 54);
        Y_ABORT_UNLESS(index < 1ull << 10);
        return cookie | index << 54;
    }

    // reverse the AddReplicaGroupIndex
    static ui64 GetOriginalCookie(ui64 cookie) {
        return cookie & std::numeric_limits<ui64>::max() >> 10;
    }

    static ui64 GetReplicaGroupIndex(ui64 cookie) {
        return cookie >> 54;
    }

    void Handle(TEvStateStorage::TEvResolveReplicasList::TPtr& ev) {
        const ui64 cookie = ev->Cookie;
        auto it = PathByCookie.find(cookie);
        if (it == PathByCookie.end()) {
            SBB_LOG_N("Unexpected cookie: " << cookie);
            return;
        }

        const TString& path = it->second;
        SBB_LOG_D("Handle " << ev->Get()->ToString() << ", path: " << path);

        const auto replicas = ev->Get()->GetPlainReplicas();
        if (replicas.empty()) {
            SBB_LOG_I("Empty replica list"
                << ", path: " << path
            );
            EmptyReplicaList.emplace(path);
            return MarkPathCompleted(it);
        }

        if (RequireMajority) {
            auto requiredDescriptions = TVector<ui32>(Reserve(ev->Get()->ReplicaGroups.size()));
            for (size_t i = 0; i < ev->Get()->ReplicaGroups.size(); ++i) {
                const auto& replicaGroup = ev->Get()->ReplicaGroups[i];
                // require descriptions from majority of replicas
                requiredDescriptions.emplace_back(replicaGroup.Replicas.size() / 2 + 1);

                for (const auto& replica : replicaGroup.Replicas) {
                    const ui64 cookieWithReplicaGroupIndex = AddReplicaGroupIndex(cookie, i);
                    Send(replica, new TSchemeBoardMonEvents::TEvDescribeRequest(path), 0, cookieWithReplicaGroupIndex);
                }
            }
            DescriptionsByCookie.emplace(cookie, TPathDescriptionAggregate(requiredDescriptions));
        } else {
            Send(SelectReplica(replicas), new TSchemeBoardMonEvents::TEvDescribeRequest(path), 0, cookie);
        }

        Schedule(DefaultTimeout, new TEvents::TEvWakeup(cookie));
    }

    static TActorId SelectReplica(const TVector<TActorId>& replicas) {
        Y_ABORT_UNLESS(!replicas.empty());
        return replicas[RandomNumber<size_t>(replicas.size())];
    }

    void Handle(TSchemeBoardMonEvents::TEvDescribeResponse::TPtr& ev) {
        const ui64 cookie = GetOriginalCookie(ev->Cookie);
        auto it = PathByCookie.find(cookie);
        if (it == PathByCookie.end()) {
            SBB_LOG_D("Received description with inactive cookie: " << cookie);
            return;
        }

        const TString path = it->second;
        SBB_LOG_D("Handle " << ev->Get()->ToString() << ", path: " << path);

        TString jsonDescription = std::move(*ev->Get()->Record.MutableJson());
        if (!jsonDescription.empty()) {
            try {
                TJsonValue parsedJson;
                if (NJson::ReadJsonFastTree(jsonDescription, &parsedJson)) {
                    if (parsedJson.Has("PathDescription")) {
                        const auto& pathDescription = parsedJson["PathDescription"];
                        if (RequireMajority) {
                            if (pathDescription.Has("Self") && pathDescription["Self"].Has("PathVersion")) {
                                const auto version = std::stoull(pathDescription["Self"]["PathVersion"].GetStringSafe());
                                auto aggregate = DescriptionsByCookie.find(cookie);
                                if (aggregate == DescriptionsByCookie.end()) {
                                    SBB_LOG_N("No description aggregate for cookie: " << cookie);
                                    return;
                                }
                                if (!aggregate->second.AddDescription(std::move(jsonDescription), version, GetReplicaGroupIndex(ev->Cookie))) {
                                    SBB_LOG_N("Invalid replica group: " << GetReplicaGroupIndex(ev->Cookie)
                                        << ", cookie: " << ev->Cookie
                                    );
                                    return;
                                }
                                if (aggregate->second.IsMajorityReached()) {
                                    const TString& chosenDescription = aggregate->second.MostRecentDescription;
                                    ParseAndAddChildrenToPending(chosenDescription, path);
                                    (*OutputFile) << chosenDescription << "\n";
                                    ++ProcessedPaths;
                                    DescriptionsByCookie.erase(aggregate);
                                    MarkPathCompleted(it);
                                }
                            } else {
                                SBB_LOG_I("No version in path description");
                            }
                        } else {
                            AddChildrenToPending(pathDescription, path);
                            (*OutputFile) << jsonDescription << "\n";
                            ++ProcessedPaths;
                            MarkPathCompleted(it);
                        }
                    }
                }
            } catch (const std::exception& e) {
                SBB_LOG_I("Parsing error: " << e.what()
                    << ", path: " << path
                );
            }
        }

        ProcessPaths();

        if (PathByCookie.empty() && PendingPaths.empty()) {
            ReplySuccess();
        }
    }

    void ParseAndAddChildrenToPending(const TString& jsonDescription, const TString& path) {
        TJsonValue parsedJson;
        if (NJson::ReadJsonFastTree(jsonDescription, &parsedJson)) {
            if (parsedJson.Has("PathDescription")) {
                const auto& pathDescription = parsedJson["PathDescription"];
                AddChildrenToPending(pathDescription, path);
            }
        }
    }

    void AddChildrenToPending(const TJsonValue& pathDescription, const TString& path) {
        if (pathDescription.Has("Children")) {
            const auto& children = pathDescription["Children"];
            SBB_LOG_D("Queue children: " << JoinSeq(", ", children.GetArraySafe() | std::views::transform([](const TJsonValue& child) {
                return child["Name"].GetStringSafe().Quote();
            })));
            for (const auto& child : children.GetArraySafe()) {
                if (child.Has("Name")) {
                    TString childPath = TStringBuilder() << path << "/" << child["Name"].GetStringSafe();
                    PendingPaths.emplace(std::move(childPath));
                    ++TotalPaths;
                }
            }
        }
    }

    void MarkPathCompleted(THashMap<ui64, TString>::iterator it) {
        PathByCookie.erase(it);
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        const ui64 cookie = GetOriginalCookie(ev->Get()->Tag);
        auto it = PathByCookie.find(cookie);
        if (it == PathByCookie.end()) {
            // assume already processed
            SBB_LOG_D("Timeout with inactive cookie: " << cookie);
            return;
        }
        SBB_LOG_I("Timeout"
            << ", path: " << it->second
        );
        Timeouts.emplace(it->second);
        MarkPathCompleted(it);
        if (RequireMajority) {
            DescriptionsByCookie.erase(cookie);
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const ui64 cookie = GetOriginalCookie(ev->Cookie);
        auto it = PathByCookie.find(cookie);
        if (it == PathByCookie.end()) {
            SBB_LOG_N("Undelivered"
                << ", unexpected cookie: " << cookie
            );
            return;
        }
        SBB_LOG_I("Undelivered"
            << ", path: " << it->second
        );
        Undelivered.emplace(it->second);
        MarkPathCompleted(it);
    }

    void SendProgressUpdate() {
        SBB_LOG_D("SendProgressUpdate"
            << ", paths in progress: " << PathByCookie.size()
            << ", processed paths: " << ProcessedPaths
            << ", total paths: " << TotalPaths
            << ", pending paths: " << PendingPaths.size()
            << ", timeouts: " << Timeouts.size()
            << ", delivery problems: " << Undelivered.size()
            << ", empty replica list: " << EmptyReplicaList.size()
        );
        Send(Parent, new TSchemeBoardMonEvents::TEvBackupProgress(TotalPaths, ProcessedPaths));
    }

    TMaybe<TString> BuildWarning() const {
        TStringBuilder warning;
        if (!Timeouts.empty()) {
            warning << "Timeout:\n  " << JoinSeq("\n  ", Timeouts) << "\n";
        }
        if (!Undelivered.empty()) {
            warning << "Undelivered:\n  " << JoinSeq("\n  ", Undelivered) << "\n";
        }
        if (!EmptyReplicaList.empty()) {
            warning << "Empty replica list:\n  " << JoinSeq("\n  ", EmptyReplicaList) << '\n';
        }
        if (warning.empty()) {
            return Nothing();
        }
        return warning;
    }

    void ReplyError(const TString& error) {
        Send(Parent, new TSchemeBoardMonEvents::TEvBackupResult(error, BuildWarning()));
        PassAway();
    }

    void ReplySuccess() {
        Send(Parent, new TSchemeBoardMonEvents::TEvBackupResult(Nothing(), BuildWarning()));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvResolveReplicasList, Handle);
            hFunc(TSchemeBoardMonEvents::TEvDescribeResponse, Handle);

            hFunc(TEvents::TEvWakeup, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    static constexpr TDuration DefaultTimeout = TDuration::Seconds(10);

    const TString FilePath;
    const ui32 InFlightLimit;
    const bool RequireMajority;
    const TActorId Parent;
    TQueue<TString> PendingPaths;
    THashMap<ui64, TString> PathByCookie;
    THashMap<ui64, TPathDescriptionAggregate> DescriptionsByCookie;
    TSet<TString> Timeouts;
    TSet<TString> Undelivered;
    TSet<TString> EmptyReplicaList;
    TMaybe<TFileOutput> OutputFile;
    ui32 ProcessedPaths = 0;
    ui32 TotalPaths = 0;
    ui64 NextCookie = 0;
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
            const auto status = google::protobuf::json::JsonStringToMessage(line, &description.Record);
            if (!status.ok()) {
                return ReplyError(TStringBuilder() << "Failed to parse JSON"
                    << ", line: " << TStringBuf(line, 0, 100)
                    << ", status: " << status.ToString());
            }
            const TPathId pathId(description.Record.GetPathOwnerId(), description.Record.GetPathId());
            if (pathId.OwnerId != SchemeShardId) {
                continue;
            }

            TotalPaths++;
            descriptions.emplace_back(pathId, std::move(description));
            if (TotalPaths % 1'000 == 0) {
                SBB_LOG_D("Reading file, parsed paths descriptions: " << TotalPaths);
                SendProgressUpdate();
            }
        }

        if (descriptions.empty()) {
            return ReplyError("No descriptions, nothing to restore");
        }
        Populate(std::move(descriptions));
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
            return;
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

IActor* CreateSchemeBoardBackuper(const TString& filePath, ui32 inFlightLimit, bool requireMajority, const TActorId& parent) {
    return new TBackupActor(filePath, inFlightLimit, requireMajority, parent);
}

IActor* CreateSchemeBoardRestorer(const TString& filePath, ui64 schemeShardId, ui64 generation, const TActorId& parent) {
    return new TRestoreActor(filePath, schemeShardId, generation, parent);
}

}
