#include "garbage_collector.h"

#include "cfg.h"
#include "log.h"
#include "events.h"
#include "executor.h"
#include "schema.h"

#include <ydb/core/base/path.h>
#include <ydb/core/mon/mon.h>

#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/string/join.h>

#include <algorithm>
#include <queue>
#include <vector>

namespace NKikimr::NSQS {

using TQueueRecord = TSqsEvents::TEvQueuesList::TQueueRecord;

using TSchemePath = TSqsEvents::TSchemePath;
using TSchemeNode = TSqsEvents::TSchemeNode;
using TGarbageHint = TSqsEvents::TEvGarbageSearchResult::TGarbageHint;
using TSchemeCacheNavigate = TSqsEvents::TSchemeCacheNavigate;
using TCleaningResult = TSqsEvents::TEvGarbageCleaningResult::TCleaningResult;

static const TString GARBAGE_CLEANER_LABEL = "GarbageCleaner";
static const TString MINIMUM_ITEM_AGE_SECONDS = "minimum_item_age_seconds";
static const TString PATH_TO_CLEAN = "path_to_clean";

static void PrintAllPaths(const TSchemeNode& node, TStringStream& ss) {
    if (node.Path.size()) {
        ss << CanonizePath(node.Path) << ", kind: " << static_cast<size_t>(node.Kind) << ", ts: " << node.CreationTs << "\n";
    }

    for (const auto& [name, child] : node.Children) {
        PrintAllPaths(child, ss);
    }
}

static TSchemeNode& FindOrAllocateSchemeNode(TSchemeNode* root, const TSchemePath& path) {
    TSchemeNode* currentNode = root;

    for (const auto& part : path) {
        currentNode = &currentNode->Children[part];
    }

    return *currentNode;
}

class TSchemeTraversalActor : public TActorBootstrapped<TSchemeTraversalActor> {
public:
    TSchemeTraversalActor(const TActorId parentId,
                          const TActorId schemeCacheId,
                          const TVector<TSchemePath>& pathsToTraverse,
                          const ui64 maxDepth = Max())
        : ParentId(parentId)
        , SchemeCacheId(schemeCacheId)
        , PathsToTraverse(pathsToTraverse)
        , MaxDepth(maxDepth)
        , CurrentDepth(0)
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_GARBAGE_COLLECTOR_ACTOR;
    }

    void Bootstrap() {
        Become(&TThis::Working);

        RootHolder = MakeHolder<TSchemeNode>();

        SendListChildrenRequest(PathsToTraverse);
    }

private:
    void SendListChildrenRequest(const TVector<TSchemePath>& paths) {
        const size_t elementsCount = paths.size();

        Y_ABORT_UNLESS(elementsCount);

        auto schemeCacheRequest = MakeHolder<TSchemeCacheNavigate>();

        schemeCacheRequest->ResultSet.resize(elementsCount);

        for (size_t i = 0; i < elementsCount; ++i) {
            schemeCacheRequest->ResultSet[i].Path = paths[i];

            schemeCacheRequest->ResultSet[i].Operation =
                ((CurrentDepth == MaxDepth) ? TSchemeCacheNavigate::OpPath : TSchemeCacheNavigate::OpList);
        }

        Send(SchemeCacheId, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.Release()));
    }

    STATEFN(Working) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleSchemeCacheResponse);
        }
    }

    void OnFinishedTraversal(bool success) {
        auto ev = MakeHolder<TSqsEvents::TEvSchemeTraversalResult>(success);

        if (success) {
            ev->RootHolder = std::move(RootHolder);
        }

        Send(ParentId, ev.Release());

        PassAway();
    }

    void HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();

        if (navigate->ErrorCount) {
            OnFinishedTraversal(false);
            return;
        }

        TVector<TSchemePath> newPathsToTraverse;

        ++CurrentDepth;

        for (const auto& entry : navigate->ResultSet) {
            auto& node = FindOrAllocateSchemeNode(&*RootHolder, entry.Path);

            node.Path = entry.Path;
            node.Kind = entry.Kind;
            node.CreationTs = TInstant::MilliSeconds(entry.CreateStep);

            if (entry.ListNodeEntry && CurrentDepth <= MaxDepth) {
                for (const auto& child : entry.ListNodeEntry->Children) {
                    auto yetAnotherPathToTraverse = entry.Path;
                    yetAnotherPathToTraverse.push_back(child.Name);

                    newPathsToTraverse.push_back(std::move(yetAnotherPathToTraverse));
                }
            }
        }

        if (newPathsToTraverse.empty()) {
            OnFinishedTraversal(true);
        } else {
            SendListChildrenRequest(newPathsToTraverse);
        }
    }

private:
    const TActorId ParentId;
    const TActorId SchemeCacheId;
    const TVector<TSchemePath> PathsToTraverse;

    const ui64 MaxDepth;
    ui64 CurrentDepth;

    THolder<TSchemeNode> RootHolder;
};

class TGarbageSearcher : public TActorBootstrapped<TGarbageSearcher> {
public:
    TGarbageSearcher(const TActorId parentId,
                     const TActorId schemeCacheId,
                     const TActorId queuesListReaderId,
                     const ui64 minimumItemAgeSeconds)
        : ParentId(parentId)
        , SchemeCacheId(schemeCacheId)
        , QueuesListReaderId(queuesListReaderId)
        , MinimumItemAge(TDuration::Seconds(minimumItemAgeSeconds))
    {
        Y_ABORT_UNLESS(parentId);
        Y_ABORT_UNLESS(schemeCacheId);
        Y_ABORT_UNLESS(queuesListReaderId);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_GARBAGE_COLLECTOR_ACTOR;
    }

    void Bootstrap() {
        Become(&TThis::ListingAccounts);

        RootPath = SplitPath(Cfg().GetRoot());

        // list accounts and system tables only
        Register(new TSchemeTraversalActor(SelfId(), SchemeCacheId, {RootPath}, 1));

        Send(QueuesListReaderId, new TSqsEvents::TEvReadQueuesList());

        RequiredResponses = 2;
    }

private:
    void ProcessQueuesListingResult(TSqsEvents::TEvQueuesList::TPtr& ev) {
        if (ev->Get()->Success) {
            SortedQueues = std::move(ev->Get()->SortedQueues);

            CompleteListing();
        } else {
            ReplyToParentAndDie(false);
        }
    }

    void ProcessAccountsListingResult(TSqsEvents::TEvSchemeTraversalResult::TPtr& ev) {
        if (ev->Get()->Success) {
            auto& node = FindOrAllocateSchemeNode(&*ev->Get()->RootHolder, RootPath);

            Y_ABORT_UNLESS(node.Kind = TSchemeCacheNavigate::EKind::KindPath);

            for (const auto& [name, child] : node.Children) {
                if (child.Kind == TSchemeCacheNavigate::EKind::KindPath && !name.StartsWith(".")) {
                    AccountsToProcess.push(name);
                }
            }

            CompleteListing();
        } else {
            ReplyToParentAndDie(false);
        }
    }

    void CompleteListing() {
        Y_ABORT_UNLESS(RequiredResponses);

        if (!--RequiredResponses) {
            Become(&TThis::ProcessingAccounts);

            ListNextAccountOrReply();

            return;
        }
    }

    void ListNextAccountOrReply() {
        if (AccountsToProcess.size()) {
            CurrentAccount = AccountsToProcess.front();
            AccountsToProcess.pop();

            CurrentAccountPath = RootPath;
            CurrentAccountPath.push_back(CurrentAccount);

            // depth == 2 since the path pattern is account/queue/queue_version
            Register(new TSchemeTraversalActor(SelfId(), SchemeCacheId, {CurrentAccountPath}, 2));
        } else {
            ReplyToParentAndDie(true);
        }
    }

    STATEFN(ListingAccounts) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSqsEvents::TEvSchemeTraversalResult, ProcessAccountsListingResult);
            hFunc(TSqsEvents::TEvQueuesList, ProcessQueuesListingResult);
        }
    }

    // per-account processing section

    STATEFN(ProcessingAccounts) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSqsEvents::TEvSchemeTraversalResult, ProcessSingleAccount);
        }
    }

    void ProcessSingleAccount(TSqsEvents::TEvSchemeTraversalResult::TPtr& ev) {
        if (ev->Get()->Success) {
            auto& node = FindOrAllocateSchemeNode(&*ev->Get()->RootHolder, CurrentAccountPath);

            // an account is always a directory
            Y_ABORT_UNLESS(node.Kind = TSchemeCacheNavigate::EKind::KindPath);

            for (auto& [queueName, queueNode] : node.Children) {
                // ignore tables (e. g. local "Queues" table)
                if (queueNode.Kind == TSchemeCacheNavigate::EKind::KindPath) {
                    ProcessSingleQueue(queueName, queueNode);
                }
            }
        } else {
            // Skip account on an error. Possible reason: some elements were removed during a listing
        }

        ListNextAccountOrReply();
    }

    void ProcessSingleQueue(const TString& queueName, TSchemeNode& node) {
        TQueueRecord fakeRecord;
        fakeRecord.UserName = CurrentAccount;
        fakeRecord.QueueName = queueName;

        auto it = std::lower_bound(SortedQueues.begin(), SortedQueues.end(), fakeRecord);
        if (it != SortedQueues.end() && it->UserName == CurrentAccount && it->QueueName == queueName) {
            const bool isLegacyQueue = it->Version == 0;

            if (isLegacyQueue) {
                // TODO: check State field and report
            } else {
                const TString versionStr = "v" + ToString(it->Version);
                for (auto& [name, child] : node.Children) {
                    // Do not ever touch fresh queues or actual versions
                    const TDuration itemAge = TActivationContext::AsActorContext().Now() - node.CreationTs;
                    if (name != versionStr && itemAge >= MinimumItemAge) {
                        AddGarbageHint(CurrentAccount, std::move(child), TGarbageHint::EReason::UnusedVersion);
                    }
                }
            }
        } else {
            AddGarbageHint(CurrentAccount, std::move(node), TGarbageHint::EReason::UnregisteredQueue);
        }
    }

    void AddGarbageHint(const TString& account, TSchemeNode&& node, const TGarbageHint::EReason reason) {
        // remove excessive data
        node.Children.clear();

        TGarbageHint hint{account, std::move(node), reason};

        const auto path = CanonizePath(hint.SchemeNode.Path);

        GarbageHints[path] = std::move(hint);
    }

    void ReplyToParentAndDie(bool success) {
        auto ev = MakeHolder<TSqsEvents::TEvGarbageSearchResult>(success);

        if (success) {
            ev->GarbageHints = std::move(GarbageHints);
        }

        Send(ParentId, ev.Release());

        PassAway();
    }

private:
    const TActorId ParentId;
    const TActorId SchemeCacheId;
    const TActorId QueuesListReaderId;
    const TDuration MinimumItemAge;

    TSchemePath RootPath;

    std::queue<TString> AccountsToProcess;
    std::vector<TQueueRecord> SortedQueues;

    ui64 RequiredResponses = 0;

    TString CurrentAccount;
    TSchemePath CurrentAccountPath;

    THashMap<TString, TGarbageHint> GarbageHints;
};

class TGarbageCleaner : public TActorBootstrapped<TGarbageCleaner> {
public:
    TGarbageCleaner(const TActorId parentId,
                    const TActorId schemeCacheId,
                    TGarbageHint&& garbageHint)
        : ParentId(parentId)
        , SchemeCacheId(schemeCacheId)
        , GarbageHint(std::move(garbageHint))
    {
        Y_ABORT_UNLESS(parentId);
        Y_ABORT_UNLESS(schemeCacheId);
        Y_ABORT_UNLESS(GarbageHint.Reason != TGarbageHint::EReason::Unknown);
        Y_ABORT_UNLESS(GarbageHint.SchemeNode.Path.size());
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_GARBAGE_COLLECTOR_ACTOR;
    }

    void Bootstrap() {
        Become(&TThis::Working);

        StartedAt = TActivationContext::AsActorContext().Now();

        Register(new TSchemeTraversalActor(SelfId(), SchemeCacheId, {GarbageHint.SchemeNode.Path}));
    }

private:
    STATEFN(Working) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSqsEvents::TEvSchemeTraversalResult, ProcessGarbageListingResult);
            hFunc(TSqsEvents::TEvExecuted, OnNodeRemoved);
        }
    }

    void DFS(const TSchemeNode& node) {
        for (const auto& [name, child] : node.Children) {
            DFS(child);
        }

        NodesToRemove.push(node);
    }

    void ProcessGarbageListingResult(TSqsEvents::TEvSchemeTraversalResult::TPtr& ev) {
        if (ev->Get()->Success) {
            auto& node = FindOrAllocateSchemeNode(&*ev->Get()->RootHolder, GarbageHint.SchemeNode.Path);

            Y_ABORT_UNLESS(node.Kind = TSchemeCacheNavigate::EKind::KindPath);

            DFS(node);

            RemoveNextNodeOrDie();
        } else {
            LOG_SQS_INFO(GARBAGE_CLEANER_LABEL << " failed to list nodes at " << CanonizePath(GarbageHint.SchemeNode.Path));

            ReportToParentAndDie(false);
        }
    }

    void RemoveNextNodeOrDie() {
        if (!NodesToRemove.empty()) {
            CurrentNode = std::move(NodesToRemove.front());
            NodesToRemove.pop();

            RemoveCurrentNode();
        } else {
            LOG_SQS_INFO(GARBAGE_CLEANER_LABEL << " finished cleaning at " << CanonizePath(GarbageHint.SchemeNode.Path));

            ReportToParentAndDie(true);
        }
    }

    void ReportToParentAndDie(const bool success) {
        auto ev = MakeHolder<TSqsEvents::TEvGarbageCleaningResult>(success);

        ev->Record.Account = GarbageHint.Account;
        ev->Record.HintPath = CanonizePath(GarbageHint.SchemeNode.Path);
        ev->Record.RemovedNodes = std::move(RemovedNodes);
        ev->Record.StartedAt = StartedAt;
        ev->Record.Duration = TActivationContext::AsActorContext().Now() - StartedAt;

        Send(ParentId, ev.Release());

        PassAway();
    }

    TQueuePath MakeAndVerifyQueuePath(const TSchemePath& path) const {
        // A certain dir structure is assumed here: SqsRoot/Account/Queue/Version
        // All path components except "Version" are required
        // The code guarantees scheme modifications locality

        Y_ABORT_UNLESS(CanonizePath(path).StartsWith(CanonizePath(GarbageHint.SchemeNode.Path)));

        auto rootPath = SplitPath(Cfg().GetRoot());

        auto rootPathIt = rootPath.begin();
        auto pathIt = path.begin();

        while (rootPathIt != rootPath.end()) {
            Y_ABORT_UNLESS(*rootPathIt++ == *pathIt++);
        }

        TQueuePath result;
        result.Root = Cfg().GetRoot();

        Y_ABORT_UNLESS(pathIt != path.end());

        result.UserName = *pathIt++;

        Y_ABORT_UNLESS(result.UserName == GarbageHint.Account);

        Y_ABORT_UNLESS(pathIt != path.end());

        result.QueueName = *pathIt++;

        if (pathIt != path.end()) {
            result.VersionSuffix = *pathIt;
        }

        return result;
    }

    void RemoveCurrentNode() const {
        auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto* trans = ev->Record.MutableTransaction()->MutableModifyScheme();

        auto path = CurrentNode.Path;

        Y_ABORT_UNLESS(path.size() > 1);

        const auto name = path.back();

        path.pop_back();

        trans->SetWorkingDir(CanonizePath(path));

        trans->MutableDrop()->SetName(name);

        if (CurrentNode.Kind == TSchemeCacheNavigate::EKind::KindPath) {
            trans->SetOperationType(NKikimrSchemeOp::ESchemeOpRmDir);
        } else if (CurrentNode.Kind == TSchemeCacheNavigate::EKind::KindTable) {
            trans->SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
        } else {
            Y_ABORT_UNLESS("Unexpected node kind");
        }

        LOG_SQS_INFO(GARBAGE_CLEANER_LABEL << " attempts to remove the node " << CanonizePath(CurrentNode.Path));

        const TQueuePath queuePath = MakeAndVerifyQueuePath(CurrentNode.Path);

        Register(new TMiniKqlExecutionActor(SelfId(), GARBAGE_CLEANER_LABEL + "Tx", std::move(ev), false, queuePath, nullptr));
    }

    void OnNodeRemoved(TSqsEvents::TEvExecuted::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto status = record.GetStatus();

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            const auto path = CanonizePath(CurrentNode.Path);

            RemovedNodes.push_back(path);

            LOG_SQS_INFO(GARBAGE_CLEANER_LABEL << " removed the node " << path);

            RemoveNextNodeOrDie();
        } else {
            LOG_SQS_INFO(GARBAGE_CLEANER_LABEL << " failed to remove the node " << CanonizePath(CurrentNode.Path));

            ReportToParentAndDie(false);
        }
    }

private:
    const TActorId ParentId;
    const TActorId SchemeCacheId;
    const TGarbageHint GarbageHint;
    std::queue<TSchemeNode> NodesToRemove;
    TSchemeNode CurrentNode;
    TVector<TString> RemovedNodes;
    TInstant StartedAt;
};

class TGarbageCollector : public TActorBootstrapped<TGarbageCollector> {
public:
    TGarbageCollector(const TActorId schemeCacheId, const TActorId queuesListReaderId)
        : SchemeCacheId(schemeCacheId)
        , QueuesListReaderId(queuesListReaderId)
    {
        Y_ABORT_UNLESS(schemeCacheId);
        Y_ABORT_UNLESS(queuesListReaderId);
        Y_UNUSED(PrintAllPaths);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_GARBAGE_COLLECTOR_ACTOR;
    }

    void Bootstrap() {
        Become(&TThis::Working);

        RegisterMonitoringPage();
    }

    void RegisterMonitoringPage() const {
        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage * page = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(page, "sqsgc", "SQS Garbage Collector", false,
                                   TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
        }
    }

    void ProcessGarbageHints(TSqsEvents::TEvGarbageSearchResult::TPtr& ev) {
        Scanning = false;

        if (ev->Get()->Success) {
            CurrentGarbageHints = std::move(ev->Get()->GarbageHints);
        }
    }

    void InitRescanAndReport(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;

        HTML(str) {
            if (Scanning) {
                str << "Already scanning. Patience is a virtue, dear user";
            } else {
                const TCgiParameters& params = ev->Get()->Request.GetParams();
                if (params.Has(MINIMUM_ITEM_AGE_SECONDS)) {
                    const TString& minimumItemAgeSecondsParam = params.Get(MINIMUM_ITEM_AGE_SECONDS);

                    ui64 minimumItemAgeSeconds = 0;
                    if (TryFromString<ui64>(minimumItemAgeSecondsParam, minimumItemAgeSeconds)) {
                        Scanning = true;

                        Register(new TGarbageSearcher(SelfId(),
                                                      SchemeCacheId,
                                                      QueuesListReaderId,
                                                      minimumItemAgeSeconds));

                        str << "Started new scan process";
                    } else {
                        str << "Can't parse minimum item age param value: " << minimumItemAgeSecondsParam;
                    }
                } else {
                    str << "Minimum item age param is missing";
                }
            }
            str << "<br></br>" << "<a href=\"/actors/sqsgc\">Back</a>";
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    void StartCleaner(TGarbageHint&& garbageHint) {
        Register(new TGarbageCleaner(SelfId(), SchemeCacheId, std::move(garbageHint)));
    }

    void InitCleaningAndReport(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;

        HTML(str) {
            const TCgiParameters& params = ev->Get()->Request.GetParams();

            if (params.Has(PATH_TO_CLEAN)) {
                const TString& pathToClean = params.Get(PATH_TO_CLEAN);
                auto it = CurrentGarbageHints.find(pathToClean);
                if (it == CurrentGarbageHints.end()) {
                    str << "Specified path doesn't match any garbage hint: " << params.Get(PATH_TO_CLEAN);
                } else {
                    StartCleaner(std::move(it->second));

                    CurrentGarbageHints.erase(it);

                    str << "Started cleaning process for " << params.Get(PATH_TO_CLEAN);
                }
            } else {
                str << "Path to clean is missing";
            }
            str << "<br></br>" << "<a href=\"/actors/sqsgc\">Back</a>";
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    void InitClearHistoryAndReport(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;

        CleaningHistory.clear();

        HTML(str) {
            str << "Cleaning history was reset";
            str << "<br></br>" << "<a href=\"/actors/sqsgc\">Back</a>";
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    static void AddCleanGarbageLink(TStringStream& str, const TString& pathToClean) {
        TCgiParameters params;
        params.InsertUnescaped(PATH_TO_CLEAN, pathToClean);

        str << "<a href=\"/actors/sqsgc/clean?" << params() << "\">" << "Clean" << "</a>";
    }

    static void AddRescanForm(TStringStream& str) {
        str << "<form method=\"GET\" action=\"/actors/sqsgc/rescan\" id=\"tblMonSQSGCfrm\" name=\"tblMonSQSGCfrm\">" << Endl;
        str << "<legend> Rescan structure and filter by item age: </legend>";
        str << "<label> " << "Minimum item age (seconds): " << " <input type=\"number\" id=\"" << "lblRescan";
        str << "\" name=\"" << MINIMUM_ITEM_AGE_SECONDS << "\"" << " value=\"" << Cfg().GetMinimumGarbageAgeSeconds() << "\"";
        str << " min=\"0\" /> </label>";
        str << "<input class=\"btn btn-default\" type=\"submit\" value=\"Rescan\"/>" << Endl;
        str << "</form>" << Endl;
    }

    void RenderGarbageHints(TStringStream& str) const {
        if (CurrentGarbageHints.empty()) {
            return;
        }

        IOutputStream& __stream(str);

        TAG(TH4) {
            str << "Garbage hints";
        }
        TABLE_SORTABLE_CLASS("hints-table") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { str << "Path"; }
                    TABLEH() { str << "Created at (UTC)"; }
                    TABLEH() { str << "Reason"; }
                    TABLEH() { str << "Action"; }
                }
            }
            TABLEBODY() {
                for (const auto& [pathToClean, hint] : CurrentGarbageHints) {
                    TABLER() {
                        TABLED() { str << pathToClean; }
                        TABLED() { str << hint.SchemeNode.CreationTs.ToRfc822String(); }
                        TABLED() { str << hint.Reason; }
                        TABLED() { AddCleanGarbageLink(str, pathToClean); }
                    }
                }
            }
        }
    }

    void RenderCleaningResults(TStringStream& str) const {
        if (CleaningHistory.empty()) {
            return;
        }

        IOutputStream& __stream(str);

        TAG(TH4) {
            str << "Cleaning history";
        }
        TABLE_SORTABLE_CLASS("cr-table") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { str << "Account"; }
                    TABLEH() { str << "Hint path"; }
                    TABLEH() { str << "Started at (UTC)"; }
                    TABLEH() { str << "Duration (ms)"; }
                    TABLEH() { str << "Nodes removed"; }
                    TABLEH() { str << "Result"; }
                }
            }
            TABLEBODY() {
                for (const auto& result : CleaningHistory) {
                    TABLER() {
                        TABLED() { str << result.Account; }
                        TABLED() { str << result.HintPath; }
                        TABLED() { str << result.StartedAt.ToRfc822String(); }
                        TABLED() { str << result.Duration.MilliSeconds(); }
                        TABLED() { str << result.RemovedNodes.size(); }
                        TABLED() { str << (result.Success ? "OK" : "FAIL"); }
                    }
                }
            }
        }
        str << "<a href=\"/actors/sqsgc/clear_history\">" << "Reset local cleaning history" << "</a>";
    }

    void RenderMainControlPage(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;

        HTML(str) {
            AddRescanForm(str);
            RenderGarbageHints(str);
            RenderCleaningResults(str);
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    void HandleCleaningResult(TSqsEvents::TEvGarbageCleaningResult::TPtr& ev) {
        CleaningHistory.push_back(std::move(ev->Get()->Record));
    }

    void HandleHttpRequest(NMon::TEvHttpInfo::TPtr& ev) {
        const TStringBuf path = ev->Get()->Request.GetPath();

        if (path.EndsWith("/rescan")) {
            InitRescanAndReport(ev);
        } else if (path.EndsWith("/clean")) {
            InitCleaningAndReport(ev);
        } else if (path.EndsWith("/clear_history")) {
            InitClearHistoryAndReport(ev);
        } else {
            RenderMainControlPage(ev);
        }
    }

    STATEFN(Working) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSqsEvents::TEvGarbageSearchResult, ProcessGarbageHints);
            hFunc(TSqsEvents::TEvGarbageCleaningResult, HandleCleaningResult);
            hFunc(NMon::TEvHttpInfo, HandleHttpRequest);
        }
    }

private:
    const TActorId SchemeCacheId;
    const TActorId QueuesListReaderId;
    bool Scanning = false;
    THashMap<TString, TGarbageHint> CurrentGarbageHints;
    TVector<TCleaningResult> CleaningHistory;
};

IActor* CreateGarbageCollector(const TActorId schemeCacheId, const TActorId queuesListReaderId) {
    return new TGarbageCollector(schemeCacheId, queuesListReaderId);
}

}; // namespace NKikimr::NSQS
