#include "schemeshard_import_kesus_restorer.h"

#include "schemeshard.h"
#include "schemeshard_private.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/ydb_convert/kesus_description.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NSchemeShard {
namespace {

class TKesusResourcesRestorer: public TActorBootstrapped<TKesusResourcesRestorer> {
    enum class EPhase {
        DescribeNode,
        DescribeResources,
        AddResource,
        DescribeResource,
        Retry,
    };

    enum EEv {
        EvRequestTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvRetry,
    };

    struct TEvRequestTimeout: public TEventLocal<TEvRequestTimeout, EvRequestTimeout> {
        const ui64 RequestCookie;

        explicit TEvRequestTimeout(ui64 requestCookie)
            : RequestCookie(requestCookie)
        {}
    };

    struct TEvRetry: public TEventLocal<TEvRetry, EvRetry> {
        const ui64 RequestCookie;

        explicit TEvRetry(ui64 requestCookie)
            : RequestCookie(requestCookie)
        {}
    };

public:
    TKesusResourcesRestorer(
        TActorId schemeShard,
        ui64 importId,
        ui32 itemIdx,
        TPathId dstPathId,
        TTxId createTxId,
        Ydb::RateLimiter::Resources resources,
        ui32 retries)
        : SchemeShard(schemeShard)
        , ImportId(importId)
        , ItemIdx(itemIdx)
        , DstPathId(dstPathId)
        , CreateTxId(createTxId)
        , Resources(std::move(resources))
        , Retries(retries)
    {}

    void Bootstrap() {
        if (!DstPathId || CreateTxId == InvalidTxId) {
            return Finish(false, "Coordination node identity is missing");
        }

        // Compare the representation produced by Kesus, including the default
        // metric fields materialized by the public/internal converters.
        for (auto& resource : *Resources.mutable_resources()) {
            NKikimrKesus::TStreamingQuoterResource internal;
            FillRateLimiterDescription(internal, resource);
            resource.Clear();
            FillRateLimiterDescription(resource, internal);
        }
        auto& resources = *Resources.mutable_resources();
        std::sort(resources.begin(), resources.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.resource_path() < rhs.resource_path();
        });
        for (int i = 0; i < Resources.resources_size(); ++i) {
            const auto& path = Resources.resources(i).resource_path();
            if (!ResourcesByPath.emplace(path, i).second) {
                return Finish(false, TStringBuilder() << "Duplicate resource path: " << path);
            }
        }

        Become(&TThis::StateWork);
        StartAttempt();
    }

private:
    ui64 StartRequest(EPhase phase) {
        Phase = phase;
        ++RequestCookie;
        Schedule(TDuration::Minutes(1), new TEvRequestTimeout(RequestCookie));
        return RequestCookie;
    }

    void StartAttempt() {
        ExistingResources.clear();
        NextResource = 0;
        const ui64 cookie = StartRequest(EPhase::DescribeNode);
        Send(SchemeShard, new TEvSchemeShard::TEvDescribeScheme(DstPathId), 0, cookie);
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        if (Phase != EPhase::DescribeNode || ev->Cookie != RequestCookie || ev->Sender != SchemeShard) {
            return;
        }
        const auto& record = ev->Get()->GetRecord();
        if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
            return Finish(false, TStringBuilder() << "Cannot describe imported coordination node: " << record.GetReason());
        }

        const auto& description = record.GetPathDescription();
        const auto& self = description.GetSelf();
        if (!description.HasKesus()
            || self.GetPathType() != NKikimrSchemeOp::EPathTypeKesus
            || TPathId(self.GetSchemeshardId(), self.GetPathId()) != DstPathId
            || self.GetCreateTxId() != ui64(CreateTxId)
            || !self.GetCreateFinished()
            || self.GetPathState() == NKikimrSchemeOp::EPathStateDrop)
        {
            return Finish(false, "Imported coordination node was removed, replaced or is not ready");
        }
        const ui64 tabletId = description.GetKesus().GetKesusTabletId();
        if (!tabletId) {
            return Finish(false, "Imported coordination node tablet id is missing");
        }

        NTabletPipe::TClientConfig config;
        config.RetryPolicy = {.RetryLimitCount = 3};
        KesusPipe = Register(NTabletPipe::CreateClient(SelfId(), tabletId, config));

        auto request = MakeHolder<NKesus::TEvKesus::TEvDescribeQuoterResources>();
        request->Record.SetRecursive(true);
        const ui64 cookie = StartRequest(EPhase::DescribeResources);
        NTabletPipe::SendData(SelfId(), KesusPipe, request.Release(), cookie);
    }

    bool IsExpectedResource(const NKikimrKesus::TStreamingQuoterResource& resource, int index) const {
        Ydb::RateLimiter::Resource actual;
        FillRateLimiterDescription(actual, resource);
        return google::protobuf::util::MessageDifferencer::Equals(Resources.resources(index), actual);
    }

    void Handle(NKesus::TEvKesus::TEvDescribeQuoterResourcesResult::TPtr& ev) {
        if (ev->Cookie != RequestCookie
            || (Phase != EPhase::DescribeResources && Phase != EPhase::DescribeResource))
        {
            return;
        }
        const auto& record = ev->Get()->Record;
        if (record.GetError().GetStatus() != Ydb::StatusIds::SUCCESS) {
            return Finish(false, TStringBuilder() << "Cannot describe imported coordination node resources: "
                << record.GetError().ShortDebugString());
        }

        if (Phase == EPhase::DescribeResource) {
            // ALREADY_EXISTS only compares DRR inside Kesus. Metering must also
            // match before a previous Add can be considered successfully replayed.
            if (record.ResourcesSize() != 1 || !IsExpectedResource(record.GetResources(0), NextResource)) {
                return ResourceConflict(Resources.resources(NextResource).resource_path());
            }
            ExistingResources.insert(Resources.resources(NextResource).resource_path());
            ++NextResource;
        } else {
            for (const auto& resource : record.GetResources()) {
                const auto& path = resource.GetResourcePath();
                const auto it = ResourcesByPath.find(path);
                if (it == ResourcesByPath.end()) {
                    return Finish(false, TStringBuilder() << "Unexpected resource in imported coordination node: " << path);
                }
                if (!IsExpectedResource(resource, it->second)) {
                    return ResourceConflict(path);
                }
                if (!ExistingResources.insert(path).second) {
                    return Finish(false, TStringBuilder() << "Duplicate resource in coordination node description: " << path);
                }
            }
        }
        AddNextResource();
    }

    void AddNextResource() {
        while (NextResource < Resources.resources_size()
            && ExistingResources.contains(Resources.resources(NextResource).resource_path()))
        {
            ++NextResource;
        }
        if (NextResource == Resources.resources_size()) {
            return Finish(true, {});
        }

        auto request = MakeHolder<NKesus::TEvKesus::TEvAddQuoterResource>();
        FillRateLimiterDescription(*request->Record.MutableResource(), Resources.resources(NextResource));
        const ui64 cookie = StartRequest(EPhase::AddResource);
        NTabletPipe::SendData(SelfId(), KesusPipe, request.Release(), cookie);
    }

    void Handle(NKesus::TEvKesus::TEvAddQuoterResourceResult::TPtr& ev) {
        if (Phase != EPhase::AddResource || ev->Cookie != RequestCookie) {
            return;
        }
        const auto& record = ev->Get()->Record;
        const auto& path = Resources.resources(NextResource).resource_path();
        switch (record.GetError().GetStatus()) {
        case Ydb::StatusIds::SUCCESS:
            ExistingResources.insert(path);
            ++NextResource;
            return AddNextResource();

        case Ydb::StatusIds::ALREADY_EXISTS: {
            auto request = MakeHolder<NKesus::TEvKesus::TEvDescribeQuoterResources>();
            request->Record.AddResourcePaths(path);
            request->Record.SetRecursive(false);
            const ui64 cookie = StartRequest(EPhase::DescribeResource);
            NTabletPipe::SendData(SelfId(), KesusPipe, request.Release(), cookie);
            return;
        }

        default:
            return Finish(false, TStringBuilder() << "Cannot restore resource '" << path << "': "
                << record.GetError().ShortDebugString());
        }
    }

    void ResourceConflict(const TString& path) {
        Finish(false, TStringBuilder() << "Resource '" << path << "' already exists with different settings");
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->ClientId == KesusPipe && ev->Get()->Status != NKikimrProto::OK) {
            Retry("Failed to connect to imported coordination node");
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->ClientId == KesusPipe) {
            Retry("Connection to imported coordination node was lost");
        }
    }

    void Handle(TEvRequestTimeout::TPtr& ev) {
        if (Phase != EPhase::Retry && ev->Get()->RequestCookie == RequestCookie) {
            Retry("Timed out restoring coordination node resources");
        }
    }

    void Retry(const TString& error) {
        ClosePipe();
        if (Attempt >= Retries) {
            return Finish(false, error);
        }
        ++Attempt;
        // Invalidate the in-flight request before waiting. Its Add may still
        // commit, so the next attempt must describe resources instead of blindly
        // continuing at the following index.
        ++RequestCookie;
        Phase = EPhase::Retry;
        Schedule(TDuration::Seconds(std::min<ui32>(Attempt, 10)), new TEvRetry(RequestCookie));
    }

    void Handle(TEvRetry::TPtr& ev) {
        if (Phase == EPhase::Retry && ev->Get()->RequestCookie == RequestCookie) {
            StartAttempt();
        }
    }

    void ClosePipe() {
        if (KesusPipe) {
            NTabletPipe::CloseClient(SelfId(), KesusPipe);
            KesusPipe = {};
        }
    }

    void PassAway() override {
        ClosePipe();
        TActorBootstrapped::PassAway();
    }

    void Finish(bool success, const TString& error) {
        Send(SchemeShard, new TEvPrivate::TEvImportKesusResourcesResult(ImportId, ItemIdx, success, error));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(NKesus::TEvKesus::TEvDescribeQuoterResourcesResult, Handle);
            hFunc(NKesus::TEvKesus::TEvAddQuoterResourceResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvRequestTimeout, Handle);
            hFunc(TEvRetry, Handle);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    const TActorId SchemeShard;
    const ui64 ImportId;
    const ui32 ItemIdx;
    const TPathId DstPathId;
    const TTxId CreateTxId;
    Ydb::RateLimiter::Resources Resources;
    const ui32 Retries;

    THashMap<TString, int> ResourcesByPath;
    THashSet<TString> ExistingResources;
    int NextResource = 0;
    TActorId KesusPipe;
    ui32 Attempt = 0;
    ui64 RequestCookie = 0;
    EPhase Phase = EPhase::DescribeNode;
};

} // anonymous namespace

IActor* CreateKesusResourcesRestorer(
    TActorId schemeShard,
    ui64 importId,
    ui32 itemIdx,
    TPathId dstPathId,
    TTxId createTxId,
    Ydb::RateLimiter::Resources resources,
    ui32 retries)
{
    return new TKesusResourcesRestorer(schemeShard, importId, itemIdx, dstPathId, createTxId, std::move(resources), retries);
}

} // namespace NKikimr::NSchemeShard
