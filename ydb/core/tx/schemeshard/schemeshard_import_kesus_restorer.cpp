#include "schemeshard_import_kesus_restorer.h"

#include "schemeshard.h"
#include "schemeshard_private.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/ydb_convert/kesus_description.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <google/protobuf/util/field_comparator.h>
#include <google/protobuf/util/message_differencer.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NSchemeShard {
namespace {

class TKesusRestorer: public TActorBootstrapped<TKesusRestorer> {
    enum class EPhase {
        DescribeNode,
        DescribeResources,
        AddResource,
        DescribeResource,
        DescribeSemaphores,
        CreateSemaphore,
        DescribeSemaphore,
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
    TKesusRestorer(
        TActorId schemeShard,
        ui64 importId,
        ui32 itemIdx,
        TPathId dstPathId,
        NKikimrSchemeOp::TImportKesus contents,
        ui32 retries)
        : SchemeShard(schemeShard)
        , ImportId(importId)
        , ItemIdx(itemIdx)
        , DstPathId(dstPathId)
        , Contents(std::move(contents))
        , Retries(retries)
    {}

    void Bootstrap() {
        if (!DstPathId) {
            return Finish(false, "Coordination node identity is missing");
        }

        // Compare the representation produced by Kesus, including the default
        // metric fields materialized by the public/internal converters.
        for (auto& resource : *Contents.mutable_resources()) {
            NKikimrKesus::TStreamingQuoterResource internal;
            FillRateLimiterDescription(internal, resource);
            resource.Clear();
            FillRateLimiterDescription(resource, internal);
        }
        for (int i = 0; i < Contents.resources_size(); ++i) {
            const auto& path = Contents.resources(i).resource_path();
            if (!ResourcesByPath.emplace(path, i).second) {
                return Finish(false, TStringBuilder() << "Duplicate resource path: " << path);
            }
        }

        for (int i = 0; i < Contents.semaphores_size(); ++i) {
            if (!SemaphoresByName.emplace(Contents.semaphores(i).name(), i).second) {
                return Finish(false, "Duplicate semaphore name in imported coordination node");
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
        ExistingSemaphores.clear();
        NextSemaphore = 0;
        AddError.clear();
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
            || !self.GetCreateFinished()
            || self.GetPathState() == NKikimrSchemeOp::EPathStateDrop)
        {
            return Finish(false, "Imported coordination node was removed, replaced or is not ready");
        }
        const ui64 tabletId = description.GetKesus().GetKesusTabletId();
        if (!tabletId) {
            return Finish(false, "Imported coordination node tablet id is missing");
        }

        KesusPath = record.GetPath();

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
        google::protobuf::util::DefaultFieldComparator comparator;
        comparator.set_treat_nan_as_equal(true);
        google::protobuf::util::MessageDifferencer differencer;
        differencer.set_field_comparator(&comparator);
        return differencer.Compare(Contents.resources(index), actual);
    }

    void Handle(NKesus::TEvKesus::TEvDescribeQuoterResourcesResult::TPtr& ev) {
        if (ev->Cookie != RequestCookie
            || (Phase != EPhase::DescribeResources && Phase != EPhase::DescribeResource))
        {
            return;
        }
        const auto& record = ev->Get()->Record;
        if (record.GetError().GetStatus() != Ydb::StatusIds::SUCCESS) {
            return Finish(false, AddError.empty()
                ? TString(TStringBuilder() << "Cannot describe imported coordination node resources: "
                    << record.GetError().ShortDebugString()) : AddError);
        }

        if (Phase == EPhase::DescribeResource) {
            // ALREADY_EXISTS only compares DRR. A repeated Add with NaN may
            // even return BAD_REQUEST; compare the full persisted definition.
            if (record.ResourcesSize() == 0 && !AddError.empty()) {
                return Finish(false, AddError);
            }
            if (record.ResourcesSize() != 1 || !IsExpectedResource(record.GetResources(0), NextResource)) {
                return ResourceConflict(Contents.resources(NextResource).resource_path());
            }
            AddError.clear();
            ExistingResources.insert(Contents.resources(NextResource).resource_path());
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
            const ui64 cookie = StartRequest(EPhase::DescribeSemaphores);
            NTabletPipe::SendData(SelfId(), KesusPipe, new NKesus::TEvKesus::TEvDescribeSemaphores(), cookie);
            return;
        }
        AddNextResource();
    }

    bool IsExpectedSemaphore(int index, const TString& name, ui64 limit, const TString& data, bool ephemeral) const {
        const auto& expected = Contents.semaphores(index);
        return !ephemeral && expected.name() == name && expected.limit() == limit && expected.data() == data;
    }

    void SemaphoreConflict(const TString& name) {
        Finish(false, TStringBuilder() << "Semaphore '" << name << "' already exists with different settings");
    }

    void Handle(NKesus::TEvKesus::TEvDescribeSemaphoresResult::TPtr& ev) {
        if (Phase != EPhase::DescribeSemaphores || ev->Cookie != RequestCookie) {
            return;
        }
        const auto& record = ev->Get()->Record;
        if (record.GetError().GetStatus() != Ydb::StatusIds::SUCCESS) {
            return Finish(false, TStringBuilder() << "Cannot describe imported coordination node semaphores: "
                << record.GetError().ShortDebugString());
        }
        for (const auto& semaphore : record.GetSemaphores()) {
            const auto& name = semaphore.GetName();
            const auto it = SemaphoresByName.find(name);
            if (it == SemaphoresByName.end()) {
                return Finish(false, TStringBuilder() << "Unexpected semaphore in imported coordination node: " << name);
            }
            if (!IsExpectedSemaphore(it->second, name, semaphore.GetLimit(), semaphore.GetData(), semaphore.GetEphemeral())) {
                return SemaphoreConflict(name);
            }
            if (!ExistingSemaphores.insert(name).second) {
                return Finish(false, "Duplicate semaphore in coordination node description");
            }
        }
        AddNextResource();
    }

    void CreateNextSemaphore() {
        while (NextSemaphore < Contents.semaphores_size()
            && ExistingSemaphores.contains(Contents.semaphores(NextSemaphore).name()))
        {
            ++NextSemaphore;
        }
        if (NextSemaphore == Contents.semaphores_size()) {
            return Finish(true, {});
        }
        const auto& semaphore = Contents.semaphores(NextSemaphore);
        const ui64 cookie = StartRequest(EPhase::CreateSemaphore);
        NTabletPipe::SendData(SelfId(), KesusPipe,
            new NKesus::TEvKesus::TEvCreateSemaphore(KesusPath, semaphore.name(), semaphore.limit(), semaphore.data()), cookie);
    }

    void Handle(NKesus::TEvKesus::TEvCreateSemaphoreResult::TPtr& ev) {
        if (Phase != EPhase::CreateSemaphore || ev->Cookie != RequestCookie) {
            return;
        }
        const auto& record = ev->Get()->Record;
        const auto& name = Contents.semaphores(NextSemaphore).name();
        switch (record.GetError().GetStatus()) {
        case Ydb::StatusIds::SUCCESS:
            ExistingSemaphores.insert(name);
            ++NextSemaphore;
            return CreateNextSemaphore();
        case Ydb::StatusIds::ALREADY_EXISTS: {
            auto request = MakeHolder<NKesus::TEvKesus::TEvDescribeSemaphore>();
            request->Record.SetKesusPath(KesusPath);
            request->Record.SetName(name);
            const ui64 cookie = StartRequest(EPhase::DescribeSemaphore);
            NTabletPipe::SendData(SelfId(), KesusPipe, request.Release(), cookie);
            return;
        }
        default:
            return Finish(false, TStringBuilder() << "Cannot restore semaphore '" << name << "': "
                << record.GetError().ShortDebugString());
        }
    }

    void Handle(NKesus::TEvKesus::TEvDescribeSemaphoreResult::TPtr& ev) {
        if (Phase != EPhase::DescribeSemaphore || ev->Cookie != RequestCookie) {
            return;
        }
        const auto& record = ev->Get()->Record;
        if (record.GetError().GetStatus() != Ydb::StatusIds::SUCCESS) {
            return Finish(false, TStringBuilder() << "Cannot describe imported semaphore: " << record.GetError().ShortDebugString());
        }
        const auto& semaphore = record.GetSemaphoreDescription();
        if (!IsExpectedSemaphore(NextSemaphore, semaphore.name(), semaphore.limit(), semaphore.data(), semaphore.ephemeral())) {
            return SemaphoreConflict(Contents.semaphores(NextSemaphore).name());
        }
        ExistingSemaphores.insert(semaphore.name());
        ++NextSemaphore;
        CreateNextSemaphore();
    }

    void AddNextResource() {
        while (NextResource < Contents.resources_size()
            && ExistingResources.contains(Contents.resources(NextResource).resource_path()))
        {
            ++NextResource;
        }
        if (NextResource == Contents.resources_size()) {
            return CreateNextSemaphore();
        }

        auto request = MakeHolder<NKesus::TEvKesus::TEvAddQuoterResource>();
        FillRateLimiterDescription(*request->Record.MutableResource(), Contents.resources(NextResource));
        const ui64 cookie = StartRequest(EPhase::AddResource);
        NTabletPipe::SendData(SelfId(), KesusPipe, request.Release(), cookie);
    }

    void Handle(NKesus::TEvKesus::TEvAddQuoterResourceResult::TPtr& ev) {
        if (Phase != EPhase::AddResource || ev->Cookie != RequestCookie) {
            return;
        }
        const auto& record = ev->Get()->Record;
        const auto& path = Contents.resources(NextResource).resource_path();
        switch (record.GetError().GetStatus()) {
        case Ydb::StatusIds::SUCCESS:
            ExistingResources.insert(path);
            ++NextResource;
            return AddNextResource();

        case Ydb::StatusIds::ALREADY_EXISTS:
        case Ydb::StatusIds::BAD_REQUEST: {
            AddError = TStringBuilder() << "Cannot restore resource '" << path << "': " << record.GetError().ShortDebugString();
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
            if (ev->Get()->Dead) {
                return Finish(false, "Imported coordination node tablet does not exist");
            }
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
            Retry("Timed out restoring coordination node contents");
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
        Send(SchemeShard, new TEvPrivate::TEvImportKesusResult(ImportId, ItemIdx, success, error));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(NKesus::TEvKesus::TEvDescribeQuoterResourcesResult, Handle);
            hFunc(NKesus::TEvKesus::TEvAddQuoterResourceResult, Handle);
            hFunc(NKesus::TEvKesus::TEvDescribeSemaphoresResult, Handle);
            hFunc(NKesus::TEvKesus::TEvCreateSemaphoreResult, Handle);
            hFunc(NKesus::TEvKesus::TEvDescribeSemaphoreResult, Handle);
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
    NKikimrSchemeOp::TImportKesus Contents;
    const ui32 Retries;

    THashMap<TString, int> ResourcesByPath;
    THashSet<TString> ExistingResources;
    int NextResource = 0;
    THashMap<TString, int> SemaphoresByName;
    THashSet<TString> ExistingSemaphores;
    int NextSemaphore = 0;
    TString KesusPath;
    TString AddError;
    TActorId KesusPipe;
    ui32 Attempt = 0;
    ui64 RequestCookie = 0;
    EPhase Phase = EPhase::DescribeNode;
};

} // anonymous namespace

IActor* CreateKesusRestorer(
    TActorId schemeShard,
    ui64 importId,
    ui32 itemIdx,
    TPathId dstPathId,
    NKikimrSchemeOp::TImportKesus contents,
    ui32 retries)
{
    return new TKesusRestorer(schemeShard, importId, itemIdx, dstPathId, std::move(contents), retries);
}

} // namespace NKikimr::NSchemeShard
