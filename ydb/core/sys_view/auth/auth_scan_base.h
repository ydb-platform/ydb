#pragma once

#include "sort_helpers.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/login/protos/login.pb.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView::NAuth {

using namespace NSchemeShard;
using namespace NActors;
using namespace NSchemeCache;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;
using TPath = TVector<TString>;

template <typename TDerived>
class TAuthScanBase : public TScanActorBase<TDerived> {
    struct TTraversingChildren {
        TNavigate::TEntry Entry;
        TVector<const TNavigate::TListNodeEntry::TChild*> SortedChildren;
        size_t Index = 0;

        TTraversingChildren() = default;

        TTraversingChildren(TNavigate::TEntry&& entry)
            : Entry(std::move(entry))
            , SortedChildren(::Reserve(Entry.ListNodeEntry->Children.size()))
        {
            for (const auto& child : Entry.ListNodeEntry->Children) {
                SortedChildren.push_back(&child);
            }
            SortBatch(SortedChildren, [](const auto* left, const auto* right) {
                return left->Name < right->Name;
            });
        }
    };

public:
    using TBase = TScanActorBase<TDerived>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TAuthScanBase(const NActors::TActorId& ownerId, ui32 scanId,
        const NKikimrSysView::TSysViewDescription& sysViewInfo,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        bool requireUserAdministratorAccess, bool applyPathTableRange)
        : TBase(ownerId, scanId, sysViewInfo, tableRange, columns)
        , UserToken(std::move(userToken))
        , RequireUserAdministratorAccess(requireUserAdministratorAccess)
    {
        if (applyPathTableRange) {
            if (auto cellsFrom = TBase::TableRange.From.GetCells(); cellsFrom.size() > 0 && !cellsFrom[0].IsNull()) {
                PathFrom = cellsFrom[0].AsBuf();
            }
            if (auto cellsTo = TBase::TableRange.To.GetCells(); cellsTo.size() > 0 && !cellsTo[0].IsNull()) {
                PathTo = cellsTo[0].AsBuf();
            }
        }
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, TBase::HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, TBase::HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::NAuth::TAuthScanBase: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

protected:
    void ProceedToScan() override {
        TBase::Become(&TAuthScanBase::StateScan);

        //NOTE: here is the earliest point when Base::DatabaseOwner is already set
        bool isClusterAdmin = IsAdministrator(AppData(), UserToken.Get());
        bool isDatabaseAdmin = (AppData()->FeatureFlags.GetEnableDatabaseAdmin() && IsDatabaseAdministrator(UserToken.Get(), TBase::DatabaseOwner));
        bool isAdmin = isClusterAdmin || isDatabaseAdmin;

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
            "ProceedToScan,"
            << " tenant name: " << TBase::TenantName
            << " tenant owner: " << TBase::DatabaseOwner
            << " subject sid: " << (UserToken ? UserToken->GetUserSID() : "empty")
            << " require admin access: " << RequireUserAdministratorAccess
            << " is admin: " << isAdmin
        );

        if (RequireUserAdministratorAccess && !isAdmin) {
            TBase::ReplyErrorAndDie(Ydb::StatusIds::UNAUTHORIZED, TStringBuilder() << "Administrator access is required");
            return;
        }

        auto& last = DeepFirstSearchStack.emplace_back();
        last.Index = Max<size_t>(); // tenant root

        if (TBase::AckReceived) {
            ContinueScan();
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        ContinueScan();
    }

    void ContinueScan() {
        while (DeepFirstSearchStack) {
            auto& last = DeepFirstSearchStack.back();

            if (last.Index == Max<size_t>()) { // tenant root
                if ((PathFrom || PathTo) && ShouldSkipSubTree(TBase::TenantName)) {
                    DeepFirstSearchStack.pop_back();
                    continue;
                }
                NavigatePath(SplitPath(TBase::TenantName));
                DeepFirstSearchStack.pop_back();
                return;
            }

            auto& children = last.SortedChildren;
            if (last.Index < children.size()) {
                const auto& child = *children.at(last.Index++);

                if (child.Kind == TSchemeCacheNavigate::KindExtSubdomain || child.Kind == TSchemeCacheNavigate::KindSubdomain) {
                    continue;
                }

                last.Entry.Path.push_back(child.Name);
                if ((PathFrom || PathTo) && ShouldSkipSubTree(CanonizePath(last.Entry.Path))) {
                    last.Entry.Path.pop_back();
                    continue;
                }

                NavigatePath(last.Entry.Path);
                last.Entry.Path.pop_back();
                return;
            } else {
                DeepFirstSearchStack.pop_back();
            }
        }

        TBase::ReplyEmptyAndDie();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        THolder<NSchemeCache::TSchemeCacheNavigate> request(ev->Get()->Request.Release());

        Y_ABORT_UNLESS(request->ResultSet.size() == 1);
        auto& entry = request->ResultSet.back();

        if (entry.Status != TNavigate::EStatus::Ok) {
            TBase::ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() <<
                "Failed to navigate " << CanonizePath(entry.Path) << ": " << entry.Status);
            return;
        }

        LOG_TRACE_S(ctx, NKikimrServices::SYSTEM_VIEWS,
            "Got navigate: " << request->ToString(*AppData()->TypeRegistry));

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(TBase::ScanId);

        FillBatch(*batch, entry);

        if (!RequireUserAdministratorAccess
                && UserToken && !UserToken->GetSerializedToken().empty()
                && entry.SecurityObject && !entry.SecurityObject->CheckAccess(NACLib::DescribeSchema, *UserToken)) {
            batch->Rows.clear();
        }

        if (!batch->Finished && entry.ListNodeEntry) {
            DeepFirstSearchStack.emplace_back(std::move(entry));
        }

        batch->Finished = DeepFirstSearchStack.empty();

        TBase::SendBatch(std::move(batch));
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        TBase::ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Failed to request path info");
    }

    void PassAway() override {
        TBase::PassAway();
    }

    void NavigatePath(TPath path) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();

        auto& entry = request->ResultSet.emplace_back();
        entry.RequestType = TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.Path = std::move(path);
        entry.Operation = TSchemeCacheNavigate::OpList;
        entry.RedirectRequired = false;

        LOG_TRACE_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
            "Navigate " << request->ToString(*AppData()->TypeRegistry));

        TBase::Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    // this method only skip foolproof useless paths
    // ignores from/to inclusive flags for simplicity
    // ignores some boundary cases for simplicity
    // precise check will be performed later on batch rows filtering
    bool ShouldSkipSubTree(const TString& path) {
        Y_DEBUG_ABORT_UNLESS(PathFrom || PathTo);

        if (PathFrom) {
            // example:
            // PathFrom = "Dir2/SubDir2"
            // skip:
            // - "Dir1"
            // - "Dir2/SubDir1"
            // do not skip:
            // - "Dir2"
            // - "Dir3"

            if (PathFrom > path && !PathFrom->StartsWith(path)) {
                return true;
            }
        }

        if (PathTo) {
            // example:
            // PathTo = "Dir2/SubDir2"
            // skip:
            // - "Dir3"
            // - "Dir2/SubDir3"
            // do not skip:
            // - "Dir1"
            // - "Dir2"

            if (PathTo < path) {
                return true;
            }
        }

        return false;
    }

    virtual void FillBatch(NKqp::TEvKqpCompute::TEvScanData& batch, const TNavigate::TEntry& entry) = 0;

protected:
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;

private:
    bool RequireUserAdministratorAccess;
    std::optional<TString> PathFrom, PathTo;
    TVector<TTraversingChildren> DeepFirstSearchStack;
};

}
