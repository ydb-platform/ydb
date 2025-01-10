#pragma once

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/schema.h>
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
        size_t Index = 0;
    };

public:
    using TBase = TScanActorBase<TDerived>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TAuthScanBase(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
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

        // TODO: support TableRange filter
        if (auto cellsFrom = TBase::TableRange.From.GetCells(); cellsFrom.size() > 0 && !cellsFrom[0].IsNull()) {
            TBase::ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "TableRange.From filter is not supported");
            return;
        }
        if (auto cellsTo = TBase::TableRange.To.GetCells(); cellsTo.size() > 0 && !cellsTo[0].IsNull()) {
            TBase::ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "TableRange.To filter is not supported");
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
                NavigatePath(SplitPath(TBase::TenantName));
                DeepFirstSearchStack.pop_back();
                return;
            }

            auto& children = last.Entry.ListNodeEntry->Children;
            if (last.Index < children.size()) {
                auto& child = children.at(last.Index++);

                if (child.Kind == TSchemeCacheNavigate::KindExtSubdomain || child.Kind == TSchemeCacheNavigate::KindSubdomain) {
                    continue;
                }

                last.Entry.Path.push_back(child.Name);
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

    virtual void FillBatch(NKqp::TEvKqpCompute::TEvScanData& batch, const TNavigate::TEntry& entry) = 0;

private:
    TVector<TTraversingChildren> DeepFirstSearchStack;
};

}
