#pragma once

#include "defs.h"
#include "query_statalgo.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexDumpActor
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelIndexDumpActor : public TActorBootstrapped<TLevelIndexDumpActor<TKey, TMemRec>> {
        using TThis = ::NKikimr::TLevelIndexDumpActor<TKey, TMemRec>;
        using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
        using TDumper = TDbDumper<TKey, TMemRec>;
        using TConstraint = typename TDumper::TConstraint;
        friend class TActorBootstrapped<TThis>;

        void Bootstrap(const TActorContext &ctx) {
            // build constraint if any
            TMaybe<TConstraint> constraint;
            if (Ev->Get()->Record.HasConstraint()) {
                const ui64 tabletId = Ev->Get()->Record.GetConstraint().GetTabletId();
                const ui32 channel = Ev->Get()->Record.GetConstraint().GetChannel();
                TConstraint c(tabletId, channel);
                constraint = c;
            }

            // create dumper
            const ui64 limitInBytes = 10u * 1024u * 1024u; // limit number of bytes in output
            TDumper dumper(HullCtx, std::move(Snapshot), limitInBytes, TString(), constraint);

            // final stream
            TStringStream str;

            // dump db
            TStringStream dump;
            typename TDumper::EDumpRes status = dumper.Dump(dump);
            if (status == TDumper::EDumpRes::Limited) {
                TString errMsg = Sprintf("Dump is limited to %" PRIu64 " bytes", ui64(dump.Str().size()));
                THtmlLightSignalRenderer(NKikimrWhiteboard::Red, errMsg).Output(str);
            }
            str << "<pre><small><small>\n";
            str << dump.Str();
            str << "\n</small></small></pre>";

            // send result
            Result->SetResult(str.Str());
            SendVDiskResponse(ctx, Ev->Sender, Result.release(), Ev->Cookie, HullCtx->VCtx, {});
            TThis::Die(ctx);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_STAT_QUERY;
        }

        TLevelIndexDumpActor(TIntrusivePtr<THullCtx> hullCtx,
                             const TActorId &parentId,
                             TLevelIndexSnapshot &&snapshot,
                             TEvBlobStorage::TEvVDbStat::TPtr &ev,
                             std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result)
            : TActorBootstrapped<TThis>()
            , HullCtx(std::move(hullCtx))
            , ParentId(parentId)
            , Snapshot(std::move(snapshot))
            , Ev(ev)
            , Result(std::move(result))
        {}

    private:
        TIntrusivePtr<THullCtx> HullCtx;
        const TActorId ParentId;
        TLevelIndexSnapshot Snapshot;
        TEvBlobStorage::TEvVDbStat::TPtr Ev;
        std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> Result;
    };

} // NKikimr
