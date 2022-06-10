#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    template<>
    TBlobDepotAgent::TExecutingQuery *TBlobDepotAgent::CreateExecutingQuery<TEvBlobStorage::EvDiscover>(std::unique_ptr<IEventHandle> ev) {
        class TDiscoverExecutingQuery : public TExecutingQuery {
        public:
            using TExecutingQuery::TExecutingQuery;

            void Initiate() override {
                auto& msg = *Event->Get<TEvBlobStorage::TEvDiscover>();

                const TLogoBlobID from(msg.TabletId, Max<ui32>(), Max<ui32>(), 0, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
                const TLogoBlobID to(msg.TabletId, 0, 0, 0, 0, 0);

                NKikimrBlobDepot::TEvResolve resolve;
                auto *item = resolve.AddItems();
                item->SetBeginningKey(from.GetRaw(), 3 * sizeof(ui64));
                item->SetIncludeBeginning(true);
                item->SetEndingKey(to.GetRaw(), 3 * sizeof(ui64));
                item->SetIncludeEnding(true);
                item->SetMaxKeys(1);
                item->SetReverse(true);

                Agent.Issue(std::move(resolve), this, std::bind(&TDiscoverExecutingQuery::HandleResolveResult,
                    this, std::placeholders::_1));

                if (msg.DiscoverBlockedGeneration) {
                    NKikimrBlobDepot::TEvQueryBlocks queryBlocks;
                    queryBlocks.AddTabletIds(msg.TabletId);
                    Agent.Issue(std::move(queryBlocks), this, std::bind(
                        &TDiscoverExecutingQuery::HandleQueryBlocksResult, this,
                        std::placeholders::_1, TActivationContext::Now()));
                }
            }

            bool HandleResolveResult(IEventBase *result) {
                if (!result) { // server has disconnected before request got processed
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                    return true;
                }

                auto& msg = static_cast<TEvBlobDepot::TEvResolveResult&>(*result);

                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDASD01, "HandleResolveResult", (VirtualGroupId, Agent.VirtualGroupId),
                    (QueryId, QueryId), (Msg, msg.Record));

                const NKikimrProto::EReplyStatus status = msg.Record.GetStatus();
                if (status != NKikimrProto::OK && status != NKikimrProto::OVERRUN) {
                    EndWithError(status, msg.Record.GetErrorReason());
                    return true;
                }

                return status != NKikimrProto::OVERRUN;
            }

            bool HandleQueryBlocksResult(IEventBase *result, TInstant /*issueTimestamp*/) {
                if (!result) {
                    EndWithError(NKikimrProto::ERROR, "BlobDepot tablet disconnected");
                    return true;
                }

                auto& msg = static_cast<TEvBlobDepot::TEvQueryBlocksResult&>(*result);

                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDASD02, "HandleQueryBlocksResult", (VirtualGroupId, Agent.VirtualGroupId),
                    (QueryId, QueryId), (Msg, msg.Record));

                return true;
            }
        };

        return new TDiscoverExecutingQuery(*this, std::move(ev));
    }

} // NKikimr::NBlobDepot
