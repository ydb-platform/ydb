#include "s3.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;

    class TS3Manager::TTxPrepareWriteS3 : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui32 NodeId;
        const ui64 AgentInstanceId;
        std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle> Request;
        std::unique_ptr<IEventHandle> Response;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_PREPARE_WRITE_S3; }

        TTxPrepareWriteS3(TBlobDepot *self, TAgent& agent, std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle> request)
            : TTransactionBase(self)
            , NodeId(agent.Connection->NodeId)
            , AgentInstanceId(*agent.AgentInstanceId)
            , Request(std::move(request))
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TAgent& agent = Self->GetAgent(NodeId);
            if (!agent.Connection || agent.AgentInstanceId != AgentInstanceId) {
                // agent has been disconnected while transaction was in queue -- do nothing
                return true;
            }

            if (!Self->Data->LoadMissingKeys(Request->Get()->Record, txc)) {
                // we haven't loaded all of the required keys
                return false;
            }

            NIceDb::TNiceDb db(txc.DB);

            NKikimrBlobDepot::TEvPrepareWriteS3Result *responseRecord;
            std::tie(Response, responseRecord) = TEvBlobDepot::MakeResponseFor(*Request);

            for (const auto& record = Request->Get()->Record; const auto& item : record.GetItems()) {
                auto *responseItem = responseRecord->AddItems();

                TString error;
                if (NKikimrProto::EReplyStatus status = CheckItem(item, error); status != NKikimrProto::OK) {
                    // basic checks have failed (blocked, item was deleted, or something else)
                    responseItem->SetStatus(status);
                    responseItem->SetErrorReason(error);
                } else {
                    responseItem->SetStatus(NKikimrProto::OK);

                    const TS3Locator locator = Self->S3Manager->AllocateS3Locator(item.GetLen());
                    locator.ToProto(responseItem->MutableS3Locator());

                    // we put it here until operation is completed; if tablet restarts and operation fails, then this
                    // key will be deleted; we also rewrite spoiled locator because Len is different
                    db.Table<Schema::TrashS3>().Key(locator.Generation, locator.KeyId).Update<Schema::TrashS3::Len>(locator.Len);

                    const bool inserted = agent.S3WritesInFlight.insert(locator).second;
                    Y_ABORT_UNLESS(inserted);
                }
            }

            return true;
        }

        NKikimrProto::EReplyStatus CheckItem(const NKikimrBlobDepot::TEvPrepareWriteS3::TItem& item, TString& error) {
            auto key = TData::TKey::FromBinaryKey(item.GetKey(), Self->Config);

            bool blocksPass = std::visit(TOverloaded{
                [](TStringBuf) { return true; },
                [&](TLogoBlobID blobId) { return Self->BlocksManager->CheckBlock(blobId.TabletID(), blobId.Generation()); }
            }, key.AsVariant());

            for (const auto& extra : item.GetExtraBlockChecks()) {
                if (!blocksPass) {
                    break;
                }
                blocksPass = Self->BlocksManager->CheckBlock(extra.GetTabletId(), extra.GetGeneration());
            }

            if (!blocksPass) {
                error = "blocked";
                return NKikimrProto::BLOCKED;
            }

            if (auto e = Self->Data->CheckKeyAgainstBarrier(key)) {
                error = TStringBuilder() << "BlobId# " << key.ToString() << " is being put beyond the barrier: " << *e;
                return NKikimrProto::ERROR;
            }

            return NKikimrProto::OK;
        }

        void Complete(const TActorContext&) override {
            TActivationContext::Send(Response.release());
        }
    };

    TS3Locator TS3Manager::AllocateS3Locator(ui32 len) {
        if (!DeleteQueue.empty()) {
            TS3Locator res = DeleteQueue.front();
            DeleteQueue.pop_front();
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_S3_TRASH_OBJECTS] = --TotalS3TrashObjects;
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_S3_TRASH_SIZE] = TotalS3TrashSize -= res.Len;
            res.Len = len;
            return res;
        }
        return {
            .Len = len,
            .Generation = Self->Executor()->Generation(),
            .KeyId = NextKeyId++,
        };
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvPrepareWriteS3::TPtr ev) {
        auto& agent = GetAgent(ev->Recipient);

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS07, "TEvPrepareWriteS3", (Id, GetLogId()), (AgentId, agent.Connection->NodeId),
            (Msg, ev->Get()->Record));

        Execute(std::make_unique<TS3Manager::TTxPrepareWriteS3>(this, agent,
            std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle>(ev.Release())));
    }

} // NKikimr::NBlobDepot
