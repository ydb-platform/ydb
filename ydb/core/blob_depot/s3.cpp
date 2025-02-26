#include "s3.h"
#include "blocks.h"
#include <ydb/core/wrappers/s3_wrapper.h>

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;

    struct TS3Manager::TEvUploadResult : TEventLocal<TEvUploadResult, TEvPrivate::EvUploadResult> {
    };

    class TS3Manager::TUploaderActor : public TActorBootstrapped<TUploaderActor> {
        const TActorId WrapperId;
        const TString BasePath;
        const TIntrusivePtr<TTabletStorageInfo> Info;
        const TData::TKey Key;
        const TValueChain ValueChain;
        ui32 RepliesPending = 0;
        TString Buffer;

    public:
        TUploaderActor(TActorId wrapperId, TString basePath, TIntrusivePtr<TTabletStorageInfo> info, TData::TKey key,
                TValueChain valueChain)
            : WrapperId(wrapperId)
            , BasePath(std::move(basePath))
            , Info(std::move(info))
            , Key(std::move(key))
            , ValueChain(std::move(valueChain))
        {}

        void Bootstrap() {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT93, "TUploaderActor::Bootstrap", (Key, Key), (ValueChain, ValueChain));
            size_t targetOffset = 0;
            EnumerateBlobsForValueChain(ValueChain, Info->TabletID, TOverloaded{
                [&](TLogoBlobID id, ui32 shift, ui32 size) {
                    const ui32 groupId = Info->GroupFor(id.Channel(), id.Generation());
                    SendToBSProxy(SelfId(), groupId, new TEvBlobStorage::TEvGet(id, shift, size, TInstant::Max(),
                        NKikimrBlobStorage::EGetHandleClass::FastRead), targetOffset);
                    ++RepliesPending;
                    targetOffset += size;
                },
                [&](TS3Locator) {
                    // already stored in S3, nothing to do
                }
            });
            Buffer = TString::Uninitialized(targetOffset);
            if (!RepliesPending) {
                // don't have to read anything?
            }
            Become(&TThis::StateFunc);
        }

        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
            auto *msg = ev->Get();
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT94, "TUploaderActor::Handle(TEvGetResult)", (Msg, *msg));
            if (msg->Status == NKikimrProto::OK && msg->ResponseSz == 1 && msg->Responses->Status == NKikimrProto::OK) {
                TRope& rope = msg->Responses->Buffer;
                char *ptr = Buffer.Detach() + ev->Cookie;
                for (auto it = rope.Begin(); it.Valid(); it.AdvanceToNextContiguousBlock()) {
                    memcpy(ptr, it.ContiguousData(), it.ContiguousSize());
                    ptr += it.ContiguousSize();
                }
                if (!--RepliesPending) {
                    auto request = Aws::S3::Model::PutObjectRequest()
                        .WithKey(TStringBuilder() << BasePath << '/' << Key.MakeTextualKey());
                    Send(WrapperId, new NWrappers::TEvExternalStorage::TEvPutObjectRequest(request, std::move(Buffer)));
                }
            } else {
            }
        }

        void Handle(NWrappers::TEvExternalStorage::TEvPutObjectResponse::TPtr ev) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT95, "TUploaderActor::Handle(TEvPutObjectResponse)", (Result, ev->Get()->Result));
            if (auto& result = ev->Get()->Result; result.IsSuccess()) {
            } else {
            }
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvBlobStorage::TEvGetResult, Handle)
            hFunc(NWrappers::TEvExternalStorage::TEvPutObjectResponse, Handle)
            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    };

    TS3Manager::TS3Manager(TBlobDepot *self)
        : Self(self)
    {}

    TS3Manager::~TS3Manager() = default;

    void TS3Manager::Init(const NKikimrBlobDepot::TS3BackendSettings *settings) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT96, "Init", (Settings, settings));
        if (settings) {
            ExternalStorageConfig = NWrappers::IExternalStorageConfig::Construct(settings->GetSettings());
            WrapperId = Self->Register(NWrappers::CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator()));
            BasePath = TStringBuilder() << settings->GetSettings().GetObjectKeyPattern() << '/' << Self->TabletID();
            SyncMode = settings->HasSyncMode();
            AsyncMode = settings->HasAsyncMode();
        } else {
            SyncMode = false;
            AsyncMode = false;
        }
    }

    void TS3Manager::TerminateAllUploaders() {
        for (TActorId actorId : ActiveUploaders) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, Self->SelfId(), nullptr, 0));
        }
    }

    void TS3Manager::OnKeyWritten(const TData::TKey& key, const TValueChain& valueChain) {
        if (AsyncMode) {
            ActiveUploaders.insert(Self->Register(new TUploaderActor(WrapperId, BasePath, Self->Info(), key, valueChain)));
        }
    }

    TS3Locator TS3Manager::AllocateS3Locator() {
        return {
            .Generation = Self->Executor()->Generation(),
            .KeyId = NextKeyId++,
        };
    }

    void TBlobDepot::InitS3Manager() {
        S3Manager->Init(Config.HasS3BackendSettings() ? &Config.GetS3BackendSettings() : nullptr);
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvPrepareWriteS3::TPtr ev) {
        class TTxPrepareWriteS3 : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
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

                        const TS3Locator locator = Self->S3Manager->AllocateS3Locator();
                        locator.ToProto(responseItem->MutableS3Locator());

                        // we put it here until operation is completed; if tablet restarts and operation fails, then this
                        // key will be deleted
                        db.Table<Schema::TrashS3>().Key(locator.Generation, locator.KeyId).Update();
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

        auto& agent = GetAgent(ev->Recipient);

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT97, "TEvPrepareWriteS3", (Id, GetLogId()), (AgentId, agent.Connection->NodeId),
            (Msg, ev->Get()->Record));

        Execute(std::make_unique<TTxPrepareWriteS3>(this, agent, std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle>(
            ev.Release())));
    }

} // NKikimr::NBlobDepot
