#include "s3.h"
#include <ydb/core/wrappers/s3_wrapper.h>

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;

    struct TS3Manager::TEvUpload : TEventLocal<TEvUpload, TEvPrivate::EvUpload> {
        TData::TKey Key;
        TValueChain ValueChain;

        TEvUpload(TData::TKey key, TValueChain valueChain)
            : Key(std::move(key))
            , ValueChain(std::move(valueChain))
        {}
    };

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
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTxx, "TUploaderActor::Bootstrap", (Key, Key), (ValueChain, ValueChain));
            size_t targetOffset = 0;
            EnumerateBlobsForValueChain(ValueChain, Info->TabletID, [&](TLogoBlobID id, ui32 shift, ui32 size) {
                const ui32 groupId = Info->GroupFor(id.Channel(), id.Generation());
                SendToBSProxy(SelfId(), groupId, new TEvBlobStorage::TEvGet(id, shift, size, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead), targetOffset);
                ++RepliesPending;
                targetOffset += size;
            });
            Buffer = TString::Uninitialized(targetOffset);
            if (!RepliesPending) {
                // don't have to read anything?
            }
            Become(&TThis::StateFunc);
        }

        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
            auto *msg = ev->Get();
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTxx, "TUploaderActor::Handle(TEvGetResult)", (Msg, *msg));
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
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTxx, "TUploaderActor::Handle(TEvPutObjectResponse)", (Result, ev->Get()->Result));
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
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDSSSxx, "Init", (Settings, settings));
        if (settings) {
            ExternalStorageConfig = NWrappers::IExternalStorageConfig::Construct(settings->GetSettings());
            BasePath = settings->GetSettings().GetObjectKeyPattern();
            WrapperId = Self->Register(NWrappers::CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator()));
        }
    }

    void TS3Manager::TerminateAllUploaders() {
        for (TActorId actorId : ActiveUploaders) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, Self->SelfId(), nullptr, 0));
        }
    }

    void TS3Manager::OnKeyWritten(const TData::TKey& key, const TValueChain& valueChain) {
        ActiveUploaders.insert(Self->Register(new TUploaderActor(WrapperId, BasePath, Self->Info(), key, valueChain)));
    }

    TString TS3Manager::MakeKeyName(const TData::TKey& key, const NKikimrBlobDepot::TS3Locator& locator) {
        return TStringBuilder() << key.MakeTextualKey() << '@' << locator.GetGeneration() << '.' << locator.GetKeyId();
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

            TTxCommitBlobSeq(TBlobDepot *self, TAgent& agent, std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle> request)
                : TTransactionBase(self)
                , NodeId(agent.Connection->NodeId)
                , AgentInstanceId(*agent.AgentInstanceId)
                , Request(std::move(request))
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                TAgent& agent = Self->GetAgent(NodeId);
                if (!agent.Connection || agent.AgentInstanceId != AgentInstanceId) {
                    // agent was disconnected while transaction was in queue -- do nothing
                    return true;
                }

                if (!LoadMissingKeys(txc)) {
                    // we haven't loaded all of the required keys
                    return false;
                }

                NIceDb::TNiceDb db(txc.DB);

                NKikimrBlobDepot::TEvPrepareWriteS3Result *responseRecord;
                std::tie(Response, responseRecord) = TEvBlobDepot::MakeResponseFor(*Request);

                const ui32 generation = Self->Executor()->Generation();

                for (const auto& record = Request->Get()->Record; const auto& item : record.GetItems()) {
                    auto *responseItem = responseRecord->AddItems();

                    TString error;
                    if (NKikimrProto::EReplyStatus status = CheckItem(item, error); status != NKikimrProto::OK) {
                        // basic checks have failed (blocked, item was deleted, or something else)
                        responseItem->SetStatus(status);
                        responseItem->SetErrorReason(error);
                    } else {
                        responseItem->SetStatus(NKikimrProto::OK);

                        auto *locator = responseItem->MutableKeyLocator();
                        locator->SetGeneration(Self->Executor()->Generation());
                        locator->SetKeyId(NextKeyId++);

                        const auto key = TData::TKey::FromBinaryKey(item.GetKey(), Self->Config);

                        // we put it here until operation is completed; if tablet restarts and operation fails, then this
                        // key will be deleted
                        db.Table<Schema::S3Trash>().Key(Self->S3Manager->MakeKeyName(key, *locator)).Update();
                    }
                }

                return true;
            }

            bool LoadMissingKeys(TTransactionContext& txc) {
                if (Self->Data->IsLoaded()) {
                    return true;
                }
                for (const auto& record = Request->Get()->Record; const auto& item : record.GetItems()) {
                    auto key = TData::TKey::FromBinaryKey(item.GetKey(), Self->Config);
                    if (!Self->Data->EnsureKeyLoaded(key, txc)) {
                        return false;
                    }
                }
                return true;
            }

            NKikimrProto::EReplyStatus CheckItem(const NKikimrBlobDepot::TEvPrepareWriteS3::TItem& item, TString& error) {
                auto key = TData::TKey::FromBinaryKey(item.GetKey(), Self->Config);

                bool blocksPass = std::visit(TOverloaded{
                    [](TString&) { return true; },
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

                if (!CheckKeyAgainstBarrier(key, &error)) {
                    error = TStringBuilder() << "BlobId# " << key.ToString() << " is being put beyond the barrier: " << error;
                    return NKikimrProto::ERROR;
                }

                return NKikimrProto::OK;
            }

            bool CheckKeyAgainstBarrier(const TData::TKey& key, TString *error) {
                const auto& v = key.AsVariant();
                if (const auto *id = std::get_if<TLogoBlobID>(&v)) {
                    bool underSoft, underHard;
                    Self->BarrierServer->GetBlobBarrierRelation(*id, &underSoft, &underHard);
                    if (underHard) {
                        *error = TStringBuilder() << "under hard barrier# " << Self->BarrierServer->ToStringBarrier(
                            id->TabletID(), id->Channel(), true);
                        return false;
                    } else if (underSoft) {
                        const TData::TValue *value = Self->Data->FindKey(key);
                        if (!value || value->KeepState != NKikimrBlobDepot::EKeepState::Keep) {
                            *error = TStringBuilder() << "under soft barrier# " << Self->BarrierServer->ToStringBarrier(
                                id->TabletID(), id->Channel(), false);
                            return false;
                        }
                    }
                }
                return true;
            }

            void Complete(const TActorContext&) override {
                TActivationContext::Send(Response.release());
            }
        };

        Execute(std::make_unique<TTxPrepareWriteS3>(this, GetAgent(ev->Recipient),
            std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle>(ev.Release())));
    }

} // NKikimr::NBlobDepot
