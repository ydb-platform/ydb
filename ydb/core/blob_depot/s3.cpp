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
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS00, "TUploaderActor::Bootstrap", (Key, Key), (ValueChain, ValueChain));
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
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS01, "TUploaderActor::Handle(TEvGetResult)", (Msg, *msg));
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
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS02, "TUploaderActor::Handle(TEvPutObjectResponse)", (Result, ev->Get()->Result));
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

    struct TS3Manager::TEvDeleteResult : TEventLocal<TEvDeleteResult, TEvPrivate::EvDeleteResult> {
        std::vector<TS3Locator> LocatorsOk;
        std::vector<TS3Locator> LocatorsError;

        TEvDeleteResult(std::vector<TS3Locator>&& locatorsOk, std::vector<TS3Locator>&& locatorsError)
            : LocatorsOk(std::move(locatorsOk))
            , LocatorsError(std::move(locatorsError))
        {}
    };

    class TS3Manager::TDeleterActor : public TActor<TDeleterActor> {
        TActorId ParentId;
        THashMap<TString, TS3Locator> Locators;
        TString LogId;

    public:
        TDeleterActor(TActorId parentId, THashMap<TString, TS3Locator> locators, TString logId)
            : TActor(&TThis::StateFunc)
            , ParentId(parentId)
            , Locators(locators)
            , LogId(std::move(logId))
        {}

        void Handle(NWrappers::TEvExternalStorage::TEvDeleteObjectResponse::TPtr ev) {
            auto& msg = *ev->Get();
            if (msg.IsSuccess()) {
                Finish(std::nullopt);
            } else if (const auto& error = msg.GetError(); error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) {
                Finish(std::nullopt);
            } else {
                Finish(error.GetMessage().c_str());
            }
        }

        void Handle(NWrappers::TEvExternalStorage::TEvDeleteObjectsResponse::TPtr ev) {
            auto& msg = *ev->Get();

            std::vector<TS3Locator> locatorsOk;
            std::vector<TS3Locator> locatorsError;

            if (msg.IsSuccess()) {
                auto& result = msg.Result.GetResult();
                for (const Aws::S3::Model::DeletedObject& deleted : result.GetDeleted()) {
                    if (deleted.KeyHasBeenSet()) {
                        if (const auto it = Locators.find(deleted.GetKey().c_str()); it != Locators.end()) {
                            locatorsOk.push_back(it->second);
                            Locators.erase(it);
                        } else {
                            STLOG(PRI_WARN, BLOB_DEPOT, BDTS09, "key not found", (Id, LogId),
                                (Key, deleted.KeyHasBeenSet() ? std::make_optional<TString>(deleted.GetKey().c_str()) : std::nullopt));
                        }
                    } else {
                        STLOG(PRI_WARN, BLOB_DEPOT, BDTS10, "key not set", (Id, LogId));
                    }
                }
                for (const Aws::S3::Model::Error& error : result.GetErrors()) {
                    if (error.KeyHasBeenSet() && error.GetCode() == "NoSuchKey") { // this key has already been deleted
                        if (const auto it = Locators.find(error.GetKey().c_str()); it != Locators.end()) {
                            locatorsOk.push_back(it->second);
                            Locators.erase(it);
                        }
                    } else {
                        STLOG(PRI_WARN, BLOB_DEPOT, BDTS11, "failed to delete object from S3", (Id, LogId),
                            (Key, error.KeyHasBeenSet() ? std::make_optional<TString>(error.GetKey().c_str()) : std::nullopt),
                            (Error, error.GetMessage().c_str()));
                    }
                }
            } else {
                STLOG(PRI_WARN, BLOB_DEPOT, BDTS12, "failed to delete object(s) from S3", (Id, LogId),
                    (Error, msg.GetError().GetMessage().c_str()));
            }

            for (const auto& [key, locator] : Locators) {
                locatorsError.push_back(locator);
                STLOG(PRI_WARN, BLOB_DEPOT, BDTS08, "failed to delete object from S3", (Id, LogId), (Locator, locator));
            }

            Send(ParentId, new TEvDeleteResult(std::move(locatorsOk), std::move(locatorsError)));
            PassAway();
        }

        void HandleUndelivered() {
            Finish("event undelivered");
        }

        void Finish(std::optional<TString> error) {
            if (error) {
                STLOG(PRI_WARN, BLOB_DEPOT, BDTS03, "failed to delete object(s) from S3", (Id, LogId), (Locators, Locators),
                    (Error, error));
            }
            std::vector<TS3Locator> locatorsOk;
            std::vector<TS3Locator> locatorsError;
            auto *target = error ? &locatorsError : &locatorsOk;
            for (const auto& [key, locator] : Locators) {
                target->push_back(locator);
            }
            Send(ParentId, new TEvDeleteResult(std::move(locatorsOk), std::move(locatorsError)));
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NWrappers::TEvExternalStorage::TEvDeleteObjectResponse, Handle)
            hFunc(NWrappers::TEvExternalStorage::TEvDeleteObjectsResponse, Handle)
            cFunc(TEvents::TSystem::Undelivered, HandleUndelivered)
            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    };

    class TS3Manager::TTxDeleteTrashS3 : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        std::vector<TS3Locator> Locators;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_DELETE_TRASH_S3; }

        TTxDeleteTrashS3(TBlobDepot *self, std::vector<TS3Locator>&& locators)
            : TTransactionBase(self)
            , Locators(std::move(locators))
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            for (const TS3Locator& locator : Locators) {
                NIceDb::TNiceDb(txc.DB).Table<Schema::TrashS3>().Key(locator.Generation, locator.KeyId).Delete();
            }
            return true;
        }

        void Complete(const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS04, "TTxDeleteTrashS3 complete", (Id, Self->GetLogId()), (Locators, Locators));

            size_t len = 0;
            for (const TS3Locator& locator : Locators) {
                len += locator.Len;
            }

            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_S3_TRASH_OBJECTS] =
                Self->S3Manager->TotalS3TrashObjects -= Locators.size();
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_S3_TRASH_SIZE] =
                Self->S3Manager->TotalS3TrashSize -= len;

            --Self->S3Manager->NumDeleteTxInFlight;

            Self->S3Manager->RunDeletersIfNeeded();
        }
    };

    TS3Manager::TS3Manager(TBlobDepot *self)
        : Self(self)
    {}

    TS3Manager::~TS3Manager() = default;

    void TS3Manager::Init(const NKikimrBlobDepot::TS3BackendSettings *settings) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS05, "Init", (Settings, settings));
        if (settings) {
            ExternalStorageConfig = NWrappers::IExternalStorageConfig::Construct(settings->GetSettings());
            WrapperId = Self->Register(NWrappers::CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator()));
            BasePath = TStringBuilder() << settings->GetSettings().GetObjectKeyPattern() << '/' << Self->Config.GetName();
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
        for (TActorId actorId : ActiveDeleters) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, Self->SelfId(), nullptr, 0));
        }
    }

    void TS3Manager::Handle(TAutoPtr<IEventHandle> ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDeleteResult, [&](TEvDeleteResult::TPtr ev) {
                const size_t numErased = ActiveDeleters.erase(ev->Sender);
                Y_ABORT_UNLESS(numErased == 1);

                auto& msg = *ev->Get();
                if (!msg.LocatorsOk.empty()) {
                    Self->Execute(std::make_unique<TTxDeleteTrashS3>(Self, std::move(msg.LocatorsOk)));
                    ++NumDeleteTxInFlight;
                    Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_DELETES_OK] += msg.LocatorsOk.size();
                    size_t len = 0;
                    for (const TS3Locator& locator : msg.LocatorsOk) {
                        len += locator.Len;
                    }
                    Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_DELETES_BYTES] += len;
                }

                if (!msg.LocatorsError.empty()) {
                    // TODO(alexvru): handle situation with double-delete (?)
                    DeleteQueue.insert(DeleteQueue.end(), msg.LocatorsError.begin(), msg.LocatorsError.end());
                    RunDeletersIfNeeded();
                    Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_DELETES_ERROR] += msg.LocatorsError.size();
                }
            });
        }
    }

    void TS3Manager::OnKeyWritten(const TData::TKey& key, const TValueChain& valueChain) {
        if (AsyncMode) {
            ActiveUploaders.insert(Self->Register(new TUploaderActor(WrapperId, BasePath, Self->Info(), key, valueChain)));
        }
    }

    TS3Locator TS3Manager::AllocateS3Locator(ui32 len) {
        return {
            .Len = len,
            .Generation = Self->Executor()->Generation(),
            .KeyId = NextKeyId++,
        };
    }

    void TS3Manager::AddTrashToCollect(TS3Locator locator) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS06, "AddTrashToCollect", (Id, Self->GetLogId()), (Locator, locator));
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_S3_TRASH_OBJECTS] = ++TotalS3TrashObjects;
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_S3_TRASH_SIZE] = TotalS3TrashSize += locator.Len;
        DeleteQueue.push_back(locator);
        RunDeletersIfNeeded();
    }

    void TS3Manager::RunDeletersIfNeeded() {
        while (!DeleteQueue.empty() && NumDeleteTxInFlight + ActiveDeleters.size() < MaxDeletesInFlight) {
            THashMap<TString, TS3Locator> locators;

            while (!DeleteQueue.empty() && locators.size() < MaxObjectsToDeleteAtOnce) {
                const TS3Locator& locator = DeleteQueue.front();
                locators.emplace(locator.MakeObjectName(BasePath), locator);
                DeleteQueue.pop_front();
            }

            const TActorId actorId = Self->Register(new TDeleterActor(Self->SelfId(), locators, Self->GetLogId()));
            ActiveDeleters.insert(actorId);

            if (locators.size() == 1) {
                TActivationContext::Send(new IEventHandle(WrapperId, actorId,
                    new NWrappers::TEvExternalStorage::TEvDeleteObjectRequest(
                        Aws::S3::Model::DeleteObjectRequest()
                            .WithKey(locators.begin()->first)
                    ),
                    IEventHandle::FlagTrackDelivery
                ));
            } else {
                auto del = Aws::S3::Model::Delete();
                for (const auto& [key, locator] : locators) {
                    del.AddObjects(Aws::S3::Model::ObjectIdentifier().WithKey(key));
                }

                TActivationContext::Send(new IEventHandle(WrapperId, actorId,
                    new NWrappers::TEvExternalStorage::TEvDeleteObjectsRequest(Aws::S3::Model::DeleteObjectsRequest()
                        .WithDelete(std::move(del))), IEventHandle::FlagTrackDelivery));
            }
        }
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

                        const TS3Locator locator = Self->S3Manager->AllocateS3Locator(item.GetLen());
                        locator.ToProto(responseItem->MutableS3Locator());

                        // we put it here until operation is completed; if tablet restarts and operation fails, then this
                        // key will be deleted
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

        auto& agent = GetAgent(ev->Recipient);

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS07, "TEvPrepareWriteS3", (Id, GetLogId()), (AgentId, agent.Connection->NodeId),
            (Msg, ev->Get()->Record));

        Execute(std::make_unique<TTxPrepareWriteS3>(this, agent, std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle>(
            ev.Release())));
    }

} // NKikimr::NBlobDepot
