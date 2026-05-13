#include "s3.h"

#include <ydb/core/wrappers/abstract.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;
    using TEvExternalStorage = NWrappers::TEvExternalStorage;

    struct TS3Manager::TEvScanFound : TEventLocal<TEvScanFound, TEvPrivate::EvScanFound> {
        std::vector<std::tuple<TString, ui64>> KeysWithoutPrefix;
        bool IsFinal;
        std::optional<TString> Error;

        TEvScanFound(std::vector<std::tuple<TString, ui64>>&& keysWithoutPrefix, bool isFinal, std::optional<TString>&& error)
            : KeysWithoutPrefix(std::move(keysWithoutPrefix))
            , IsFinal(isFinal)
            , Error(std::move(error))
        {}
    };

    class TS3Manager::TScannerActor : public TActorBootstrapped<TScannerActor> {
        const TActorId ParentId;
        const TActorId WrapperId;
        TString Prefix;
        const TString Bucket;
        const TString LogId;
        std::optional<TString> Marker;

    public:
        TScannerActor(TActorId parentId, TActorId wrapperId, TString prefix, TString bucket, TString logId)
            : ParentId(parentId)
            , WrapperId(wrapperId)
            , Prefix(std::move(prefix))
            , Bucket(std::move(bucket))
            , LogId(std::move(logId))
        {
            Prefix += '/';
        }

        void Bootstrap() {
            IssueNextRequest();
            Become(&TThis::StateFunc);
        }

        void IssueNextRequest() {
            auto request = Aws::S3::Model::ListObjectsRequest()
                .WithBucket(Bucket)
                .WithPrefix(Prefix)
            ;
            if (Marker) {
                request.SetMarker(*Marker);
            }
            request.SetMaxKeys(100);

            YDB_LOG_DEBUG("TScannerActor::IssueNextRequest",
                {"Marker", "BDTS18"},
                {"Id", LogId},
                {"Prefix", Prefix},
                {"#_Marker", Marker});

            Send(WrapperId, new TEvExternalStorage::TEvListObjectsRequest(request), IEventHandle::FlagTrackDelivery);
        }

        void Handle(TEvExternalStorage::TEvListObjectsResponse::TPtr ev) {
            auto& msg = *ev->Get();
            if (!msg.IsSuccess()) {
                FinishWithError(msg.GetError().GetMessage().c_str());
            } else {
                const auto& result = msg.Result.GetResult();
                TString lastKey;
                std::vector<std::tuple<TString, ui64>> keysWithoutPrefix;

                for (const auto& item : result.GetContents()) {
                    if (!item.KeyHasBeenSet()) {
                        return FinishWithError("invalid response: no key set in listing");
                    } else if (!item.SizeHasBeenSet()) {
                        return FinishWithError(TStringBuilder() << "invalid response: no size for key " << item.GetKey());
                    }
                    lastKey = item.GetKey();
                    if (!lastKey.StartsWith(Prefix)) {
                        return FinishWithError("returned key does not start with specified prefix");
                    }
                    keysWithoutPrefix.emplace_back(lastKey.substr(Prefix.length()), item.GetSize());
                }

                const bool isFinal = !result.GetIsTruncated();

                Send(ParentId, new TEvScanFound(std::move(keysWithoutPrefix), isFinal, std::nullopt));

                if (isFinal) {
                    PassAway();
                } else {
                    Marker.emplace(std::move(lastKey));
                }
            }
        }

        void HandleUndelivered() {
            FinishWithError("event undelivered");
        }

        void FinishWithError(TString error) {
            Send(ParentId, new TEvScanFound({}, true, std::move(error)));
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvExternalStorage::TEvListObjectsResponse, Handle)
            cFunc(TEvents::TSystem::Undelivered, HandleUndelivered)
            cFunc(TEvPrivate::EvScanContinue, IssueNextRequest)
            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    };

    class TS3Manager::TTxProcessScannedKeys : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        THashSet<TS3Locator> UnprocessedLocators;
        THashSet<TS3Locator> LocatorsToDelete;

    public:
        TTxProcessScannedKeys(TBlobDepot *self, THashSet<TS3Locator>&& locators)
            : TTransactionBase(self)
            , UnprocessedLocators(std::move(locators))
        {}

        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_PROCESS_SCANNED_KEYS; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);

            for (auto it = UnprocessedLocators.begin(); it != UnprocessedLocators.end(); ) {
                auto row = db.Table<Schema::TrashS3>().Key(it->Generation, it->KeyId).Select();
                if (!row.IsReady()) {
                    return false;
                } else if (!row.IsValid()) {
                    const bool useful = Self->Data->IsUseful(*it);
                    if (useful) {
                        YDB_LOG_CRIT("trying to delete useful S3 locator",
                            {"Marker", "BDTS13"},
                            {"Id", Self->GetLogId()},
                            {"Locator", *it});
                        Y_DEBUG_ABORT("trying to delete useful S3 locator");
                    } else {
                        LocatorsToDelete.insert(*it);
                    }
                }
                UnprocessedLocators.erase(it++);
            }

            Y_ABORT_UNLESS(UnprocessedLocators.empty());

            for (const TS3Locator& locator : LocatorsToDelete) {
                db.Table<Schema::TrashS3>().Key(locator.Generation, locator.KeyId).Update<Schema::TrashS3::Len>(locator.Len);
            }

            return true;
        }

        void Complete(const TActorContext&) override {
            for (const auto& locator : LocatorsToDelete) {
                Self->S3Manager->AddTrashToCollect(locator);
            }
            if (const auto& actorId = Self->S3Manager->ScannerActorId) {
                TActivationContext::Send(new IEventHandle(TEvPrivate::EvScanContinue, 0, actorId, Self->SelfId(), nullptr, 0));
            }
        }
    };

    void TS3Manager::RunScannerActor() {
        Y_ABORT_UNLESS(!ScannerActorId);
        ScannerActorId = Self->Register(new TScannerActor(Self->SelfId(), WrapperId, BasePath, Bucket, Self->GetLogId()));
    }

    void TS3Manager::HandleScanner(TAutoPtr<IEventHandle> ev) {
        STRICT_STFUNC_BODY(
            hFunc(TEvScanFound, [&](TEvScanFound::TPtr ev) {
                auto& msg = *ev->Get();

                YDB_LOG_DEBUG("TEvScanFound received",
                    {"Marker", "BDTS17"},
                    {"Id", Self->GetLogId()},
                    {"IsFinal", msg.IsFinal},
                    {"Error", msg.Error},
                    {"KeysWithoutPrefix.size", msg.KeysWithoutPrefix.size()});

                Y_ABORT_UNLESS(ScannerActorId);
                Y_ABORT_UNLESS(ev->Sender == ScannerActorId);

                if (msg.Error) {
                    YDB_LOG_WARN("scanner error",
                        {"Marker", "BDTS14"},
                        {"Id", Self->GetLogId()},
                        {"Error", msg.Error});
                    // TODO(alexvru): restart scanner in some time
                }

                THashSet<TS3Locator> trash;

                const ui32 generation = Self->Executor()->Generation();

                for (const auto& [key, len] : msg.KeysWithoutPrefix) {
                    TString error;
                    if (const auto& locator = TS3Locator::FromObjectName(key, len, &error)) {
                        const bool useful = Self->Data->IsUseful(*locator);
                        const bool allow = locator->Generation < generation;
                        YDB_LOG_DEBUG("TEvScanFound: found key",
                            {"Marker", "BDTS15"},
                            {"Id", Self->GetLogId()},
                            {"Locator", *locator},
                            {"Useful", useful},
                            {"Allow", allow},
                            {"Error", error});
                        BDEV(BDEV35, "scan_S3_found_key", (BDT, Self->TabletID()), (Locator, *locator), (Useful, useful),
                            (Allow, allow));
                        if (!useful && allow) {
                            trash.insert(*locator);
                        }
                    } else {
                        YDB_LOG_WARN("TEvScanFound: incorrect key name",
                            {"Marker", "BDTS16"},
                            {"Id", Self->GetLogId()},
                            {"Key", key},
                            {"Len", len});
                    }
                }

                if (msg.IsFinal) {
                    ScannerActorId = {};
                }

                if (!trash.empty()) {
                    Self->Execute(std::make_unique<TTxProcessScannedKeys>(Self, std::move(trash)));
                } else if (!msg.IsFinal) {
                    TActivationContext::Send(new IEventHandle(TEvPrivate::EvScanContinue, 0, ev->Sender, ev->Recipient,
                        nullptr, 0));
                }
            })
        )
    }

} // NKikimr::NBlobDepot
