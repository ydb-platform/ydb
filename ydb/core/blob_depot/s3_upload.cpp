#include "s3.h"
#include "config.h"
#include "coro_tx.h"
#include "s3_request_timer.h"
#include "s3_router_events.h"
#include "s3_error.h"
#include "schema.h"

#include <ydb/core/wrappers/abstract.h>

#include <util/generic/hash_multi_map.h>

#include <atomic>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;
    using ECoolingPutOutcome = TBlobDepot::TData::ECoolingPutOutcome;
    using TEvExternalStorage = NWrappers::TEvExternalStorage;
    using NTabletFlatExecutor::TTransactionContext;

    struct TS3Manager::TEvUploadResult : TEventLocal<TEvUploadResult, TEvPrivate::EvUploadResult> {
        TString Error;
        std::optional<size_t> FailedSource;
        ECoolingPutOutcome PutOutcome;

        TEvUploadResult(TString error, std::optional<size_t> failedSource, ECoolingPutOutcome putOutcome)
            : Error(std::move(error))
            , FailedSource(failedSource)
            , PutOutcome(putOutcome)
        {}
    };

    class TS3Manager::TUploaderActor : public TActorBootstrapped<TUploaderActor> {
        static constexpr size_t MaxReadsInFlight = 16;

        struct TRead {
            std::variant<TLogoBlobID, TS3Locator> Source;
            ui32 Begin;
            ui32 End;
            ui32 OutputOffset;
            size_t ItemIndex;

            ui32 Size() const { return End - Begin; }
        };

        const TActorId Owner;
        const TActorId WrapperId;
        const TString BasePath;
        const TString Bucket;
        const TIntrusivePtr<TTabletStorageInfo> Info;
        const TS3Locator Locator;
        const TDuration PutTimeout;
        const TString ObjectName;
        std::vector<TRead> Reads;
        size_t NextRead = 0;
        std::map<size_t, TMonotonic> ReadDeadlines;
        const std::shared_ptr<std::atomic_bool> PutRetried = std::make_shared<std::atomic_bool>(false);
        TS3RequestTimer Timer;
        TString Buffer;
        THashMultiMap<std::tuple<TString, ui64, ui64>, size_t> PendingS3Reads;

    public:
        TUploaderActor(TActorId owner, TActorId wrapperId, TString basePath, TString bucket,
                TIntrusivePtr<TTabletStorageInfo> info, TS3Locator locator, const TPack& pack, TDuration putTimeout)
            : Owner(owner)
            , WrapperId(wrapperId)
            , BasePath(std::move(basePath))
            , Bucket(std::move(bucket))
            , Info(std::move(info))
            , Locator(locator)
            , PutTimeout(putTimeout)
            , ObjectName(Locator.MakeObjectName(BasePath))
        {
            for (size_t itemIndex = 0; itemIndex < pack.Items.size(); ++itemIndex) {
                const auto& item = pack.Items[itemIndex];
                ui32 offset = item.Begin;
                const bool valid = EnumerateValueChainPayload(item.SourceChain, Info->TabletID,
                    [&](auto source, ui32 begin, ui32 end) {
                        Reads.push_back({source, begin, end, offset, itemIndex});
                        offset += end - begin;
                    });
                Y_ABORT_UNLESS(valid && offset == item.End);
            }
        }

        void Bootstrap() {
            Buffer = TString::Uninitialized(Locator.Len);
            Become(&TThis::StateWork);
            PumpReads();
        }

    private:
        void PumpReads() {
            const TMonotonic deadline = TActivationContext::Monotonic() + TS3RequestTimer::Timeout;
            while (NextRead < Reads.size() && ReadDeadlines.size() < MaxReadsInFlight) {
                const size_t index = NextRead++;
                const auto& read = Reads[index];
                ReadDeadlines.emplace(index, deadline);
                if (const auto *id = std::get_if<TLogoBlobID>(&read.Source)) {
                    const ui32 groupId = Info->GroupFor(id->Channel(), id->Generation());
                    SendToBSProxy(SelfId(), groupId, new TEvBlobStorage::TEvGet(*id, read.Begin, read.Size(),
                        TActivationContext::Now() + TS3RequestTimer::Timeout, NKikimrBlobStorage::EGetHandleClass::LowRead), index);
                } else {
                    const TString name = std::get<TS3Locator>(read.Source).MakeObjectName(BasePath);
                    const ui64 inclusiveEnd = read.End - 1;
                    PendingS3Reads.emplace(std::tuple{name, ui64(read.Begin), inclusiveEnd}, index);
                    auto request = Aws::S3::Model::GetObjectRequest()
                        .WithBucket(Bucket)
                        .WithKey(name)
                        .WithRange(TStringBuilder() << "bytes=" << read.Begin << '-' << inclusiveEnd);
                    Send(WrapperId, new TEvExternalStorage::TEvGetObjectRequest(request), IEventHandle::FlagTrackDelivery);
                }
            }

            if (!ReadDeadlines.empty()) {
                // Reads are dispatched in index order and receive the same timeout.
                Timer.Arm(ReadDeadlines.begin()->second);
                return;
            }

            const auto putDeadline = TActivationContext::Monotonic() + PutTimeout;
            Timer.Arm(putDeadline);
            auto request = Aws::S3::Model::PutObjectRequest().WithBucket(Bucket).WithKey(ObjectName);
            // Queued SDK work can outlive the tablet. Stop it when its uploader has exited.
            const std::weak_ptr<std::atomic_bool> put = PutRetried;
            request.SetContinueRequestHandler([put](const auto*) { return !put.expired(); });
            // A final 4xx only rules out the last attempt. An earlier request may still reach S3.
            request.SetRequestRetryHandler([put](const auto&) {
                if (const auto retried = put.lock()) {
                    retried->store(true);
                }
            });
            Send(WrapperId, new TEvExternalStorage::TEvPutObjectRequest(request, std::move(Buffer),
                std::make_shared<TS3PutContext>(putDeadline, TS3PutContext::ESource::Cooling)),
                IEventHandle::FlagTrackDelivery);
        }

        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
            const size_t index = ev->Cookie;
            if (!ReadDeadlines.contains(index)) {
                return;
            }
            const auto& read = Reads[index];
            auto& result = *ev->Get();
            if (result.Status != NKikimrProto::OK || result.ResponseSz != 1 ||
                    result.Responses[0].Status != NKikimrProto::OK ||
                    result.Responses[0].Id != std::get<TLogoBlobID>(read.Source)) {
                return Finish(TStringBuilder() << "local read failed: " << result.ToString(), read.ItemIndex);
            }
            auto& rope = result.Responses[0].Buffer;
            if (rope.GetSize() != read.Size()) {
                return Finish("local read returned unexpected length", read.ItemIndex);
            }
            const bool copied = rope.ExtractFrontPlain(Buffer.Detach() + read.OutputOffset, read.Size());
            Y_ABORT_UNLESS(copied);
            ReadDeadlines.erase(index);
            PumpReads();
        }

        void Handle(TEvExternalStorage::TEvGetObjectResponse::TPtr ev) {
            auto& response = *ev->Get();
            const auto [begin, end] = response.GetReadInterval();
            const auto it = response.Key ? PendingS3Reads.find(std::tuple{*response.Key, begin, end}) : PendingS3Reads.end();
            if (!response.IsSuccess()) {
                const bool missing = IsS3MissingObject(response.GetError());
                const auto failed = missing && it != PendingS3Reads.end()
                    ? std::optional(Reads[it->second].ItemIndex) : std::nullopt;
                return Finish(TStringBuilder() << "S3 read failed: " << response.GetError().GetMessage().c_str(), failed);
            }
            if (it == PendingS3Reads.end()) {
                return Finish("S3 read returned an unexpected object or range");
            }
            const size_t index = it->second;
            const auto& read = Reads[index];
            if (response.Body.size() != read.Size()) {
                return Finish("S3 read returned unexpected length", read.ItemIndex);
            }
            memcpy(Buffer.Detach() + read.OutputOffset, response.Body.data(), response.Body.size());
            PendingS3Reads.erase(it);
            ReadDeadlines.erase(index);
            PumpReads();
        }

        void Handle(TEvExternalStorage::TEvPutObjectResponse::TPtr ev) {
            if (ev->Get()->IsSuccess()) {
                Finish({}, {}, PutRetried->load() ? ECoolingPutOutcome::MayArriveLate : ECoolingPutOutcome::Sent);
            } else {
                const auto& error = ev->Get()->GetError();
                const int status = static_cast<int>(error.GetResponseCode());
                const bool ambiguous = PutRetried->load() || status <= 0 || status >= 500 || status == 408;
                Finish(TStringBuilder() << "S3 upload failed: " << error.GetMessage().c_str(), {},
                    ambiguous ? ECoolingPutOutcome::MayArriveLate : ECoolingPutOutcome::Sent);
            }
        }

        void Finish(TString error, std::optional<size_t> failedSource = std::nullopt,
                ECoolingPutOutcome outcome = ECoolingPutOutcome::NotSent) {
            Send(Owner, new TEvUploadResult(std::move(error), failedSource, outcome));
            PassAway();
        }

        STRICT_STFUNC(StateWork,
            hFunc(TEvBlobStorage::TEvGetResult, Handle)
            hFunc(TEvExternalStorage::TEvGetObjectResponse, Handle)
            hFunc(TEvExternalStorage::TEvPutObjectResponse, Handle)
            hFunc(TEvents::TEvUndelivered, HandleUndelivered)
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout)
            cFunc(TEvents::TSystem::Poison, PassAway)
        )

        void HandleUndelivered(TEvents::TEvUndelivered::TPtr ev) {
            const auto& msg = *ev->Get();
            const bool putMayHaveArrived = msg.SourceType == TEvExternalStorage::TEvPutObjectRequest::EventType && msg.Unsure;
            Finish("cooling I/O request was undelivered", {},
                putMayHaveArrived ? ECoolingPutOutcome::MayArriveLate : ECoolingPutOutcome::NotSent);
        }

        void HandleTimeout() {
            if (!Timer.OnWakeup()) {
                return;
            }
            if (ReadDeadlines.empty()) {
                return Finish("cooling S3 PUT deadline exceeded", {}, ECoolingPutOutcome::MayArriveLate);
            }
            std::optional<size_t> failedSource;
            const auto& read = Reads[ReadDeadlines.begin()->first];
            if (std::holds_alternative<TLogoBlobID>(read.Source)) {
                failedSource = read.ItemIndex;
            }
            Finish("cooling source read deadline exceeded", failedSource);
        }
    };

    class TS3Manager::TTxStageCoolingPack : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const TS3Locator Locator;
        TPack Pack;

    public:
        TTxStageCoolingPack(TBlobDepot *self, TS3Locator locator, TPack pack)
            : TTransactionBase(self)
            , Locator(locator)
            , Pack(std::move(pack))
        {}

        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_STAGE_COOLING_PACK; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb(txc.DB).Table<Schema::CoolingS3Puts>().Key(Locator.Generation, Locator.KeyId)
                .Update(NIceDb::TUpdate<Schema::CoolingS3Puts::Len>(Locator.Len),
                    NIceDb::TUpdate<Schema::CoolingS3Puts::RetireAt>(Self->S3Manager->GetPutWatchRetireAt()));
            return true;
        }

        void Complete(const TActorContext&) override {
            auto& manager = *Self->S3Manager;
            const TActorId actorId = Self->Register(new TUploaderActor(Self->SelfId(), manager.WrapperId,
                manager.BasePath, manager.Bucket, Self->Info(), Locator, Pack,
                GetS3PutTimeout(Self->Config.GetS3BackendSettings().GetSettings(), AppData()->AwsClientConfig)));
            manager.ActiveUploaders.emplace(actorId, std::make_pair(Locator, std::move(Pack)));
        }
    };

    class TS3Manager::TTxPublishCoolingPack : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const TS3Locator Locator;
        const TPack Pack;
        const ECoolingPutOutcome PutOutcome;
        bool Published = false;

    public:
        TTxPublishCoolingPack(TBlobDepot *self, TS3Locator locator, TPack pack, ECoolingPutOutcome putOutcome)
            : TTransactionBase(self)
            , Locator(locator)
            , Pack(std::move(pack))
            , PutOutcome(putOutcome)
        {}

        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_PUBLISH_COOLING_PACK; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            for (const auto& item : Pack.Items) {
                Y_ABORT_UNLESS(item.Begin < item.End && item.End <= Locator.Len);
                TValueChain replacement;
                auto* link = replacement.Add();
                Locator.ToProto(link->MutableS3Locator());
                link->SetSubrangeBegin(item.Begin);
                link->SetSubrangeEnd(item.End);
                Published |= Self->Data->ReplaceValueForCooling(item.Key, item.ValueVersion, item.SourceChain,
                    std::move(replacement), txc, this);
            }
            Self->Data->FinishCoolingPut(Locator, PutOutcome, Published, txc, this);
            return true;
        }

        void Complete(const TActorContext&) override {
            Self->Data->CommitTrash(this);
            auto& manager = *Self->S3Manager;
            manager.FinishCoolingPack(Locator, Pack);
        }
    };

    void TS3Manager::MaybeScheduleCooling() {
        if (Self->CoolingIndex && Self->Data->IsLoaded() && Self->CoolingIndex->HasWork(Self->GetHotWriteCoolingBytesNeeded())) {
            ScheduleCooling(TActivationContext::Monotonic());
        }
    }

    void TS3Manager::ScheduleCooling(TMonotonic when) {
        when = Max(when, CoolingBackoffUntil);
        if (when <= TActivationContext::Monotonic()) {
            if (!std::exchange(CoolingRunScheduled, true)) {
                Self->Send(new IEventHandle(TEvPrivate::EvRunCooling, 0,
                    Self->SelfId(), {}, nullptr, 0));
            }
        } else {
            CoolingTimer.Schedule(when, Self->SelfId(), TEvPrivate::EvCoolingTimer);
        }
    }

    void TS3Manager::HandleCoolingWakeup(TAutoPtr<IEventHandle> ev) {
        if (ev->GetTypeRewrite() == TEvPrivate::EvRunCooling) {
            CoolingRunScheduled = false;
        } else if (!CoolingTimer.Consume(ev->Cookie)) {
            return;
        }
        if (TActivationContext::Monotonic() < CoolingBackoffUntil) {
            ScheduleCooling(CoolingBackoffUntil);
        } else {
            RunCooling();
        }
    }

    void TS3Manager::RunCooling() {
        if (!Self->IsDataCoolingEnabled() || !Self->Data->IsLoaded()) {
            return;
        }

        const TMonotonic now = TActivationContext::Monotonic();
        const ui32 concurrency = Min(UploadPutsInFlight, MaxWritesInFlight());
        if (CoolingObjectsInFlight.size() >= concurrency) {
            return;
        }
        auto pack = Self->CoolingIndex->SelectPack(Self->GetHotWriteCoolingBytesNeeded(), now,
            [this](TMonotonic when) { ScheduleCooling(when); });
        if (pack.Items.empty()) {
            return;
        }
        const auto locator = AllocateS3Locator(pack.Size);
        CoolingObjectsInFlight.insert(locator.GetObjectKey());
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_COOLING_UPLOADS_IN_FLIGHT] =
            CoolingObjectsInFlight.size();
        Self->Execute(std::make_unique<TTxStageCoolingPack>(Self, locator, std::move(pack)));
        // Reenter the mailbox before filling another upload slot.
        if (CoolingObjectsInFlight.size() < concurrency) {
            MaybeScheduleCooling();
        }
    }

    void TS3Manager::HandleUploadResult(TAutoPtr<IEventHandle> ev) {
        const auto it = ActiveUploaders.find(ev->Sender);
        if (it == ActiveUploaders.end()) {
            return;
        }
        auto [locator, pack] = std::move(it->second);
        ActiveUploaders.erase(it);
        const auto& result = *ev->Get<TEvUploadResult>();
        if (result.Error) {
            YDB_LOG_WARN("Cooling pack failed",
                {"id", Self->GetLogId()},
                {"locator", locator},
                {"error", result.Error});
            Self->Execute(std::make_unique<TCoroTx>(Self, TTokens{{Self->Token}},
                [this, locator, outcome = result.PutOutcome](TCoroTx::TContextBase& tx) {
                    Self->Data->FinishCoolingPut(locator, outcome, false, *tx, &tx);
                    tx.FinishTx();
                    Self->Data->CommitTrash(&tx);
                }));
            FinishCoolingPack(locator, pack, &result);
        } else {
            auto& counters = Self->TabletCounters->Cumulative();
            counters[NKikimrBlobDepot::COUNTER_S3_PUTS_OK].Increment(1);
            counters[NKikimrBlobDepot::COUNTER_S3_PUTS_BYTES].Increment(locator.Len);
            Self->Execute(std::make_unique<TTxPublishCoolingPack>(Self, locator, std::move(pack), result.PutOutcome));
        }
    }

    void TS3Manager::FinishCoolingPack(TS3Locator locator, const TPack& pack, const TEvUploadResult *failure) {
        CoolingObjectsInFlight.erase(locator.GetObjectKey());
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_COOLING_UPLOADS_IN_FLIGHT] =
            CoolingObjectsInFlight.size();

        const auto now = TActivationContext::Monotonic();
        const auto failedSource = failure ? failure->FailedSource : std::nullopt;
        Self->CoolingIndex->FinishPack(pack, failedSource, now);
        if (failure) {
            Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_COOLING_UPLOAD_FAILURES].Increment(1);
            if (!failedSource) {
                CoolingBackoffUntil = Max(CoolingBackoffUntil, now + CoolingBackoff.Next());
            }
        } else if (now >= CoolingBackoffUntil) {
            CoolingBackoff.Reset();
        }
        MaybeScheduleCooling();
    }

    bool TS3Manager::CanCollectScannedObject(const TS3Locator& locator) const {
        const ui32 generation = Self->Executor()->Generation();
        return OrphanScanEnabled && (locator.Generation < generation ||
            (Self->IsDataCoolingEnabled() && locator.Generation == generation && locator.KeyId < NextKeyId &&
                !CoolingObjectsInFlight.contains(locator.GetObjectKey()))) &&
            !Self->Data->IsS3ObjectReferenced(locator);
    }

} // NKikimr::NBlobDepot
