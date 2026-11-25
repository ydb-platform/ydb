#include "s3_router.h"
#include "config.h"
#include "events.h"
#include "s3_request_timer.h"
#include "s3_collection.h"
#include "types.h"

#include <ydb/core/util/backoff.h>
#include <ydb/core/wrappers/abstract.h>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {
    namespace {
        using TEvStorage = NWrappers::TEvExternalStorage;

        class TRetiredS3Sweep : public TActorBootstrapped<TRetiredS3Sweep> {
            struct TEvNextPage : TEventLocal<TEvNextPage, EventSpaceBegin(TEvents::ES_PRIVATE)> {};

            const TActorId Parent;
            const ui64 TabletId;
            TS3Listing Listing;
            const TString Bucket;
            NKikimrBlobDepot::TS3BackendSettings Settings;
            TActorId Router;
            bool MorePages = false;
            bool Incomplete = false;
            THashSet<TString> PendingDeletes;
            TS3RequestTimer Timer;
            TBackoff PageBackoff{TDuration::MilliSeconds(100), TDuration::Minutes(1)};

        public:
            TRetiredS3Sweep(TActorId parent, ui64 tabletId, const NKikimrBlobDepot::TBlobDepotConfig& config)
                : Parent(parent)
                , TabletId(tabletId)
                , Listing(MakeS3BasePath(config.GetS3BackendSettings().GetSettings(), config.GetName()))
                , Bucket(config.GetS3BackendSettings().GetSettings().GetBucket())
                , Settings(config.GetS3BackendSettings())
            {}

            void Bootstrap() {
                Router = Register(CreateBlobDepotS3Router(std::move(Settings), TabletId));
                Become(&TThis::StateWork);
                ListPage();
            }

            void ListPage() {
                Timer.SendWithTimeout(Router, new TEvStorage::TEvListObjectsRequest(Listing.MakeRequest(Bucket)));
            }

            void Handle(TEvStorage::TEvListObjectsResponse::TPtr ev) {
                Timer.Clear();
                const auto& msg = *ev->Get();
                if (!msg.IsSuccess()) {
                    return Finish(msg.GetError().GetMessage().c_str());
                }
                const auto& result = msg.Result.GetResult();
                MorePages = result.GetIsTruncated();
                Aws::S3::Model::Delete del;
                if (auto error = Listing.ReadPage(result, [&](const TString& key, TStringBuf name, ui64) {
                    // A slash-separated group name can nest another depot below this prefix.
                    // Only remove exact BlobDepot locator names at this level.
                    TString parseError;
                    if (TS3Locator::FromObjectName(TString(name), 0, &parseError)) {
                        del.AddObjects(Aws::S3::Model::ObjectIdentifier().WithKey(key));
                        PendingDeletes.insert(key);
                    }
                })) {
                    return Finish(error->c_str());
                }
                if (PendingDeletes.empty()) {
                    return Advance();
                }
                Timer.SendWithTimeout(Router, new TEvStorage::TEvDeleteObjectsRequest(Aws::S3::Model::DeleteObjectsRequest()
                    .WithBucket(Bucket).WithDelete(std::move(del))));
            }

            void Handle(TEvStorage::TEvDeleteObjectsResponse::TPtr ev) {
                Timer.Clear();
                const auto& msg = *ev->Get();
                if (!msg.IsSuccess()) {
                    return Finish(msg.GetError().GetMessage().c_str());
                }
                bool throttled = false;
                VisitS3DeleteResults(msg.Result.GetResult(), [&](const auto& key, ES3DeleteStatus status, TStringBuf) {
                    if (status == ES3DeleteStatus::Deleted || status == ES3DeleteStatus::Missing) {
                        PendingDeletes.erase(TString(key.data(), key.size()));
                    } else if (status == ES3DeleteStatus::SlowDown) {
                        throttled = true;
                    }
                });
                if (!PendingDeletes.empty()) {
                    // Complete the scan even when a subset of keys cannot be deleted.
                    Incomplete = true;
                    PendingDeletes.clear();
                }
                Advance(throttled);
            }

            void Advance(bool throttled = false) {
                if (!MorePages) {
                    Finish(Incomplete ? "incomplete DELETE response for retired namespace" : nullptr);
                } else if (throttled) {
                    // Leave failed keys for the next sweep and pace subsequent pages.
                    Schedule(PageBackoff.Next(), new TEvNextPage);
                } else {
                    PageBackoff.Reset();
                    ListPage();
                }
            }

            void Finish(const char* error = nullptr) {
                if (error) {
                    YDB_LOG_WARN("Retired BlobDepot S3 cleanup failed", {"tabletId", TabletId}, {"error", error});
                }
                Send(Parent, new TEvBlobDepot::TEvRetiredS3SweepComplete(!error));
                PassAway();
            }

            void PassAway() override {
                Send(Router, new TEvents::TEvPoison);
                TActorBootstrapped::PassAway();
            }

            STRICT_STFUNC(StateWork,
                hFunc(TEvStorage::TEvListObjectsResponse, Handle)
                hFunc(TEvStorage::TEvDeleteObjectsResponse, Handle)
                cFunc(TEvNextPage::EventType, ListPage)
                cFunc(TEvents::TSystem::Poison, PassAway)
                cFunc(TEvents::TSystem::Undelivered, [&] { Finish("S3 request undelivered"); })
                cFunc(TEvents::TSystem::Wakeup, [&] {
                    if (Timer.OnWakeup()) {
                        Finish("S3 request timed out");
                    }
                })
            )
        };
    }

    IActor* CreateRetiredS3Sweep(TActorId parent, ui64 tabletId, const NKikimrBlobDepot::TBlobDepotConfig& config) {
        return new TRetiredS3Sweep(parent, tabletId, config);
    }
}
