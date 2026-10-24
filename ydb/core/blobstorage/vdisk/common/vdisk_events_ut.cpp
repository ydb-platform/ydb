#include "vdisk_events.h"
#include "blobstorage_event_filter.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TVDiskEventScopes) {
        Y_UNIT_TEST(VRestorePart) {
            auto filter = std::make_shared<TEventFilter>();
            RegisterBlobStorageEventScopes(filter);
            const TScopeId systemScope(0, 1);
            const TScopeId peerSystemScope(0, 2);
            const TScopeId tenantScope(100, 200);
            const TScopeId otherTenantScope(100, 201);
            auto allowed = [&](ui32 type, const TScopeId& origin, const TScopeId& local) {
                IEventHandle ev(TActorId(), type, 0, TActorId(), TActorId(), nullptr, 0, origin, {});
                return filter->CheckIncomingEvent(ev, local);
            };

            UNIT_ASSERT(allowed(TEvBlobStorage::EvVRestorePart, systemScope, peerSystemScope));
            UNIT_ASSERT(allowed(TEvBlobStorage::EvVRestorePartResult, systemScope, peerSystemScope));
            UNIT_ASSERT(allowed(TEvBlobStorage::EvVRestorePart, tenantScope, systemScope));
            UNIT_ASSERT(allowed(TEvBlobStorage::EvVRestorePartResult, systemScope, tenantScope));
            UNIT_ASSERT(!allowed(TEvBlobStorage::EvVRestorePart, systemScope, tenantScope));
            UNIT_ASSERT(!allowed(TEvBlobStorage::EvVRestorePartResult, tenantScope, systemScope));
            UNIT_ASSERT(!allowed(TEvBlobStorage::EvVRestorePart, tenantScope, otherTenantScope));
            UNIT_ASSERT(!allowed(TEvBlobStorage::EvVRestorePartResult, tenantScope, otherTenantScope));
        }
    }

    Y_UNIT_TEST_SUITE(TVDiskEventsToString) {

        Y_UNIT_TEST(VPutWithoutPayload) {
            TEvBlobStorage::TEvVPut event;

            UNIT_ASSERT_STRING_CONTAINS(event.ToString(), "invalid payload count# 0");
        }

        Y_UNIT_TEST(VMultiPutWithoutPayload) {
            TEvBlobStorage::TEvVMultiPut event;
            event.Record.AddItems();

            UNIT_ASSERT_STRING_CONTAINS(event.ToString(), "missing payload");
        }

        Y_UNIT_TEST(VGetWithoutQuery) {
            TEvBlobStorage::TEvVGet event;

            UNIT_ASSERT(!event.ToString().empty());
        }

        Y_UNIT_TEST(VGetWithMalformedQueries) {
            TEvBlobStorage::TEvVGet event;
            event.Record.MutableRangeQuery();
            event.Record.AddExtremeQueries();

            UNIT_ASSERT_STRING_CONTAINS(event.ToString(), "<missing>");
        }
    }

} // NKikimr
