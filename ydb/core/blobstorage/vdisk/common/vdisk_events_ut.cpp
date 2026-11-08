#include "vdisk_events.h"
#include "blobstorage_event_filter.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBlobStorageEventScopes) {
        Y_UNIT_TEST(ExternalDataSourcePoolResultCanReachTenantSchemeShard) {
            auto filter = std::make_shared<NActors::TEventFilter>();
            RegisterBlobStorageEventScopes(filter);
            const NActors::TScopeId system{0, 1};
            const NActors::TScopeId tenant{10, 20};
            const auto allowed = [&](ui32 eventType, NActors::TScopeId from, NActors::TScopeId to) {
                NActors::IEventHandle incoming({}, eventType, 0, {}, {}, nullptr, 0, from, {});
                return filter->CheckIncomingEvent(incoming, to);
            };

            UNIT_ASSERT(allowed(TEvBlobStorage::EvExternalDataSourcePoolResult, system, tenant));
            UNIT_ASSERT(allowed(TEvBlobStorage::EvExternalDataSourcePoolResult, system, system));
            UNIT_ASSERT(!allowed(TEvBlobStorage::EvExternalDataSourcePoolResult, tenant, system));
            UNIT_ASSERT(!allowed(TEvBlobStorage::EvExternalDataSourcePoolResult, tenant, tenant));
            UNIT_ASSERT(!allowed(TEvBlobStorage::EvControllerConfigResponse, system, tenant));
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
