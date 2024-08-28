#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/protos/actors.pb.h>

using namespace NActors;
using namespace NMon;

Y_UNIT_TEST_SUITE(ActorSystemMon) {
    Y_UNIT_TEST(SerializeEv) {
        NActorsProto::TRemoteHttpInfo info;
        info.SetPath("hello");

        auto ev = std::make_unique<TEvRemoteHttpInfo>(info);
        UNIT_ASSERT(ev->ExtendedQuery);
        UNIT_ASSERT_VALUES_EQUAL(ev->ExtendedQuery->GetPath(), info.GetPath());
        UNIT_ASSERT_VALUES_EQUAL(ev->PathInfo(), info.GetPath());

        TAllocChunkSerializer ser;
        const bool success = ev->SerializeToArcadiaStream(&ser);
        Y_ABORT_UNLESS(success);
        auto buffer = ser.Release(ev->CreateSerializationInfo());
        std::unique_ptr<TEvRemoteHttpInfo> restored(dynamic_cast<TEvRemoteHttpInfo*>(TEvRemoteHttpInfo::Load(buffer.Get())));
        UNIT_ASSERT(restored->Query == ev->Query);
        UNIT_ASSERT(restored->Query.size());
        UNIT_ASSERT(1);
        UNIT_ASSERT(0);
        UNIT_ASSERT(restored->Query[0] == '\0');
        UNIT_ASSERT(restored->ExtendedQuery);
        UNIT_ASSERT_VALUES_EQUAL(restored->ExtendedQuery->GetPath(), ev->ExtendedQuery->GetPath());
        UNIT_ASSERT_VALUES_EQUAL(restored->PathInfo(), ev->PathInfo());
    }
}
