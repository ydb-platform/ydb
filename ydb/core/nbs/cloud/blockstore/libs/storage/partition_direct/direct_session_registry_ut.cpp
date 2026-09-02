#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/direct_session_registry.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/testlib/fake_direct_session.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

using namespace NActors;
using namespace NTestLib;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] std::unique_ptr<TTestActorRuntime> MakeRuntime()
{
    auto runtime = std::make_unique<TTestActorRuntime>();
    runtime->Initialize(TTestActorRuntime::TEgg{
        .App0 = new NKikimr::
            TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr),
        .Opaque = nullptr,
        .KeyConfigGenerator = nullptr,
        .Icb = {},
        .Dcb = {}});
    return runtime;
}

[[nodiscard]] TSessionEntry MakeEntry(
    TTestActorRuntime* runtime,
    std::shared_ptr<IDirectSession> session)
{
    return MakeSessionEntry(runtime->GetActorSystem(0), std::move(session));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDirectSessionRegistryTest)
{
    Y_UNIT_TEST(SetGetResetClear)
    {
        auto runtime = MakeRuntime();

        TDirectSessionRegistry registry;
        UNIT_ASSERT(!registry.Get(1));

        auto session =
            std::make_shared<TFakeDirectSession>(runtime->GetActorSystem(0));
        auto entry = MakeEntry(runtime.get(), session);
        registry.Set(1, entry);
        auto got = registry.Get(1);
        UNIT_ASSERT(got);
        UNIT_ASSERT_EQUAL(got.Session, session);
        UNIT_ASSERT(got.Router);
        UNIT_ASSERT(got.ReplyActorId);
        UNIT_ASSERT(!registry.Get(2));

        registry.Reset(1);
        UNIT_ASSERT(!registry.Get(1));

        registry.Set(1, MakeEntry(runtime.get(), session));
        registry.Set(2, MakeEntry(runtime.get(), session));
        registry.Clear();
        UNIT_ASSERT(!registry.Get(1));
        UNIT_ASSERT(!registry.Get(2));
    }

    Y_UNIT_TEST(SetEmptyResets)
    {
        auto runtime = MakeRuntime();

        TDirectSessionRegistry registry;
        auto session =
            std::make_shared<TFakeDirectSession>(runtime->GetActorSystem(0));
        registry.Set(7, MakeEntry(runtime.get(), session));
        registry.Set(7, TSessionEntry{});
        UNIT_ASSERT(!registry.Get(7));
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
