#include "restore_request.h"

#include "base_test_fixture.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRestoreRequestTest)
{
    Y_UNIT_TEST_F(ShouldRestoreQuorumFromAddedHost, TBaseFixture)
    {
        Init();
        VChunkConfig.AppendHost();
        UNIT_ASSERT_VALUES_EQUAL("", VChunkConfig.DemoteHost(THostIndex{2}));
        VChunkConfig.PromoteHost(THostIndex{5});
        UNIT_ASSERT(VChunkConfig.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(
            "[H0,H1,H5]",
            VChunkConfig.GetDesiredPBuffers().Print());
        DirtyMap->UpdateConfig(VChunkConfig, true);

        const auto hosts = THostMask::MakeAll(VChunkConfig.GetHostCount());
        const auto key = MakeKey(5);
        const auto range = TBlockRange16::WithLength(10, 1);
        TMap<THostIndex, TPromise<TListPBufferResponse>> promises;
        DirectBlockGroup->ListPBuffersHandler = [&promises](THostIndex host)
        {
            auto promise = NewPromise<TListPBufferResponse>();
            promises.emplace(host, promise);
            return promise.GetFuture();
        };

        const auto request = std::make_shared<TRestoreRequestExecutor>(
            Runtime->GetActorSystem(0),
            DirectBlockGroup,
            hosts);
        const auto future = request->GetFuture();
        request->Run();
        UNIT_ASSERT_VALUES_EQUAL(6, promises.size());

        for (const auto host: hosts) {
            TListPBufferResponse response;
            if (VChunkConfig.GetDesiredPBuffers().Get(host)) {
                response.Meta.push_back(
                    {.VChunkIndex = VChunkConfig.GetVChunkIndex(),
                     .PBufferKey = key,
                     .Range = range});
            }
            if (host == THostIndex{5}) {
                UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
            }
            promises.at(host).SetValue(std::move(response));
        }

        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(6, response.Meta.size());
        for (const auto& [host, entries]: response.Meta) {
            for (const auto& entry: entries) {
                DirtyMap->RestorePBuffer(entry.PBufferKey, entry.Range, host);
            }
        }
        const auto readHint = DirtyMap->MakeReadHint(range);
        UNIT_ASSERT_VALUES_EQUAL(
            "1:5{[H0,H1,H5][10..10][0..0]};",
            readHint.DebugPrint());
    }

    Y_UNIT_TEST_F(ShouldListNonContiguousHosts, TBaseFixture)
    {
        Init();
        const auto hosts = THostMask::MakeMask({0, 1, 3, 5});
        THostMask requested;
        DirectBlockGroup->ListPBuffersHandler = [&requested](THostIndex host)
        {
            requested.Set(host);
            return MakeFuture(TListPBufferResponse{});
        };
        const auto request = std::make_shared<TRestoreRequestExecutor>(
            Runtime->GetActorSystem(0),
            DirectBlockGroup,
            hosts);
        const auto future = request->GetFuture();
        request->Run();

        UNIT_ASSERT_VALUES_EQUAL(hosts.Print(), requested.Print());
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, future.GetValue().Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(hosts.Count(), future.GetValue().Meta.size());
    }

    Y_UNIT_TEST_F(ShouldPropagateAddedHostListError, TBaseFixture)
    {
        Init();
        auto addedHost = NewPromise<TListPBufferResponse>();
        DirectBlockGroup->ListPBuffersHandler = [addedHost](THostIndex host)
        {
            return host == THostIndex{5} ? addedHost.GetFuture()
                                         : MakeFuture(TListPBufferResponse{});
        };
        const auto request = std::make_shared<TRestoreRequestExecutor>(
            Runtime->GetActorSystem(0),
            DirectBlockGroup,
            THostMask::MakeAll(6));
        const auto future = request->GetFuture();
        request->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        addedHost.SetValue({.Error = MakeError(E_FAIL)});
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, future.GetValue().Error.GetCode());
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
