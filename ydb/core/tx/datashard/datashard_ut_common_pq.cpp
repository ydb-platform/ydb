#include "datashard_ut_common_pq.h"

#include <ydb/core/persqueue/user_info.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

namespace NKikimr {

    using namespace NYdb::NPersQueue;
    using namespace NYdb::NDataStreams::V1;

    ui64 ResolvePqTablet(TTestActorRuntime& runtime, const TActorId& sender, const TString& path, ui32 partitionId) {
        auto streamDesc = Ls(runtime, sender, path);

        const auto& streamEntry = streamDesc->ResultSet.at(0);
        UNIT_ASSERT(streamEntry.ListNodeEntry);

        const auto& children = streamEntry.ListNodeEntry->Children;
        UNIT_ASSERT_VALUES_EQUAL(children.size(), 1);

        auto topicDesc = Navigate(runtime, sender, JoinPath(ChildPath(SplitPath(path), children.at(0).Name)),
            NSchemeCache::TSchemeCacheNavigate::EOp::OpTopic);

        const auto& topicEntry = topicDesc->ResultSet.at(0);
        UNIT_ASSERT(topicEntry.PQGroupInfo);

        const auto& pqDesc = topicEntry.PQGroupInfo->Description;
        for (const auto& partition : pqDesc.GetPartitions()) {
            if (partitionId == partition.GetPartitionId()) {
                return partition.GetTabletId();
            }
        }

        UNIT_ASSERT_C(false, "Cannot find partition: " << partitionId);
        return 0;
    }

    TVector<std::pair<TString, TString>> GetPqRecords(TTestActorRuntime& runtime, const TActorId& sender, const TString& path, ui32 partitionId) {
        NKikimrClient::TPersQueueRequest request;
        request.MutablePartitionRequest()->SetTopic(path);
        request.MutablePartitionRequest()->SetPartition(partitionId);

        auto& cmd = *request.MutablePartitionRequest()->MutableCmdRead();
        cmd.SetClientId(NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER);
        cmd.SetCount(10000);
        cmd.SetOffset(0);
        cmd.SetReadTimestampMs(0);
        cmd.SetExternalOperation(true);

        auto req = MakeHolder<TEvPersQueue::TEvRequest>();
        req->Record = std::move(request);
        ForwardToTablet(runtime, ResolvePqTablet(runtime, sender, path, partitionId), sender, req.Release());

        auto resp = runtime.GrabEdgeEventRethrow<TEvPersQueue::TEvResponse>(sender);
        UNIT_ASSERT(resp);

        TVector<std::pair<TString, TString>> result;
        for (const auto& r : resp->Get()->Record.GetPartitionResponse().GetCmdReadResult().GetResult()) {
            const auto data = NKikimr::GetDeserializedData(r.GetData());
            result.emplace_back(r.GetPartitionKey(), data.GetData());
        }

        return result;
    }

} // namespace NKikimr
