#include <yt/yt/client/chaos_client/replication_card.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NChaosClient {
namespace {

using namespace NTabletClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

inline constexpr auto NoneFetchOptions = TReplicationCardFetchOptions{
    .IncludeCoordinators = false,
    .IncludeProgress = false,
    .IncludeHistory = false,
    .IncludeReplicatedTableOptions = false,
};

TYsonString ToYson(const std::invocable<IYsonConsumer*> auto& func)
{
    TStringStream stream;
    TBufferedBinaryYsonWriter writer(&stream);
    std::invoke(func, &writer);
    writer.Flush();
    return TYsonString(stream.Str());
}

INodePtr FromYson(const TYsonString& yson)
{
    return ConvertToNode(yson);
}
////////////////////////////////////////////////////////////////////////////////

TEST(TReplicaInfoSerializationTest, Simple)
{
    auto check = [] (const TReplicaInfo& replicaInfo) {
        auto str = ToYson([&replicaInfo](IYsonConsumer* consumer)  {
            Serialize(replicaInfo, consumer, NoneFetchOptions);
        });

        TReplicaInfo resultReplicaInfo;
        Deserialize(resultReplicaInfo, FromYson(str));
        EXPECT_EQ(replicaInfo, resultReplicaInfo);
    };

    TReplicaInfo replicaInfo;
    replicaInfo.ClusterName = "seneca-myt";
    replicaInfo.ReplicaPath = "//home/path";
    replicaInfo.ContentType = ETableReplicaContentType::Data;
    replicaInfo.Mode = ETableReplicaMode::Async;
    replicaInfo.State = ETableReplicaState::Enabled;
    replicaInfo.EnableReplicatedTableTracker = true;

    check(replicaInfo);

    replicaInfo.EnableReplicatedTableTracker = false;
    check(replicaInfo);
}

////////////////////////////////////////////////////////////////////////////////

} // namepace
} // namepace NYT::NChaosClient
