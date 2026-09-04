#include "helpers.h"

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/yt/assert/assert.h>

#include <vector>

namespace NYT::NBus::NTests {

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray CreateMessage(int partCount, int partSize)
{
    auto data = TSharedMutableRef::Allocate(partCount * partSize);

    std::vector<TSharedRef> parts;
    for (int i = 0; i < partCount; ++i) {
        parts.push_back(data.Slice(i * partSize, (i + 1) * partSize));
    }

    return TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{});
}

TSharedRefArray Serialize(std::string str)
{
    return TSharedRefArray(TSharedRef::FromString(std::move(str)));
}

std::string Deserialize(TSharedRefArray message)
{
    YT_VERIFY(message.Size() == 1);
    const auto& part = message[0];
    return std::string(part.Begin(), part.Size());
}

int ReadActiveConnectionCount(const IBusServerPtr& server)
{
    auto countsYson = NYTree::SyncYPathGet(server->GetOrchidService(), "/connection_counts");
    auto counts = NYTree::ConvertTo<THashMap<std::string, int>>(countsYson);
    int total = 0;
    for (const auto& [_, count] : counts) {
        total += count;
    }
    return total;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests
