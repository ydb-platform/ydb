#include "helpers.h"

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

TSharedRefArray Serialize(TString str)
{
    return TSharedRefArray(TSharedRef::FromString(std::move(str)));
}

TString Deserialize(TSharedRefArray message)
{
    YT_VERIFY(message.Size() == 1);
    const auto& part = message[0];
    return TString(part.Begin(), part.Size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests
