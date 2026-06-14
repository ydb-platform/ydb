#pragma once

#include <library/cpp/yt/memory/ref.h>

#include <util/generic/string.h>

namespace NYT::NBus::NTests {

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray CreateMessage(int partCount, int partSize = 1);
TSharedRefArray Serialize(std::string str);
std::string Deserialize(TSharedRefArray message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests
