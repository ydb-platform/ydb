#pragma once

#include <yt/yt/core/bus/public.h>

#include <library/cpp/yt/memory/ref.h>

#include <util/generic/string.h>


namespace NYT::NBus::NTests {

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray CreateMessage(int partCount, int partSize = 1);
TSharedRefArray Serialize(std::string str);
std::string Deserialize(TSharedRefArray message);

//! Reads `/connection_counts` from the server's orchid and returns the sum
//! across all networks.
int ReadActiveConnectionCount(const IBusServerPtr& server);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests
