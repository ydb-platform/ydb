#pragma once

#include <string>
#include <string_view>

namespace NYdb::inline Dev::NTopic {

    std::string FullTopicPath(const std::string& dbPath, std::string_view topic);

} // namespace NYdb::inline Dev::NTopic
