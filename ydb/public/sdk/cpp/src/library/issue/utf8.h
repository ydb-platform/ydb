#pragma once

#include <string_view>

namespace NYdb::NIssue {

bool IsUtf8(const std::string_view& str);

}
