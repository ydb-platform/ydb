#pragma once

#include "log.h"
#include "log_backend.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <functional>

namespace NYdb::NTPCC {

ftxui::Component LogsScroller(TLogBackendWithCapture& logBackend);

} // namespace NYdb::NTPCC
