#pragma once

// taken from https://github.com/ArthurSonzogni/git-tui/blob/master/src/scroller.hpp

#include <ftxui/component/component.hpp>

#include "ftxui/component/component_base.hpp"  // for Component

namespace ftxui {
Component Scroller(Component child, const char* window_title);
}

// Copyright 2021 Arthur Sonzogni. All rights reserved.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.