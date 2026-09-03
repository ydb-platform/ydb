#pragma once

#include <cstddef>

namespace NYdb::inline Dev::NTopic {

// Shared limit for StreamWrite / BeginPublication ext_publication_id (and related strings).
constexpr size_t MaxDeferredPublishExtIdLength = 2048;

} // namespace NYdb::NTopic
