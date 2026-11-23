#pragma once

// Arcadia's protobuf patches this alias into google/protobuf/stubs/common.h.
// The vanilla runtime carries no such patch, so declare it here instead; the
// declaration is idempotent and stays correct against either runtime.

namespace google {
    namespace protobuf {
    }
} // namespace google

namespace NProtoBuf {
    using namespace google;
    using namespace google::protobuf;
} // namespace NProtoBuf
