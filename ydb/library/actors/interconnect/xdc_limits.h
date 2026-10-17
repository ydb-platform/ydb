#pragma once

#include <ydb/library/actors/core/event_load.h>
#include <ydb/library/actors/core/event_pb.h>

#include <util/generic/utility.h>

namespace NActors {

    // Peer-supplied XDC section geometry is untrusted. Alignment 0 (none) is allowed:
    // (0 & (0 - 1)) == 0. A non-power-of-two alignment or a field above EventMaxByteSize
    // makes the receive-side allocation wrap.
    inline bool IsXdcSectionGeometryInRange(size_t size, size_t headroom, size_t tailroom, size_t alignment) noexcept {
        return size <= EventMaxByteSize
            && headroom <= EventMaxByteSize
            && tailroom <= EventMaxByteSize
            && alignment <= EventMaxByteSize
            && (alignment & (alignment - 1)) == 0;
    }

    inline bool FitsXdcDeclaredLimit(size_t size, size_t declaredSoFar, size_t limit) noexcept {
        return declaredSoFar <= limit && size <= limit - declaredSoFar;
    }

    inline bool CanAddXdcSection(size_t size, size_t headroom, size_t tailroom, size_t alignment,
            size_t declaredSoFar, size_t limit) noexcept {
        return IsXdcSectionGeometryInRange(size, headroom, tailroom, alignment)
            && FitsXdcDeclaredLimit(size, declaredSoFar, limit);
    }

    // True when this event's section table may be turned into XDC DECLARE records.
    inline bool IsXdcDeclareWithinLimit(const TEventSerializationInfo& info, size_t serializedSize,
            size_t maxEventSize) noexcept {
        const size_t limit = Min<size_t>(maxEventSize, EventMaxByteSize);
        if (serializedSize > limit) {
            return false;
        }
        size_t declared = 0;
        for (const auto& section : info.Sections) {
            if (!CanAddXdcSection(section.Size, section.Headroom, section.Tailroom, section.Alignment,
                    declared, limit)) {
                return false;
            }
            declared += section.Size;
        }
        return true;
    }

} // namespace NActors
