#pragma once

#include <util/generic/fwd.h>
#include <util/generic/ptr.h>
#include <util/stream/fwd.h>

/**
 * Convenience function for opening standard input.
 *
 * @param bufSize                       Buffer size.
 */
THolder<IInputStream> OpenStdin(size_t bufSize = 1 << 13);

