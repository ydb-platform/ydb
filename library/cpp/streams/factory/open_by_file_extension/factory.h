#pragma once

#include <util/generic/fwd.h>
#include <util/generic/ptr.h>
#include <util/stream/fwd.h>
#include <util/stream/output.h>  // IOutputStream type must be complete to destroy in THolder below

/**
 * Convenience function for opening an input file passed as one of program
 * arguments. Handles `-` as standard input, and creates a decompressing stream
 * for `gz` and `bz2` files.
 *
 * @param url                           File to open.
 */
THolder<IInputStream> OpenInput(const TString& url);

enum class ECompression {
    L1 = 1,
    L2,
    L3,
    L4,
    L5,
    L6,
    L7,
    L8,
    L9,
    FAST = 1,
    DEFAULT = 6,
    BEST = 9
};

/**
 * Convenience function for opening an output file passed as one of program
 * arguments. Handles `-` as standard output, and creates a compressing stream
 * for `gz` and `bz2` files with given compression level and buffer size.
 *
 * @param url                           File to open.
 * @param compression_level             Compression level.
 * @param buflen                        Compression buffer length in bytes.
 */
THolder<IOutputStream> OpenOutput(const TString& url, ECompression compressionLevel, size_t buflen);

inline THolder<IOutputStream> OpenOutput(const TString& url, ECompression compressionLevel) {
    return ::OpenOutput(url, compressionLevel, 8 * 1024);
}

inline THolder<IOutputStream> OpenOutput(const TString& url) {
    return ::OpenOutput(url, ECompression::DEFAULT);
}

