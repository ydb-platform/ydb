#pragma once

#include <util/generic/string.h>

#include <cstddef>

namespace NBus {
    namespace NPrivate {
        struct TMessageCounter {
            size_t BytesData;
            size_t BytesNetwork;
            size_t Count;
            size_t CountCompressed;
            size_t CountCompressionRequests; // reader only

            void AddMessage(size_t bytesData, size_t bytesCompressed, bool Compressed, bool compressionRequested) {
                BytesData += bytesData;
                BytesNetwork += bytesCompressed;
                Count += 1;
                if (Compressed) {
                    CountCompressed += 1;
                }
                if (compressionRequested) {
                    CountCompressionRequests += 1;
                }
            }

            TMessageCounter& operator+=(const TMessageCounter& that);

            TString ToString(bool reader) const;

            TMessageCounter();
        };

    }
}
