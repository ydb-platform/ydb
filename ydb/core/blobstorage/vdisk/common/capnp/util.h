#pragma once

#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <kj/io.h>
#include <library/cpp/actors/core/event_pb.h>


namespace NKikimrCapnProtoUtil {
    struct TRopeStream : public kj::InputStream {
        NActors::TRopeStream *underlying;

        explicit TRopeStream(NActors::TRopeStream *underlying) : underlying(underlying) {}

        virtual size_t tryRead(void* dst, size_t minBytes, size_t) override {
            size_t bytesRead = 0;
            while (bytesRead < minBytes) {
                const void* buf;
                int size;
                if (!underlying->Next(&buf, &size)) {
                    break;
                }
                memcpy((char*)dst + bytesRead, buf, size);
                bytesRead += size;
            }
            return bytesRead;
        }
    };
};
