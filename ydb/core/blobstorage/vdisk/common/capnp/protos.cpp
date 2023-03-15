#include "protos.h"
#include "util.h"

namespace NKikimrCapnProto {
    bool TEvVGet::Reader::ParseFromZeroCopyStream(NActors::TRopeStream *input) {
        NKikimrCapnProtoUtil::TRopeStream stream;
        stream.underlying = input;

        kj::BufferedInputStreamWrapper buffered(stream);

        messageReader = std::make_unique<capnp::PackedMessageReader>(buffered);
        static_cast<NKikimrCapnProto_::TEvVGet::Reader &>(*this) = messageReader->getRoot<NKikimrCapnProto_::TEvVGet>();
        if (hasExtremeQueries()) {
            elements.reserve(getExtremeQueries().size());
            for (TExtremeQuery::Reader extremeQuery: getExtremeQueries()) {
                elements.push_back(extremeQuery);
            }
        }
        return true;
    }
};
