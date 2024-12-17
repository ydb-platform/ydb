#include "blobstorage_synclogmsgwriter.h"
#include "blobstorage_synclogmsgimpl.h"

#include <library/cpp/blockcodecs/codecs.h>
#include <util/thread/singleton.h>
#include <util/generic/algorithm.h>

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TNaiveFragmentWriter
        ////////////////////////////////////////////////////////////////////////////
        void TNaiveFragmentWriter::Clear() {
            Chain = {{64 << 10}}; // 64 KiB initial storage
            DataSize = 0;
        }

        void TNaiveFragmentWriter::Finish(TString *respData) {
            respData->clear();
            respData->reserve(DataSize);
            for (TBuffer& buffer : Chain) {
                respData->append(buffer.Data(), buffer.Size());
            }
        }

        ////////////////////////////////////////////////////////////////////////////
        // TLz4FragmentWriter
        ////////////////////////////////////////////////////////////////////////////
        void TLz4FragmentWriter::Finish(TString *respData) {
            // construct result and compress it
            respData->clear();
            TNaiveFragmentWriter::Finish(respData);
            const TString compressed = GetLz4Codec()->Encode(*respData);

            // header
            std::pair<const char *, size_t> hdr = GetLz4Header();

            // finalize
            respData->clear();
            respData->reserve(hdr.second + compressed.size());
            respData->append(hdr.first, hdr.second);
            respData->append(compressed);
        }

        ////////////////////////////////////////////////////////////////////////////
        // TOrderedLz4FragmentWriter
        ////////////////////////////////////////////////////////////////////////////
        void TOrderedLz4FragmentWriter::Finish(TString *respData) {
            // reorder
            TReorderCodec codec(TReorderCodec::EEncoding::Trivial);
            const TString reordered = codec.Encode(Records);
            // compress
            const TString compressed = GetLz4Codec()->Encode(reordered);
            // header
            std::pair<const char *, size_t> hdr = GetOrderedLz4Header();

            // finalize
            respData->clear();
            respData->reserve(hdr.second + compressed.size());
            respData->append(hdr.first, hdr.second);
            respData->append(compressed);
        }

        ////////////////////////////////////////////////////////////////////////////
        // TCustomCodecFragmentWriter
        ////////////////////////////////////////////////////////////////////////////
        void TCustomCodecFragmentWriter::Finish(TString *respData) {
            // reorder
            TReorderCodec codec(TReorderCodec::EEncoding::Custom);
            const TString result = codec.Encode(Records);
            // header
            std::pair<const char *, size_t> hdr = GetCustomCodecHeader();

            // finalize
            respData->clear();
            respData->reserve(hdr.second + result.size());
            respData->append(hdr.first, hdr.second);
            respData->append(result);
        }

    } // NSyncLog
} // NKikimr
