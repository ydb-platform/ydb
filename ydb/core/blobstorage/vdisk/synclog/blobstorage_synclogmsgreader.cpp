#include "blobstorage_synclogmsgreader.h"
#include "blobstorage_synclogmsgimpl.h"

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TNaiveFragmentReader
        ////////////////////////////////////////////////////////////////////////////
        void TNaiveFragmentReader::ForEach(TReadLogoBlobRec fblob, TReadBlockRec fblock, TReadBarrierRec fbar, TReadBlockRecV2 fblock2) {
            ForEach(Data, fblob, fblock, fbar, fblock2);
        }

        void TNaiveFragmentReader::ForEach(const TString &d, TReadLogoBlobRec fblob, TReadBlockRec fblock,
                                           TReadBarrierRec fbar, TReadBlockRecV2 fblock2) {
            const TRecordHdr *begin = (const TRecordHdr *)(d.data());
            const TRecordHdr *end = (const TRecordHdr *)(d.data() + d.size());

            for (const TRecordHdr *it = begin; it < end; it = it->Next()) {
                switch (it->RecType) {
                    case TRecordHdr::RecLogoBlob:
                        fblob(it->GetLogoBlob());
                        break;
                    case TRecordHdr::RecBlock:
                        fblock(it->GetBlock());
                        break;
                    case TRecordHdr::RecBarrier:
                        fbar(it->GetBarrier());
                        break;
                    case TRecordHdr::RecBlockV2:
                        fblock2(it->GetBlockV2());
                        break;
                    default:
                        Y_ABORT("Unknown RecType: %s", it->ToString().data());
                }
            }
        }

        bool TNaiveFragmentReader::Check(TString &errorString) {
            return TSerializeRoutines::CheckData(Data, errorString);
        }

        ////////////////////////////////////////////////////////////////////////////
        // TLz4FragmentReader
        ////////////////////////////////////////////////////////////////////////////
        void TLz4FragmentReader::ForEach(TReadLogoBlobRec fblob, TReadBlockRec fblock, TReadBarrierRec fbar, TReadBlockRecV2 fblock2) {
            Decompress();
            TNaiveFragmentReader::ForEach(Uncompressed, fblob, fblock, fbar, fblock2);
        }

        bool TLz4FragmentReader::Check(TString &errorString) {
            Decompress();
            return TSerializeRoutines::CheckData(Uncompressed, errorString);
        }

        void TLz4FragmentReader::Decompress() {
            if (Uncompressed.empty()) {
                // remove header from original string
                size_t hdrSize = GetLz4HeaderSize();
                TStringBuf d(Data.data() + hdrSize, Data.size() - hdrSize);
                GetLz4Codec()->Decode(d, Uncompressed);
            }
        }

        ////////////////////////////////////////////////////////////////////////////
        // TBaseOrderedFragmentReader
        ////////////////////////////////////////////////////////////////////////////
        void TBaseOrderedFragmentReader::ForEach(TReadLogoBlobRec fblob, TReadBlockRec fblock, TReadBarrierRec fbar, TReadBlockRecV2 fblock2) {
            Decompress();

            using THeapItem = std::tuple<TRecordHdr::ESyncLogRecType, const void*, ui32>;
            auto comp = [](const THeapItem& x, const THeapItem& y) { return std::get<2>(y) < std::get<2>(x); };

            TStackVec<THeapItem, 4> heap;

#define ADD_HEAP(NAME, TYPE) \
            if (!Records.NAME.empty()) { \
                heap.emplace_back(TRecordHdr::TYPE, &Records.NAME.front(), Records.NAME.front().Counter); \
            }
            ADD_HEAP(LogoBlobs, RecLogoBlob)
            ADD_HEAP(Blocks, RecBlock)
            ADD_HEAP(Barriers, RecBarrier)
            ADD_HEAP(BlocksV2, RecBlockV2)

            std::make_heap(heap.begin(), heap.end(), comp);

            while (!heap.empty()) {
                std::pop_heap(heap.begin(), heap.end(), comp);
                auto& item = heap.back(); // say thanks to Microsoft compiler for not supporting tuple binding correctly
                auto& type = std::get<0>(item);
                auto& ptr = std::get<1>(item);
                auto& counter = std::get<2>(item);
                switch (type) {
#define PROCESS(NAME, TYPE, FUNC) \
                    case TRecordHdr::TYPE: { \
                        using T = std::decay_t<decltype(Records.NAME)>::value_type; \
                        const T *item = static_cast<const T*>(ptr); \
                        FUNC(item); \
                        if (++item != Records.NAME.data() + Records.NAME.size()) { \
                            ptr = item; \
                            counter = item->Counter; \
                            std::push_heap(heap.begin(), heap.end(), comp); \
                        } else { \
                            heap.pop_back(); \
                        } \
                        break; \
                    }
                    PROCESS(LogoBlobs, RecLogoBlob, fblob)
                    PROCESS(Blocks, RecBlock, fblock)
                    PROCESS(Barriers, RecBarrier, fbar)
                    PROCESS(BlocksV2, RecBlockV2, fblock2)
                }
            }
        }

        ////////////////////////////////////////////////////////////////////////////
        // TOrderedLz4FragmentReader
        ////////////////////////////////////////////////////////////////////////////
        bool TOrderedLz4FragmentReader::Check(TString &errorString) {
            Y_UNUSED(errorString);
            return Decompress();
        }

        bool TOrderedLz4FragmentReader::Decompress() {
            if (!Decompressed) {
                // remove header from original string
                TString uncompressed;
                size_t hdrSize = GetOrderedLz4HeaderSize();
                TStringBuf d(Data.data() + hdrSize, Data.size() - hdrSize);
                GetLz4Codec()->Decode(d, uncompressed);

                // build vectors
                TReorderCodec codec(TReorderCodec::EEncoding::Trivial);
                Decompressed = codec.DecodeString(uncompressed, Records);
            }

            return Decompressed;
        }

        ////////////////////////////////////////////////////////////////////////////
        // TCustomCodecFragmentReader
        ////////////////////////////////////////////////////////////////////////////
        bool TCustomCodecFragmentReader::Check(TString &errorString) {
            Y_UNUSED(errorString);
            return Decompress();
        }

        bool TCustomCodecFragmentReader::Decompress() {
            if (!Decompressed) {
                // build vectors
                size_t hdrSize = GetCustomCodecHeaderSize();
                TReorderCodec codec(TReorderCodec::EEncoding::Custom);
                const char *pos = Data.data() + hdrSize;
                const char *end = pos + (Data.size() - hdrSize);
                Decompressed = codec.Decode(pos, end, Records);
            }

            return Decompressed;
        }

        ////////////////////////////////////////////////////////////////////////////
        // TFragmentReader
        ////////////////////////////////////////////////////////////////////////////
        TFragmentReader::TFragmentReader(const TString &data) {
            ECodec codec = FragmentCodecDetector(data);
            switch (codec) {
                case ECodec::Naive:
                    Impl.reset(new TNaiveFragmentReader(data));
                    break;
                case ECodec::Lz4:
                    Impl.reset(new TLz4FragmentReader(data));
                    break;
                case ECodec::OrderedLz4:
                    Impl.reset(new TOrderedLz4FragmentReader(data));
                    break;
                case ECodec::CustomCodec:
                    Impl.reset(new TCustomCodecFragmentReader(data));
                    break;
                default:
                    Y_ABORT("Unknwon codec");
            }
        }

    } // NSyncLog
} // NKikimr
