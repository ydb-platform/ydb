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

        std::vector<const TRecordHdr*> TNaiveFragmentReader::ListRecords() {
            std::vector<const TRecordHdr*> records = {};
            TWriteRecordToList writeToList{records};

            ForEach(Data, writeToList, writeToList, writeToList, writeToList);

            return std::move(records);
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

        std::vector<const TRecordHdr*> TLz4FragmentReader::ListRecords() {
            Decompress();
            std::vector<const TRecordHdr*> records = {};
            TWriteRecordToList writeToList{records};

            TNaiveFragmentReader::ForEach(Uncompressed, writeToList, writeToList, writeToList, writeToList);

            return std::move(records);
        }

        ////////////////////////////////////////////////////////////////////////////
        // TBaseOrderedFragmentReader
        ////////////////////////////////////////////////////////////////////////////

        void TBaseOrderedFragmentReader::ForEach(TReadLogoBlobRec fblob, TReadBlockRec fblock, TReadBarrierRec fbar, TReadBlockRecV2 fblock2) {
            return ForEachImpl(fblob, fblock, fbar, fblock2);
        }

        std::vector<const TRecordHdr*> TBaseOrderedFragmentReader::ListRecords() {
            std::vector<const TRecordHdr*> records;
            TWriteRecordToList writeToList{records};

            ForEachImpl(writeToList, writeToList, writeToList, writeToList);
            return std::move(records);
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
