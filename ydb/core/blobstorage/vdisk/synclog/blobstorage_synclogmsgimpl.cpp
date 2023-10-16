#include "blobstorage_synclogmsgimpl.h"
#include "codecs.h"

#include <library/cpp/codecs/pfor_codec.h>
#include <util/system/unaligned_mem.h>

using namespace NCodecs;

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // Codec detector
        ////////////////////////////////////////////////////////////////////////////
        ECodec FragmentCodecDetector(const TString &data) {
            if (data.size() < 48)
                return ECodec::Naive;

            const ui64 *firstWords = reinterpret_cast<const ui64 *>(data.data());
            const bool firstWordsAreZero = (firstWords[0] == 0 &&
                                            firstWords[1] == 0 &&
                                            firstWords[2] == 0 &&
                                            firstWords[3] == 0 &&
                                            firstWords[4] == 0);
            if (!firstWordsAreZero)
                return ECodec::Naive;

            const ui64 signature = firstWords[5];
            switch (signature) {
                case Lz4Signature: return ECodec::Lz4;
                case OrderedLz4Signature: return ECodec::OrderedLz4;
                case CustomCodecSignature: return ECodec::CustomCodec;
                default: Y_ABORT("Unknown codec; signature# %" PRIu64, signature);
            }
        }


        ////////////////////////////////////////////////////////////////////////////
        // Column oriented format
        ////////////////////////////////////////////////////////////////////////////
        class TLogoBlobColumns {
        protected:
            TVector<ui64> TabletIds;
            TVector<ui8>  Channels;
            TVector<ui32> Generations;
            TVector<ui32> Steps;
            TVector<ui32> Cookies;
            TVector<ui32> Sizes;
            TVector<ui64> Ingresses;
            TVector<ui32> Counters;

            const size_t LogoBlobColumnizedRowSize = (8 + 1 + 4 + 4 + 4 + 4 + 8 + 4);

        public:
            virtual ~TLogoBlobColumns() {
            }

            void Columnize(TVector<TLogoBlobRecWithSerial> &logoBlobs) {
                Clear();
                auto comp = [](const TLogoBlobRecWithSerial& x, const TLogoBlobRecWithSerial& y) {
                    return std::make_tuple(x.LogoBlobID(), x.Ingress.Raw(), x.Counter) <
                        std::make_tuple(y.LogoBlobID(), y.Ingress.Raw(), y.Counter);
                };
                Sort(logoBlobs.begin(), logoBlobs.end(), comp);

                const ui32 blobsSize = logoBlobs.size();
                TabletIds.reserve(blobsSize);
                Channels.reserve(blobsSize);
                Generations.reserve(blobsSize);
                Steps.reserve(blobsSize);
                Cookies.reserve(blobsSize);
                Sizes.reserve(blobsSize);
                Ingresses.reserve(blobsSize);
                Counters.reserve(blobsSize);

                for (const auto &x : logoBlobs) {
                    const TLogoBlobID id = x.LogoBlobID();
                    TabletIds.push_back(id.TabletID());
                    Channels.push_back(id.Channel());
                    Generations.push_back(id.Generation());
                    Steps.push_back(id.Step());
                    Cookies.push_back(id.Cookie());
                    Sizes.push_back(id.BlobSize());
                    Y_DEBUG_ABORT_UNLESS(id.PartId() == 0);
                    Ingresses.push_back(x.Ingress.Raw());
                    Counters.push_back(x.Counter);
                }
            }

            void Decolumnize(TVector<TLogoBlobRecWithSerial> &logoBlobs) {
                logoBlobs.clear();

                const ui32 blobsSize = Size();
                logoBlobs.reserve(blobsSize);
                for (size_t i = 0; i < blobsSize; i++) {
                    TLogoBlobID id(TabletIds[i], Generations[i], Steps[i], Channels[i], Sizes[i], Cookies[i]);
                    logoBlobs.emplace_back(id, Ingresses[i], Counters[i]);
                }

                // restore original order
                auto lessRestore = [] (const TLogoBlobRecWithSerial &x, const TLogoBlobRecWithSerial &y) {
                    return x.Counter < y.Counter;
                };
                Sort(logoBlobs.begin(), logoBlobs.end(), lessRestore);
            }

            ui32 Size() const {
                const ui32 blobsSize = TabletIds.size();
                Y_DEBUG_ABORT_UNLESS(blobsSize == Channels.size() && blobsSize == Generations.size() &&
                             blobsSize == Steps.size() && blobsSize == Cookies.size() &&
                             blobsSize == Sizes.size() && blobsSize == Ingresses.size() &&
                             blobsSize == Counters.size());
                return blobsSize;
            }

            void Clear() {
                TabletIds.clear();
                Channels.clear();
                Generations.clear();
                Steps.clear();
                Cookies.clear();
                Sizes.clear();
                Ingresses.clear();
                Counters.clear();
            }

            void ResizeAll(ui32 blobsSize) {
                TabletIds.resize(blobsSize);
                Channels.resize(blobsSize);
                Generations.resize(blobsSize);
                Steps.resize(blobsSize);
                Cookies.resize(blobsSize);
                Sizes.resize(blobsSize);
                Ingresses.resize(blobsSize);
                Counters.resize(blobsSize);
            }

            virtual void Encode(IOutputStream &str) = 0;
            virtual const char *Decode(const char *pos, const char *end) = 0;
            virtual size_t GetEncodedApproximationSize() const = 0;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TLogoBlobColumnsTrivialCodec
        ////////////////////////////////////////////////////////////////////////////
        class TLogoBlobColumnsTrivialCodec : public TLogoBlobColumns {
        public:
            virtual void Encode(IOutputStream &str) override {
                const ui32 blobsSize = Size();
                str.Write(&blobsSize, sizeof(ui32));
                if (blobsSize) {
                    str.Write(&TabletIds[0],    8 * blobsSize);
                    str.Write(&Channels[0],     1 * blobsSize);
                    str.Write(&Generations[0],  4 * blobsSize);
                    str.Write(&Steps[0],        4 * blobsSize);
                    str.Write(&Cookies[0],      4 * blobsSize);
                    str.Write(&Sizes[0],        4 * blobsSize);
                    str.Write(&Ingresses[0],    8 * blobsSize);
                    str.Write(&Counters[0],     4 * blobsSize);
                }
            }

            // returns position, nullptr in case of error
            virtual const char *Decode(const char *pos, const char *end) override {
                Clear();

                if (end - pos < 4)
                    return nullptr;

                const ui32 blobsSize = ReadUnaligned<ui32>(pos);
                pos += 4;

                if (size_t(end - pos) < (blobsSize * LogoBlobColumnizedRowSize))
                    return nullptr;

                if (blobsSize) {
                    ResizeAll(blobsSize);

                    memcpy(&TabletIds[0],    pos, 8 * blobsSize); pos += 8 * blobsSize;
                    memcpy(&Channels[0],     pos, 1 * blobsSize); pos += 1 * blobsSize;
                    memcpy(&Generations[0],  pos, 4 * blobsSize); pos += 4 * blobsSize;
                    memcpy(&Steps[0],        pos, 4 * blobsSize); pos += 4 * blobsSize;
                    memcpy(&Cookies[0],      pos, 4 * blobsSize); pos += 4 * blobsSize;
                    memcpy(&Sizes[0],        pos, 4 * blobsSize); pos += 4 * blobsSize;
                    memcpy(&Ingresses[0],    pos, 8 * blobsSize); pos += 8 * blobsSize;
                    memcpy(&Counters[0],     pos, 4 * blobsSize); pos += 4 * blobsSize;
                }

                return pos;
            }

            virtual size_t GetEncodedApproximationSize() const override {
                return LogoBlobColumnizedRowSize;
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // TLogoBlobColumnsCustomCodec
        ////////////////////////////////////////////////////////////////////////////
        class TLogoBlobColumnsCustomCodec : public TLogoBlobColumns {
        private:
            TCodecPtr TabletIdsCodec;
            TCodecPtr ChannelsCodec;
            TCodecPtr GenerationsCodec;
            TCodecPtr StepsCodec;
            TCodecPtr CookiesCodec;
            TCodecPtr PFor32Codec;
            TCodecPtr PFor64Codec;

            template <class TNumber>
            void EncodeVector(const TVector<TNumber> &v,
                              const ICodec &codec,
                              IOutputStream &str)
            {
                const char *begin = (const char *)(&v[0]);
                const char *end = begin + sizeof(v[0]) * v.size();
                TStringBuf src(begin, end);

                TBuffer encoded;
                codec.Encode(src, encoded);

                const ui32 bufSize = encoded.Size();
                str.Write(&bufSize, 4);
                str.Write(encoded.Data(), bufSize);
            }

            template <class TNumber>
            const char *DecodeVector(TVector<TNumber> &v,
                                     const ICodec &codec,
                                     const char *pos,
                                     const char *end,
                                     ui32 blobsSize)
            {
                if (end - pos < 4)
                    return nullptr;

                const ui32 bufSize = ReadUnaligned<ui32>(pos);
                pos += 4;
                if (size_t(end - pos) < bufSize)
                    return nullptr;

                TBuffer decoded;
                decoded.Reserve(4 * bufSize); // reserve four times more space as coded
                codec.Decode(TStringBuf(pos, pos + bufSize), decoded);
                pos += bufSize;

                size_t decodedSize = decoded.Size();
                Y_ABORT_UNLESS(decodedSize == blobsSize * sizeof(TNumber),
                       "decodedSize# %zu blobSize# %" PRIu32 " sizeof(TNumber)=%zu",
                       decodedSize, blobsSize, sizeof(TNumber));
                v.clear();
                v.resize(blobsSize);
                memcpy(&v[0], decoded.Data(), decodedSize);

                return pos;
            }

        public:
            TLogoBlobColumnsCustomCodec() {
                // TODO: avoid allocations
                TabletIdsCodec.Reset(new TPipelineCodec(new TRunLengthCodec<ui64>, new TVarLengthIntCodec<ui64>));
                ChannelsCodec.Reset(new TPipelineCodec(new TRunLengthCodec<ui8, ui32>, new TVarLengthIntCodec<ui32>));
                GenerationsCodec.Reset(new TPipelineCodec(new TRunLengthCodec<ui32>, new TVarLengthIntCodec<ui32>));
                StepsCodec.Reset(new TPipelineCodec(new TSemiSortedDeltaCodec<ui32>, new TVarLengthIntCodec<ui32>));
                PFor32Codec.Reset(new TPForCodec<ui32, false>);
                PFor64Codec.Reset(new TPForCodec<ui64, false>);
            }

            virtual void Encode(IOutputStream &str) override {
                const ui32 blobsSize = Size();
                str.Write(&blobsSize, sizeof(ui32));
                if (blobsSize) {
                    EncodeVector(TabletIds, *TabletIdsCodec, str);
                    EncodeVector(Channels, *ChannelsCodec, str);
                    EncodeVector(Generations, *ChannelsCodec, str);
                    EncodeVector(Steps, *StepsCodec, str);
                    EncodeVector(Cookies, *PFor32Codec, str); // TODO: optimize
                    EncodeVector(Sizes, *PFor32Codec, str);
                    EncodeVector(Ingresses, *PFor64Codec, str);
                    EncodeVector(Counters, *PFor32Codec, str);
                }
            }

            // returns position, nullptr in case of error
            virtual const char *Decode(const char *pos, const char *end) override {
                Clear();

                if (end - pos < 4)
                    return nullptr;

                const ui32 blobsSize = ReadUnaligned<ui32>(pos);
                pos += 4;

                if (blobsSize) {
                    pos = DecodeVector(TabletIds, *TabletIdsCodec, pos, end, blobsSize);
                    pos = DecodeVector(Channels, *ChannelsCodec, pos, end, blobsSize);
                    pos = DecodeVector(Generations, *ChannelsCodec, pos, end, blobsSize);
                    pos = DecodeVector(Steps, *StepsCodec, pos, end, blobsSize);
                    pos = DecodeVector(Cookies, *PFor32Codec, pos, end, blobsSize); // TODO: optimize
                    pos = DecodeVector(Sizes, *PFor32Codec, pos, end, blobsSize);
                    pos = DecodeVector(Ingresses, *PFor64Codec, pos, end, blobsSize);
                    pos = DecodeVector(Counters, *PFor32Codec, pos, end, blobsSize);

                }

                return pos;
            }

            virtual size_t GetEncodedApproximationSize() const override {
                return LogoBlobColumnizedRowSize;
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // TReorderCodec::TImpl
        ////////////////////////////////////////////////////////////////////////////
        class TReorderCodec::TImpl {
        public:
            TImpl(EEncoding enc) {
                switch (enc) {
                    case EEncoding::Trivial:
                        LogoBlobsColumns.reset(new TLogoBlobColumnsTrivialCodec);
                        break;
                    case EEncoding::Custom:
                        LogoBlobsColumns.reset(new TLogoBlobColumnsCustomCodec);
                        break;
                    default:
                        Y_ABORT("Unexpected case");
                }
            }

            TString Encode(TRecordsWithSerial& records) {
                const ui32 blobsSize = records.LogoBlobs.size();
                LogoBlobsColumns->Columnize(records.LogoBlobs);

                const ui32 blocksSize = records.Blocks.size();
                const ui32 barriersSize = records.Barriers.size();
                const ui32 blocksSizeV2 = records.BlocksV2.size();
                size_t blobsSerializedSize = sizeof(ui32) + blobsSize * LogoBlobsColumns->GetEncodedApproximationSize();
                size_t blocksSerializedSize = sizeof(ui32) + blocksSize * sizeof(TBlockRecWithSerial);
                size_t barriersSerializedSize = sizeof(ui32) + barriersSize * sizeof(TBarrierRecWithSerial);
                size_t blocksSerializedSizeV2 = blocksSizeV2 ? sizeof(ui32) + blocksSizeV2 * sizeof(TBlockRecWithSerialV2) : 0;
                size_t serializedSize = blobsSerializedSize + blocksSerializedSize + barriersSerializedSize + blocksSerializedSizeV2;

                TStringStream str;
                str.Reserve(serializedSize);

                LogoBlobsColumns->Encode(str);

                str.Write(&blocksSize, sizeof(ui32));
                if (blocksSize) {
                    str.Write(records.Blocks.data(), sizeof(TBlockRecWithSerial) * blocksSize);
                }

                str.Write(&barriersSize, sizeof(ui32));
                if (barriersSize) {
                    str.Write(records.Barriers.data(), sizeof(TBarrierRecWithSerial) * barriersSize);
                }

                if (blocksSizeV2) {
                    str.Write(&blocksSizeV2, sizeof(ui32));
                    str.Write(records.BlocksV2.data(), sizeof(TBlockRecWithSerialV2) * blocksSizeV2);
                }

                return str.Str();
            }

            bool Decode(const char *pos, const char *end, TRecordsWithSerial& records) {
                // clear output
                records.LogoBlobs.clear();
                records.Blocks.clear();
                records.Barriers.clear();
                records.BlocksV2.clear();

                // logoblobs
                pos = LogoBlobsColumns->Decode(pos, end);
                if (!pos) {
                    return false;
                }
                LogoBlobsColumns->Decolumnize(records.LogoBlobs);

                // blocks
                if (size_t(end - pos) < sizeof(ui32)) {
                    return false;
                }

                const ui32 blocksSize = ReadUnaligned<ui32>(pos);
                pos += sizeof(ui32);
                if (size_t(end - pos) < sizeof(TBlockRecWithSerial) * blocksSize) {
                    return false;
                }

                if (blocksSize) {
                    records.Blocks.resize(blocksSize);
                    memcpy(records.Blocks.data(), pos, sizeof(TBlockRecWithSerial) * blocksSize);
                    pos += sizeof(TBlockRecWithSerial) * blocksSize;
                }

                // barriers
                if (size_t(end - pos) < sizeof(ui32)) {
                    return false;
                }

                const ui32 barriersSize = ReadUnaligned<ui32>(pos);
                pos += sizeof(ui32);
                if (size_t(end - pos) < sizeof(TBarrierRecWithSerial) * barriersSize) {
                    return false;
                }

                if (barriersSize) {
                    records.Barriers.resize(barriersSize);
                    memcpy(records.Barriers.data(), pos, sizeof(TBarrierRecWithSerial) * barriersSize);
                    pos += sizeof(TBarrierRecWithSerial) * barriersSize;
                }

                if (end != pos) {
                    if (size_t(end - pos) < sizeof(ui32)) {
                        return false;
                    }
                    const ui32 blocksSizeV2 = ReadUnaligned<ui32>(pos);
                    pos += sizeof(ui32);
                    if (!blocksSizeV2) {
                        return false;
                    }
                    const size_t len = sizeof(TBlockRecWithSerialV2) * blocksSizeV2;
                    if (size_t(end - pos) < len) {
                        return false;
                    }
                    records.BlocksV2.resize(blocksSizeV2);
                    memcpy(records.BlocksV2.data(), pos, len);
                    pos += len;
                }

                if (end != pos) {
                    return false;
                }

                return true;
            }

        private:
            std::unique_ptr<TLogoBlobColumns> LogoBlobsColumns;
        };


        ////////////////////////////////////////////////////////////////////////////
        // TReorderCodec
        // Sort logoblobs, store them by columns
        ////////////////////////////////////////////////////////////////////////////
        TReorderCodec::TReorderCodec(EEncoding enc)
            : Impl(new TReorderCodec::TImpl(enc))
        {}

        TReorderCodec::~TReorderCodec() {}

        TString TReorderCodec::Encode(TRecordsWithSerial& records) {
            return Impl->Encode(records);
        }

        bool TReorderCodec::Decode(const char *pos, const char *end, TRecordsWithSerial& records) {
            return Impl->Decode(pos, end, records);
        }

    } // NSyncLog
} // NKikimr
