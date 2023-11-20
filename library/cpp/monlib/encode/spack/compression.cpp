#include "compression.h"

#include <util/generic/buffer.h>
#include <util/generic/cast.h>
#include <util/generic/ptr.h>
#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/stream/format.h>
#include <util/stream/output.h>
#include <util/stream/walk.h>

#include <contrib/libs/lz4/lz4.h>
#include <contrib/libs/xxhash/xxhash.h>
#include <zlib.h>
#define ZSTD_STATIC_LINKING_ONLY
#include <contrib/libs/zstd/include/zstd.h>

namespace NMonitoring {
    namespace {
        ///////////////////////////////////////////////////////////////////////////////
        // Frame
        ///////////////////////////////////////////////////////////////////////////////
        using TCompressedSize = ui32;
        using TUncompressedSize = ui32;
        using TCheckSum = ui32;

        constexpr size_t COMPRESSED_FRAME_SIZE_LIMIT = 512_KB;
        constexpr size_t UNCOMPRESSED_FRAME_SIZE_LIMIT = COMPRESSED_FRAME_SIZE_LIMIT;
        constexpr size_t FRAME_SIZE_LIMIT = 2_MB;
        constexpr size_t DEFAULT_FRAME_LEN = 64_KB;

        struct Y_PACKED TFrameHeader {
            TCompressedSize CompressedSize;
            TUncompressedSize UncompressedSize;
        };

        struct Y_PACKED TFrameFooter {
            TCheckSum CheckSum;
        };

        ///////////////////////////////////////////////////////////////////////////////
        // TBlock
        ///////////////////////////////////////////////////////////////////////////////
        struct TBlock: public TStringBuf {
            template <typename T>
            TBlock(T&& t)
                : TStringBuf(t.data(), t.size())
            {
                Y_ENSURE(t.data() != nullptr);
            }

            char* data() noexcept {
                return const_cast<char*>(TStringBuf::data());
            }
        };

        ///////////////////////////////////////////////////////////////////////////////
        // XXHASH
        ///////////////////////////////////////////////////////////////////////////////
        struct TXxHash32 {
            static TCheckSum Calc(TBlock in) {
                static const ui32 SEED = 0x1337c0de;
                return XXH32(in.data(), in.size(), SEED);
            }

            static bool Check(TBlock in, TCheckSum checksum) {
                return Calc(in) == checksum;
            }
        };

        ///////////////////////////////////////////////////////////////////////////////
        // Adler32
        ///////////////////////////////////////////////////////////////////////////////
        struct TAdler32 {
            static TCheckSum Calc(TBlock in) {
                return adler32(1L, reinterpret_cast<const Bytef*>(in.data()), in.size());
            }

            static bool Check(TBlock in, TCheckSum checksum) {
                return Calc(in) == checksum;
            }
        };

        ///////////////////////////////////////////////////////////////////////////////
        // LZ4
        ///////////////////////////////////////////////////////////////////////////////
        struct TLz4Codec {
            static size_t MaxCompressedLength(size_t in) {
                int result = LZ4_compressBound(static_cast<int>(in));
                Y_ENSURE(result != 0, "lz4 input size is too large");
                return result;
            }

            static size_t Compress(TBlock in, TBlock out) {
                int rc = LZ4_compress_default(
                    in.data(),
                    out.data(),
                    SafeIntegerCast<int>(in.size()),
                    SafeIntegerCast<int>(out.size()));
                Y_ENSURE(rc != 0, "lz4 compression failed");
                return rc;
            }

            static void Decompress(TBlock in, TBlock out) {
                int rc = LZ4_decompress_safe(
                    in.data(),
                    out.data(),
                    SafeIntegerCast<int>(in.size()),
                    SafeIntegerCast<int>(out.size()));
                Y_ENSURE(rc >= 0, "the lz4 stream is detected malformed");
            }
        };

        ///////////////////////////////////////////////////////////////////////////////
        // ZSTD
        ///////////////////////////////////////////////////////////////////////////////
        struct TZstdCodec {
            static const int LEVEL = 11;

            static size_t MaxCompressedLength(size_t in) {
                return ZSTD_compressBound(in);
            }

            static size_t Compress(TBlock in, TBlock out) {
                size_t rc = ZSTD_compress(out.data(), out.size(), in.data(), in.size(), LEVEL);
                if (Y_UNLIKELY(ZSTD_isError(rc))) {
                    ythrow yexception() << TStringBuf("zstd compression failed: ")
                                        << ZSTD_getErrorName(rc);
                }
                return rc;
            }

            static void Decompress(TBlock in, TBlock out) {
                size_t rc = ZSTD_decompress(out.data(), out.size(), in.data(), in.size());
                if (Y_UNLIKELY(ZSTD_isError(rc))) {
                    ythrow yexception() << TStringBuf("zstd decompression failed: ")
                                        << ZSTD_getErrorName(rc);
                }
                Y_ENSURE(rc == out.size(), "zstd decompressed wrong size");
            }
        };

        ///////////////////////////////////////////////////////////////////////////////
        // ZLIB
        ///////////////////////////////////////////////////////////////////////////////
        struct TZlibCodec {
            static const int LEVEL = 6;

            static size_t MaxCompressedLength(size_t in) {
                return compressBound(in);
            }

            static size_t Compress(TBlock in, TBlock out) {
                uLong ret = out.size();
                int rc = compress2(
                    reinterpret_cast<Bytef*>(out.data()),
                    &ret,
                    reinterpret_cast<const Bytef*>(in.data()),
                    in.size(),
                    LEVEL);
                Y_ENSURE(rc == Z_OK, "zlib compression failed");
                return ret;
            }

            static void Decompress(TBlock in, TBlock out) {
                uLong ret = out.size();
                int rc = uncompress(
                    reinterpret_cast<Bytef*>(out.data()),
                    &ret,
                    reinterpret_cast<const Bytef*>(in.data()),
                    in.size());
                Y_ENSURE(rc == Z_OK, "zlib decompression failed");
                Y_ENSURE(ret == out.size(), "zlib decompressed wrong size");
            }
        };

        //
        // Framed streams use next frame structure:
        //
        // +-----------------+-------------------+============+------------------+
        // | compressed size | uncompressed size |    data    |     check sum    |
        // +-----------------+-------------------+============+------------------+
        //    4 bytes           4 bytes            var len       4 bytes
        //

        ///////////////////////////////////////////////////////////////////////////////
        // TFramedInputStream
        ///////////////////////////////////////////////////////////////////////////////
        template <typename TCodecAlg, typename TCheckSumAlg>
        class TFramedDecompressStream final: public IWalkInput {
        public:
            explicit TFramedDecompressStream(IInputStream* in)
                : In_(in)
            {
            }

        private:
            size_t DoUnboundedNext(const void** ptr) override {
                if (!In_) {
                    return 0;
                }

                TFrameHeader header;
                In_->LoadOrFail(&header, sizeof(header));

                if (header.CompressedSize == 0) {
                    In_ = nullptr;
                    return 0;
                }

                Y_ENSURE(header.CompressedSize <= COMPRESSED_FRAME_SIZE_LIMIT, "Compressed frame size is limited to "
                    << HumanReadableSize(COMPRESSED_FRAME_SIZE_LIMIT, SF_BYTES)
                    << " but is " <<  HumanReadableSize(header.CompressedSize, SF_BYTES));

                Y_ENSURE(header.UncompressedSize <= UNCOMPRESSED_FRAME_SIZE_LIMIT, "Uncompressed frame size is limited to "
                    << HumanReadableSize(UNCOMPRESSED_FRAME_SIZE_LIMIT, SF_BYTES)
                    << " but is " <<  HumanReadableSize(header.UncompressedSize, SF_BYTES));

                Compressed_.Resize(header.CompressedSize);
                In_->LoadOrFail(Compressed_.Data(), header.CompressedSize);

                TFrameFooter footer;
                In_->LoadOrFail(&footer, sizeof(footer));
                Y_ENSURE(TCheckSumAlg::Check(Compressed_, footer.CheckSum),
                         "corrupted stream: check sum mismatch");

                Uncompressed_.Resize(header.UncompressedSize);
                TCodecAlg::Decompress(Compressed_, Uncompressed_);

                *ptr = Uncompressed_.Data();
                return Uncompressed_.Size();
            }

        private:
            IInputStream* In_;
            TBuffer Compressed_;
            TBuffer Uncompressed_;
        };

        ///////////////////////////////////////////////////////////////////////////////
        // TFramedOutputStream
        ///////////////////////////////////////////////////////////////////////////////
        template <typename TCodecAlg, typename TCheckSumAlg>
        class TFramedCompressStream final: public IFramedCompressStream {
        public:
            explicit TFramedCompressStream(IOutputStream* out)
                : Out_(out)
                , Uncompressed_(DEFAULT_FRAME_LEN)
            {
            }

            ~TFramedCompressStream() override {
                try {
                    Finish();
                } catch (...) {
                }
            }

        private:
            void DoWrite(const void* buf, size_t len) override {
                const char* in = static_cast<const char*>(buf);

                while (len != 0) {
                    const size_t avail = Uncompressed_.Avail();
                    if (len < avail) {
                        Uncompressed_.Append(in, len);
                        return;
                    }

                    Uncompressed_.Append(in, avail);
                    Y_ASSERT(Uncompressed_.Avail() == 0);

                    in += avail;
                    len -= avail;

                    WriteCompressedFrame();
                }
            }

            void FlushWithoutEmptyFrame() override {
                if (Out_ && !Uncompressed_.Empty()) {
                    WriteCompressedFrame();
                }
            }

            void FinishAndWriteEmptyFrame() override {
                if (Out_) {
                    Y_DEFER {
                        Out_ = nullptr;
                    };

                    if (!Uncompressed_.Empty()) {
                        WriteCompressedFrame();
                    }

                    WriteEmptyFrame();
                }
            }

            void DoFlush() override {
                FlushWithoutEmptyFrame();
            }

            void DoFinish() override {
                FinishAndWriteEmptyFrame();
            }

            void WriteCompressedFrame() {
                static const auto framePayload = sizeof(TFrameHeader) + sizeof(TFrameFooter);
                const auto maxFrameSize = ui64(TCodecAlg::MaxCompressedLength(Uncompressed_.Size())) + framePayload;
                Y_ENSURE(maxFrameSize <= FRAME_SIZE_LIMIT, "Frame size in encoder is limited to "
                    << HumanReadableSize(FRAME_SIZE_LIMIT, SF_BYTES)
                    << " but is " <<  HumanReadableSize(maxFrameSize, SF_BYTES));

                Frame_.Resize(maxFrameSize);

                // compress
                TBlock compressedBlock = Frame_;
                compressedBlock.Skip(sizeof(TFrameHeader));
                compressedBlock.Trunc(TCodecAlg::Compress(Uncompressed_, compressedBlock));

                // add header
                auto header = reinterpret_cast<TFrameHeader*>(Frame_.Data());
                header->CompressedSize = SafeIntegerCast<TCompressedSize>(compressedBlock.size());
                header->UncompressedSize = SafeIntegerCast<TUncompressedSize>(Uncompressed_.Size());

                // add footer
                auto footer = reinterpret_cast<TFrameFooter*>(
                    Frame_.Data() + sizeof(TFrameHeader) + header->CompressedSize);
                footer->CheckSum = TCheckSumAlg::Calc(compressedBlock);

                // write
                Out_->Write(Frame_.Data(), header->CompressedSize + framePayload);
                Uncompressed_.Clear();
            }

            void WriteEmptyFrame() {
                static const auto framePayload = sizeof(TFrameHeader) + sizeof(TFrameFooter);
                char buf[framePayload] = {0};
                Out_->Write(buf, sizeof(buf));
            }

        private:
            IOutputStream* Out_;
            TBuffer Uncompressed_;
            TBuffer Frame_;
        };

    }

    THolder<IInputStream> CompressedInput(IInputStream* in, ECompression alg) {
        switch (alg) {
            case ECompression::IDENTITY:
                return nullptr;
            case ECompression::ZLIB:
                return MakeHolder<TFramedDecompressStream<TZlibCodec, TAdler32>>(in);
            case ECompression::ZSTD:
                return MakeHolder<TFramedDecompressStream<TZstdCodec, TXxHash32>>(in);
            case ECompression::LZ4:
                return MakeHolder<TFramedDecompressStream<TLz4Codec, TXxHash32>>(in);
            case ECompression::UNKNOWN:
                return nullptr;
        }
        Y_ABORT("invalid compression algorithm");
    }

    THolder<IFramedCompressStream> CompressedOutput(IOutputStream* out, ECompression alg) {
        switch (alg) {
            case ECompression::IDENTITY:
                return nullptr;
            case ECompression::ZLIB:
                return MakeHolder<TFramedCompressStream<TZlibCodec, TAdler32>>(out);
            case ECompression::ZSTD:
                return MakeHolder<TFramedCompressStream<TZstdCodec, TXxHash32>>(out);
            case ECompression::LZ4:
                return MakeHolder<TFramedCompressStream<TLz4Codec, TXxHash32>>(out);
            case ECompression::UNKNOWN:
                return nullptr;
        }
        Y_ABORT("invalid compression algorithm");
    }

}
