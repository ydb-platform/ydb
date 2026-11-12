#pragma once

#include "flat_page_iface.h"

#include <contrib/libs/zstd/include/zstd.h>
#include <library/cpp/blockcodecs/codecs.h>
#include <library/cpp/blockcodecs/core/common.h>
#include <util/generic/ptr.h>

namespace NKikimr::NTable::NPage {

    // Keep the block-codec framing while avoiding context setup on every page,
    // including repeated CPU feature detection. Reinitializing preallocated
    // contexts retained the overhead, so workspace allocation was not the cause.
    // Reuse both contexts: compaction also compresses many independent pages.
    class TZstdPageCodec final : public NBlockCodecs::TAddLengthCodec<TZstdPageCodec> {
        struct TFreeCompressionContext {
            static void Destroy(ZSTD_CCtx* context) noexcept {
                ZSTD_freeCCtx(context);
            }
        };

        struct TFreeDecompressionContext {
            static void Destroy(ZSTD_DCtx* context) noexcept {
                ZSTD_freeDCtx(context);
            }
        };

        static size_t CheckError(size_t result, const char* operation) {
            if (ZSTD_isError(result)) {
                ythrow yexception() << operation << " zstd error: " << ZSTD_getErrorName(result);
            }
            return result;
        }

    public:
        void SetCompressionLevel(ui32 level) {
            Y_ENSURE(level <= static_cast<ui32>(ZSTD_maxCLevel()), "Invalid page compression level " << level);
            CompressionLevel = level;
        }

        static size_t DoMaxCompressedLength(size_t size) noexcept {
            return ZSTD_compressBound(size);
        }

        size_t DoCompress(const NBlockCodecs::TData& in, void* out) const {
            if (!CompressionContext) {
                CompressionContext.Reset(ZSTD_createCCtx());
                Y_ENSURE(CompressionContext, "Failed to allocate ZSTD compression context");
            }
            return CheckError(ZSTD_compressCCtx(CompressionContext.Get(), out, DoMaxCompressedLength(in.size()),
                in.data(), in.size(), CompressionLevel), "compress");
        }

        void DoDecompress(const NBlockCodecs::TData& in, void* out, size_t size) const {
            if (!DecompressionContext) {
                DecompressionContext.Reset(ZSTD_createDCtx());
                Y_ENSURE(DecompressionContext, "Failed to allocate ZSTD decompression context");
            }
            const auto result = CheckError(ZSTD_decompressDCtx(DecompressionContext.Get(), out, size,
                in.data(), in.size()), "decompress");
            if (result != size) {
                ythrow NBlockCodecs::TDecompressError(size, result);
            }
        }

        TStringBuf Name() const noexcept override {
            return "zstd";
        }

    private:
        ui32 CompressionLevel = 1;
        mutable THolder<ZSTD_CCtx, TFreeCompressionContext> CompressionContext;
        mutable THolder<ZSTD_DCtx, TFreeDecompressionContext> DecompressionContext;
    };

    inline const NBlockCodecs::ICodec* GetBlockCodec(ECodec codec)
    {
        switch (codec) {
            case ECodec::LZ4: {
                static const auto* impl = NBlockCodecs::Codec("lz4fast");
                return impl;
            }
            case ECodec::ZSTD: {
                // Readers can be short-lived or copied. Sharing the decoder on
                // the executing thread avoids a context per page or iterator.
                static thread_local TZstdPageCodec impl;
                return &impl;
            }
            case ECodec::Plain:
                break;
        }
        Y_ENSURE(false, "Unsupported page compression codec " << ui32(codec));
    }

}
