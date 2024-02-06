#pragma once

#include "util_fmt_abort.h"
#include "flat_sausage_grind.h"
#include <ydb/core/base/tablet.h>
#include <util/thread/singleton.h>
#include <library/cpp/blockcodecs/codecs.h>

namespace NKikimr {
namespace NPageCollection {

    struct TSlicer {

        TSlicer(ui8 channel, TCookieAllocator *cookieAllocator, ui32 block)
            : Channel(channel)
            , Block(block)
            , CookieAllocator(cookieAllocator)
        {

        }

        TGlobId One(TVector<TEvTablet::TLogEntryReference> &refs, TString body, bool lz4) const
        {
            if (body.size() > Block) {
                Y_Fail(
                    "Cannot put " << body.size() << "b to "<< NFmt::Do(*CookieAllocator)
                    << " as single blob, block limit is " << Block << "b");
            }

            Y_ABORT_UNLESS(body.size() < Block, "Too large blob to be a TGlobId");

            if (lz4) std::exchange(body, Lz4()->Encode(body));

            auto glob = CookieAllocator->Do(Channel, body.size());

            refs.push_back({ glob.Logo, std::move(body) });

            return glob;
        }

        TLargeGlobId Do(TVector<TEvTablet::TLogEntryReference> &refs, TString body, bool lz4) const
        {
            if (body.size() >= Max<ui32>()) {
                Y_Fail(
                    "Cannot put " << body.size() << "b to "<< NFmt::Do(*CookieAllocator)
                    << " as a TSloid, blob have to be less than 4GiB");
            }

            if (lz4) std::exchange(body, Lz4()->Encode(body));

            const ui32 size = body.size();
            auto largeGlobId = CookieAllocator->Do(Channel, size, Block);

            if (size <= Block) {
                refs.push_back({ largeGlobId.Lead, std::move(body) });
            } else {
                ui32 off = 0;
                for (const auto& blobId : largeGlobId.Blobs()) {
                    const ui32 chunk = Min(Block, size - off);
                    Y_DEBUG_ABORT_UNLESS(chunk == blobId.BlobSize());
                    refs.push_back({ blobId, body.substr(off, chunk) });
                    off += chunk;
                }
                Y_DEBUG_ABORT_UNLESS(off == largeGlobId.Bytes);
            }

            return largeGlobId;
        }

        static inline const NBlockCodecs::ICodec* Lz4() noexcept
        {
            auto **lz4 = FastTlsSingleton<const NBlockCodecs::ICodec*>();

            return *lz4 ? *lz4 : (*lz4 = NBlockCodecs::Codec("lz4fast"));
        }

    private:
        const ui8 Channel = Max<ui8>();
        const ui32 Block = Max<ui32>();
        TCookieAllocator * const CookieAllocator = nullptr;
    };
}
}
