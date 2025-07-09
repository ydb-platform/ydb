#pragma once

#include "flat_sausage_layout.h"
#include "flat_util_binary.h"
#include "util_basics.h"

#include <util/generic/array_ref.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NPageCollection {

    class TRecord {
    public:
        TRecord(ui32 group): Group(group) { }

        ui32 Pages() const
        {
            return Index.size();
        }

        TRecord& Push(TArrayRef<const TBlobId> blobs)
        {
            Blobs.reserve(Blobs.size() + blobs.size());

            for (auto &one: blobs) Blobs.emplace_back(one);

            return *this;
        }

        TRecord& Push(const TBlobId &one)
        {
            return Blobs.emplace_back(one), *this;
        }

        ui32 Push(ui32 type, TArrayRef<const char> body)
        {
            Index.push_back({ Offset += body.size(), Inbound.size() });
            Extra.push_back({ type, Checksum(body) });

            return Index.size() - 1;
        }

        void PushInplace(ui32 page, TArrayRef<const char> body)
        {
            Y_ENSURE(Index && page == Index.size() - 1);

            Inbound.append(body.data(), body.size());
            Index.back().Inplace = Inbound.size();
        }

        TSharedData Finish()
        {
            TSharedData raw = TSharedData::Uninitialized(Bytes());
            char* ptr = raw.mutable_begin();

            {
                NPageCollection::THeader hdr;

                hdr.Magic = NPageCollection::Magic;
                hdr.Blobs = Blobs.size();
                hdr.Pages = Index.size();

                ::memcpy(ptr, &hdr, sizeof(hdr));
                ptr += sizeof(hdr);
            }

            Add(ptr, Blobs), Add(ptr, Index), Add(ptr, Extra);

            ::memcpy(ptr, Inbound.data(), Inbound.size());
            ptr += Inbound.size();

            {
                const ui32 crc = Checksum(raw.Slice(0, ptr - raw.begin()));

                ::memcpy(ptr, &crc, sizeof(crc));
                ptr += sizeof(crc);
            }

            Y_ENSURE(ptr == raw.mutable_end());
            NSan::CheckMemIsInitialized(raw.data(), raw.size());

            Blobs.clear();
            Index.clear();
            Extra.clear();
            Offset = 0;
            Inbound = { };

            return raw;
        }

        size_t Bytes() const
        {
            return
                sizeof(THeader) + Inbound.size() + sizeof(ui32)
                + NUtil::NBin::SizeOf(Blobs, Index, Extra);
        }

    private:
        template<typename TVal>
        static inline void Add(char* &ptr, const TVector<TVal> &vec)
        {
            const auto bytes = NUtil::NBin::SizeOf(vec);

            ::memcpy(ptr, vec.data(), bytes);
            ptr += bytes;
        }

    public:
        const ui32 Group = TLargeGlobId::InvalidGroup;

    private:
        TVector<TBlobId> Blobs;
        TVector<TEntry> Index;
        TVector<TExtra> Extra;

        ui64 Offset = 0;
        TString Inbound;
    };
}
}
