#pragma once

#include "flat_sausage_record.h"
#include "flat_sausage_grind.h"
#include "flat_page_iface.h"
#include "util_basics.h"

namespace NKikimr {
namespace NPageCollection {

    class TWriter {
    public:
        TWriter(TCookieAllocator &cookieAllocator, ui8 channel, ui32 maxBlobSize, bool v2Mode = false)
            : MaxBlobSize(maxBlobSize)
            , Channel(channel)
            , CookieAllocator(cookieAllocator)
            , Record(cookieAllocator.GroupBy(channel))
            , V2OnlyMode(v2Mode)
        {

        }

        explicit operator bool() const
        {
            return Record.Pages() || Blobs || Buffer;
        }

        ui32 AddPage(const TArrayRef<const char> body, ui32 type, ui32* crc32 = nullptr)
        {
            for (size_t offset = 0; offset < body.size(); ) {
                if (Buffer.capacity() == 0 && MaxBlobSize != Max<ui32>())
                    Buffer.reserve(Min(MaxBlobSize, ui32(16 * 1024 * 1024)));

                auto piece = Min(body.size() - offset, MaxBlobSize - Buffer.size());
                auto chunk = body.Slice(offset, piece);

                Buffer.append(chunk.data(), chunk.size());
                offset += piece;

                if (Buffer.size() >= MaxBlobSize) {
                    Flush();
                }
            }

            bool pageSkipped = (type == ui32(NTable::NPage::EPage::BTreeIndexV2) ||
                                (V2OnlyMode && type == ui32(NTable::NPage::EPage::DataPage)));
            /* No TEntry/TExtra entry created */
            if (pageSkipped) {
                SkippedBytes += body.size();
                SkippedPages += 1;
                if (crc32)
                    *crc32 = Checksum(body);
                return Max<ui32>();
            }

            return Record.Push(type, body, crc32);
        }

        void PushSkipEntry()
        {
            if (SkippedBytes > 0) {
                Record.PushSkip(Record.GetOffset() + SkippedBytes, (ui32)NTable::NPage::EPage::Skip, SkippedPages);
                SkippedBytes = 0;
                SkippedPages = 0;
            }
        }

        void AddInplace(ui32 page, TArrayRef<const char> body)
        {
            Record.PushInplace(page, body);
        }


        TSharedData Finish(bool empty)
        {
            Flush();

            TSharedData meta;

            if (Record.Pages() || empty) {
                meta = Record.Finish();
            }

            return meta;
        }

        TVector<TGlob> Grab()
        {
            return std::exchange(Blobs, TVector<TGlob>());
        }

    private:
        void Flush()
        {
            if (Buffer) {
                auto glob = CookieAllocator.Do(Channel, Buffer.size());

                Y_ENSURE(glob.Group == Record.Group, "Unexpected BS group");

                Blobs.emplace_back(glob, TakeBuffer());
                Record.Push(glob.Logo);
            }
        }

        TString TakeBuffer()
        {
            TString data;

            if (Buffer.size() >= (Buffer.capacity() >> 2)) {
                // More than a quarter of capacity occupied
                // Avoid excessive copy and take it as is
                data = std::exchange(Buffer, TString{ });
            } else {
                // Copy relevant data and keep current capacity
                data.assign(Buffer.data(), Buffer.size());
                Buffer.clear();
            }

            return data;
        }

    public:
        const ui32 MaxBlobSize = Max<ui32>();
        const ui8 Channel = Max<ui8>();
    private:
        TString Buffer;
        TCookieAllocator &CookieAllocator;
        TVector<TGlob> Blobs;
        NPageCollection::TRecord Record;
        bool V2OnlyMode = false;
        ui64 SkippedBytes = 0;
        ui32 SkippedPages = 0;
    };

}
}
