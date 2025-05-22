#pragma once

#include "flat_page_frames.h"
#include "flat_page_blobs.h"
#include "flat_util_binary.h"
#include "util_fmt_abort.h"

#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NTable {
namespace NPage {

    class TFrameWriter {
        using THeader = TFrames::THeader;
        using TEntry = TFrames::TEntry;

        struct TFresh {
            ui16 Tag;
            ui32 Len;
        };

    public:
        TFrameWriter(ui32 tags = 1)
            : TagsCount(tags)
        {
            Tags.resize(TagsCount + (TagsCount & 1), 0);
            Cook.reserve(tags);

            Y_ENSURE(tags <= ui32(-Min<i16>()), "Too many column tags");
        }

        void Put(TRowId row, ui16 tag, ui32 bytes)
        {
            if (row < Last && Last != Max<TRowId>()) {
                Y_TABLET_ERROR("Frame items have to follow sorted by row");
            } else if (tag >= TagsCount) {
                Y_TABLET_ERROR("Frame item component tag is out of range");
            } else if (Last != row) {
                Flush();
            }

            Size += bytes;
            Tags[tag] += 1;
            Last = row;
            Cook.emplace_back(TFresh{ tag, bytes });
        }

        void FlushRow()
        {
            Flush();
        }

        TSharedData Make()
        {
            Flush();

            return Array ? MakeAnyway() : TSharedData{ };
        }

        ui64 EstimateBytesUsed(size_t extraItems) const noexcept
        {
            if (size_t items = Array.size() + Cook.size() + extraItems) {
                return sizeof(NPage::TLabel) + sizeof(THeader)
                        + NUtil::NBin::SizeOf(Tags)
                        + sizeof(TEntry) * items;
            }

            return 0;
        }

        void Reset()
        {
            Last = Max<TRowId>();
            Rows = 0;
            Size = 0;
            std::fill(Tags.begin(), Tags.end(), 0);
            Array.clear();
            Cook.clear();
        }

    private:
        TSharedData MakeAnyway()
        {
            auto size = sizeof(NPage::TLabel) + sizeof(THeader)
                            + NUtil::NBin::SizeOf(Tags, Array);

            TSharedData buf = TSharedData::Uninitialized(size);

            NUtil::NBin::TPut out(buf.mutable_begin());

            WriteUnaligned<TLabel>(out.Skip<TLabel>(), TLabel::Encode(EPage::Frames, 0, size));

            if (auto *post = out.Skip<THeader>()) {
                Zero(*post);

                post->Skip = sizeof(THeader) + NUtil::NBin::SizeOf(Tags);
                post->Rows = Rows;
                post->Size = Size;
                post->Tags = TagsCount;
            }

            out.Put(Tags).Put(Array);

            Y_ENSURE(*out == buf.mutable_end());
            Y_ENSURE(buf.size() % alignof(TEntry) == 0);
            NSan::CheckMemIsInitialized(buf.data(), buf.size());

            return buf;
        }

        void Flush()
        {
            for (auto it: xrange(Cook.size())) {
                const i16 ref = it ? i16(it) : -i16(Cook.size());

                Array.push_back({ Last, Cook[it].Tag, ref, Cook[it].Len });
            }

            Rows += Cook ? 1 : 0;
            Cook.clear();
        }

    private:
        TRowId Last = Max<TRowId>();    /* Current frame row number     */
        ui32 TagsCount = 0;
        ui32 Rows = 0;                  /* Unique rows in frame index   */
        ui64 Size = 0;                  /* Sum of all Size fields in arr*/
        TVector<ui32> Tags;             /* By tag frequency historgram  */
        TVector<TEntry> Array;
        TVector<TFresh> Cook;
    };

    class TExtBlobsWriter {
        using THeader = TExtBlobs::THeader;
        using TEntry = TExtBlobs::TEntry;

    public:
        ui32 Put(const NPageCollection::TGlobId &glob)
        {
            Bytes += glob.Logo.BlobSize();
            Globs.emplace_back(glob);
            return Globs.size() - 1;
        }

        TSharedData Make(bool force = false) const
        {
            return (Globs  || force) ? MakeAnyway() : TSharedData{ };
        }

        ui32 Size() const noexcept
        {
            return Globs.size();
        }

        ui64 EstimateBytesUsed(size_t extraItems) const noexcept
        {
            if (size_t items = Globs.size() + extraItems) {
                return sizeof(NPage::TLabel) + sizeof(THeader)
                        + sizeof(TEntry) * items;
            }

            return 0;
        }

        void Reset()
        {
            Globs.clear();
        }

    private:
        TSharedData MakeAnyway() const
        {
            auto size = sizeof(NPage::TLabel) + sizeof(THeader)
                            + NUtil::NBin::SizeOf(Globs);

            TSharedData buf = TSharedData::Uninitialized(size);

            NUtil::NBin::TPut out(buf.mutable_begin());

            WriteUnaligned<TLabel>(out.Skip<TLabel>(), TLabel::Encode(EPage::Globs, 1, size));

            if (auto *post = out.Skip<THeader>()) {
                Zero(*post);

                post->Skip = sizeof(THeader);
                post->Bytes = Bytes;
            }

            out.Put(Globs);

            Y_ENSURE(*out == buf.mutable_end());
            Y_ENSURE(buf.size() % alignof(TEntry) == 0);
            NSan::CheckMemIsInitialized(buf.data(), buf.size());

            return buf;
        }

    private:
        ui64 Bytes = 0;
        TVector<TEntry> Globs;
    };

}
}
}
