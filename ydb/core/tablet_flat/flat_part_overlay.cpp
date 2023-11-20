#include "flat_part_overlay.h"
#include "util_fmt_abort.h"

#include <ydb/core/tablet_flat/protos/flat_table_part.pb.h>
#include <ydb/core/util/pb.h>

namespace NKikimr {
namespace NTable {

namespace {
    TVector<TSlice> SlicesFromProto(const NProtoBuf::RepeatedPtrField<NProto::TSliceBounds>& protos)
    {
        TVector<TSlice> slices;
        slices.reserve(protos.size());

        for (const auto& proto : protos) {
            slices.emplace_back(
                TSerializedCellVec(proto.GetFirstKey()),
                TSerializedCellVec(proto.GetLastKey()),
                proto.GetFirstRowId(),
                proto.GetLastRowId(),
                proto.GetFirstInclusive(),
                proto.GetLastInclusive());
        }

        return slices;
    }

    void SlicesToProto(TConstArrayRef<TSlice> slices, NProtoBuf::RepeatedPtrField<NProto::TSliceBounds>* protos)
    {
        protos->Reserve(protos->size() + slices.size());

        for (const auto& slice : slices) {
            auto* proto = protos->Add();
            proto->SetFirstKey(slice.FirstKey.GetBuffer());
            proto->SetLastKey(slice.LastKey.GetBuffer());
            proto->SetFirstRowId(slice.FirstRowId);
            proto->SetLastRowId(slice.LastRowId);
            proto->SetFirstInclusive(slice.FirstInclusive);
            proto->SetLastInclusive(slice.LastInclusive);
        }
    }
}

TString TOverlay::Encode() const noexcept
{
    if (!Screen && !Slices) {
        return { };
    }

    NProto::TOverlay plain;

    if (Screen) {
        Y_ABORT_UNLESS(Screen->Size() > 0,
            "Cannot serialize a screen with 0 holes");

        Screen->Validate();

        for (auto &hole: *Screen) {
            plain.AddScreen(hole.Begin);
            plain.AddScreen(hole.End);
        }
    }

    if (Slices) {
        Y_ABORT_UNLESS(Slices->size() > 0,
            "Cannot serialize a run with 0 slices");

        Slices->Validate();

        SlicesToProto(*Slices, plain.MutableSlices());
    }

    TString encoded;
    Y_PROTOBUF_SUPPRESS_NODISCARD plain.SerializeToString(&encoded);
    return encoded;
}

TOverlay TOverlay::Decode(TArrayRef<const char> opaque, TArrayRef<const char> opaqueExt) noexcept
{
    TOverlay overlay;

    if (opaque || opaqueExt) {
        bool ok = true;

        NProto::TOverlay plain;

        if (opaque) {
            ok &= MergeFromStringNoSizeLimit(plain, opaque);
        }

        if (opaqueExt) {
            ok &= MergeFromStringNoSizeLimit(plain, opaqueExt);
        }

        if (!ok) {
            Y_Fail("Failed to parse part overlay");
        }

        if (plain.ScreenSize() & 1) {
            Y_Fail("Overlay has invalid screen size " << plain.ScreenSize());
        }

        TScreen::TVec holes;
        holes.reserve(plain.ScreenSize() >> 1);

        for (size_t it = 0; it < plain.ScreenSize(); it += 2) {
            auto begin = plain.GetScreen(it);
            auto end = plain.GetScreen(it + 1);

            holes.emplace_back(TScreen::THole{ begin, end });
        }

        if (holes) {
            overlay.Screen = new TScreen(std::move(holes));
        }

        if (plain.SlicesSize() > 0) {
            overlay.Slices = new TSlices(SlicesFromProto(plain.GetSlices()));
        }
    }

    return overlay;
}

void TOverlay::Validate() const noexcept
{
    if (Screen) {
        Screen->Validate();
    }

    if (Slices) {
        Slices->Validate();
    }

    if (!Screen || !Slices) {
        // Cannot compare screen and bounds when one is missing
        return;
    }

    auto screen = TScreenRowsIterator(*Screen);
    auto slices = TSlicesRowsIterator(*Slices);

    while (screen) {
        if (!slices) {
            Y_ABORT("Found screen hole [%lu,%lu) that has no matching slices", screen->Begin, screen->End);
        }

        if (screen->End == Max<TRowId>()) {
            if (slices.HasNext()) {
                auto mid = *slices;
                ++slices;
                Y_ABORT("Found screen hole [%lu,+inf) that does not match slices [%lu,%lu) and [%lu,%lu)",
                    screen->Begin, mid.Begin, mid.End, slices->Begin, slices->End);
            }
            if (screen->Begin != slices->Begin) {
                Y_ABORT("Found screen hole [%lu,+inf) that does not match slice [%lu,%lu)",
                    screen->Begin, slices->Begin, slices->End);
            }
        } else if (!(*screen == *slices)) {
            Y_ABORT("Found screen hole [%lu,%lu) that does not match slice [%lu,%lu)",
                screen->Begin, screen->End, slices->Begin, slices->End);
        }

        ++screen;
        ++slices;
    }

    if (slices) {
        Y_ABORT("Found slice [%lu,%lu) that has no matching screen holes", slices->Begin, slices->End);
    }
}

void TOverlay::ApplyDelta(TArrayRef<const char> rawDelta) noexcept
{
    NProto::TOverlayDelta plain;

    if (!ParseFromStringNoSizeLimit(plain, rawDelta)) {
        Y_Fail("Failed to parse overlay delta");
    }

    if (auto removedSlices = SlicesFromProto(plain.GetRemovedSlices())) {
        TIntrusiveConstPtr<TSlices> removed = new TSlices(std::move(removedSlices));
        if (!TSlices::SupersetByRowId(Slices, removed)) {
            Y_Fail("Removing slices that are not a subset of existing slices");
        }

        Slices = TSlices::Subtract(Slices, removed);

        if (Slices->empty()) {
            Y_Fail("Removing slices produced an empty result");
        }

        Screen = Slices->ToScreen();
    }

    if (auto changedSlices = SlicesFromProto(plain.GetChangedSlices())) {
        Slices = TSlices::Replace(Slices, changedSlices);
    }
}

TString TOverlay::EncodeRemoveSlices(const TIntrusiveConstPtr<TSlices>& slices) noexcept
{
    NProto::TOverlayDelta plain;

    Y_ABORT_UNLESS(slices, "Cannot encode an empty remove slices");

    SlicesToProto(*slices, plain.MutableRemovedSlices());

    TString encoded;
    Y_PROTOBUF_SUPPRESS_NODISCARD plain.SerializeToString(&encoded);
    return encoded;
}

TString TOverlay::EncodeChangeSlices(TConstArrayRef<TSlice> slices) noexcept
{
    NProto::TOverlayDelta plain;

    Y_ABORT_UNLESS(slices, "Cannot encode an empty change slices");

    SlicesToProto(slices, plain.MutableChangedSlices());

    TString encoded;
    Y_PROTOBUF_SUPPRESS_NODISCARD plain.SerializeToString(&encoded);
    return encoded;
}

TString TOverlay::MaybeUnsplitSlices(const TString& opaque, size_t maxSize) noexcept
{
    if (opaque.size() <= maxSize) {
        return { };
    }

    NProto::TOverlay proto;
    if (!ParseFromStringNoSizeLimit(proto, opaque)) {
        Y_Fail("Unexpected failure to parse part overlay");
    }

    TVector<TSlice> slices = SlicesFromProto(proto.GetSlices());
    if (slices.empty()) {
        return { };
    }

    bool changed = false;
    auto last = slices.begin();
    auto dst = std::next(last);
    for (auto src = dst; src != slices.end(); ++src) {
        if (last->EndRowId() == src->BeginRowId()) {
            // Merge two adjacent slices
            last->LastKey = std::move(src->LastKey);
            last->LastInclusive = src->LastInclusive;
            last->LastRowId = src->LastRowId;
            changed = true;
        } else {
            // Reuse a new slice
            if (dst != src) {
                *dst = std::move(*src);
            }
            last = dst;
            ++dst;
        }
    }

    if (!changed) {
        return { };
    }

    slices.erase(dst, slices.end());
    proto.ClearSlices();
    SlicesToProto(slices, proto.MutableSlices());

    TString modified;
    if (!proto.SerializeToString(&modified)) {
        Y_Fail("Unexpected failure to serialize part overlay");
    }

    return modified;
}

}
}
