#include "unicode_set.h"

#include "category_ranges.h"
#include "unicode_set_parser.h"

#include <util/ysaveload.h>
#include <util/charset/wide.h>
#include <util/digest/numeric.h>
#include <util/generic/buffer.h>
#include <util/generic/yexception.h>
#include <util/stream/format.h>
#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/string/cast.h>

// The original idea of unicode set implementation was taken from the icu::UnicodeSet.
// UnicodeSet has a set of ranges [from, to), where upper boundary is exclusive.
// The list of ranges always has a terminal value CODEPOINT_HIGH at the end.

namespace NUnicode {
    namespace NPrivate {
        inline wchar32 Bound(wchar32 c) {
            return c < TUnicodeSet::CODEPOINT_HIGH ? c : TUnicodeSet::CODEPOINT_HIGH - 1;
        }

        inline void CheckWcType(WC_TYPE c) {
            if (static_cast<size_t>(c) >= CCL_NUM) {
                throw yexception() << "Category ID must be less than CCL_NUM (" << static_cast<size_t>(CCL_NUM) << "), specified: " << static_cast<size_t>(c);
            }
        }

    }

    // Returns the smallest value i >= from such that 'c' < Ranges[i].
    // Some examples:
    //                                         GetRangeItem(c, 0)
    //       set              Ranges[]         c=0 1 3 4 7 8
    //       ===              ==============     ===========
    //       []               [0x110000]         0 0 0 0 0 0
    //       [:Any:]          [0, 0x110000]      1 1 1 1 1 1
    //       [\u0000-\u0003]  [0, 4, 0x110000]   1 1 1 2 2 2
    //       [\u0004-\u0007]  [4, 8, 0x110000]   0 0 0 1 1 2
    //
    // So, if method returns an odd value then 'c' falls to the {Range[i-1],Range[i]} range.
    size_t TUnicodeSet::GetRangeItem(wchar32 c, size_t from) const {
        Y_ASSERT(Valid());
        Y_ASSERT(from < Length);
        if (c < Ranges[from])
            return from;
        size_t lo = from;
        size_t hi = Length - 1;
        if (lo >= hi || c >= Ranges[hi - 1]) {
            return hi;
        }
        for (;;) {
            size_t i = (lo + hi) >> 1;
            if (i == lo) {
                break;
            } else if (c < Ranges[i]) {
                hi = i;
            } else {
                lo = i;
            }
        }
        return hi;
    }

    wchar32* TUnicodeSet::EnsureCapacity(size_t capacity) {
        if (capacity <= Capacity) {
            return const_cast<wchar32*>(Ranges);
        }

        TDynamicBuffer buf = new wchar32[capacity];
        Copy<const wchar32*, wchar32*>(Ranges, Ranges + Length, buf.Get());
        DoSwap(buf, DynBuffer);
        Ranges = DynBuffer.Get();
        Capacity = capacity;
        return DynBuffer.Get();
    }

    wchar32* TUnicodeSet::InsertRangeSlots(const size_t pos, const size_t count) {
        Y_ASSERT(pos < Length);
        wchar32* src = EnsureCapacity(Length + count) + Length - 1;
        wchar32* dst = src + count;
        for (size_t i = 0; i < Length - pos; ++i) {
            *dst-- = *src--;
        }
        Length += count;
        return src + 1;
    }

    void TUnicodeSet::EraseRangeSlots(const size_t pos, const size_t count) {
        Y_ASSERT(pos < Length);
        Y_ASSERT(pos + count <= Length);
        wchar32* dst = EnsureWritable() + pos;
        wchar32* src = dst + count;
        for (size_t i = 0; i < Length - pos - count; ++i) {
            *dst++ = *src++;
        }
        Length -= count;
    }

    TUnicodeSet::TUnicodeSet()
        : Ranges(ShortBuffer)
        , Length(0)
        , Capacity(Y_ARRAY_SIZE(ShortBuffer))
    {
        Clear();
    }

    TUnicodeSet::TUnicodeSet(const TUnicodeSet& s)
        : Ranges(ShortBuffer)
        , Length(0)
        , Capacity(Y_ARRAY_SIZE(ShortBuffer))
    {
        Set(s);
    }

    // from, to - inclusive
    TUnicodeSet::TUnicodeSet(wchar32 from, wchar32 to)
        : Ranges(ShortBuffer)
        , Length(0)
        , Capacity(Y_ARRAY_SIZE(ShortBuffer))
    {
        Set(from, to);
    }

    TUnicodeSet::TUnicodeSet(const TWtringBuf& s)
        : Ranges(ShortBuffer)
        , Length(0)
        , Capacity(Y_ARRAY_SIZE(ShortBuffer))
    {
        Set(s);
    }

    TUnicodeSet::TUnicodeSet(WC_TYPE c)
        : Ranges(ShortBuffer)
        , Length(0)
        , Capacity(Y_ARRAY_SIZE(ShortBuffer))
    {
        Set(c);
    }

    void TUnicodeSet::AddPredefRanges(const NPrivate::TCategoryRanges& ranges) {
        if (ranges.Count > 0) {
            for (size_t i = 0; i + 1 < ranges.Count; i += 2) {
                Add(ranges.Data[i], ranges.Data[i + 1] - 1);
            }
        }
    }

    TUnicodeSet& TUnicodeSet::Add(const TUnicodeSet& s) {
        if (Empty()) {
            TUnicodeSet::operator=(s);
            return *this;
        }
        for (size_t i = 0; i + 1 < s.Length; i += 2) {
            Add(s.Ranges[i], s.Ranges[i + 1] - 1);
        }
        return *this;
    }

    TUnicodeSet& TUnicodeSet::Add(const TWtringBuf& s) {
        const wchar16* begin = s.data();
        const wchar16* end = s.data() + s.size();
        while (begin < end) {
            Add(ReadSymbolAndAdvance(begin, end));
        }
        return *this;
    }

    TUnicodeSet& TUnicodeSet::Add(wchar32 c) {
        c = NPrivate::Bound(c);
        const size_t i = GetRangeItem(c);
        if (i & 1) {
            return *this;
        }
        if (c == Ranges[i] - 1) {              // The char adjoins with the next range
            if (i > 0 && Ranges[i - 1] == c) { // The char adjoins with the previous range too
                if (i + 1 == Length) {         // Don't delete the last TERMINAL
                    EraseRangeSlots(i - 1, 1);
                } else {
                    EraseRangeSlots(i - 1, 2); // Collapse ranges
                }
            } else {
                EnsureWritable()[i] = c;
            }
        } else if (i > 0 && Ranges[i - 1] == c) {
            ++(EnsureWritable()[i - 1]);
        } else {
            wchar32* target = InsertRangeSlots(i, 2);
            *target++ = c;
            *target = c + 1;
        }
        Y_ASSERT(Valid());

        return *this;
    }

    TUnicodeSet& TUnicodeSet::Add(wchar32 from, wchar32 to) {
        from = NPrivate::Bound(from);
        to = NPrivate::Bound(to);
        Y_ASSERT(from <= to);
        if (to == from) {
            return Add(to);
        } else if (from > to) {
            return *this;
        }

        size_t i = GetRangeItem(from);

        if (to < Ranges[i]) {
            if (i & 1) {
                return *this;
            }
            if (i > 0 && Ranges[i - 1] == from) {
                if (Ranges[i] == to + 1) {
                    if (i + 1 == Length) {
                        EraseRangeSlots(i - 1, 1);
                    } else {
                        EraseRangeSlots(i - 1, 2);
                    }
                } else {
                    EnsureWritable()[i - 1] = to + 1;
                }
            } else if (Ranges[i] == to + 1) {
                if (i + 1 == Length) {
                    *InsertRangeSlots(i, 1) = from;
                } else {
                    EnsureWritable()[i] = from;
                }
            } else {
                wchar32* target = InsertRangeSlots(i, 2);
                *target++ = from;
                *target = to + 1;
            }
            Y_ASSERT(Valid());
            return *this;
        }

        size_t j = GetRangeItem(to, i);
        Y_ASSERT(i < j);

        if (0 == (j & 1)) { // 'to' falls between ranges
            if (Ranges[j] > to + 1) {
                *InsertRangeSlots(j, 1) = to + 1;
            } else if (j + 1 < Length) { // Exclude last TERMINAL element
                Y_ASSERT(Ranges[j] == to + 1);
                // The next range adjoins with the current one. Join them
                ++j;
            }
        }

        if (0 == (i & 1)) { // 'from' falls between ranges
            if (i > 0 && Ranges[i - 1] == from) {
                --i;
            } else {
                *InsertRangeSlots(i, 1) = from;
                ++i;
                ++j;
            }
        }

        // Erase ranges, which are covered by the new one
        Y_ASSERT(i <= j);
        Y_ASSERT(i <= Length);
        Y_ASSERT(j <= Length);
        EraseRangeSlots(i, j - i);

        Y_ASSERT(Valid());
        return *this;
    }

    TUnicodeSet& TUnicodeSet::Add(WC_TYPE c) {
        NPrivate::CheckWcType(c);
        if (Empty()) {
            return Set(c);
        }
        AddPredefRanges(NPrivate::GetCategoryRanges(c));
        return *this;
    }

    TUnicodeSet& TUnicodeSet::AddCategory(const TStringBuf& catName) {
        if (Empty()) {
            return SetCategory(catName);
        }
        AddPredefRanges(NPrivate::GetCategoryRanges(catName));
        return *this;
    }

    void TUnicodeSet::SetPredefRanges(const NPrivate::TCategoryRanges& ranges) {
        Clear();
        if (ranges.Count > 0) {
            DynBuffer.Drop();
            Ranges = ranges.Data;
            Length = ranges.Count;
            Capacity = 0;
        }
    }

    TUnicodeSet& TUnicodeSet::Set(const TUnicodeSet& s) {
        if (0 == s.Capacity) {
            DynBuffer.Drop();
            Ranges = s.Ranges;
            Length = s.Length;
            Capacity = 0;
        } else if (s.Ranges == s.DynBuffer.Get()) {
            DynBuffer = s.DynBuffer;
            Ranges = DynBuffer.Get();
            Length = s.Length;
            Capacity = s.Capacity;
        } else {
            ::Copy(s.Ranges, s.Ranges + s.Length, EnsureCapacity(s.Length));
            Length = s.Length;
        }
        return *this;
    }

    TUnicodeSet& TUnicodeSet::Set(wchar32 from, wchar32 to) {
        from = NPrivate::Bound(from);
        to = NPrivate::Bound(to);
        Y_ASSERT(from <= to);

        Clear();

        if (to == from) {
            return Add(to);
        } else if (from > to) {
            return *this;
        }

        if (to + 1 != CODEPOINT_HIGH) {
            wchar32* target = InsertRangeSlots(0, 2);
            *target++ = from;
            *target = to + 1;
        } else {
            *InsertRangeSlots(0, 1) = from;
        }
        Y_ASSERT(Valid());
        return *this;
    }

    TUnicodeSet& TUnicodeSet::Set(const TWtringBuf& s) {
        Clear();
        return Add(s);
    }

    TUnicodeSet& TUnicodeSet::Set(WC_TYPE c) {
        NPrivate::CheckWcType(c);
        SetPredefRanges(NPrivate::GetCategoryRanges(c));
        return *this;
    }

    TUnicodeSet& TUnicodeSet::SetCategory(const TStringBuf& catName) {
        SetPredefRanges(NPrivate::GetCategoryRanges(catName));
        return *this;
    }

    TUnicodeSet& TUnicodeSet::Invert() {
        Y_ASSERT(Valid());
        if (0 == Ranges[0]) {
            EraseRangeSlots(0, 1);
        } else {
            *InsertRangeSlots(0, 1) = 0;
        }
        return *this;
    }

    TUnicodeSet& TUnicodeSet::MakeCaseInsensitive() {
        TVector<wchar32> oldRanges(Ranges, Ranges + Length);
        for (size_t i = 0; i + 1 < oldRanges.size(); i += 2) {
            for (wchar32 c = oldRanges[i]; c < oldRanges[i + 1]; ++c) {
                const ::NUnicode::NPrivate::TProperty& p = ::NUnicode::NPrivate::CharProperty(c);
                if (p.Lower) {
                    Add(static_cast<wchar32>(c + p.Lower));
                }
                if (p.Upper) {
                    Add(static_cast<wchar32>(c + p.Upper));
                }
                if (p.Title) {
                    Add(static_cast<wchar32>(c + p.Title));
                }
            }
        }
        return *this;
    }

    TUnicodeSet& TUnicodeSet::Clear() {
        if (IsStatic() || IsShared()) {
            DynBuffer.Drop();
            ShortBuffer[0] = CODEPOINT_HIGH;
            Capacity = Y_ARRAY_SIZE(ShortBuffer);
            Ranges = ShortBuffer;
        } else {
            const_cast<wchar32*>(Ranges)[0] = CODEPOINT_HIGH;
        }
        Length = 1;
        return *this;
    }

    size_t TUnicodeSet::Hash() const {
        size_t res = 0;
        for (size_t i = 0; i < Length; ++i) {
            res = ::CombineHashes(size_t(Ranges[i]), res);
        }
        return res;
    }

    inline void WriteUnicodeChar(IOutputStream& out, wchar32 c, bool needEscape = false) {
        switch (c) {
            case wchar32('-'):
            case wchar32('\\'):
            case wchar32('^'):
                needEscape = true;
                break;
            default:
                break;
        }
        if (::IsGraph(c) && !needEscape) {
            char buf[4]; // Max utf8 char length is 4
            size_t wr = 0;
            WideToUTF8(&c, 1, buf, wr);
            Y_ASSERT(wr <= Y_ARRAY_SIZE(buf));
            out.Write(buf, wr);
        } else {
            TString hexRepr = IntToString<16>(c);
            if (c >> 8 == 0) {
                out << "\\x" << LeftPad(hexRepr, 2, '0');
            } else if (c >> 16 == 0) {
                out << "\\u" << LeftPad(hexRepr, 4, '0');
            } else {
                out << "\\U" << LeftPad(hexRepr, 8, '0');
            }
        }
    }

    TString TUnicodeSet::ToString(bool escapeAllChars /* = false*/) const {
        Y_ASSERT(Valid());
        TStringStream str;
        str.Reserve(Length * 4 + Length / 2 + 2);

        str.Write('[');
        for (size_t i = 0; i + 1 < Length; i += 2) {
            WriteUnicodeChar(str, Ranges[i], escapeAllChars);
            if (Ranges[i] + 1 < Ranges[i + 1]) {
                // Don't write dash for two-symbol ranges
                if (Ranges[i] + 2 < Ranges[i + 1]) {
                    str.Write('-');
                }
                WriteUnicodeChar(str, Ranges[i + 1] - 1, escapeAllChars);
            }
        }
        str.Write(']');

        return str.Str();
    }

    void TUnicodeSet::Save(IOutputStream* out) const {
        ::SaveSize(out, Length);
        ::SaveArray(out, Ranges, Length);
    }

    void TUnicodeSet::Load(IInputStream* in) {
        const size_t length = ::LoadSize(in);
        if (length > 0) {
            ::LoadArray(in, EnsureCapacity(length), length);
        }
        Length = length;
        if (!Valid()) {
            ythrow TSerializeException() << "Loaded broken unicode set";
        }
    }

    TUnicodeSet& TUnicodeSet::Parse(const TWtringBuf& data) {
        Clear();
        NPrivate::ParseUnicodeSet(*this, data);
        return *this;
    }

}
