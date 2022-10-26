#pragma once

#include <util/str_stl.h>
#include <util/charset/unidata.h>
#include <util/generic/algorithm.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>

class IInputStream;
class IOutputStream;

namespace NUnicode {
    namespace NPrivate {
        struct TCategoryRanges;
    }

    class TUnicodeSet {
    private:
        typedef TSimpleSharedPtr<wchar32, TDeleteArray> TDynamicBuffer;

        // Ranges can point to:
        // 1) ShortBuffer for short sets (not more than 2 ranges)
        // 2) static data (for predefined unicode categories)
        // 3) or DynBuffer for big sets
        const wchar32* Ranges;
        wchar32 ShortBuffer[5];
        TDynamicBuffer DynBuffer; // Can be shared between multiple sets
        size_t Length;            // Number of slots in Ranges
        size_t Capacity;          // Capacity of currently used buffer. Zero value means reference to static data

    private:
        Y_FORCE_INLINE bool IsShared() const {
            return Ranges == DynBuffer.Get() && DynBuffer.RefCount() > 1;
        }

        Y_FORCE_INLINE bool IsStatic() const {
            return 0 == Capacity;
        }

        size_t GetRangeItem(wchar32 c, size_t from = 0) const;

        // Extends buffer capacity if required and returns pointer to the writable buffer of slots
        wchar32* EnsureCapacity(size_t capacity);

        // Makes the copy of buffer if the unicode set points to the static or shared data, and returns pointer to the writable buffer of slots
        wchar32* EnsureWritable() {
            if (IsShared()) {
                // If multiple UnicodeSets refer to the same buffer then make the copy
                Capacity = 0;
            }
            if (IsStatic()) {
                // Copy static or shared data to own buffer before modifying
                return EnsureCapacity(Length);
            }
            return const_cast<wchar32*>(Ranges);
        }

        // Returns pointer to the first inserted slot
        wchar32* InsertRangeSlots(const size_t pos, const size_t count);
        void EraseRangeSlots(const size_t pos, const size_t count);

        void AddPredefRanges(const NPrivate::TCategoryRanges& ranges);
        void SetPredefRanges(const NPrivate::TCategoryRanges& ranges);

    public:
        enum {
            CODEPOINT_HIGH = 0x110000 // Next value after maximum valid code point
        };

        TUnicodeSet();
        TUnicodeSet(const TUnicodeSet& s);
        // Unicode set for specific character range. "from", "to" are inclusive
        TUnicodeSet(wchar32 from, wchar32 to);
        // Unicode set consists of all characters from the specified string
        TUnicodeSet(const TWtringBuf& s);
        // Unicode set for predefined category
        TUnicodeSet(WC_TYPE c);

        TUnicodeSet& operator=(const TUnicodeSet& s) {
            return Set(s);
        }

        inline bool operator==(const TUnicodeSet& s) const {
            return Length == s.Length && (Ranges == s.Ranges || ::Equal(Ranges, Ranges + Length, s.Ranges));
        }

        friend inline TUnicodeSet operator~(TUnicodeSet s) {
            return s.Invert();
        }

        friend inline TUnicodeSet operator+(const TUnicodeSet& s1, const TUnicodeSet& s2) {
            return TUnicodeSet(s1).Add(s2);
        }

        TUnicodeSet& Add(const TUnicodeSet& s);
        TUnicodeSet& Add(const TWtringBuf& s);
        TUnicodeSet& Add(wchar32 c);
        // from, to - inclusive
        TUnicodeSet& Add(wchar32 from, wchar32 to);
        TUnicodeSet& Add(WC_TYPE c);
        // Add unicode category by name (one- or two-letter)
        TUnicodeSet& AddCategory(const TStringBuf& catName);

        TUnicodeSet& Set(const TUnicodeSet& s);
        // from, to - inclusive
        TUnicodeSet& Set(wchar32 from, wchar32 to);
        TUnicodeSet& Set(const TWtringBuf& s);
        TUnicodeSet& Set(WC_TYPE c);
        TUnicodeSet& SetCategory(const TStringBuf& catName);

        TUnicodeSet& Invert();
        // Converts existing unicode set to the case-insensitive set
        TUnicodeSet& MakeCaseInsensitive();
        TUnicodeSet& Clear();

        size_t Hash() const;
        TString ToString(bool escapeAllChars = false) const;

        inline bool Valid() const {
            return Length > 0 && Ranges[Length - 1] == CODEPOINT_HIGH;
        }

        inline bool Has(wchar32 c) const {
            if (Y_UNLIKELY(c >= CODEPOINT_HIGH)) {
                return false;
            }
            const size_t item = GetRangeItem(c);
            return (item & 1);
        }

        inline bool Empty() const {
            Y_ASSERT(Valid());
            return Length < 2;
        }

        void Save(IOutputStream* out) const;
        void Load(IInputStream* in);

        TUnicodeSet& Parse(const TWtringBuf& data);
    };

    using TUnicodeSetPtr = TSimpleSharedPtr<TUnicodeSet>;

}

template <>
struct THash<NUnicode::TUnicodeSet> {
    size_t operator()(const NUnicode::TUnicodeSet& s) const {
        return s.Hash();
    }
};
