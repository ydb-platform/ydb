#pragma once

#include "unversioned_row.h"
#include "key.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

//! This class represents a (contextually) schemaful key bound. It defines
//! an open or closed ray in a space of all possible keys.
//! This is a CRTP base for common boilerplate code for owning and non-owning versions.
template <class TRow, class TKeyBound>
class TKeyBoundImpl
{
public:
    // If #Prefix is a null row, key bound has a special meaning of being null bound.
    // It is different from universal or empty bound; use it as a replacement for
    // std::nullopt.
    TRow Prefix;
    bool IsInclusive = false;
    bool IsUpper = false;

    //! Construct from a given row and validate that row does not contain
    //! setntinels of types Min, Max and Bottom.
    static TKeyBound FromRow(const TRow& row, bool isInclusive, bool isUpper);

    //! Same as previous but for rvalue refs.
    static TKeyBound FromRow(TRow&& row, bool isInclusive, bool isUpper);

    //! Construct from a given row without checking presence of types Min, Max and Bottom.
    //! NB: in debug mode value type check is still performed, but results in YT_ABORT().
    static TKeyBound FromRowUnchecked(const TRow& row, bool isInclusive, bool isUpper);

    //! Same as previous but for rvalue refs.
    static TKeyBound FromRowUnchecked(TRow&& row, bool isInclusive, bool isUpper);

    // In order to reduce amount of repetitive constructions like
    //
    //   TKeyBound::FromRow(row, /* isInclusive */ true, /* isUpper */ false)
    //
    // with two flags known in compile-time, we introduce a helper class
    // allowing you to build key bounds as follows:
    //
    //   TKeyBound::FromRow() >= row
    //
    // These helpers also support unchecked variant of static constructors above.
    // They work both for owning and non-owning kinds of key bound.
    #define XX(suffix) \
        struct TBuilder ## suffix { \
            TKeyBound operator > (const TRow& row) { return TKeyBound::FromRow ## suffix(row, false, false); } \
            TKeyBound operator >=(const TRow& row) { return TKeyBound::FromRow ## suffix(row, true , false); } \
            TKeyBound operator < (const TRow& row) { return TKeyBound::FromRow ## suffix(row, false, true ); } \
            TKeyBound operator <=(const TRow& row) { return TKeyBound::FromRow ## suffix(row, true , true ); } \
            TKeyBound operator > (TRow&& row) { return TKeyBound::FromRow ## suffix(row, false, false); } \
            TKeyBound operator >=(TRow&& row) { return TKeyBound::FromRow ## suffix(row, true , false); } \
            TKeyBound operator < (TRow&& row) { return TKeyBound::FromRow ## suffix(row, false, true ); } \
            TKeyBound operator <=(TRow&& row) { return TKeyBound::FromRow ## suffix(row, true , true ); } \
        }; \
        TBuilder ## suffix static FromRow ## suffix() { return TBuilder##suffix(); }

    XX()
    XX(Unchecked)

    #undef XX

    //! Return a key bound that allows any key.
    static TKeyBound MakeUniversal(bool isUpper);

    //! Return a key bound that does not allow any key.
    static TKeyBound MakeEmpty(bool isUpper);

    void FormatValue(TStringBuilderBase* builder) const;

    explicit operator bool() const;

    //! Test if this key bound allows any key.
    bool IsUniversal() const;

    //! Test if this key bound allows no keys.
    bool IsEmpty() const;

    //! Return key bound which is complementary to current.
    TKeyBound Invert() const;

    //! Return key bound with same prefix and direction but toggled inclusiveness.
    TKeyBound ToggleInclusiveness() const;

    //! Return key bound that is upper among {*this, this->Invert()}.
    TKeyBound UpperCounterpart() const;
    //! Return key bound that is lower among {*this, this->Invert()}.
    TKeyBound LowerCounterpart() const;

    //! Returns string among {">=", ">", "<=", "<"} defining this key bound kind.
    TStringBuf GetRelation() const;

    void Persist(const TPersistenceContext& context);

    void Serialize(NYson::IYsonConsumer* consumer) const;

private:
    static void ValidateValueTypes(const TRow& row);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TKeyBound
    : public NDetail::TKeyBoundImpl<TUnversionedRow, TKeyBound>
{
public:
    TOwningKeyBound ToOwning() const;
};

void FormatValue(TStringBuilderBase* builder, const TKeyBound& keyBound, TStringBuf spec);

void Serialize(const TKeyBound& keyBound, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TOwningKeyBound
    : public NDetail::TKeyBoundImpl<TUnversionedOwningRow, TOwningKeyBound>
{
public:
    operator TKeyBound() const;
};

void FormatValue(TStringBuilderBase* builder, const TOwningKeyBound& keyBound, TStringBuf spec);

void Serialize(const TOwningKeyBound& keyBound, NYson::IYsonConsumer* consumer);
void Deserialize(TOwningKeyBound& keyBound, const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

bool operator ==(const TKeyBound& lhs, const TKeyBound& rhs);
bool operator ==(const TOwningKeyBound& lhs, const TOwningKeyBound& rhs);

////////////////////////////////////////////////////////////////////////////////

// Interop functions.

//! Returns significant prefix length and inclusiveness.
std::pair<int, bool> GetBoundPrefixAndInclusiveness(TUnversionedRow row, bool isUpper, int keyLength);

//! Convert legacy key bound expressed as a row possibly containing Min/Max to owning key bound.
//! NB: key length is needed to properly distinguish if K + [min] is an inclusive K or exclusive K.
TOwningKeyBound KeyBoundFromLegacyRow(TUnversionedRow row, bool isUpper, int keyLength);

//! Same as previous, but non-owning variant over row buffer.
TKeyBound KeyBoundFromLegacyRow(TUnversionedRow row, bool isUpper, int keyLength, const TRowBufferPtr& rowBuffer);

//! Convert key bound to legacy key bound.
TUnversionedOwningRow KeyBoundToLegacyRow(TKeyBound keyBound);

//! Same as previous, but non-owning variant over row buffer.
TUnversionedRow KeyBoundToLegacyRow(TKeyBound keyBound, const TRowBufferPtr& rowBuffer);

//! Build the most accurate key bound of length #length corresponding to the ray containing
//! ray corresponding to #keyBound.
TKeyBound ShortenKeyBound(TKeyBound keyBound, int length, const TRowBufferPtr& rowBuffer);

//! Owning version of #ShortenKeyBound.
TOwningKeyBound ShortenKeyBound(TOwningKeyBound keyBound, int length);

////////////////////////////////////////////////////////////////////////////////

// Debug printers for Gtest unittests.

void PrintTo(const TKeyBound& key, ::std::ostream* os);
void PrintTo(const TOwningKeyBound& key, ::std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

//! A hasher for TKeyBound.
template <>
struct THash<NYT::NTableClient::TKeyBound>
{
    size_t operator()(const NYT::NTableClient::TKeyBound& keyBound) const;
};
