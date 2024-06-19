#include "read_limit.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NChunkClient {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TLegacyReadLimit::TLegacyReadLimit(const NProto::TReadLimit& protoLimit)
{
    InitCopy(protoLimit);
}

TLegacyReadLimit::TLegacyReadLimit(NProto::TReadLimit&& protoLimit)
{
    InitMove(std::move(protoLimit));
}

TLegacyReadLimit::TLegacyReadLimit(const std::unique_ptr<NProto::TReadLimit>& protoLimit)
{
    if (protoLimit) {
        InitCopy(*protoLimit);
    }
}

TLegacyReadLimit::TLegacyReadLimit(const TLegacyOwningKey& key)
{
    SetLegacyKey(key);
}

TLegacyReadLimit::TLegacyReadLimit(TLegacyOwningKey&& key)
{
    SetLegacyKey(std::move(key));
}

TLegacyReadLimit& TLegacyReadLimit::operator= (const NProto::TReadLimit& protoLimit)
{
    InitCopy(protoLimit);
    return *this;
}

TLegacyReadLimit& TLegacyReadLimit::operator= (NProto::TReadLimit&& protoLimit)
{
    InitMove(std::move(protoLimit));
    return *this;
}

TLegacyReadLimit TLegacyReadLimit::GetSuccessor() const
{
    TLegacyReadLimit result;
    if (HasLegacyKey()) {
        auto key = GetLegacyKey();
        result.SetLegacyKey(GetKeyPrefixSuccessor(key, key.GetCount()));
    }
    if (HasRowIndex()) {
        result.SetRowIndex(GetRowIndex() + 1);
    }
    if (HasChunkIndex()) {
        result.SetChunkIndex(GetChunkIndex() + 1);
    }
    if (HasTabletIndex()) {
        // We use tabletIndex in ordered dynamic tables, where indexing is over pairs (tabletIndex, rowIndex).
        result.SetTabletIndex(GetTabletIndex());
    }
    return result;
}

const NProto::TReadLimit& TLegacyReadLimit::AsProto() const
{
    return ReadLimit_;
}

const TLegacyOwningKey& TLegacyReadLimit::GetLegacyKey() const
{
    YT_ASSERT(HasLegacyKey());
    return Key_;
}

bool TLegacyReadLimit::HasLegacyKey() const
{
    return ReadLimit_.has_legacy_key();
}

TLegacyReadLimit& TLegacyReadLimit::SetLegacyKey(const TLegacyOwningKey& key)
{
    Key_ = key;
    ToProto(ReadLimit_.mutable_legacy_key(), Key_);
    return *this;
}

TLegacyReadLimit& TLegacyReadLimit::SetLegacyKey(TLegacyOwningKey&& key)
{
    swap(Key_, key);
    ToProto(ReadLimit_.mutable_legacy_key(), Key_);
    return *this;
}

i64 TLegacyReadLimit::GetRowIndex() const
{
    YT_ASSERT(HasRowIndex());
    return ReadLimit_.row_index();
}

bool TLegacyReadLimit::HasRowIndex() const
{
    return ReadLimit_.has_row_index();
}

TLegacyReadLimit& TLegacyReadLimit::SetRowIndex(i64 rowIndex)
{
    ReadLimit_.set_row_index(rowIndex);
    return *this;
}

i64 TLegacyReadLimit::GetOffset() const
{
    YT_ASSERT(HasOffset());
    return ReadLimit_.offset();
}

bool TLegacyReadLimit::HasOffset() const
{
    return ReadLimit_.has_offset();
}

TLegacyReadLimit& TLegacyReadLimit::SetOffset(i64 offset)
{
    ReadLimit_.set_offset(offset);
    return *this;
}

i64 TLegacyReadLimit::GetChunkIndex() const
{
    YT_ASSERT(HasChunkIndex());
    return ReadLimit_.chunk_index();
}

bool TLegacyReadLimit::HasChunkIndex() const
{
    return ReadLimit_.has_chunk_index();
}

TLegacyReadLimit& TLegacyReadLimit::SetChunkIndex(i64 chunkIndex)
{
    ReadLimit_.set_chunk_index(chunkIndex);
    return *this;
}

i32 TLegacyReadLimit::GetTabletIndex() const
{
    YT_ASSERT(HasTabletIndex());
    return ReadLimit_.tablet_index();
}

bool TLegacyReadLimit::HasTabletIndex() const
{
    return ReadLimit_.has_tablet_index();
}

TLegacyReadLimit& TLegacyReadLimit::SetTabletIndex(i32 tabletIndex)
{
    ReadLimit_.set_tablet_index(tabletIndex);
    return *this;
}

bool TLegacyReadLimit::IsTrivial() const
{
    return NChunkClient::IsTrivial(ReadLimit_);
}

void TLegacyReadLimit::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ReadLimit_);
    Persist(context, Key_);
}

void TLegacyReadLimit::MergeLowerLegacyKey(const TLegacyOwningKey& key)
{
    if (!HasLegacyKey() || GetLegacyKey() < key) {
        SetLegacyKey(key);
    }
}

void TLegacyReadLimit::MergeUpperLegacyKey(const TLegacyOwningKey& key)
{
    if (!HasLegacyKey() || GetLegacyKey() > key) {
        SetLegacyKey(key);
    }
}

void TLegacyReadLimit::MergeLowerRowIndex(i64 rowIndex)
{
    if (!HasRowIndex() || GetRowIndex() < rowIndex) {
        SetRowIndex(rowIndex);
    }
}

void TLegacyReadLimit::MergeUpperRowIndex(i64 rowIndex)
{
    if (!HasRowIndex() || GetRowIndex() > rowIndex) {
        SetRowIndex(rowIndex);
    }
}

void TLegacyReadLimit::InitKey()
{
    if (ReadLimit_.has_legacy_key()) {
        FromProto(&Key_, ReadLimit_.legacy_key());
    }
}

void TLegacyReadLimit::InitCopy(const NProto::TReadLimit& readLimit)
{
    ReadLimit_.CopyFrom(readLimit);
    InitKey();
}

void TLegacyReadLimit::InitMove(NProto::TReadLimit&& readLimit)
{
    ReadLimit_.Swap(&readLimit);
    InitKey();
}

size_t TLegacyReadLimit::SpaceUsed() const
{
    return sizeof(*this) +
        ReadLimit_.SpaceUsed() - sizeof(ReadLimit_) +
        Key_.GetSpaceUsed() - sizeof(Key_);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TLegacyReadLimit& limit, TStringBuf /*spec*/)
{
    builder->AppendChar('{');

    bool firstToken = true;
    auto append = [&] (const char* label, const auto& value) {
        if (!firstToken) {
            builder->AppendString(", ");
        }
        firstToken = false;
        builder->AppendFormat("%v: %v", label, value);
    };

    if (limit.HasLegacyKey()) {
        append("Key", limit.GetLegacyKey());
    }

    if (limit.HasRowIndex()) {
        append("RowIndex", limit.GetRowIndex());
    }

    if (limit.HasOffset()) {
        append("Offset", limit.GetOffset());
    }

    if (limit.HasChunkIndex()) {
        append("ChunkIndex", limit.GetChunkIndex());
    }

    if (limit.HasTabletIndex()) {
        append("TabletIndex", limit.GetTabletIndex());
    }

    builder->AppendChar('}');
}

bool IsTrivial(const TLegacyReadLimit& limit)
{
    return limit.IsTrivial();
}

bool IsTrivial(const NProto::TReadLimit& limit)
{
    return
        !limit.has_row_index() &&
        !limit.has_legacy_key() &&
        !limit.has_offset() &&
        !limit.has_chunk_index() &&
        !limit.has_tablet_index();
}

void ToProto(NProto::TReadLimit* protoReadLimit, const TLegacyReadLimit& readLimit)
{
    protoReadLimit->CopyFrom(readLimit.AsProto());
}

void FromProto(TLegacyReadLimit* readLimit, const NProto::TReadLimit& protoReadLimit)
{
    *readLimit = protoReadLimit;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TLegacyReadLimit& readLimit, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(readLimit.HasLegacyKey(), [&] (TFluentMap fluent) {
                fluent.Item("key").Value(readLimit.GetLegacyKey());
            })
            .DoIf(readLimit.HasRowIndex(), [&] (TFluentMap fluent) {
                fluent.Item("row_index").Value(readLimit.GetRowIndex());
            })
            .DoIf(readLimit.HasOffset(), [&] (TFluentMap fluent) {
                fluent.Item("offset").Value(readLimit.GetOffset());
            })
            .DoIf(readLimit.HasChunkIndex(), [&] (TFluentMap fluent) {
                fluent.Item("chunk_index").Value(readLimit.GetChunkIndex());
            })
            .DoIf(readLimit.HasTabletIndex(), [&] (TFluentMap fluent) {
                fluent.Item("tablet_index").Value(readLimit.GetTabletIndex());
            })
        .EndMap();
}

namespace {

template <class T>
std::optional<T> FindReadLimitComponent(const IAttributeDictionaryPtr& attributes, const TString& key)
{
    try {
        return attributes->Find<T>(key);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing %Qv component of a read limit",
            key)
            << ex;
    }
}

} // namespace

void Deserialize(TLegacyReadLimit& readLimit, INodePtr node)
{
    if (node->GetType() != NYTree::ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Error parsing read limit: expected %Qlv, actual %Qlv",
            NYTree::ENodeType::Map,
            node->GetType());
    }

    readLimit = TLegacyReadLimit();
    auto attributes = ConvertToAttributes(node);

    auto optionalKey = FindReadLimitComponent<TLegacyOwningKey>(attributes, "key");
    if (optionalKey) {
        readLimit.SetLegacyKey(*optionalKey);
    }

    auto optionalRowIndex = FindReadLimitComponent<i64>(attributes, "row_index");
    if (optionalRowIndex) {
        readLimit.SetRowIndex(*optionalRowIndex);
    }

    auto optionalOffset = FindReadLimitComponent<i64>(attributes, "offset");
    if (optionalOffset) {
        readLimit.SetOffset(*optionalOffset);
    }

    auto optionalChunkIndex = FindReadLimitComponent<i64>(attributes, "chunk_index");
    if (optionalChunkIndex) {
        readLimit.SetChunkIndex(*optionalChunkIndex);
    }

    auto optionalTabletIndex = FindReadLimitComponent<i32>(attributes, "tablet_index");
    if (optionalTabletIndex) {
        readLimit.SetTabletIndex(*optionalTabletIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

TLegacyReadRange::TLegacyReadRange(const TLegacyReadLimit& exact)
    : LowerLimit_(exact)
    , UpperLimit_(exact.GetSuccessor())
{ }

TLegacyReadRange::TLegacyReadRange(const TLegacyReadLimit& lowerLimit, const TLegacyReadLimit& upperLimit)
    : LowerLimit_(lowerLimit)
    , UpperLimit_(upperLimit)
{ }

TLegacyReadRange::TLegacyReadRange(const NProto::TReadRange& range)
{
    InitCopy(range);
}

TLegacyReadRange::TLegacyReadRange(NProto::TReadRange&& range)
{
    InitMove(std::move(range));
}

TLegacyReadRange& TLegacyReadRange::operator= (const NProto::TReadRange& range)
{
    InitCopy(range);
    return *this;
}

TLegacyReadRange& TLegacyReadRange::operator= (NProto::TReadRange&& range)
{
    InitMove(std::move(range));
    return *this;
}

void TLegacyReadRange::InitCopy(const NProto::TReadRange& range)
{
    LowerLimit_ = range.has_lower_limit() ? TLegacyReadLimit(range.lower_limit()) : TLegacyReadLimit();
    UpperLimit_ = range.has_upper_limit() ? TLegacyReadLimit(range.upper_limit()) : TLegacyReadLimit();
}

void TLegacyReadRange::InitMove(NProto::TReadRange&& range)
{
    LowerLimit_ = range.has_lower_limit() ? TLegacyReadLimit(range.lower_limit()) : TLegacyReadLimit();
    UpperLimit_ = range.has_upper_limit() ? TLegacyReadLimit(range.upper_limit()) : TLegacyReadLimit();
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TLegacyReadRange& range, TStringBuf spec)
{
    FormatValue(builder, Format("[<%v> : <%v>]", range.LowerLimit(), range.UpperLimit()), spec);
}

void ToProto(NProto::TReadRange* protoReadRange, const TLegacyReadRange& readRange)
{
    if (!readRange.LowerLimit().IsTrivial()) {
        ToProto(protoReadRange->mutable_lower_limit(), readRange.LowerLimit());
    }
    if (!readRange.UpperLimit().IsTrivial()) {
        ToProto(protoReadRange->mutable_upper_limit(), readRange.UpperLimit());
    }
}

void FromProto(TLegacyReadRange* readRange, const NProto::TReadRange& protoReadRange)
{
    *readRange = TLegacyReadRange(protoReadRange);
}

void Serialize(const TLegacyReadRange& readRange, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(!readRange.LowerLimit().IsTrivial(), [&] (TFluentMap fluent) {
                fluent.Item("lower_limit").Value(readRange.LowerLimit());
            })
            .DoIf(!readRange.UpperLimit().IsTrivial(), [&] (TFluentMap fluent) {
                fluent.Item("upper_limit").Value(readRange.UpperLimit());
            })
        .EndMap();

}

namespace {

template <class T>
std::optional<T> FindReadRangeComponent(const IAttributeDictionaryPtr& attributes, const TString& key)
{
    try {
        return attributes->Find<T>(key);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing %Qv component of a read range",
            key)
            << ex;
    }
}

} // namespace

void Deserialize(TLegacyReadRange& readRange, NYTree::INodePtr node)
{
    if (node->GetType() != NYTree::ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Error parsing read range: expected %Qlv, actual %Qlv",
            NYTree::ENodeType::Map,
            node->GetType());
    }

    readRange = TLegacyReadRange();
    auto attributes = ConvertToAttributes(node);
    auto optionalExact = FindReadRangeComponent<TLegacyReadLimit>(attributes, "exact");
    auto optionalLowerLimit = FindReadRangeComponent<TLegacyReadLimit>(attributes, "lower_limit");
    auto optionalUpperLimit = FindReadRangeComponent<TLegacyReadLimit>(attributes, "upper_limit");

    if (optionalExact) {
        if (optionalLowerLimit || optionalUpperLimit) {
            THROW_ERROR_EXCEPTION("\"lower_limit\" and \"upper_limit\" attributes cannot be specified "
                "together with \"exact\" attribute");
        }
        readRange = TLegacyReadRange(*optionalExact);
    }

    if (optionalLowerLimit) {
        readRange.LowerLimit() = *optionalLowerLimit;
    }

    if (optionalUpperLimit) {
        readRange.UpperLimit() = *optionalUpperLimit;
    }
}

void TLegacyReadRange::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, LowerLimit_);
    Persist(context, UpperLimit_);
}

////////////////////////////////////////////////////////////////////////////////

TReadLimit::TReadLimit(const TKeyBound& keyBound)
    : KeyBound_(keyBound.ToOwning())
{ }

TReadLimit::TReadLimit(TOwningKeyBound keyBound)
    : KeyBound_(std::move(keyBound))
{ }

TReadLimit::TReadLimit(
    const NProto::TReadLimit& readLimit,
    bool isUpper,
    int keyLength)
{
    if (readLimit.has_key_bound_prefix()) {
        NTableClient::FromProto(&KeyBound_.Prefix, readLimit.key_bound_prefix());
        KeyBound_.IsUpper = isUpper;
        KeyBound_.IsInclusive = readLimit.key_bound_is_inclusive();
    } else if (readLimit.has_legacy_key()) {
        YT_VERIFY(keyLength > 0);
        TUnversionedOwningRow legacyKey;
        FromProto(&legacyKey, readLimit.legacy_key());
        KeyBound_ = KeyBoundFromLegacyRow(legacyKey, isUpper, keyLength);
    }

    if (readLimit.has_row_index()) {
        RowIndex_ = readLimit.row_index();
    }
    if (readLimit.has_offset()) {
        Offset_ = readLimit.offset();
    }
    if (readLimit.has_chunk_index()) {
        ChunkIndex_ = readLimit.chunk_index();
    }
    if (readLimit.has_tablet_index()) {
        TabletIndex_ = readLimit.tablet_index();
    }
}

bool TReadLimit::IsTrivial() const
{
    return GetSelectorCount() == 0;
}

bool TReadLimit::HasIndependentSelectors() const
{
    if (GetSelectorCount() >= 3) {
        return true;
    }
    if (GetSelectorCount() == 2) {
        if (RowIndex_ && TabletIndex_) {
            // Row index and tablet index are not independent.
            return false;
        } else {
            return true;
        }
    }
    // Having single or no selector at all means that there are no independent selectors.
    return false;
}

int TReadLimit::GetSelectorCount() const
{
    int selectorCount = 0;

    if (KeyBound_) {
        ++selectorCount;
    }

    if (RowIndex_) {
        ++selectorCount;
    }

    if (Offset_) {
        ++selectorCount;
    }

    if (ChunkIndex_) {
        ++selectorCount;
    }

    if (TabletIndex_) {
        ++selectorCount;
    }

    return selectorCount;
}

TReadLimit TReadLimit::ToExactUpperCounterpart() const
{
    YT_VERIFY(!HasIndependentSelectors());

    TReadLimit result = *this;

    if (KeyBound_) {
        YT_VERIFY(!KeyBound_.IsUpper);
        // We are either a special >[] empty key bound, or a lower part of some
        // key bound from an exact read limit, in which case we must be inclusive.
        if (!KeyBound_.IsEmpty()) {
            YT_VERIFY(KeyBound_.IsInclusive);
        }
        result.KeyBound_ = result.KeyBound_.Invert().ToggleInclusiveness();
    }

    // Ordered dyntables obey pretty tricky logic. All rows are ordered by
    // (tablet_index, row_index) tuple lexicographically. Exact selector is
    // transformed to lower+upper limits like following:
    // - {tablet_index = 42} -> {tablet_index = 42}..{tablet_index = 43}
    // - {tablet_index = 42; row_index = 123} ->
    //   {tablet_index = 42; row_index = 123}..{tablet_index = 42; row_index = 124}
    // Thus, both for static and ordered dynamic tables we should increment (only)
    // row index if it is present. Addititonally, for ordered dynamic tables,
    // if row index is missing, we must increment tablet index if it is present.

    if (RowIndex_) {
        ++*result.RowIndex_;
    } else if (TabletIndex_) {
        ++*result.TabletIndex_;
    }

    if (Offset_) {
        ++*result.Offset_;
    }

    if (ChunkIndex_) {
        ++*result.ChunkIndex_;
    }

    return result;
}

TReadLimit TReadLimit::Invert() const
{
    YT_VERIFY(!HasIndependentSelectors());

    auto result = *this;

    if (KeyBound_) {
        result.KeyBound() = result.KeyBound().Invert();
    }

    return result;
}

bool TReadLimit::operator == (const TReadLimit& other) const
{
    return
        KeyBound_ == other.KeyBound_ &&
        RowIndex_ == other.RowIndex_ &&
        Offset_ == other.Offset_ &&
        ChunkIndex_ == other.ChunkIndex_ &&
        TabletIndex_ == other.TabletIndex_;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TReadLimit& readLimit, TStringBuf /*spec*/)
{
    builder->AppendChar('{');

    bool firstToken = true;
    auto append = [&] (const char* label, const auto& value) {
        if (!firstToken) {
            builder->AppendString(", ");
        }
        firstToken = false;
        builder->AppendFormat("%v: %v", label, value);
    };

    if (readLimit.KeyBound()) {
        append("Key", readLimit.KeyBound());
    }

    if (readLimit.GetRowIndex()) {
        append("RowIndex", readLimit.GetRowIndex());
    }

    if (readLimit.GetOffset()) {
        append("Offset", readLimit.GetOffset());
    }

    if (readLimit.GetChunkIndex()) {
        append("ChunkIndex", readLimit.GetChunkIndex());
    }

    if (readLimit.GetTabletIndex()) {
        append("TabletIndex", readLimit.GetTabletIndex());
    }

    builder->AppendChar('}');
}

void ToProto(NProto::TReadLimit* protoReadLimit, const TReadLimit& readLimit)
{
    if (readLimit.KeyBound()) {
        ToProto(protoReadLimit->mutable_key_bound_prefix(), readLimit.KeyBound().Prefix);
        protoReadLimit->set_key_bound_is_inclusive(readLimit.KeyBound().IsInclusive);
        ToProto(protoReadLimit->mutable_legacy_key(), KeyBoundToLegacyRow(readLimit.KeyBound()));
    }

    if (readLimit.GetRowIndex()) {
        protoReadLimit->set_row_index(*readLimit.GetRowIndex());
    }
    if (readLimit.GetOffset()) {
        protoReadLimit->set_offset(*readLimit.GetOffset());
    }
    if (readLimit.GetChunkIndex()) {
        protoReadLimit->set_chunk_index(*readLimit.GetChunkIndex());
    }
    if (readLimit.GetTabletIndex()) {
        protoReadLimit->set_tablet_index(*readLimit.GetTabletIndex());
    }
}

void FromProto(TReadLimit* readLimit, const NProto::TReadLimit& protoReadLimit, bool isUpper, int keyLength)
{
    // Formally speaking two exceptions in this method could be YT_VERIFY, but let's
    // try to be more tolerant to possible bugs. After all, this code is used in
    // master a lot.

    auto validateKeyLengthIsPresent = [=] {
        if (keyLength == 0) {
            THROW_ERROR_EXCEPTION(
                "Read limit contains key, but key length is not provided");
        }
    };

    readLimit->KeyBound().IsUpper = isUpper;
    if (protoReadLimit.has_key_bound_prefix()) {
        validateKeyLengthIsPresent();

        FromProto(&readLimit->KeyBound().Prefix, protoReadLimit.key_bound_prefix());
        readLimit->KeyBound().IsInclusive = protoReadLimit.key_bound_is_inclusive();

        if (readLimit->KeyBound().Prefix.GetCount() > keyLength) {
            THROW_ERROR_EXCEPTION(
                "Invalid key bound prefix length; expected no more than %v, actual %v",
                keyLength,
                readLimit->KeyBound().Prefix.GetCount());
        }
    } else if (protoReadLimit.has_legacy_key()) {
        validateKeyLengthIsPresent();

        TLegacyOwningKey legacyKey;
        FromProto(&legacyKey, protoReadLimit.legacy_key());
        readLimit->KeyBound() = KeyBoundFromLegacyRow(legacyKey, isUpper, keyLength);
    }

    if (protoReadLimit.has_row_index()) {
        readLimit->SetRowIndex(protoReadLimit.row_index());
    }
    if (protoReadLimit.has_offset()) {
        readLimit->SetOffset(protoReadLimit.offset());
    }
    if (protoReadLimit.has_chunk_index()) {
        readLimit->SetChunkIndex(protoReadLimit.chunk_index());
    }
    if (protoReadLimit.has_tablet_index()) {
        readLimit->SetTabletIndex(protoReadLimit.tablet_index());
    }
}

void Serialize(const TReadLimit& readLimit, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(static_cast<bool>(readLimit.KeyBound()), [&] (TFluentMap fluent) {
                fluent
                    .Item("key_bound").Value(readLimit.KeyBound())
                    // COMPAT(max42): it is important to serialize also a key in order to keep
                    // backward compatibility with old clusters. Note that modern version of code
                    // should ignore "key" selector if "key_bound" is present.
                    .Item("key").Value(KeyBoundToLegacyRow(readLimit.KeyBound()));
            })
            .OptionalItem("row_index", readLimit.GetRowIndex())
            .OptionalItem("offset", readLimit.GetOffset())
            .OptionalItem("chunk_index", readLimit.GetChunkIndex())
            .OptionalItem("tablet_index", readLimit.GetTabletIndex())
        .EndMap();
}

void Deserialize(TReadLimit& readLimit, const INodePtr& node)
{
    if (node->GetType() != NYTree::ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Error parsing read limit: expected %Qlv node, actual %Qlv node",
            NYTree::ENodeType::Map,
            node->GetType());
    }

    readLimit = TReadLimit();
    auto attributes = ConvertToAttributes(node);

    if (auto optionalKeyBound = FindReadLimitComponent<TOwningKeyBound>(attributes, "key_bound")) {
        readLimit.KeyBound() = *optionalKeyBound;
    }

    if (auto optionalRowIndex = FindReadLimitComponent<i64>(attributes, "row_index")) {
        readLimit.SetRowIndex(*optionalRowIndex);
    }

    if (auto optionalOffset = FindReadLimitComponent<i64>(attributes, "offset")) {
        readLimit.SetOffset(*optionalOffset);
    }

    if (auto optionalChunkIndex = FindReadLimitComponent<i64>(attributes, "chunk_index")) {
        readLimit.SetChunkIndex(*optionalChunkIndex);
    }

    if (auto optionalTabletIndex = FindReadLimitComponent<i32>(attributes, "tablet_index")) {
        readLimit.SetTabletIndex(*optionalTabletIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

TReadRange::TReadRange(TReadLimit lowerLimit, TReadLimit upperLimit)
    : LowerLimit_(std::move(lowerLimit))
    , UpperLimit_(std::move(upperLimit))
{
    if (LowerLimit_.KeyBound()) {
        YT_VERIFY(!LowerLimit_.KeyBound().IsUpper);
    }
    if (UpperLimit_.KeyBound()) {
        YT_VERIFY(UpperLimit_.KeyBound().IsUpper);
    }
}

TReadRange::TReadRange(
    const NProto::TReadRange& range,
    int keyLength)
{
    if (range.has_lower_limit()) {
        LowerLimit_ = TReadLimit(range.lower_limit(), /* isUpper */ false, keyLength);
    }
    if (range.has_upper_limit()) {
        UpperLimit_ = TReadLimit(range.upper_limit(), /* isUpper */ true, keyLength);
    }
}

bool TReadRange::operator == (const TReadRange& other) const
{
    return LowerLimit_ == other.LowerLimit_ && UpperLimit_ == other.UpperLimit_;
}

TReadRange TReadRange::MakeEmpty()
{
    TReadRange result;
    result.LowerLimit_.SetRowIndex(0);
    result.UpperLimit_.SetRowIndex(0);
    YT_ASSERT(result.LowerLimit() == result.UpperLimit() && !result.LowerLimit().IsTrivial());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TReadRange& readRange, TStringBuf spec)
{
    FormatValue(builder, Format("[<%v> : <%v>]", readRange.LowerLimit(), readRange.UpperLimit()), spec);
}

void ToProto(NProto::TReadRange* protoReadRange, const TReadRange& readRange)
{
    if (!readRange.LowerLimit().IsTrivial()) {
        if (readRange.LowerLimit().KeyBound()) {
            YT_VERIFY(!readRange.LowerLimit().KeyBound().IsUpper);
        }
        ToProto(protoReadRange->mutable_lower_limit(), readRange.LowerLimit());
    }
    if (!readRange.UpperLimit().IsTrivial()) {
        if (readRange.UpperLimit().KeyBound()) {
            YT_VERIFY(readRange.UpperLimit().KeyBound().IsUpper);
        }
        ToProto(protoReadRange->mutable_upper_limit(), readRange.UpperLimit());
    }
}

void FromProto(TReadRange* readRange, const NProto::TReadRange& protoReadRange, int keyLength)
{
    if (protoReadRange.has_lower_limit()) {
        FromProto(&readRange->LowerLimit(), protoReadRange.lower_limit(), /* isUpper */ false, keyLength);
    }
    if (protoReadRange.has_upper_limit()) {
        FromProto(&readRange->UpperLimit(), protoReadRange.upper_limit(), /* isUpper */ true, keyLength);
    }
}

void Serialize(const TReadRange& readLimit, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("lower_limit").Value(readLimit.LowerLimit())
            .Item("upper_limit").Value(readLimit.UpperLimit())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_INTERMEDIATE_PROTO_INTEROP_BYTES_FIELD_REPRESENTATION(NProto::TReadLimit, /*key*/ 4, TUnversionedOwningRow)

////////////////////////////////////////////////////////////////////////////////

TReadLimit ReadLimitFromLegacyReadLimit(const TLegacyReadLimit& legacyReadLimit, bool isUpper, int keyLength)
{
    TReadLimit result;
    if (legacyReadLimit.HasRowIndex()) {
        result.SetRowIndex(legacyReadLimit.GetRowIndex());
    }
    if (legacyReadLimit.HasChunkIndex()) {
        result.SetChunkIndex(legacyReadLimit.GetChunkIndex());
    }
    if (legacyReadLimit.HasOffset()) {
        result.SetOffset(legacyReadLimit.GetOffset());
    }
    if (legacyReadLimit.HasTabletIndex()) {
        result.SetTabletIndex(legacyReadLimit.GetTabletIndex());
    }
    if (legacyReadLimit.HasLegacyKey()) {
        result.KeyBound() = KeyBoundFromLegacyRow(legacyReadLimit.GetLegacyKey(), isUpper, keyLength);
    }

    return result;
}

TReadLimit ReadLimitFromLegacyReadLimitKeyless(const TLegacyReadLimit& legacyReadLimit)
{
    YT_VERIFY(!legacyReadLimit.HasLegacyKey());

    TReadLimit result;
    if (legacyReadLimit.HasRowIndex()) {
        result.SetRowIndex(legacyReadLimit.GetRowIndex());
    }
    if (legacyReadLimit.HasChunkIndex()) {
        result.SetChunkIndex(legacyReadLimit.GetChunkIndex());
    }
    if (legacyReadLimit.HasOffset()) {
        result.SetOffset(legacyReadLimit.GetOffset());
    }
    if (legacyReadLimit.HasTabletIndex()) {
        result.SetTabletIndex(legacyReadLimit.GetTabletIndex());
    }

    return result;
}

TLegacyReadLimit ReadLimitToLegacyReadLimit(const TReadLimit& readLimit)
{
    TLegacyReadLimit result;
    if (const auto& rowIndex = readLimit.GetRowIndex()) {
        result.SetRowIndex(*rowIndex);
    }
    if (const auto& chunkIndex = readLimit.GetChunkIndex()) {
        result.SetChunkIndex(*chunkIndex);
    }
    if (const auto& offset = readLimit.GetOffset()) {
        result.SetOffset(*offset);
    }
    if (const auto& tabletIndex = readLimit.GetTabletIndex()) {
        result.SetTabletIndex(*tabletIndex);
    }
    if (readLimit.KeyBound()) {
        result.SetLegacyKey(KeyBoundToLegacyRow(readLimit.KeyBound()));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TReadRange ReadRangeFromLegacyReadRange(const TLegacyReadRange& legacyReadRange, int keyLength)
{
    TReadRange result;

    result.LowerLimit() = ReadLimitFromLegacyReadLimit(legacyReadRange.LowerLimit(), /* isUpper */ false, keyLength);
    result.UpperLimit() = ReadLimitFromLegacyReadLimit(legacyReadRange.UpperLimit(), /* isUpper */ true, keyLength);

    return result;
}

TReadRange ReadRangeFromLegacyReadRangeKeyless(const TLegacyReadRange& legacyReadRange)
{
    TReadRange result;

    result.LowerLimit() = ReadLimitFromLegacyReadLimitKeyless(legacyReadRange.LowerLimit());
    result.UpperLimit() = ReadLimitFromLegacyReadLimitKeyless(legacyReadRange.UpperLimit());

    return result;
}

TLegacyReadRange ReadRangeToLegacyReadRange(const TReadRange& readRange)
{
    TLegacyReadRange result;

    result.LowerLimit() = ReadLimitToLegacyReadLimit(readRange.LowerLimit());
    result.UpperLimit() = ReadLimitToLegacyReadLimit(readRange.UpperLimit());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
