#pragma once

#include "unversioned_row.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! This class represents a (contextually) schemaful comparable row. It behaves
//! similarly to TUnversionedRow.
class TKey
{
public:
    //! A special null key, use it instead of enclosing TKey in std::optional.
    TKey() = default;
    explicit TKey(TUnversionedValueRange range);

    //! Returns true if key is non-null and false otherwise.
    explicit operator bool() const;

    //! Construct from a given row and possibly key length and validate that row does not contain
    //! setntinels of types Min, Max and Bottom. If key length is not specified, row length will be used instead.
    static TKey FromRow(TUnversionedRow row, std::optional<int> length = {});

    //! Same as above, but does not check that row does not contain sentinels.
    //! NB: in debug mode value type check is still performed, but results in YT_ABORT().
    static TKey FromRowUnchecked(TUnversionedRow row, std::optional<int> length = {});

    //! Performs a deep copy of underlying values into owning row.
    TUnversionedOwningRow AsOwningRow() const;

    const TUnversionedValue& operator[](int index) const;

    int GetLength() const;

    //! Helpers for printing and hashing.
    const TUnversionedValue* Begin() const;
    const TUnversionedValue* End() const;
    TUnversionedValueRange Elements() const;

    void Persist(const TPersistenceContext& context);

private:
    TUnversionedValueRange Elements_;

    static void ValidateValueTypes(TUnversionedValueRange range);
};

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TKey& lhs, const TKey& rhs);

void FormatValue(TStringBuilderBase* builder, const TKey& key, TStringBuf spec);

void Serialize(const TKey& key, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

//! Helper interop function for fixing master's last_key returned from BeginUpload() RPC call
//! and some other places.
//!
//! This is closely related to TKey since original key returned by master may contain sentinels
//! or be of wrong length, so it is not suitable for TKey construction. We fix it by
//! replacing all sentinels to <null> and by padding row with nulls or shortening it so that
//! row length is exactly keyLength.
//!
//! NB: this method is inefficient (as it deals with owning rows), do not use it on hot path.
TUnversionedOwningRow LegacyKeyToKeyFriendlyOwningRow(TUnversionedRow row, int keyLength);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

//! A hasher for TKey.
template <>
struct THash<NYT::NTableClient::TKey>
{
    inline size_t operator()(const NYT::NTableClient::TKey& key) const
    {
        return NYT::NTableClient::TDefaultUnversionedValueRangeHash()(key.Elements());
    }
};
