#pragma once

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/generic/string.h>

#include <memory>
#include <optional>
#include <vector>

namespace NKikimr::NStat {

// One tuple element for TStablePickleTupleBuilder: a column's type, nullability and textual value.
struct TPickleColumnValue {
    NScheme::TTypeId Type;             // scheme type id (Bool, Int32, Timestamp, String, Uuid, Decimal, ...)
    bool Nullable = true;              // element type is Optional<T> when true (a nullable column)
    std::optional<TString> Value;      // textual literal (the same string a YQL type ctor takes); nullopt = NULL
    ui8 DecimalPrecision = 0;          // Decimal only
    ui8 DecimalScale = 0;              // Decimal only
};

// Reproduces YQL StablePickle(AsTuple(<per-column Optional<T> or T>...)) purely in C++ (no MiniKQL
// program), so the cost-based optimizer can build multi-column Count-Min-Sketch probe keys that are
// byte-identical to those ANALYZE hashed (ANALYZE aggregates StablePickle(AsTuple(<columns>))).
//
// The object owns the MiniKQL machinery once and reuses a cached tuple type + value packer while the
// column type signature is unchanged, so repeated Pack() calls cost the same as the StablePickle
// builtin's per-row Pack(). The allocator is thread-bound: use one builder on a single thread.
class TStablePickleTupleBuilder {
public:
    TStablePickleTupleBuilder();
    ~TStablePickleTupleBuilder();

    TStablePickleTupleBuilder(const TStablePickleTupleBuilder&) = delete;
    TStablePickleTupleBuilder& operator=(const TStablePickleTupleBuilder&) = delete;

    TString Pack(const std::vector<TPickleColumnValue>& columns);

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

// One-shot convenience: constructs a builder and packs once. For tests / cold callers, not hot paths.
TString StablePickleTuple(const std::vector<TPickleColumnValue>& columns);

} // namespace NKikimr::NStat
