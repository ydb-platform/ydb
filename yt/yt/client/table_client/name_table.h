#pragma once

#include "public.h"

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! A thread-safe id-to-name mapping.
class TNameTable
    : public virtual TRefCounted
{
public:
    static TNameTablePtr FromSchema(const TTableSchema& schema);
    static TNameTablePtr FromKeyColumns(const TKeyColumns& keyColumns);
    static TNameTablePtr FromSortColumns(const TSortColumns& sortColumns);

    // TODO(levysotsky): This one is currently used only for
    // chunk name table. A separate type to chunk name table
    // may be handy.
    static TNameTablePtr FromSchemaStable(const TTableSchema& schema);

    int GetSize() const;
    i64 GetByteSize() const;

    void SetEnableColumnNameValidation();

    std::optional<int> FindId(TStringBuf name) const;
    int GetIdOrThrow(TStringBuf name) const;
    int GetId(TStringBuf name) const;
    int RegisterName(TStringBuf name);
    int RegisterNameOrThrow(TStringBuf name);
    int GetIdOrRegisterName(TStringBuf name);

    TStringBuf GetName(int id) const;
    TStringBuf GetNameOrThrow(int id) const;

    std::vector<TString> GetNames() const;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    bool EnableColumnNameValidation_ = false;

    // String values are owned by IdToName_.
    // NB: Names may be SSO-strings, using a deque to avoid string view invalidation.
    std::deque<TString> IdToName_;
    THashMap<TStringBuf, int> NameToId_;
    i64 ByteSize_ = 0;

    int DoRegisterName(TStringBuf name);
    int DoRegisterNameOrThrow(TStringBuf name);
};

DEFINE_REFCOUNTED_TYPE(TNameTable)

void FormatValue(TStringBuilderBase* builder, const TNameTable& nameTable, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

//! A non thread-safe read-only wrapper for TNameTable.
class TNameTableReader
    : private TNonCopyable
{
public:
    explicit TNameTableReader(TNameTablePtr nameTable);

    TStringBuf FindName(int id) const;
    TStringBuf GetName(int id) const;
    int GetSize() const;

private:
    const TNameTablePtr NameTable_;

    mutable std::deque<std::string> IdToNameCache_; // Addresses of string data are immutable.

    void Fill() const;

};

////////////////////////////////////////////////////////////////////////////////

//! A non thread-safe read-write wrapper for TNameTable.
class TNameTableWriter
{
public:
    explicit TNameTableWriter(TNameTablePtr nameTable);

    std::optional<int> FindId(TStringBuf name) const;
    int GetIdOrThrow(TStringBuf name) const;
    int GetIdOrRegisterName(TStringBuf name);

private:
    const TNameTablePtr NameTable_;

    // String values are owned by Names_
    // NB: Names may be SSO-strings, using a deque to avoid string view invalidation
    mutable std::deque<TString> Names_;
    mutable THashMap<TStringBuf, int> NameToId_;

};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNameTableExt* protoNameTable, const TNameTablePtr& nameTable);
void FromProto(TNameTablePtr* nameTable, const NProto::TNameTableExt& protoNameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
