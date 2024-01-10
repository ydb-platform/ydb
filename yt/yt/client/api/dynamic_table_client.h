#pragma once

#include "client_common.h"

#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/client/query_client/query_statistics.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TLookupRequestOptions
    : public TFallbackReplicaOptions
{
    NTableClient::TColumnFilter ColumnFilter;
    bool KeepMissingRows = false;
    bool EnablePartialResult = false;
    std::optional<bool> UseLookupCache;
    TDetailedProfilingInfoPtr DetailedProfilingInfo;
};

struct TLookupRowsOptionsBase
    : public TTabletReadOptions
    , public TLookupRequestOptions
    , public TMultiplexingBandOptions
{ };

struct TLookupRowsOptions
    : public TLookupRowsOptionsBase
{ };

struct TVersionedLookupRowsOptions
    : public TLookupRowsOptionsBase
{
    NTableClient::TRetentionConfigPtr RetentionConfig;
};

struct TMultiLookupSubrequest
{
    NYPath::TYPath Path;
    NTableClient::TNameTablePtr NameTable;
    TSharedRange<NTableClient::TLegacyKey> Keys;

    // NB: Other options from TLookupRowsOptions that are absent from TLookupRequestOptions are
    // common and included in TMultiLookupOptions.
    TLookupRequestOptions Options;
};

struct TMultiLookupOptions
    : public TTimeoutOptions
    , public TTabletReadOptionsBase
    , public TMultiplexingBandOptions
{ };

struct TExplainQueryOptions
    : public TSelectRowsOptionsBase
{
    bool VerboseOutput = false;
};

struct TSelectRowsResult
{
    IUnversionedRowsetPtr Rowset;
    NQueryClient::TQueryStatistics Statistics;
};

template <class IRowset>
struct TLookupRowsResult
{
    TIntrusivePtr<IRowset> Rowset;
};

using TUnversionedLookupRowsResult = TLookupRowsResult<IUnversionedRowset>;
using TVersionedLookupRowsResult = TLookupRowsResult<IVersionedRowset>;

////////////////////////////////////////////////////////////////////////////////

struct IDynamicTableClientBase
{
    virtual ~IDynamicTableClientBase() = default;

    virtual TFuture<TUnversionedLookupRowsResult> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options = {}) = 0;
    virtual TFuture<TVersionedLookupRowsResult> VersionedLookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TVersionedLookupRowsOptions& options = {}) = 0;
    virtual TFuture<std::vector<TUnversionedLookupRowsResult>> MultiLookupRows(
        const std::vector<TMultiLookupSubrequest>& subrequests,
        const TMultiLookupOptions& options = {}) = 0;

    virtual TFuture<TSelectRowsResult> SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> ExplainQuery(
        const TString& query,
        const TExplainQueryOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
