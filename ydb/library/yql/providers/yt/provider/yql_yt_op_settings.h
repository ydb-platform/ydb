#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/flags.h>
#include <util/generic/strbuf.h>
#include <util/system/types.h>
#include <util/string/cast.h>
#include <util/str_stl.h>

#include <utility>

namespace NYql {

///////////////////////////////////////////////////////////////////////////////////////////////

enum class EYtWriteMode: ui32 {
    Renew           /* "renew" */,
    RenewKeepMeta   /* "renew_keep_meta" */,
    Append          /* "append" */,
    Drop            /* "drop" */,
    Flush           /* "flush" */,
};

///////////////////////////////////////////////////////////////////////////////////////////////

enum class EYtSampleMode: ui32 {
    System      /* "system" */,
    Bernoulli   /* "bernoulli" */,
};

struct TSampleParams {
    EYtSampleMode Mode;
    double Percentage;
    ui64 Repeat;

    friend bool operator==(const TSampleParams& l, const TSampleParams& r) {
        return l.Mode == r.Mode
            && l.Percentage == r.Percentage
            && l.Repeat == r.Repeat;
    }
};

///////////////////////////////////////////////////////////////////////////////////////////////

enum class EYtSettingType: ui64 {
    // Table reads
    Initial                     = 1ull << 0  /* "initial" */,
    InferScheme                 = 1ull << 1  /* "infer_scheme" "inferscheme" "infer_schema" "inferschema" */,
    ForceInferScheme            = 1ull << 2  /* "force_infer_schema" "forceinferschema" */,
    DoNotFailOnInvalidSchema    = 1ull << 3  /* "do_not_fail_on_invalid_schema" */,
    DirectRead                  = 1ull << 4  /* "direct_read" "directread"*/,
    View                        = 1ull << 5  /* "view" */,
    Mode                        = 1ull << 6  /* "mode" */,
    Scheme                      = 1ull << 7  /* "scheme" */,
    WeakConcat                  = 1ull << 8  /* "weak_concat" */,
    Anonymous                   = 1ull << 9  /* "anonymous" */,
    WithQB                      = 1ull << 10 /* "with_qb" */,
    Inline                      = 1ull << 11 /* "inline" */,
    Sample                      = 1ull << 12 /* "sample" */,
    JoinLabel                   = 1ull << 13 /* "joinLabel" */,
    IgnoreNonExisting           = 1ull << 14 /* "ignore_non_existing" "ignorenonexisting" */,
    WarnNonExisting             = 1ull << 15 /* "warn_non_existing" "warnnonexisting" */,
    XLock                       = 1ull << 16 /* "xlock" */,
    Unordered                   = 1ull << 17 /* "unordered" */,
    NonUnique                   = 1ull << 18 /* "nonUnique" */,
    UserSchema                  = 1ull << 19 /* "userschema" */,
    UserColumns                 = 1ull << 20 /* "usercolumns" */,
    StatColumns                 = 1ull << 21 /* "statcolumns" */,
    SysColumns                  = 1ull << 22 /* "syscolumns" */,
    IgnoreTypeV3                = 1ull << 23 /* "ignoretypev3" "ignore_type_v3" */,
    // Table content
    MemUsage                    = 1ull << 24 /* "memUsage" */,
    ItemsCount                  = 1ull << 25 /* "itemsCount" */,
    RowFactor                   = 1ull << 26 /* "rowFactor" */,
    // Operations
    Ordered                     = 1ull << 27 /* "ordered" */,                  // hybrid supported
    KeyFilter                   = 1ull << 28 /* "keyFilter" */,
    KeyFilter2                  = 1ull << 29 /* "keyFilter2" */,
    Take                        = 1ull << 30 /* "take" */,
    Skip                        = 1ull << 31 /* "skip" */,
    Limit                       = 1ull << 32 /* "limit" */,                    // hybrid supported
    SortLimitBy                 = 1ull << 33 /* "sortLimitBy" */,              // hybrid supported
    SortBy                      = 1ull << 34 /* "sortBy" */,                   // hybrid supported
    ReduceBy                    = 1ull << 35 /* "reduceBy" */,                 // hybrid supported
    ReduceFilterBy              = 1ull << 36 /* "reduceFilterBy" */,
    ForceTransform              = 1ull << 37 /* "forceTransform" */,           // hybrid supported
    WeakFields                  = 1ull << 38 /* "weakFields" */,
    Sharded                     = 1ull << 39 /* "sharded" */,
    CombineChunks               = 1ull << 40 /* "combineChunks" */,
    JobCount                    = 1ull << 41 /* "jobCount" */,                 // hybrid supported
    JoinReduce                  = 1ull << 42 /* "joinReduce" */,               // hybrid supported
    FirstAsPrimary              = 1ull << 43 /* "firstAsPrimary" */,           // hybrid supported
    Flow                        = 1ull << 44 /* "flow" */,                     // hybrid supported
    KeepSorted                  = 1ull << 45 /* "keepSorted" */,               // hybrid supported
    KeySwitch                   = 1ull << 46 /* "keySwitch" */,                // hybrid supported
    // Out tables
    UniqueBy                    = 1ull << 47 /* "uniqueBy" */,
    OpHash                      = 1ull << 48 /* "opHash" */,
    // Operations
    MapOutputType               = 1ull << 49 /* "mapOutputType" */,            // hybrid supported
    ReduceInputType             = 1ull << 50 /* "reduceInputType" */,          // hybrid supported
    NoDq                        = 1ull << 51 /* "noDq" */,
    // Read
    Split                       = 1ull << 52 /* "split" */,
    // Write hints
    CompressionCodec            = 1ull << 53 /* "compression_codec" "compressioncodec"*/,
    ErasureCodec                = 1ull << 54 /* "erasure_codec" "erasurecodec" */,
    Expiration                  = 1ull << 55 /* "expiration" */,
    ReplicationFactor           = 1ull << 56 /* "replication_factor" "replicationfactor" */,
    UserAttrs                   = 1ull << 57 /* "user_attrs", "userattrs" */,
    Media                       = 1ull << 58 /* "media" */,
    PrimaryMedium               = 1ull << 59 /* "primary_medium", "primarymedium" */,
    KeepMeta                    = 1ull << 60 /* "keep_meta", "keepmeta" */,
    MonotonicKeys               = 1ull << 61 /* "monotonic_keys", "monotonickeys" */,
    MutationId                  = 1ull << 62 /* "mutationid", "mutation_id" */,
    ColumnGroups                = 1ull << 63 /* "column_groups", "columngroups" */,
};

Y_DECLARE_FLAGS(EYtSettingTypes, EYtSettingType);
Y_DECLARE_OPERATORS_FOR_FLAGS(EYtSettingTypes);

constexpr auto DqReadSupportedSettings = EYtSettingType::SysColumns | EYtSettingType::Sample | EYtSettingType::Unordered | EYtSettingType::NonUnique | EYtSettingType::KeyFilter2;
constexpr auto DqOpSupportedSettings = EYtSettingType::Ordered | EYtSettingType::Limit | EYtSettingType::SortLimitBy | EYtSettingType::SortBy |
                                       EYtSettingType::ReduceBy | EYtSettingType::ForceTransform | EYtSettingType::JobCount | EYtSettingType::JoinReduce |
                                       EYtSettingType::FirstAsPrimary | EYtSettingType::Flow | EYtSettingType::KeepSorted | EYtSettingType::KeySwitch |
                                       EYtSettingType::ReduceInputType | EYtSettingType::MapOutputType | EYtSettingType::Sharded;

///////////////////////////////////////////////////////////////////////////////////////////////

bool ValidateSettings(const TExprNode& settingsNode, EYtSettingTypes accepted, TExprContext& ctx);

template <class TContainer>
TExprNode::TPtr ToAtomList(const TContainer& columns, TPositionHandle pos, TExprContext& ctx) {
    TExprNode::TListType children;
    for (auto& column : columns) {
        children.push_back(ctx.NewAtom(pos, column));
    }

    return ctx.NewList(pos, std::move(children));
}

bool ValidateColumnGroups(const TExprNode& setting, const TStructExprType& rowType, TExprContext& ctx);
TString NormalizeColumnGroupSpec(const TStringBuf spec);
const TString& GetSingleColumnGroupSpec();

TExprNode::TPtr ToColumnPairList(const TVector<std::pair<TString, bool>>& columns, TPositionHandle pos, TExprContext& ctx);

TExprNode::TPtr GetSetting(const TExprNode& settings, EYtSettingType type);
TExprNode::TPtr UpdateSettingValue(const TExprNode& settings, EYtSettingType type, TExprNode::TPtr&& value, TExprContext& ctx);
TExprNode::TPtr AddOrUpdateSettingValue(const TExprNode& settings, EYtSettingType type, TExprNode::TPtr&& value, TExprContext& ctx);

TExprNode::TListType GetAllSettingValues(const TExprNode& settings, EYtSettingType type);
TVector<TString> GetSettingAsColumnList(const TExprNode& settings, EYtSettingType type);
TVector<std::pair<TString, bool>> GetSettingAsColumnPairList(const TExprNode& settings, EYtSettingType type);

TExprNode::TListType GetSettingAsColumnAtomList(const TExprNode& settings, EYtSettingType type);
std::vector<std::pair<TExprNode::TPtr, bool>> GetSettingAsColumnAtomPairList(const TExprNode& settings, EYtSettingType type);

bool HasSetting(const TExprNode& settings, EYtSettingType type);
bool HasAnySetting(const TExprNode& settings, EYtSettingTypes types);
bool HasSettingsExcept(const TExprNode& settings, EYtSettingTypes types);
bool EqualSettingsExcept(const TExprNode& lhs, const TExprNode& rhs, EYtSettingTypes types);

TExprNode::TPtr RemoveSetting(const TExprNode& settings, EYtSettingType type, TExprContext& ctx);
TExprNode::TPtr RemoveSettings(const TExprNode& settings, EYtSettingTypes types, TExprContext& ctx);
TExprNode::TPtr KeepOnlySettings(const TExprNode& settings, EYtSettingTypes types, TExprContext& ctx);
TExprNode::TPtr AddSetting(const TExprNode& settings, EYtSettingType type, const TExprNode::TPtr& value, TExprContext& ctx);
TExprNode::TPtr AddSettingAsColumnList(const TExprNode& settings, EYtSettingType type,
    const TVector<TString>& columns, TExprContext& ctx);
TExprNode::TPtr AddSettingAsColumnPairList(const TExprNode& settings, EYtSettingType type,
    const TVector<std::pair<TString, bool>>& columns, TExprContext& ctx);
TMaybe<TSampleParams> GetSampleParams(const TExprNode& settings);

const TStringBuf JoinReduceForSecondAsPrimaryName = "joinReduceForSecond";
const TStringBuf MaxJobSizeForFirstAsPrimaryName = "maxJobSize";

TMaybe<ui64> GetMaxJobSizeForFirstAsPrimary(const TExprNode& settings);
bool UseJoinReduceForSecondAsPrimary(const TExprNode& settings);

ui32 GetMinChildrenForIndexedKeyFilter(EYtSettingType type);

} // NYql


template <>
struct THash<NYql::TSampleParams> {
    size_t operator()(const NYql::TSampleParams& p) const {
        return CombineHashes(NumericHash(static_cast<ui32>(p.Mode)), CombineHashes(THash<double>()(p.Percentage), NumericHash(p.Repeat)));
    }
};
