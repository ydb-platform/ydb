#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/flags.h>
#include <util/generic/strbuf.h>
#include <util/system/types.h>
#include <util/string/cast.h>
#include <util/str_stl.h>

#include <utility>
#include <bitset>

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

constexpr auto YtSettingTypesCount = 64;

enum class EYtSettingType: ui64 {
    // Table reads
    Initial                           = 0  /* "initial" */,
    InferScheme                       = 1  /* "infer_scheme" "inferscheme" "infer_schema" "inferschema" */,
    ForceInferScheme                  = 2  /* "force_infer_schema" "forceinferschema" */,
    DoNotFailOnInvalidSchema          = 3  /* "do_not_fail_on_invalid_schema" */,
    DirectRead                        = 4  /* "direct_read" "directread"*/,
    View                              = 5  /* "view" */,
    Mode                              = 6  /* "mode" */,
    Scheme                            = 7  /* "scheme" */,
    WeakConcat                        = 8  /* "weak_concat" */,
    Anonymous                         = 9  /* "anonymous" */,
    WithQB                            = 10 /* "with_qb" */,
    Inline                            = 11 /* "inline" */,
    Sample                            = 12 /* "sample" */,
    JoinLabel                         = 13 /* "joinLabel" */,
    IgnoreNonExisting                 = 14 /* "ignore_non_existing" "ignorenonexisting" */,
    WarnNonExisting                   = 15 /* "warn_non_existing" "warnnonexisting" */,
    XLock                             = 16 /* "xlock" */,
    Unordered                         = 17 /* "unordered" */,
    NonUnique                         = 18 /* "nonUnique" */,
    UserSchema                        = 19 /* "userschema" */,
    UserColumns                       = 20 /* "usercolumns" */,
    StatColumns                       = 21 /* "statcolumns" */,
    SysColumns                        = 22 /* "syscolumns" */,
    IgnoreTypeV3                      = 23 /* "ignoretypev3" "ignore_type_v3" */,
    // Table content      
    MemUsage                          = 24 /* "memUsage" */,
    ItemsCount                        = 25 /* "itemsCount" */,
    RowFactor                         = 26 /* "rowFactor" */,
    // Operations      
    Ordered                           = 27 /* "ordered" */,                  // hybrid supported
    KeyFilter                         = 28 /* "keyFilter" */,
    KeyFilter2                        = 29 /* "keyFilter2" */,
    Take                              = 30 /* "take" */,
    Skip                              = 31 /* "skip" */,
    Limit                             = 32 /* "limit" */,                    // hybrid supported
    SortLimitBy                       = 33 /* "sortLimitBy" */,              // hybrid supported
    SortBy                            = 34 /* "sortBy" */,                   // hybrid supported
    ReduceBy                          = 35 /* "reduceBy" */,                 // hybrid supported
    ReduceFilterBy                    = 36 /* "reduceFilterBy" */,
    ForceTransform                    = 37 /* "forceTransform" */,           // hybrid supported
    WeakFields                        = 38 /* "weakFields" */,
    Sharded                           = 39 /* "sharded" */,
    CombineChunks                     = 40 /* "combineChunks" */,
    JobCount                          = 41 /* "jobCount" */,                 // hybrid supported
    JoinReduce                        = 42 /* "joinReduce" */,               // hybrid supported
    FirstAsPrimary                    = 43 /* "firstAsPrimary" */,           // hybrid supported
    Flow                              = 44 /* "flow" */,                     // hybrid supported
    KeepSorted                        = 45 /* "keepSorted" */,               // hybrid supported
    KeySwitch                         = 46 /* "keySwitch" */,                // hybrid supported
    // Out tables      
    UniqueBy                          = 47 /* "uniqueBy" */,
    OpHash                            = 48 /* "opHash" */,
    // Operations      
    MapOutputType                     = 49 /* "mapOutputType" */,            // hybrid supported
    ReduceInputType                   = 50 /* "reduceInputType" */,          // hybrid supported
    NoDq                              = 51 /* "noDq" */,
    // Read      
    Split                             = 52 /* "split" */,
    // Write hints      
    CompressionCodec                  = 53 /* "compression_codec" "compressioncodec"*/,
    ErasureCodec                      = 54 /* "erasure_codec" "erasurecodec" */,
    Expiration                        = 55 /* "expiration" */,
    ReplicationFactor                 = 56 /* "replication_factor" "replicationfactor" */,
    UserAttrs                         = 57 /* "user_attrs", "userattrs" */,
    Media                             = 58 /* "media" */,
    PrimaryMedium                     = 59 /* "primary_medium", "primarymedium" */,
    KeepMeta                          = 60 /* "keep_meta", "keepmeta" */,
    MonotonicKeys                     = 61 /* "monotonic_keys", "monotonickeys" */,
    MutationId                        = 62 /* "mutationid", "mutation_id" */,
    ColumnGroups = YtSettingTypesCount - 1 /* "column_groups", "columngroups" */,
};

class EYtSettingTypes : std::bitset<YtSettingTypesCount> {
    explicit EYtSettingTypes(const std::bitset<YtSettingTypesCount>& bitset) : ::NYql::EYtSettingTypes::bitset(bitset) {}

public:
    using ::NYql::EYtSettingTypes::bitset::bitset;

    EYtSettingTypes(EYtSettingType type) : ::NYql::EYtSettingTypes::bitset(std::bitset<YtSettingTypesCount>(1) << static_cast<ui64>(type)) {}

    EYtSettingTypes operator|(const EYtSettingTypes& other) const {
        return EYtSettingTypes {
            static_cast<const std::bitset<YtSettingTypesCount>&>(*this) | 
            static_cast<const std::bitset<YtSettingTypesCount>&>(other)
        };
    }

    EYtSettingTypes& operator|=(const EYtSettingTypes& other) {
        return *this = *this | other;
    }

    EYtSettingTypes operator&(const EYtSettingTypes& other) const {
        return EYtSettingTypes {
            static_cast<const std::bitset<YtSettingTypesCount>&>(*this) &
            static_cast<const std::bitset<YtSettingTypesCount>&>(other)
        };
    }

    EYtSettingTypes& operator&=(const EYtSettingTypes& other) {
        return *this = *this & other;
    }

    bool HasFlags(const EYtSettingTypes& other) {
        return *this & other;
    }

    operator bool() const {
        return count() != 0;
    }
};

EYtSettingTypes operator&(EYtSettingType left, const EYtSettingTypes& right);
EYtSettingTypes operator|(EYtSettingType left, EYtSettingType right);

const auto DqReadSupportedSettings = EYtSettingType::SysColumns | EYtSettingType::Sample | EYtSettingType::Unordered | EYtSettingType::NonUnique | EYtSettingType::KeyFilter2;
const auto DqOpSupportedSettings = EYtSettingType::Ordered | EYtSettingType::Limit | EYtSettingType::SortLimitBy | EYtSettingType::SortBy |
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
