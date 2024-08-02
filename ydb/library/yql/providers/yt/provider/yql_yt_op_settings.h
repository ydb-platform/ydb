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

enum class EYtSettingType: ui64 {
    // Table reads
    Initial           /* "initial" */,
    InferScheme              /* "infer_scheme" "inferscheme" "infer_schema" "inferschema" */,
    ForceInferScheme         /* "force_infer_schema" "forceinferschema" */,
    DoNotFailOnInvalidSchema /* "do_not_fail_on_invalid_schema" */,
    DirectRead               /* "direct_read" "directread"*/,
    View                     /* "view" */,
    Mode                     /* "mode" */,
    Scheme                   /* "scheme" */,
    WeakConcat               /* "weak_concat" */,
    Anonymous                /* "anonymous" */,
    WithQB                   /* "with_qb" */,
    Inline                   /* "inline" */,
    Sample                   /* "sample" */,
    JoinLabel                /* "joinLabel" */,
    IgnoreNonExisting        /* "ignore_non_existing" "ignorenonexisting" */,
    WarnNonExisting          /* "warn_non_existing" "warnnonexisting" */,
    XLock                    /* "xlock" */,
    Unordered                /* "unordered" */,
    NonUnique                /* "nonUnique" */,
    UserSchema               /* "userschema" */,
    UserColumns              /* "usercolumns" */,
    StatColumns              /* "statcolumns" */,
    SysColumns               /* "syscolumns" */,
    IgnoreTypeV3             /* "ignoretypev3" "ignore_type_v3" */,
    // Table content         
    MemUsage                 /* "memUsage" */,
    ItemsCount               /* "itemsCount" */,
    RowFactor                /* "rowFactor" */,
    // Operations            
    Ordered                  /* "ordered" */,                  // hybrid supported
    KeyFilter                /* "keyFilter" */,
    KeyFilter2               /* "keyFilter2" */,
    Take                     /* "take" */,
    Skip                     /* "skip" */,
    Limit                    /* "limit" */,                    // hybrid supported
    SortLimitBy              /* "sortLimitBy" */,              // hybrid supported
    SortBy                   /* "sortBy" */,                   // hybrid supported
    ReduceBy                 /* "reduceBy" */,                 // hybrid supported
    ReduceFilterBy           /* "reduceFilterBy" */,
    ForceTransform           /* "forceTransform" */,           // hybrid supported
    WeakFields               /* "weakFields" */,
    Sharded                  /* "sharded" */,
    CombineChunks            /* "combineChunks" */,
    JobCount                 /* "jobCount" */,                 // hybrid supported
    JoinReduce               /* "joinReduce" */,               // hybrid supported
    FirstAsPrimary           /* "firstAsPrimary" */,           // hybrid supported
    Flow                     /* "flow" */,                     // hybrid supported
    KeepSorted               /* "keepSorted" */,               // hybrid supported
    KeySwitch                /* "keySwitch" */,                // hybrid supported
    // Out tables            
    UniqueBy                 /* "uniqueBy" */,
    OpHash                   /* "opHash" */,
    // Operations            
    MapOutputType            /* "mapOutputType" */,            // hybrid supported
    ReduceInputType          /* "reduceInputType" */,          // hybrid supported
    NoDq                     /* "noDq" */,
    // Read      
    Split                    /* "split" */,
    // Write hints           
    CompressionCodec         /* "compression_codec" "compressioncodec"*/,
    ErasureCodec             /* "erasure_codec" "erasurecodec" */,
    Expiration               /* "expiration" */,
    ReplicationFactor        /* "replication_factor" "replicationfactor" */,
    UserAttrs                /* "user_attrs", "userattrs" */,
    Media                    /* "media" */,
    PrimaryMedium            /* "primary_medium", "primarymedium" */,
    KeepMeta                 /* "keep_meta", "keepmeta" */,
    MonotonicKeys            /* "monotonic_keys", "monotonickeys" */,
    MutationId               /* "mutationid", "mutation_id" */,
    ColumnGroups             /* "column_groups", "columngroups" */,
    SecurityTags             /* "security_tags", "securitytags" */,

    LAST
};

constexpr auto YtSettingTypesCount = static_cast<ui64>(EYtSettingType::LAST);

class EYtSettingTypes : std::bitset<YtSettingTypesCount> {
using TBase = std::bitset<YtSettingTypesCount>;
    explicit EYtSettingTypes(const std::bitset<YtSettingTypesCount>& bitset)
        : TBase(bitset)
    {}

public:
    using ::NYql::EYtSettingTypes::bitset::bitset;

    EYtSettingTypes(EYtSettingType type)
        : TBase(std::bitset<YtSettingTypesCount>(1) << static_cast<ui64>(type)) 
    {}

    EYtSettingTypes& operator|=(const EYtSettingTypes& other) {
        TBase::operator|=(other);
        return *this;
    }

    friend EYtSettingTypes operator|(EYtSettingTypes, const EYtSettingTypes&);

    EYtSettingTypes& operator&=(const EYtSettingTypes& other) {
        TBase::operator&=(other);
        return *this;
    }

    friend EYtSettingTypes operator&(EYtSettingTypes, const EYtSettingTypes&);

    bool HasFlags(const EYtSettingTypes& other) {
        return *this & other;
    }

    operator bool() const {
        return count() != 0;
    }
};

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
