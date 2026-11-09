#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/tablet.pb.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>

#include <optional>

namespace NKikimr {
namespace NHive {

/*
 * Named resources are resources identified by a string name rather than by a dedicated
 * field in the metrics (see NKikimrTabletBase::TNamedResource). The set of resources is
 * not hard-coded in Hive: tablets and nodes report whatever they have, and the Hive
 * config (with a few built-in defaults) defines how each of them is balanced.
 */

struct TNamedResourceValue {
    TString Name;
    ui64 Value = 0;
    // The part of Value which belongs to transferable tablets, i.e. which a move would not interrupt.
    // For a single tablet it is either Value or 0, for sums over tablets anything in between.
    ui64 TransferableValue = 0;
    // Non-zero for values estimated by Hive rather than reported, see CarryOverAsEstimate
    TInstant EstimateDeadline;

    bool IsTransferable() const { return TransferableValue == Value; }

    bool operator==(const TNamedResourceValue&) const = default;
};

// Small set of named values sorted by name, entries with zero value are not kept.
class TNamedResourceValues {
public:
    using TStorage = TVector<TNamedResourceValue>;

    const TNamedResourceValue* Find(TStringBuf name) const;
    ui64 GetValue(TStringBuf name) const;
    ui64 GetTransferableValue(TStringBuf name) const;

    // Removes the resource when value is 0
    void Set(const TString& name, ui64 value, bool transferable, TInstant estimateDeadline = TInstant::Zero());
    // For a restarted tablet: the work follows the tablet to the new node, but the values are only an estimate
    // until the new incarnation reports them. Returns true if anything changed.
    bool CarryOverAsEstimate(TInstant estimateDeadline);
    // Removes estimates with deadline before now. Returns true if anything changed.
    bool DropExpiredEstimates(TInstant now);
    // The earliest deadline among estimates, Zero if there are no estimates
    TInstant GetEarliestEstimateDeadline() const;
    void Add(const TNamedResourceValues& other);
    void Subtract(const TNamedResourceValues& other);

    bool Empty() const { return Values.empty(); }
    void Clear() { Values.clear(); }
    TStorage::const_iterator begin() const { return Values.begin(); }
    TStorage::const_iterator end() const { return Values.end(); }

    // Transferable is written only when it is false (or, for sums, when not all of the value is transferable)
    void ToProto(::google::protobuf::RepeatedPtrField<NKikimrTabletBase::TNamedResource>* proto) const;
    void FromProto(const ::google::protobuf::RepeatedPtrField<NKikimrTabletBase::TNamedResource>& proto);
    TString ToString() const;

    bool operator==(const TNamedResourceValues&) const = default;

private:
    TStorage::iterator LowerBound(TStringBuf name);
    TStorage::const_iterator LowerBound(TStringBuf name) const;
    void Apply(const TNamedResourceValues& other, bool subtract);

    TStorage Values;
};

// How long the named resources of a restarted tablet are assumed to stay with it (as an estimate, see
// TNamedResourceValues::CarryOverAsEstimate) until reported by the new incarnation
constexpr TDuration NAMED_RESOURCE_ESTIMATE_TIMEOUT = TDuration::Minutes(2);

using ENamedResourceBalanceMode = NKikimrConfig::THiveConfig::TNamedResourceSettings::EBalanceMode;

struct TNamedResourceSettings {
    ENamedResourceBalanceMode BalanceMode = NKikimrConfig::THiveConfig::TNamedResourceSettings::NO_BALANCE;
    double MinScatterToBalance = 0.5;
    std::optional<ui64> DefaultMaximum;

    bool IsBalanced() const {
        return BalanceMode != NKikimrConfig::THiveConfig::TNamedResourceSettings::NO_BALANCE;
    }
};

class TNamedResourceRegistry {
public:
    using TConfig = ::google::protobuf::RepeatedPtrField<NKikimrConfig::THiveConfig::TNamedResourceSettings>;

    TNamedResourceRegistry();

    void Configure(const TConfig& config);

    // Resolves settings: exact match wins over prefix ('*' at the end), longer prefix wins
    // over a shorter one, config entries win over built-in defaults.
    const TNamedResourceSettings& GetSettings(const TString& name) const;

    // Remembers the resource as seen in metrics, so that it is considered for balancing
    void Register(const TString& name);
    const TVector<TString>& GetKnownNames() const { return KnownNames; }

    static TConfig GetBuiltInDefaults();

private:
    static const NKikimrConfig::THiveConfig::TNamedResourceSettings* FindBestMatch(const TConfig& config, const TString& name);
    static TNamedResourceSettings MakeSettings(const NKikimrConfig::THiveConfig::TNamedResourceSettings* config);

    TConfig Config;
    TConfig BuiltInDefaults;
    mutable THashMap<TString, TNamedResourceSettings> Cache;
    THashSet<TString> KnownNamesSet;
    TVector<TString> KnownNames; // sorted
};

} // NHive
} // NKikimr
