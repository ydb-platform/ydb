#pragma once
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NMiniKQL {

enum class ETestedJoinAlgo { kScalarGrace, kScalarMap, kBlockMap, kBlockHash, kScalarHash };
enum class ETestedJoinKeyType { kString, kInteger };
enum class ETestedInputFlavour { kSameSizeTable, kLittleRightTable };
// Number of non-key columns per row. Wide rows shift the cost from hash lookups
// towards copying tuples around, so the two shapes stress different parts of a
// join.
enum class ETestedPayload { kNarrow, kWide };
// Hash-join predicates pushed into DqBlockHashJoin / DqScalarHashJoin. Map and
// grace ignore them.
enum class ETestedFilter { kNone, kLeft, kRight, kCommon, kAll };

inline constexpr int DefaultStringBytes = 27;
inline constexpr int DefaultWidePayloadColumns = 16;

// Types of the leading columns that form the join key, in order. A single
// element is a plain key, several elements are a composite key and may mix
// types, for example {int, int, string}.
using TKeySchema = TVector<ETestedJoinKeyType>;

struct TTableSizes {
    int Left;
    int Right;
};

// Selectivity of the join, expressed as two independent properties of the
// generated keys: MatchRate is the share of left rows whose key exists on the
// right side, DupsPerKey is how many right rows share a key value. An inner
// join emits about leftRows * MatchRate * DupsPerKey rows, so a MatchRate below
// 1 makes it filter and a DupsPerKey above 1 makes it multiply.
struct TSelectivity {
    double MatchRate = 1.0;
    int DupsPerKey = 2;
};

struct TPreset {
    TVector<TTableSizes> Sizes;
    TString PresetName;
};

struct TBenchmarkSettings {
    int Seed;
    int Scale;
    int Samples;
    int Warmup = 0;
    // A sample is repeated until it lasts at least MinSampleMs, so that cases too
    // short for the timer resolution still produce a usable number. Defaults keep
    // one run per sample.
    int MinSampleMs = 0;
    int MaxItersPerSample = 1;
    int StringBytes = DefaultStringBytes;
    int WidePayloadColumns = DefaultWidePayloadColumns;
    // Lengths of the input blocks, cycled over the input. Several values make one
    // run feed blocks of uneven size, which is what a join sees downstream of
    // filters and other joins.
    TVector<int> BlockSizes{128};
    TPreset Preset;
    // Everything below is swept: the benchmark measures the cross product of
    // these.
    TVector<TKeySchema> KeySchemas;
    TVector<TSelectivity> Selectivities{TSelectivity{}};
    TSet<ETestedPayload> Payloads;
    TSet<ETestedJoinAlgo> Algorithms;
    TSet<ETestedInputFlavour> Flavours;
    TVector<EJoinKind> JoinKinds{EJoinKind::Inner};
    TVector<ETestedFilter> Filters{ETestedFilter::kNone};
};

TString CaseName(ETestedJoinAlgo algo, const TKeySchema& keySchema, ETestedPayload payload, TSelectivity selectivity,
                 EJoinKind joinKind, ETestedFilter filter, ETestedInputFlavour inputFlavour,
                 const TBenchmarkSettings& preset, TTableSizes sizes);

// Names as they appear on the command line, so that a reported case can be
// replayed from it.
TString AlgoOptionName(ETestedJoinAlgo algo);
TString KeySchemaOptionName(const TKeySchema& keySchema);
TString PayloadOptionName(ETestedPayload payload);
TString FlavourOptionName(ETestedInputFlavour flavour);
TString JoinKindOptionName(EJoinKind joinKind);
TString FilterOptionName(ETestedFilter filter);

// Parses a key schema written as a comma separated list of column types, for
// example "int,int,string". Accepts "int" and "string", abbreviated to "i" and
// "s".
TKeySchema ParseKeySchema(const TString& spec);

// Parses a selectivity point written as "matchRate:dupsPerKey", for example
// "0.05:1".
TSelectivity ParseSelectivity(const TString& spec);

// Parses a join filter: none, left, right, common or all.
ETestedFilter ParseFilter(const TString& spec);

// Parses a join kind supported by DqBlockHashJoin / DqScalarHashJoin.
EJoinKind ParseJoinKind(const TString& spec);

// Parses a comma separated list of block lengths, for example "8192,1024,128".
TVector<int> ParseBlockSizes(const TString& spec);

TVector<TPreset> ParsePresetsFile(const TString& path);

} // namespace NKikimr::NMiniKQL
