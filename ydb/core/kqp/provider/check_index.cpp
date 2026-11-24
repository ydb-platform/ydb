#include "check_index.h"
#include "fulltext_analyze.h"
#include "yql_kikimr_expr_nodes.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_expr_type_annotation_pg.h>
#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

#include <functional>

namespace NYql {
namespace {

using namespace NKikimr;
using namespace NNodes;
using namespace NTableIndex::NFulltext;

using TNode = TExprNode::TPtr;
using TNodes = TExprNode::TListType;
using TColumns = TVector<TString>;
using TReverseProbe = std::function<TNode(TNode)>;
using TDiscrepancyReason = std::function<TNode(const TNode&)>;
using TNamedTables = THashMap<TString, TKikimrTableMetadataPtr>;

struct TDiagnosticCounter {
    TString Name;
    std::function<TNode(const TNode&)> Predicate;
};

constexpr ui64 SampleLimit = 100;
constexpr ui32 SampleEntryBytes = 8 * 1024;
constexpr TStringBuf EmptyTokenPosting = "EMPTY_TOKEN_POSTING";

TColumns AppendColumns(TColumns columns, const TColumns& suffix) {
    columns.insert(columns.end(), suffix.begin(), suffix.end());
    return columns;
}

struct TKeyedRole {
    TStringBuf Name;
    const TKikimrTableMetadata& Table;
    TColumns Values;
    TNode Expected;
    TReverseProbe Extras;
    TDiscrepancyReason Reason;
    TVector<TDiagnosticCounter> Counters;
};

class TIndexValidation {
public:
    TIndexValidation(const TExprNode& read, const TKikimrTableDescription& base,
        TKikimrSessionContext& session, TTypeAnnotationContext& types, TExprContext& ctx)
        : Ctx(ctx)
        , Pos(read.Pos())
        , Base(base)
        , Session(session)
        , Types(types)
        , World(read.ChildPtr(0))
        , Source(read.ChildPtr(1))
        , BaseKey(read.ChildPtr(2))
    {}

    TNode Atom(TStringBuf value) const {
        return Ctx.NewAtom(Pos, value);
    }

    TNode List(TNodes items) const {
        return Ctx.NewList(Pos, std::move(items));
    }

    TNode Call(TStringBuf name, TNodes args) const {
        return Ctx.NewCallable(Pos, name, std::move(args));
    }

    TNode String(TStringBuf value) const {
        return Call("String", {Atom(value)});
    }

    TNode Uint64(ui64 value) const {
        return Call("Uint64", {Atom(ToString(value))});
    }

    TNode Bool(bool value) const {
        return Call("Bool", {Atom(value ? "true" : "false")});
    }

    TNode Member(const TNode& row, TStringBuf name) const {
        return Call("Member", {row, Atom(name)});
    }

    TNode Nth(const TNode& row, ui32 index) const {
        return Call("Nth", {row, Atom(ToString(index))});
    }

    TNode Field(TStringBuf name, TNode value) const {
        return List({Atom(name), std::move(value)});
    }

    TNode Arg() const {
        return Ctx.NewArgument(Pos, "row");
    }

    TNode Lambda(const TNode& arg, TNode body) const {
        return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {arg}), std::move(body));
    }

    TNode Lambda2(const TNode& first, const TNode& second, TNode body) const {
        return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {first, second}), std::move(body));
    }

    TNode Filter(TNode input, const TNode& arg, TNode predicate) const {
        return Call("Filter", {std::move(input), Lambda(arg, std::move(predicate))});
    }

    TNode FilterMap(TNode input, const TNode& arg, TNode predicate, TNode value) const {
        return Call("FlatMap", {std::move(input), Lambda(arg, Call("OptionalIf", {
            std::move(predicate), std::move(value)}))});
    }

    TNode Map(TNode input, const TNode& arg, TNode value) const {
        return Call("Map", {std::move(input), Lambda(arg, std::move(value))});
    }

    TNode Project(const TNode& row, const TColumns& columns) const {
        TNodes fields;
        THashSet<TStringBuf> seen;
        for (const auto& column : columns) {
            if (seen.insert(column).second) {
                fields.push_back(Field(column, Member(row, column)));
            }
        }
        return Call("AsStruct", std::move(fields));
    }

    TNode Cross(TNode left, TNode right) const {
        return Call("EquiJoin", {
            List({std::move(left), Atom("left")}),
            List({std::move(right), Atom("right")}),
            List({Atom("Cross"), Atom("left"), Atom("right"), List({}), List({}), List({})}),
            List({}),
        });
    }

    TNode GateRows(TNode rows, TNode gate, const TColumns& columns) const {
        auto row = Arg();
        TNodes fields;
        for (const auto& column : columns) {
            fields.push_back(Field(column, Member(row, TStringBuilder() << "left." << column)));
        }
        return Map(Cross(std::move(rows), std::move(gate)), row, Call("AsStruct", std::move(fields)));
    }

    TNode ReadBase(const TColumns& columns) const {
        return Read(BaseKey, columns);
    }

    TNode ReadPhysical(const TKikimrTableMetadata& table, const TColumns& columns) const {
        return Read(Call("Key", {List({Atom("table"), String(table.Name)})}), columns);
    }

    TNode Lookup(const TKikimrTableMetadata& table, TNode input, const TColumns& keyColumns,
        const TColumns& resultColumns) const
    {
        auto row = Arg();
        auto keys = Map(std::move(input), row, List({row, Call("Just", {Project(row, keyColumns)})}));
        auto metadata = Build<TKqpTable>(Ctx, Pos)
            .Path().Build(table.Name)
            .PathId().Build(table.PathId.ToString())
            .SysView().Build(table.SysView)
            .Version().Build(ToString(table.SchemaVersion))
            .Done();
        auto settings = List({
            Field("Strategy", Atom("LookupJoinRows")),
            Field("AllowNullKeysPrefixSize", Atom(ToString(keyColumns.size()))),
        });
        return Build<TKqlStreamLookupTable>(Ctx, Pos)
            .Table(metadata)
            .LookupKeys(TExprBase(keys))
            .Columns(TCoAtomList(ColumnAtoms(resultColumns)))
            .Settings(TCoNameValueTupleList(settings))
            .Done().Ptr();
    }

    TNode Analyze(const TNode& row, const TIndexDescription& index) const {
        return BuildFulltextAnalyze(Base, row, index, Pos, Ctx);
    }

    // The physical compiler copies these identities from the logical plan.
    void TrackGeneration(const TIndexDescription& index) {
        Generations.push_back(Build<TKqpIndexValidationGeneration>(Ctx, Pos)
            .BaseTableOwnerId().Build(ToString(Base.Metadata->PathId.OwnerId()))
            .BaseTableLocalId().Build(ToString(Base.Metadata->PathId.TableId()))
            .IndexName().Build(index.Name)
            .IndexOwnerId().Build(ToString(index.PathOwnerId))
            .IndexLocalId().Build(ToString(index.LocalPathId))
            .Version().Build(ToString(index.SchemaVersion))
            .Done().Ptr());
    }

    TNode FieldsEqual(const TNode& lhs, const TNode& rhs, const TKikimrTableDescription& table,
        const TColumns& columns) const
    {
        TNodes equal;
        for (const auto& column : columns) {
            const auto* type = table.GetColumnType(column);
            YQL_ENSURE(type);
            auto left = Call("AsOptional", {Member(lhs, column)});
            auto right = Call("AsOptional", {Member(rhs, column)});
            if (!type->IsEquatable()) {
                left = Call("StablePickle", {left});
                right = Call("StablePickle", {right});
            }
            equal.push_back(Call("AggrEquals", {left, right}));
        }
        if (equal.empty()) {
            return Bool(true);
        }
        return equal.size() == 1 ? equal.front() : Call("And", std::move(equal));
    }

    TNode CompareKeys(const TKeyedRole& role) const {
        const auto& keys = role.Table.KeyColumnNames;
        auto counts = LookupCounts(role);

        // Materialize only the one-row summary before issuing any reverse lookups.
        auto totals = Call("Unwrap", {Call("Head", {counts})});
        auto extra = ExtraCount(totals);
        auto available = SampleSlots(totals);
        auto candidate = Arg();
        auto keysForSample = Filter(ReadPhysical(role.Table, keys), candidate, WantsExtraSample(totals));
        auto limit = Call("If", {Call("AggrLess", {extra, available}), extra, available});
        auto samples = SampleExtras(role, std::move(keysForSample), limit);
        auto row = Arg();
        TNodes diagnostics;
        for (const auto& counter : role.Counters) {
            diagnostics.push_back(Field(counter.Name, Member(totals, counter.Name)));
        }
        return Map(samples, row, RoleResult(Member(totals, "Missing"), extra, Member(totals, "ValueMismatch"),
            Call("Extend", {Member(totals, "Sample"), Member(row, "Sample")}), std::move(diagnostics)));
    }

    TNode CompareKeysRelational(const TKeyedRole& role) const {
        const auto& keys = role.Table.KeyColumnNames;
        auto counts = LookupCounts(role);

        // Validate reverse candidates independently of the count summary. Equal
        // physical reads may be shared, so gating this consumer on final counts
        // would let bounded output buffers block the producer of those counts.
        // This costs one batched reverse probe per row until the sample fills.
        auto samples = SampleExtras(role, ReadPhysical(role.Table, keys), Uint64(SampleLimit));

        TColumns diagnosticNames;
        TNodes forwardDiagnostics;
        TNodes reverseDiagnostics;
        auto totals = Arg();
        for (const auto& counter : role.Counters) {
            diagnosticNames.push_back(counter.Name);
            forwardDiagnostics.push_back(Field(counter.Name, Member(totals, counter.Name)));
            reverseDiagnostics.push_back(Field(counter.Name, Uint64(0)));
        }
        auto forward = Map(counts, totals, RoleResult(Member(totals, "Missing"), ExtraCount(totals),
            Member(totals, "ValueMismatch"), Member(totals, "Sample"), std::move(forwardDiagnostics)));
        auto sample = Arg();
        auto reverse = Map(samples, sample, RoleResult(Uint64(0), Uint64(0), Uint64(0),
            Member(sample, "Sample"), std::move(reverseDiagnostics)));
        return SummarizeRoles({forward, reverse}, diagnosticNames);
    }

    TNode CompareScans(const TKeyedRole& role) const {
        const auto& keys = role.Table.KeyColumnNames;
        const auto& table = Session.Tables().ExistingTable(role.Table.Cluster, role.Table.Name);
        const auto columns = AppendColumns(keys, role.Values);
        TColumns sortKeys;
        for (size_t i = 0; i < keys.size(); ++i) {
            sortKeys.push_back(TStringBuilder() << "Key" << i);
        }

        const auto tag = [&](TNode input, bool expected) {
            auto row = Arg();
            TNodes payloadFields;
            THashSet<TStringBuf> seen;
            for (const auto& column : columns) {
                if (seen.insert(column).second) {
                    payloadFields.push_back(Field(column, Call("AsOptional", {Member(row, column)})));
                }
            }
            auto payload = Call("AsStruct", std::move(payloadFields));
            auto absent = Call("Nothing", {Call("OptionalType", {Call("TypeOf", {payload})})});
            TNodes fields;
            for (size_t i = 0; i < keys.size(); ++i) {
                fields.push_back(Field(sortKeys[i], Call("AsOptional", {Member(row, keys[i])})));
            }
            fields.push_back(Field("Expected", expected ? Call("Just", {payload}) : absent));
            fields.push_back(Field("Actual", expected ? absent : Call("Just", {payload})));
            fields.push_back(Field("ExpectedCount", Uint64(expected ? 1 : 0)));
            fields.push_back(Field("ActualCount", Uint64(expected ? 0 : 1)));
            return Map(std::move(input), row, Call("AsStruct", std::move(fields)));
        };

        auto rows = Call("Extend", {tag(role.Expected, true), tag(ReadPhysical(role.Table, columns), false)});
        auto stream = Arg();
        auto first = Arg();
        auto next = Arg();
        auto previous = Arg();
        auto item = Arg();
        auto state = Arg();
        const auto envelopeColumns = AppendColumns(sortKeys, {"Expected", "Actual", "ExpectedCount", "ActualCount"});
        auto sorted = SortStream(stream, envelopeColumns, sortKeys);
        TNodes updated;
        for (const auto& key : sortKeys) {
            updated.push_back(Field(key, Member(state, key)));
        }
        for (const auto* side : {"Expected", "Actual"}) {
            updated.push_back(Field(side, Call("Coalesce", {Member(state, side), Member(item, side)})));
            const TString count = TStringBuilder() << side << "Count";
            updated.push_back(Field(count, Call("Add", {Member(state, count), Member(item, count)})));
        }
        // A constant shuffle key gathers every input partition before sorting.
        // OrderedLMap alone may be pushed into parallel input stages, separating
        // matching expected and actual keys. Condense1 retains one payload from
        // each side, including when corrupt input repeats an expected key.
        auto partitionKey = Arg();
        auto groups = Call("ShuffleByKeys", {rows, Lambda(partitionKey, List({})), Lambda(stream, Call("Condense1", {
            sorted,
            Lambda(first, first),
            Lambda2(next, previous, Call("AggrNotEquals", {KeyTuple(next, sortKeys), KeyTuple(previous, sortKeys)})),
            Lambda2(item, state, Call("AsStruct", std::move(updated))),
        }))});

        auto group = Arg();
        auto expected = Member(group, "Expected");
        auto actual = Member(group, "Actual");
        auto hasExpected = Call("AggrGreater", {Member(group, "ExpectedCount"), Uint64(0)});
        auto hasActual = Call("AggrGreater", {Member(group, "ActualCount"), Uint64(0)});
        auto missing = Call("And", {hasExpected, Call("Not", {hasActual})});
        auto extra = Call("And", {Call("Not", {hasExpected}), hasActual});
        auto mismatch = role.Values.empty() ? Bool(false) : Call("If", {
            Call("And", {hasExpected, hasActual}),
            Call("Not", {FieldsEqual(Call("Unwrap", {expected}), Call("Unwrap", {actual}), table, role.Values)}),
            Bool(false),
        });
        auto payload = Call("Unwrap", {Call("Coalesce", {expected, actual})});
        auto expectedRow = Arg();
        auto actualRow = Arg();
        auto sample = Sample(role,
            Call("If", {missing, String("MISSING"), Call("If", {extra, String("EXTRA"), String("VALUE_MISMATCH")})}),
            Call("If", {extra, Reason(role, payload), String("")}),
            DescribeFields(payload, table, keys),
            Map(expected, expectedRow, DescribeFields(expectedRow, table, role.Values)),
            Map(actual, actualRow, DescribeFields(actualRow, table, role.Values)));
        TNodes measurements{
            Field("Missing", missing),
            Field("Extra", extra),
            Field("ValueMismatch", mismatch),
            Field("Sample", BoundedSample(Call("Or", {missing, extra, mismatch}), sample)),
        };
        TColumns counted{"Missing", "Extra", "ValueMismatch"};
        for (const auto& counter : role.Counters) {
            measurements.push_back(Field(counter.Name, Call("If", {
                hasActual, counter.Predicate(Call("Unwrap", {actual})), Bool(false),
            })));
            counted.push_back(counter.Name);
        }
        return Aggregate(Map(groups, group, Call("AsStruct", std::move(measurements))), counted);
    }

    TNode CountDuplicateKeys(TNode rows, const TColumns& keys, bool orderedTableScan = false) const {
        auto stream = Arg();
        auto first = Arg();
        auto next = Arg();
        auto previous = Arg();
        auto item = Arg();
        auto state = Arg();
        if (orderedTableScan) {
            // Request the physical key prefix order; the existing sort-removal
            // rule turns this into an ordered table scan across partitions.
            auto row = Arg();
            rows = Call("Sort", {std::move(rows), Bool(true), Lambda(row, KeyTuple(row, keys))});
        }
        auto sorted = orderedTableScan ? stream : SortStream(stream, keys, keys);
        auto handler = Lambda(stream, Call("Condense1", {
            sorted,
            Lambda(first, Call("AsStruct", {
                Field("Key", KeyTuple(first, keys)), Field("Repeated", Bool(false)),
            })),
            Lambda2(next, previous, Call("AggrNotEquals", {KeyTuple(next, keys), Member(previous, "Key")})),
            Lambda2(item, state, Call("AsStruct", {
                Field("Key", Member(state, "Key")), Field("Repeated", Bool(true)),
            })),
        }));
        TNode groups;
        if (orderedTableScan) {
            groups = Call("OrderedLMap", {std::move(rows), std::move(handler)});
        } else {
            // The fallback must detect equal IDs from different base shards.
            auto partitionKey = Arg();
            groups = Call("ShuffleByKeys", {
                std::move(rows), Lambda(partitionKey, List({})), std::move(handler),
            });
        }
        auto group = Arg();
        return Aggregate(Map(groups, group, Call("AsStruct", {
            Field("Duplicates", Member(group, "Repeated")),
        })), {"Duplicates"}, {}, false);
    }

    TNode Report(TNodes roles, const TColumns& diagnostics = {}, TNodes extraFields = {}) const {
        auto counts = SummarizeRoles(std::move(roles), diagnostics);
        auto row = Arg();
        const auto total = [&](TStringBuf name) {
            return Call("Coalesce", {Member(row, name), Uint64(0)});
        };
        auto missing = total("Missing");
        auto extra = total("Extra");
        auto mismatch = total("ValueMismatch");
        auto clean = Call("And", {
            Call("AggrEquals", {missing, Uint64(0)}),
            Call("AggrEquals", {extra, Uint64(0)}),
            Call("AggrEquals", {mismatch, Uint64(0)}),
        });
        TNodes fields{
            Field("Status", Call("If", {clean, String("OK"), String("INCONSISTENT")})),
            Field("SnapshotStep", Call(TKqpIndexValidationSnapshotStep::CallableName(), {List(Generations)})),
            Field("SnapshotTxId", Call(TKqpIndexValidationSnapshotTxId::CallableName(), {})),
            Field("Missing", missing),
            Field("Extra", extra),
            Field("ValueMismatch", mismatch),
            Field("Sample", Member(row, "Sample")),
        };
        for (const auto& diagnostic : diagnostics) {
            fields.push_back(Field(diagnostic, total(diagnostic)));
        }
        fields.insert(fields.end(), extraFields.begin(), extraFields.end());
        Ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
        return Map(counts, row, Call("AsStruct", std::move(fields)));
    }

    TNode Error(EYqlIssueCode code, const TString& message) const {
        Ctx.AddError(YqlIssue(Ctx.GetPosition(Pos), code, message));
        return nullptr;
    }

private:
    TNode LookupCounts(const TKeyedRole& role) const {
        const auto& keys = role.Table.KeyColumnNames;
        const auto& table = Session.Tables().ExistingTable(role.Table.Cluster, role.Table.Name);
        auto lookups = Lookup(role.Table, role.Expected, keys, role.Values.empty() ? TColumns{keys.front()} : role.Values);

        // Every expected full key is unique. Each lookup emits one result, even
        // when absent, so physical cardinality minus found keys counts extras.
        auto hit = Arg();
        auto expected = Nth(hit, 0);
        auto actual = Nth(hit, 1);
        auto actualRow = Arg();
        auto found = Call("Exists", {actual});
        auto missing = Call("Not", {found});
        auto mismatch = role.Values.empty() ? Bool(false) : Call("Coalesce", {
            Call("Map", {actual, Lambda(actualRow, Call("Not", {FieldsEqual(expected, actualRow, table, role.Values)}))}),
            Bool(false),
        });
        auto expectedValues = Call("Just", {DescribeFields(expected, table, role.Values)});
        auto actualValue = Arg();
        auto actualValues = Call("Map", {actual, Lambda(actualValue, DescribeFields(actualValue, table, role.Values))});
        auto discrepancy = Sample(role, Call("If", {missing, String("MISSING"), String("VALUE_MISMATCH")}), String(""),
            DescribeFields(expected, table, keys), expectedValues, actualValues);
        TNodes forwardCounters;
        for (const auto& counter : role.Counters) {
            forwardCounters.push_back(Field(counter.Name, Bool(false)));
        }
        auto forward = Map(std::move(lookups), hit, Measurement(found, Bool(false), missing, mismatch,
            BoundedSample(Call("Or", {missing, mismatch}), discrepancy), std::move(forwardCounters)));

        auto stored = Arg();
        TNodes physicalCounters;
        TColumns counted{"Missing", "ValueMismatch", "Found", "Physical"};
        for (const auto& counter : role.Counters) {
            physicalCounters.push_back(Field(counter.Name, counter.Predicate(stored)));
            counted.push_back(counter.Name);
        }
        auto physical = Map(ReadPhysical(role.Table, keys), stored,
            Measurement(Bool(false), Bool(true), Bool(false), Bool(false), NoSample(), std::move(physicalCounters)));
        return Aggregate(Call("Extend", {forward, physical}), counted);
    }

    TNode SummarizeRoles(TNodes roles, const TColumns& diagnostics) const {
        auto sums = AppendColumns({"Missing", "Extra", "ValueMismatch"}, diagnostics);
        auto counts = Aggregate(Call("Extend", std::move(roles)), {}, {}, true, sums);
        auto row = Arg();
        const auto total = [&](TStringBuf name) {
            return Call("Coalesce", {Member(row, name), Uint64(0)});
        };
        auto sampleList = Arg();
        auto samples = Call("FlatMap", {Member(row, "Sample"), Lambda(sampleList, sampleList)});
        TNodes diagnosticValues;
        for (const auto& name : diagnostics) {
            diagnosticValues.push_back(Field(name, total(name)));
        }
        return Map(counts, row, RoleResult(total("Missing"), total("Extra"), total("ValueMismatch"),
            Call("Take", {samples, Uint64(SampleLimit)}), std::move(diagnosticValues)));
    }

    TNode KeyTuple(const TNode& row, const TColumns& keys) const {
        TNodes values;
        for (const auto& key : keys) {
            values.push_back(Member(row, key));
        }
        return List(std::move(values));
    }

    TNode SortStream(TNode stream, const TColumns& columns, const TColumns& keys) const {
        // Sort over an Extend can remain a narrow, non-spilling runtime sort.
        // Use the existing wide sorter explicitly until strategy selection is
        // moved into the optimizer. It obeys the query's memory/spilling policy.
        auto row = Arg();
        TNodes values;
        TNodes arguments;
        TNodes fields;
        THashMap<TStringBuf, size_t> positions;
        for (size_t i = 0; i < columns.size(); ++i) {
            positions.emplace(columns[i], i);
            values.push_back(Member(row, columns[i]));
            arguments.push_back(Arg());
            fields.push_back(Field(columns[i], arguments.back()));
        }
        auto wide = Call("ExpandMap", {Call("ToFlow", {std::move(stream)}),
            Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {row}), std::move(values))});
        TNodes directions;
        for (const auto& key : keys) {
            directions.push_back(List({Atom(ToString(positions.at(key))), Bool(true)}));
        }
        auto sorted = Call("WideSort", {std::move(wide), List(std::move(directions))});
        return Call("FromFlow", {Call("NarrowMap", {std::move(sorted),
            Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(arguments)), Call("AsStruct", std::move(fields)))})});
    }

    TNode ExtraCount(const TNode& totals) const {
        auto physical = Member(totals, "Physical");
        auto found = Member(totals, "Found");
        return Call("Sub", {Call("Ensure", {physical, Call("AggrGreaterOrEqual", {physical, found}),
            String("Index validation expected keys must be unique")}), found});
    }

    TNode SampleSlots(const TNode& totals) const {
        return Call("Sub", {Uint64(SampleLimit), Call("Length", {Member(totals, "Sample")})});
    }

    TNode WantsExtraSample(const TNode& totals) const {
        return Call("And", {
            Call("AggrGreater", {ExtraCount(totals), Uint64(0)}),
            Call("AggrGreater", {SampleSlots(totals), Uint64(0)}),
        });
    }

    TNode SampleExtras(const TKeyedRole& role, TNode candidates, TNode limit) const {
        const auto& keys = role.Table.KeyColumnNames;
        const auto& table = Session.Tables().ExistingTable(role.Table.Cluster, role.Table.Name);
        // Fetch wide COVER columns only for the small sample of physical keys.
        auto extras = Call("Take", {role.Extras(std::move(candidates)), std::move(limit)});
        auto extra = Arg();
        auto extraKey = extra;
        auto extraValues = Call("Just", {DescribeFields(extra, table, {})});
        if (!role.Values.empty()) {
            extras = Lookup(role.Table, std::move(extras), keys, role.Values);
            extraKey = Nth(extra, 0);
            auto value = Arg();
            extraValues = Call("Map", {Nth(extra, 1), Lambda(value, DescribeFields(value, table, role.Values))});
        }
        auto discrepancy = Sample(role, String("EXTRA"), Reason(role, extraKey),
            DescribeFields(extraKey, table, keys), NoValues(), extraValues);
        auto rows = Map(extras, extra, Call("AsStruct", {Field("Sample", BoundedSample(Bool(true), discrepancy))}));
        return Aggregate(rows, {});
    }

    TNode Aggregate(TNode evidence, const TColumns& counts,
        const TColumns& keys = {}, bool sampleRows = true, const TColumns& sums = {}) const
    {
        const auto* module = Types.Modules->GetModule("/lib/yql/aggregate.yqls");
        YQL_ENSURE(module);
        TNodeOnNodeOwnedMap clones;
        const auto factory = [&](TStringBuf name) {
            const auto* symbol = module->Symbols().FindPtr(name);
            YQL_ENSURE(symbol);
            return Ctx.DeepCopy(**symbol, module->ExprCtx(), clones, /*internStrings=*/true, /*copyTypes=*/false);
        };

        const auto listType = Call("TypeOf", {evidence});
        TNodes handlers;
        auto addFields = [&](TStringBuf factoryName, const TColumns& fields) {
            if (fields.empty()) {
                return;
            }
            auto traits = factory(factoryName);
            for (const auto& name : fields) {
                auto row = Arg();
                handlers.push_back(Field(name, Call("Apply", {traits, listType, Lambda(row, Member(row, name))})));
            }
        };
        addFields("count_if_traits_factory", counts);
        addFields("sum_traits_factory", sums);
        if (sampleRows) {
            auto sample = Arg();
            handlers.push_back(Field("Sample", Call("Apply", {factory("list2_traits_factory"), listType,
                Lambda(sample, Member(sample, "Sample")), Uint64(SampleLimit)})));
        }
        TNodes keyAtoms;
        for (const auto& key : keys) {
            keyAtoms.push_back(Atom(key));
        }
        return Call("Aggregate", {std::move(evidence), List(std::move(keyAtoms)), List(std::move(handlers)), List({})});
    }

    TNode ColumnAtoms(const TColumns& columns) const {
        TNodes select;
        THashSet<TStringBuf> seen;
        for (const auto& column : columns) {
            if (seen.insert(column).second) {
                select.push_back(Atom(column));
            }
        }
        return List(std::move(select));
    }

    TNode Read(TNode key, const TColumns& columns) const {
        auto tags = key->ChildrenList();
        tags.push_back(List({Atom("primary_view")}));
        auto read = Build<TKiReadTable>(Ctx, Pos)
            .World(TExprBase(World))
            .DataSource(TKiDataSource(Source))
            .TableKey(TExprBase(Call("Key", std::move(tags))))
            .Select(TExprBase(ColumnAtoms(columns)))
            .Settings(TCoNameValueTupleList(List({})))
            .Done();
        return Call("Right!", {read.Ptr()});
    }

    TNode ValueListType() const {
        auto stringType = Call("DataType", {Atom("String")});
        return Call("ListType", {Call("StructType", {
            Field("Name", stringType), Field("Type", stringType),
            Field("Value", Call("OptionalType", {stringType})),
        })});
    }

    TNode DescribeFields(const TNode& row, const TKikimrTableDescription& table, const TColumns& columns) const {
        if (columns.empty()) {
            return Call("List", {ValueListType()});
        }
        TNodes descriptors;
        for (const auto& column : columns) {
            const auto* type = table.GetColumnType(column);
            YQL_ENSURE(type);
            if (type->GetKind() == ETypeAnnotationKind::Optional) {
                type = type->Cast<TOptionalExprType>()->GetItemType();
            }
            auto value = Member(row, column);
            if (type->GetKind() == ETypeAnnotationKind::Pg) {
                value = Call("FromPg", {WrapWithPgCast(value, NPg::LookupType("text").TypeId, Ctx)});
            }
            value = Call("ToString", {value});
            descriptors.push_back(Call("AsStruct", {
                Field("Name", String(column)),
                Field("Type", String(FormatType(type))),
                Field("Value", Call("AsOptional", {value})),
            }));
        }
        return Call("AsList", std::move(descriptors));
    }

    TNode NoValues() const {
        return Call("Nothing", {Call("OptionalType", {ValueListType()})});
    }

    TNode Reason(const TKeyedRole& role, const TNode& row) const {
        return role.Reason ? role.Reason(row) : String("");
    }

    TNode Sample(const TKeyedRole& role, TNode kind, TNode reason, TNode key, TNode expected, TNode actual) const {
        return Call("AsStruct", {
            Field("Kind", std::move(kind)),
            Field("Role", String(role.Name)),
            Field("Reason", std::move(reason)),
            Field("Key", std::move(key)),
            Field("Expected", std::move(expected)),
            Field("Actual", std::move(actual)),
        });
    }

    TNode NoSample() const {
        return Call("Nothing", {Call("OptionalType", {Call("StructType", {
            Field("Kind", Call("DataType", {Atom("String")})),
            Field("Role", Call("DataType", {Atom("String")})),
            Field("Reason", Call("DataType", {Atom("String")})),
            Field("Key", ValueListType()),
            Field("Expected", Call("OptionalType", {ValueListType()})),
            Field("Actual", Call("OptionalType", {ValueListType()})),
        })})});
    }

    TNode BoundedSample(TNode discrepancy, const TNode& sample) const {
        auto fits = Call("AggrLessOrEqual", {
            Call("Size", {Call("StablePickle", {sample})}), Call("Uint32", {Atom(ToString(SampleEntryBytes))}),
        });
        return Call("If", {std::move(discrepancy), Call("OptionalIf", {fits, sample}), NoSample()});
    }

    TNode RoleResult(TNode missing, TNode extra, TNode mismatch, TNode samples, TNodes diagnostics = {}) const
    {
        TNodes fields{
            Field("Missing", std::move(missing)),
            Field("Extra", std::move(extra)),
            Field("ValueMismatch", std::move(mismatch)),
            Field("Sample", std::move(samples)),
        };
        fields.insert(fields.end(), diagnostics.begin(), diagnostics.end());
        return Call("AsStruct", std::move(fields));
    }

    TNode Measurement(TNode found, TNode physical, TNode missing, TNode mismatch, TNode sample,
        TNodes diagnostics = {}) const
    {
        TNodes fields{
            Field("Found", std::move(found)),
            Field("Physical", std::move(physical)),
            Field("Missing", std::move(missing)),
            Field("ValueMismatch", std::move(mismatch)),
            Field("Sample", std::move(sample)),
        };
        fields.insert(fields.end(), diagnostics.begin(), diagnostics.end());
        return Call("AsStruct", std::move(fields));
    }

    TExprContext& Ctx;
    TPositionHandle Pos;
    const TKikimrTableDescription& Base;
    TKikimrSessionContext& Session;
    TTypeAnnotationContext& Types;
    TNode World;
    TNode Source;
    TNode BaseKey;
    TNodes Generations;
};

TColumns SecondaryKey(const TKikimrTableMetadata& base, const TIndexDescription& index) {
    NTableIndex::TTableColumns baseColumns;
    baseColumns.Keys = base.KeyColumnNames;
    NTableIndex::TIndexColumns indexColumns{index.KeyColumns, index.DataColumns};
    return NTableIndex::CalcTableImplDescription(NKikimrSchemeOp::EIndexTypeGlobal,
        baseColumns, indexColumns).Keys;
}

TKeyedRole SecondaryRole(const TIndexValidation& plan, const TKikimrTableDescription& base, TNode baseRows,
    const TKikimrTableMetadata& impl, const TIndexDescription& index, TStringBuf role)
{
    auto keys = SecondaryKey(*base.Metadata, index);
    auto row = plan.Arg();
    auto columns = AppendColumns(keys, index.DataColumns);
    auto expected = plan.Map(std::move(baseRows), row, plan.Project(row, columns));
    auto extras = [&plan, &base, keys](TNode actual) {
        auto lookup = plan.Lookup(*base.Metadata, std::move(actual), base.Metadata->KeyColumnNames, keys);
        auto hit = plan.Arg();
        auto baseRow = plan.Arg();
        auto matches = plan.Call("Coalesce", {plan.Call("Map", {plan.Nth(hit, 1),
            plan.Lambda(baseRow, plan.FieldsEqual(plan.Nth(hit, 0), baseRow, base, keys))}), plan.Bool(false)});
        return plan.FilterMap(lookup, hit, plan.Call("Not", {matches}), plan.Nth(hit, 0));
    };
    return {role, impl, index.DataColumns, expected, extras, {}, {}};
}

TNamedTables NameImplementationTables(TKikimrTableMetadataPtr first) {
    TNamedTables tables;
    for (auto table = std::move(first); table; table = table->Next) {
        const auto slash = table->Name.rfind('/');
        auto name = slash == TString::npos ? table->Name : table->Name.substr(slash + 1);
        YQL_ENSURE(tables.emplace(std::move(name), table).second, "Duplicate index implementation table name");
    }
    return tables;
}

const TKikimrTableMetadata* RequireSingleDatashardTable(const TIndexValidation& plan,
    const TIndexDescription& index, const TNamedTables& tables, TStringBuf name)
{
    const auto* table = tables.FindPtr(TString(name));
    if (tables.size() != 1 || !table || (*table)->Kind != EKikimrTableKind::Datashard) {
        plan.Error(TIssuesIds::KIKIMR_UNSUPPORTED,
            TStringBuilder() << "Index validation is not implemented for " << index.Type);
        return nullptr;
    }
    return table->Get();
}

TNode ValidateSecondary(TIndexValidation& plan, const TKikimrTableDescription& base,
    const TIndexDescription& index, const TNamedTables& tables)
{
    const auto* impl = RequireSingleDatashardTable(plan, index, tables, NTableIndex::ImplTable);
    if (!impl) {
        return nullptr;
    }
    auto columns = AppendColumns(SecondaryKey(*base.Metadata, index), index.DataColumns);
    return plan.Report({plan.CompareKeys(SecondaryRole(plan, base, plan.ReadBase(columns), *impl, index, "INDEX"))},
        {}, {plan.Field("EmptyTokenPostings", plan.Uint64(0))});
}

TNode ValidateJson(TIndexValidation& plan, const TKikimrTableDescription& base,
    const TIndexDescription& index, const TNamedTables& tables)
{
    const auto* impl = RequireSingleDatashardTable(plan, index, tables, NTableIndex::ImplTable);
    if (!impl) {
        return nullptr;
    }
    const auto* options = std::get_if<NKikimrSchemeOp::TFulltextIndexDescription>(&index.SpecializedIndexDescription);
    const bool useRowId = options && options->GetUseRowIdAsDocId();
    const TIndexDescription* mapping = nullptr;
    const TKikimrTableMetadata* mappingImpl = nullptr;
    if (useRowId) {
        auto [mappingTables, mappingIndex] = base.Metadata->FindRowIdMappingIndex();
        if (!mappingTables || !mappingIndex) {
            return plan.Error(TIssuesIds::KIKIMR_INDEX_IS_NOT_READY, "The row-ID mapping index is not ready");
        }
        mapping = mappingIndex;
        mappingImpl = RequireSingleDatashardTable(plan, *mapping,
            NameImplementationTables(mappingTables), NTableIndex::ImplTable);
        if (!mappingImpl) {
            return nullptr;
        }
        plan.TrackGeneration(*mapping);
    }

    auto baseColumns = AppendColumns(index.KeyColumns, base.Metadata->KeyColumnNames);
    if (mapping) {
        baseColumns.push_back(RowIdColumn);
        baseColumns = AppendColumns(std::move(baseColumns), mapping->DataColumns);
    }
    auto baseRows = plan.ReadBase(baseColumns);
    auto document = plan.Arg();
    auto token = plan.Arg();
    TNodes fields;
    for (const auto& column : impl->KeyColumnNames) {
        auto value = column == TokenColumn ? plan.Member(token, TokenColumn) : plan.Member(document, column);
        fields.push_back(plan.Field(column, std::move(value)));
    }
    auto tokens = plan.Analyze(document, index);
    auto expected = plan.Call("FlatMap", {baseRows, plan.Lambda(document,
        plan.Map(tokens, token, plan.Call("AsStruct", std::move(fields))))});

    const auto emptyToken = [&plan, prefixed = index.KeyColumns.size() > 1](const TNode& row) {
        return prefixed ? plan.Call("AggrEquals", {
            plan.Call("AsOptional", {plan.Member(row, TokenColumn)}),
            plan.Call("Just", {plan.String("")}),
        }) : plan.Bool(false);
    };
    auto reason = [&plan, emptyToken](const TNode& row) {
        return plan.Call("If", {emptyToken(row), plan.String(EmptyTokenPosting), plan.String("")});
    };
    TNodes roles{plan.CompareScans({"POSTINGS", *impl, {}, expected, {}, reason,
        {{"EmptyTokenPostings", emptyToken}}})};
    TNode duplicateBaseKeys;
    if (mapping) {
        auto mappingSummary = plan.CompareKeysRelational(SecondaryRole(plan, base, baseRows, *mappingImpl, *mapping, "ROW_ID_MAPPING"));
        auto summary = plan.Arg();
        roles.push_back(plan.Map(mappingSummary, summary, plan.Call("AddMember", {
            summary, plan.Atom("EmptyTokenPostings"), plan.Uint64(0),
        })));

        const auto& primaryKey = base.Metadata->KeyColumnNames;
        const bool rowIdIsPrimaryKey = primaryKey.size() == 1 && primaryKey.front() == RowIdColumn;
        // Storage already enforces uniqueness when the document ID is the full
        // base primary key. This also avoids making the fallback read identical
        // to the mapping's base-key projection.
        if (!rowIdIsPrimaryKey) {
            // Full-key coverage and distinct physical mapping IDs prove that
            // stored base IDs are unique. Cover-value mismatches and unrelated
            // extra mapping rows do not invalidate that proof. A corrupt map
            // alone cannot prove the base has duplicate IDs.
            auto mappingDuplicates = plan.CountDuplicateKeys(plan.ReadPhysical(*mappingImpl, {RowIdColumn}), {RowIdColumn}, true);
            auto proof = plan.Arg();
            auto proofs = plan.Map(plan.Cross(mappingSummary, mappingDuplicates), proof, plan.Call("AsStruct", {
                plan.Field("NeedsCheck", plan.Call("Or", {
                    plan.Call("AggrGreater", {plan.Member(proof, "left.Missing"), plan.Uint64(0)}),
                    plan.Call("AggrGreater", {plan.Member(proof, "right.Duplicates"), plan.Uint64(0)}),
                })),
            }));
            auto gate = plan.Arg();
            auto failedProof = plan.Filter(proofs, gate, plan.Member(gate, "NeedsCheck"));
            // Full-key mapping lookups require an additional base PK column,
            // keeping their read distinct from this ID-only fallback. The gate
            // precedes Sort; a healthy proof retains no base-ID sort state.
            auto fallbackIds = plan.GateRows(plan.ReadBase({RowIdColumn}), failedProof, {RowIdColumn});
            duplicateBaseKeys = plan.CountDuplicateKeys(std::move(fallbackIds), {RowIdColumn});
        }
    }
    auto report = plan.Report(std::move(roles), {"EmptyTokenPostings"});
    if (!duplicateBaseKeys) {
        return report;
    }

    auto row = plan.Arg();
    auto boxedReport = plan.Map(report, row, plan.Call("AsStruct", {plan.Field("Report", row)}));
    auto result = plan.Arg();
    return plan.Map(plan.Cross(boxedReport, duplicateBaseKeys), result, plan.Call("Ensure", {
        plan.Member(result, "left.Report"),
        plan.Call("AggrEquals", {plan.Member(result, "right.Duplicates"), plan.Uint64(0)}),
        plan.String("Stored document IDs must be unique"),
    }));
}

} // namespace

TExprNode::TPtr BuildCheckIndex(const TExprNode& read, const TKikimrTableDescription& base,
    TStringBuf indexName, TKikimrSessionContext& session, TTypeAnnotationContext& types, TExprContext& ctx)
{
    TIndexValidation plan(read, base, session, types, ctx);
    const auto& table = *base.Metadata;
    auto [impl, index] = table.Indexes.empty()
        ? std::pair<TKikimrTableMetadataPtr, const TIndexDescription*>{}
        : table.GetIndex(indexName);
    if (!index && !table.IsIndexNotReady(indexName)) {
        return plan.Error(TIssuesIds::KIKIMR_SCHEME_ERROR, TStringBuilder() << "Index not found: " << indexName);
    }
    if (!index || index->State != TIndexDescription::EIndexState::Ready) {
        return plan.Error(TIssuesIds::KIKIMR_INDEX_IS_NOT_READY, TStringBuilder() << "Index is not ready: " << indexName);
    }

    decltype(&ValidateSecondary) validator = nullptr;
    switch (index->Type) {
        case TIndexDescription::EType::GlobalSync:
        case TIndexDescription::EType::GlobalSyncUnique:
            validator = ValidateSecondary;
            break;
        case TIndexDescription::EType::GlobalJson:
            validator = ValidateJson;
            break;
        default:
            break;
    }
    if (!validator) {
        return plan.Error(TIssuesIds::KIKIMR_UNSUPPORTED,
            TStringBuilder() << "Index validation is not implemented for " << index->Type);
    }
    if (!impl) {
        return plan.Error(TIssuesIds::KIKIMR_INDEX_IS_NOT_READY, TStringBuilder() << "Index is not ready: " << indexName);
    }

    plan.TrackGeneration(*index);
    return validator(plan, base, *index, NameImplementationTables(impl));
}

} // namespace NYql
