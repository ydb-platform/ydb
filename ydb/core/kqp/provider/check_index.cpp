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
    bool IsMapping = false;
    bool RequiresMapping = false;
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
        for (const auto& column : columns) {
            fields.push_back(Field(column, Member(row, column)));
        }
        return Call("AsStruct", std::move(fields));
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
        const auto& table = Session.Tables().ExistingTable(role.Table.Cluster, role.Table.Name);
        auto lookups = Lookup(role.Table, role.Expected, keys, role.Values.empty() ? TColumns{keys.front()} : role.Values);

        // Every expected key is unique. A full-key lookup emits exactly one result, even when absent.
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
        auto forward = Map(std::move(lookups), hit, Measurement(found, Bool(false), missing, mismatch, Bool(false),
            BoundedSample(Call("Or", {missing, mismatch}), discrepancy)));

        // A key-only scan supplies the exact physical cardinality independently of sample collection.
        auto stored = Arg();
        auto physical = Map(ReadPhysical(role.Table, keys), stored,
            Measurement(Bool(false), Bool(true), Bool(false), Bool(false),
                Call("AggrEquals", {Reason(role, stored), String(EmptyTokenPosting)}), NoSample()));
        auto counts = Aggregate(Call("Extend", {forward, physical}),
            {"Missing", "ValueMismatch", "Found", "Physical", "EmptyTokenPostings"});
        if (role.IsMapping || role.RequiresMapping) {
            return CompareMappedRole(role, counts);
        }

        // Materialize only the one-row summary before issuing any reverse lookups.
        auto totals = Call("Unwrap", {Call("Head", {counts})});
        auto extra = ExtraCount(totals);
        auto available = SampleSlots(totals);
        auto candidate = Arg();
        auto keysForSample = Filter(ReadPhysical(role.Table, keys), candidate, WantsExtraSample(totals));
        auto limit = Call("If", {Call("AggrLess", {extra, available}), extra, available});
        auto samples = SampleExtras(role, std::move(keysForSample), limit);
        auto row = Arg();
        return Map(samples, row, RoleResult(Member(totals, "Missing"), extra, Member(totals, "ValueMismatch"),
            Member(totals, "EmptyTokenPostings"), Bool(false), Bool(false),
            Call("Extend", {Member(totals, "Sample"), Member(row, "Sample")})));
    }

    TNode CheckUniqueRowIds() const {
        // Group only row IDs. The stored mapping itself may be corrupt.
        auto row = Arg();
        auto ids = Map(ReadBase({RowIdColumn}), row, Call("AsStruct", {
            Field(RowIdColumn, Member(row, RowIdColumn)),
            Field("Present", Bool(true)),
        }));
        auto counts = Aggregate(ids, {"Present"}, {RowIdColumn}, false);
        auto group = Arg();
        auto duplicates = Filter(counts, group, Call("AggrGreater", {Member(group, "Present"), Uint64(1)}));
        auto duplicate = Arg();
        return Map(Call("Take", {duplicates, Uint64(1)}), duplicate,
            RoleResult(Uint64(0), Uint64(0), Uint64(0), Uint64(0), Bool(false), Bool(true), Call("ToList", {NoSample()})));
    }

    TNode Report(TNodes roles) const {
        auto counts = Aggregate(Call("Extend", std::move(roles)), {"MappingInvalid", "DuplicateRowIds"}, {}, true,
            {"Missing", "Extra", "ValueMismatch", "EmptyTokenPostings"});
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
        auto sampleList = Arg();
        auto samples = Call("FlatMap", {Member(row, "Sample"), Lambda(sampleList, sampleList)});
        auto mappingValid = Call("AggrEquals", {Member(row, "MappingInvalid"), Uint64(0)});
        auto item = Arg();
        samples = Filter(samples, item, Call("Or", {Call("Not", {Member(item, "RequiresMapping")}), mappingValid}));
        auto sampleItem = Arg();
        samples = Map(samples, sampleItem, Call("RemoveMember", {sampleItem, Atom("RequiresMapping")}));
        auto report = Call("AsStruct", {
            Field("Status", Call("If", {clean, String("OK"), String("INCONSISTENT")})),
            Field("SnapshotStep", Call(TKqpIndexValidationSnapshotStep::CallableName(), {List(Generations)})),
            Field("SnapshotTxId", Call(TKqpIndexValidationSnapshotTxId::CallableName(), {})),
            Field("Missing", missing),
            Field("Extra", extra),
            Field("ValueMismatch", mismatch),
            Field("EmptyTokenPostings", total("EmptyTokenPostings")),
            Field("Sample", Call("Take", {samples, Uint64(SampleLimit)})),
        });
        auto valid = Call("AggrEquals", {Member(row, "DuplicateRowIds"), Uint64(0)});
        report = Call("Ensure", {report, valid, String("Stored document IDs must be unique")});
        Ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
        return Map(counts, row, std::move(report));
    }

    TNode Error(EYqlIssueCode code, const TString& message) const {
        Ctx.AddError(YqlIssue(Ctx.GetPosition(Pos), code, message));
        return nullptr;
    }

private:
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

    TNode CompareMappedRole(const TKeyedRole& role, const TNode& counts) const {
        // Broadcast the optional one-row summary to the physical key stream.
        // Multiple roles can share a base scan without nesting scalar precomputes.
        const auto& keys = role.Table.KeyColumnNames;
        auto gateRow = Arg();
        auto gate = Filter(counts, gateRow, WantsExtraSample(gateRow));
        auto joined = Call("EquiJoin", {
            List({ReadPhysical(role.Table, keys), Atom("physical")}),
            List({gate, Atom("gate")}),
            List({Atom("Cross"), Atom("physical"), Atom("gate"), List({}), List({}), List({})}),
            List({}),
        });
        auto joinedRow = Arg();
        TNodes fields;
        for (const auto& key : keys) {
            fields.push_back(Field(key, Member(joinedRow, TStringBuilder() << "physical." << key)));
        }
        auto keysForSample = Map(joined, joinedRow, Call("AsStruct", std::move(fields)));
        auto samples = SampleExtras(role, keysForSample, Uint64(SampleLimit));
        auto sample = Arg();
        auto reverse = Map(samples, sample,
            RoleResult(Uint64(0), Uint64(0), Uint64(0), Uint64(0), Bool(false), Bool(false), Member(sample, "Sample")));

        auto totals = Arg();
        auto missing = Member(totals, "Missing");
        auto extra = ExtraCount(totals);
        auto mappingInvalid = role.IsMapping ? Call("Or", {
            Call("AggrGreater", {missing, Uint64(0)}), Call("AggrGreater", {extra, Uint64(0)}),
        }) : Bool(false);
        auto forward = Map(counts, totals, RoleResult(missing, extra, Member(totals, "ValueMismatch"),
            Member(totals, "EmptyTokenPostings"), mappingInvalid, Bool(false), Member(totals, "Sample")));
        return Call("Extend", {forward, reverse});
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

    TNode Aggregate(TNode evidence, std::initializer_list<TStringBuf> counts,
        std::initializer_list<TStringBuf> keys = {}, bool sampleRows = true,
        std::initializer_list<TStringBuf> sums = {}) const
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
        auto addFields = [&](TStringBuf factoryName, std::initializer_list<TStringBuf> fields) {
            if (!fields.size()) {
                return;
            }
            auto traits = factory(factoryName);
            for (const auto name : fields) {
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
        for (const auto key : keys) {
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
        auto requiresMapping = role.RequiresMapping
            ? Call("And", {Call("AggrEquals", {kind, String("EXTRA")}), Call("AggrEquals", {reason, String("")})})
            : Bool(false);
        return Call("AsStruct", {
            Field("Kind", std::move(kind)),
            Field("Role", String(role.Name)),
            Field("Reason", std::move(reason)),
            Field("Key", std::move(key)),
            Field("Expected", std::move(expected)),
            Field("Actual", std::move(actual)),
            Field("RequiresMapping", std::move(requiresMapping)),
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
            Field("RequiresMapping", Call("DataType", {Atom("Bool")})),
        })})});
    }

    TNode BoundedSample(TNode discrepancy, const TNode& sample) const {
        auto fits = Call("AggrLessOrEqual", {
            Call("Size", {Call("StablePickle", {sample})}), Call("Uint32", {Atom(ToString(SampleEntryBytes))}),
        });
        return Call("If", {std::move(discrepancy), Call("OptionalIf", {fits, sample}), NoSample()});
    }

    TNode RoleResult(TNode missing, TNode extra, TNode mismatch, TNode emptyToken, TNode mappingInvalid,
        TNode duplicateRowIds, TNode samples) const
    {
        return Call("AsStruct", {
            Field("Missing", std::move(missing)),
            Field("Extra", std::move(extra)),
            Field("ValueMismatch", std::move(mismatch)),
            Field("EmptyTokenPostings", std::move(emptyToken)),
            Field("MappingInvalid", std::move(mappingInvalid)),
            Field("DuplicateRowIds", std::move(duplicateRowIds)),
            Field("Sample", std::move(samples)),
        });
    }

    TNode Measurement(TNode found, TNode physical, TNode missing, TNode mismatch, TNode emptyToken, TNode sample) const {
        return Call("AsStruct", {
            Field("Found", std::move(found)),
            Field("Physical", std::move(physical)),
            Field("Missing", std::move(missing)),
            Field("ValueMismatch", std::move(mismatch)),
            Field("EmptyTokenPostings", std::move(emptyToken)),
            Field("Sample", std::move(sample)),
        });
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
    const TKikimrTableMetadata& impl, const TIndexDescription& index, TStringBuf role, bool isMapping = false)
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
    return {role, impl, index.DataColumns, expected, extras, {}, isMapping};
}

TNodes ValidateSecondary(TIndexValidation& plan, const TKikimrTableDescription& base,
    const TIndexDescription& index, const TKikimrTableMetadata& impl)
{
    auto columns = AppendColumns(SecondaryKey(*base.Metadata, index), index.DataColumns);
    return {plan.CompareKeys(SecondaryRole(plan, base, plan.ReadBase(columns), impl, index, "INDEX"))};
}

TReverseProbe JsonExtras(const TIndexValidation& plan, const TKikimrTableDescription& base,
    const TIndexDescription& index, const TKikimrTableMetadata* mapping)
{
    return [&plan, &base, &index, mapping](TNode actual) {
        auto posting = plan.Arg();
        auto token = plan.Arg();
        auto document = plan.Arg();
        auto hit = plan.Arg();
        auto candidate = plan.Nth(hit, 0);
        auto baseKeys = base.Metadata->KeyColumnNames;
        // A valid row-ID map has exactly one result for each stored row ID. If the
        // map is inconsistent, its results cannot prove absence of a posting.
        if (mapping) {
            auto mapHit = plan.Arg();
            auto mapRow = plan.Arg();
            auto mapLookup = plan.Lookup(*mapping, actual, {RowIdColumn}, baseKeys);
            TNodes fields;
            for (const auto& key : baseKeys) {
                fields.push_back(plan.Field(key, plan.Member(mapRow, key)));
            }
            actual = plan.Map(std::move(mapLookup), mapHit, plan.Call("AsStruct", {
                plan.Field("Posting", plan.Nth(mapHit, 0)),
                plan.Field("Pk", plan.Call("Map", {plan.Nth(mapHit, 1),
                    plan.Lambda(mapRow, plan.Call("AsStruct", std::move(fields)))})),
            }));
            // Unresolved mapping rows still yield a left row from the lookup.
            TNodes inputFields{plan.Field("Posting", plan.Member(posting, "Posting"))};
            for (const auto& key : baseKeys) {
                inputFields.push_back(plan.Field(key, plan.Member(plan.Member(posting, "Pk"), key)));
            }
            actual = plan.Map(std::move(actual), posting, plan.Call("AsStruct", std::move(inputFields)));
            candidate = plan.Member(candidate, "Posting");
        }
        auto columns = AppendColumns(index.KeyColumns, baseKeys);
        if (mapping) {
            columns.push_back(RowIdColumn);
        }
        auto lookup = plan.Lookup(*base.Metadata, std::move(actual), baseKeys, columns);
        auto prefix = index.KeyColumns;
        prefix.pop_back();
        if (mapping) {
            prefix.push_back(RowIdColumn);
        }
        auto matchingTokens = plan.Filter(plan.Analyze(document, index), token, plan.Call("AggrEquals", {
            plan.Member(token, TokenColumn), plan.Member(candidate, TokenColumn),
        }));
        auto matchesRow = plan.Call("And", {
            plan.FieldsEqual(candidate, document, base, prefix),
            plan.Call("HasItems", {matchingTokens}),
        });
        auto matches = plan.Call("Coalesce", {plan.Call("Map", {plan.Nth(hit, 1),
            plan.Lambda(document, matchesRow)}), plan.Bool(false)});
        return plan.FilterMap(lookup, hit, plan.Call("Not", {matches}), candidate);
    };
}

TNodes ValidateJson(TIndexValidation& plan, const TKikimrTableDescription& base,
    const TIndexDescription& index, const TKikimrTableMetadata& impl)
{
    const auto* options = std::get_if<NKikimrSchemeOp::TFulltextIndexDescription>(&index.SpecializedIndexDescription);
    const bool useRowId = options && options->GetUseRowIdAsDocId();
    const TIndexDescription* mapping = nullptr;
    TKikimrTableMetadataPtr mappingImpl;
    if (useRowId) {
        std::tie(mappingImpl, mapping) = base.Metadata->FindRowIdMappingIndex();
        if (!mappingImpl) {
            plan.Error(TIssuesIds::KIKIMR_INDEX_IS_NOT_READY, "The row-ID mapping index is not ready");
            return {};
        }
        plan.TrackGeneration(*mapping);
    }

    auto baseColumns = AppendColumns(index.KeyColumns, base.Metadata->KeyColumnNames);
    if (useRowId) {
        baseColumns.push_back(RowIdColumn);
    }
    auto baseRows = plan.ReadBase(baseColumns);
    auto document = plan.Arg();
    auto token = plan.Arg();
    TNodes fields;
    for (const auto& column : impl.KeyColumnNames) {
        auto value = column == TokenColumn ? plan.Member(token, TokenColumn) : plan.Member(document, column);
        fields.push_back(plan.Field(column, std::move(value)));
    }
    auto tokens = plan.Analyze(document, index);
    auto expected = plan.Call("FlatMap", {baseRows, plan.Lambda(document,
        plan.Map(tokens, token, plan.Call("AsStruct", std::move(fields))))});

    TDiscrepancyReason reason;
    if (index.KeyColumns.size() > 1) {
        reason = [&plan](const TNode& row) {
            auto empty = plan.Call("AggrEquals", {plan.Member(row, TokenColumn), plan.String("")});
            return plan.Call("If", {empty, plan.String(EmptyTokenPosting), plan.String("")});
        };
    }
    TNodes roles{plan.CompareKeys({"POSTINGS", impl, {}, expected, JsonExtras(plan, base, index, mappingImpl.Get()),
        reason, false, useRowId})};
    if (mapping) {
        roles.push_back(plan.CompareKeys(SecondaryRole(plan, base, baseRows, *mappingImpl, *mapping, "ROW_ID_MAPPING", true)));
        roles.push_back(plan.CheckUniqueRowIds());
    }
    return roles;
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
    if (!validator || (impl && (impl->Next || impl->Kind != EKikimrTableKind::Datashard))) {
        return plan.Error(TIssuesIds::KIKIMR_UNSUPPORTED,
            TStringBuilder() << "Index validation is not implemented for " << index->Type);
    }
    if (!impl) {
        return plan.Error(TIssuesIds::KIKIMR_INDEX_IS_NOT_READY, TStringBuilder() << "Index is not ready: " << indexName);
    }

    plan.TrackGeneration(*index);
    auto roles = validator(plan, base, *index, *impl);
    return roles.empty() ? nullptr : plan.Report(std::move(roles));
}

} // namespace NYql
