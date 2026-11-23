#include "check_index.h"
#include "yql_kikimr_expr_nodes.h"

#include <ydb/core/base/fulltext.h>
#include <ydb/core/base/table_index.h>

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/issue/yql_issue.h>

namespace NYql {
namespace {

using namespace NKikimr;
using namespace NNodes;
using namespace NTableIndex::NFulltext;

using TNode = TExprNode::TPtr;
using TNodes = TExprNode::TListType;

struct TIndexEntries {
    TStringBuf Role;
    TNode Expected;
    TNode Actual;
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

    TNode Member(const TNode& row, TStringBuf name) const {
        return Call("Member", {row, Atom(name)});
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

    TNode ReadBase(const TVector<TString>& columns) const {
        return Read(BaseKey, columns);
    }

    TNode ReadPhysical(const TKikimrTableMetadata& table, const TVector<TString>& columns) const {
        return Read(Call("Key", {List({Atom("table"), String(table.Name)})}), columns);
    }

    const TKikimrTableDescription& Describe(const TKikimrTableMetadata& table) const {
        return Session.Tables().ExistingTable(table.Cluster, table.Name);
    }

    // Record the published index identity in addition to the physical table schema identities.
    void TrackGeneration(const TIndexDescription& index) const {
        if (auto& preparing = Session.Query().PreparingQuery) {
            auto* info = preparing->MutablePhysicalQuery()->AddIndexValidations();
            info->MutableBaseTableId()->SetOwnerId(Base.Metadata->PathId.OwnerId());
            info->MutableBaseTableId()->SetTableId(Base.Metadata->PathId.TableId());
            info->SetName(index.Name);
            info->SetLocalPathId(index.LocalPathId);
            info->SetPathOwnerId(index.PathOwnerId);
            info->SetSchemaVersion(index.SchemaVersion);
        }
    }

    TNode Entry(TNode key, TNode value, TNode comparableValue, TNode reason) const {
        return Call("AsStruct", {
            Field("Key", Call("StablePickle", {std::move(key)})),
            Field("Value", std::move(value)),
            Field("ComparableValue", std::move(comparableValue)),
            Field("Reason", std::move(reason)),
        });
    }

    TNode Compare(const TIndexEntries& entries) const {
        auto join = Call("EquiJoin", {
            List({entries.Expected, Atom("expected")}),
            List({entries.Actual, Atom("actual")}),
            List({Atom("Full"), Atom("expected"), Atom("actual"),
                List({Atom("expected"), Atom("Key")}), List({Atom("actual"), Atom("Key")}), List({})}),
            List({}),
        });
        auto row = Arg();
        auto expectedKey = Member(row, "expected.Key");
        auto actualKey = Member(row, "actual.Key");
        auto missing = Call("Not", {Call("Exists", {actualKey})});
        auto extra = Call("Not", {Call("Exists", {expectedKey})});
        auto mismatch = Call("AggrNotEquals", {
            Member(row, "expected.ComparableValue"), Member(row, "actual.ComparableValue")});
        auto value = [&](TStringBuf name) {
            auto present = Arg();
            return Call("Map", {Member(row, name), Lambda(present, Call("StablePickle", {present}))});
        };
        auto discrepancy = Call("AsStruct", {
            Field("Kind", Call("If", {missing, String("MISSING"),
                Call("If", {extra, String("EXTRA"), String("VALUE_MISMATCH")})})),
            Field("Role", String(entries.Role)),
            Field("Reason", Call("Coalesce", {Member(row, "actual.Reason"), String("")})),
            Field("Key", Call("Coalesce", {expectedKey, actualKey})),
            Field("Expected", value("expected.Value")),
            Field("Actual", value("actual.Value")),
        });
        return Call("FlatMap", {join, Lambda(row, Call("OptionalIf", {
            Call("Or", {missing, extra, mismatch}), discrepancy}))});
    }

    TNode Report(TNode diff) const {
        // Use the same mergeable bounded-list aggregation as AGGREGATE_LIST(value, limit).
        const auto* module = Types.Modules->GetModule("/lib/yql/aggregate.yqls");
        YQL_ENSURE(module);
        TNodeOnNodeOwnedMap clones;
        const auto factory = [&](TStringBuf name) {
            const auto* symbol = module->Symbols().FindPtr(name);
            YQL_ENSURE(symbol);
            return Ctx.DeepCopy(**symbol, module->ExprCtx(), clones, /*internStrings=*/true, /*copyTypes=*/false);
        };

        const auto countIf = factory("count_if_traits_factory");
        const auto listType = Call("TypeOf", {diff});
        TNodes handlers;
        for (const auto& [name, kind] : {std::pair<TStringBuf, TStringBuf>{"Missing", "MISSING"},
            {"Extra", "EXTRA"}, {"ValueMismatch", "VALUE_MISMATCH"}})
        {
            auto row = Arg();
            auto matches = Call("AggrEquals", {Member(row, "Kind"), String(kind)});
            handlers.push_back(Field(name, Call("Apply", {countIf, listType, Lambda(row, matches)})));
        }
        auto sample = Arg();
        handlers.push_back(Field("Sample", Call("Apply", {factory("list2_traits_factory"), listType,
            Lambda(sample, sample), Call("Uint64", {Atom("100")})})));
        auto aggregate = Call("Aggregate", {std::move(diff), List({}), List(std::move(handlers)), List({})});

        auto row = Arg();
        auto clean = Call("And", {
            Call("AggrEquals", {Member(row, "Missing"), Call("Uint64", {Atom("0")})}),
            Call("AggrEquals", {Member(row, "Extra"), Call("Uint64", {Atom("0")})}),
            Call("AggrEquals", {Member(row, "ValueMismatch"), Call("Uint64", {Atom("0")})}),
        });
        auto report = Call("AddMember", {row, Atom("Status"), Call("If", {clean, String("OK"), String("INCONSISTENT")})});
        report = Call("AddMember", {report, Atom("SnapshotStep"), Call("KqpIndexValidationSnapshotStep", {})});
        report = Call("AddMember", {report, Atom("SnapshotTxId"), Call("KqpIndexValidationSnapshotTxId", {})});
        Ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
        return Call("Map", {aggregate, Lambda(row, std::move(report))});
    }

    TNode Error(EYqlIssueCode code, const TString& message) const {
        Ctx.AddError(YqlIssue(Ctx.GetPosition(Pos), code, message));
        return nullptr;
    }

    const TKikimrTableDescription& BaseDescription() const {
        return Base;
    }

private:
    TNode Read(TNode key, const TVector<TString>& columns) const {
        auto tags = key->ChildrenList();
        tags.push_back(List({Atom("primary_view")}));
        key = Call("Key", std::move(tags));
        TNodes select;
        THashSet<TStringBuf> seen;
        for (const auto& column : columns) {
            if (seen.insert(column).second) {
                select.push_back(Atom(column));
            }
        }
        return Call("Right!", {Call(TKiReadTable::CallableName(), {
            World, Source, std::move(key), List(std::move(select)), List({})})});
    }

    TExprContext& Ctx;
    TPositionHandle Pos;
    const TKikimrTableDescription& Base;
    TKikimrSessionContext& Session;
    TTypeAnnotationContext& Types;
    TNode World;
    TNode Source;
    TNode BaseKey;
};

TVector<TString> AppendColumns(TVector<TString> columns, const TVector<TString>& suffix) {
    columns.insert(columns.end(), suffix.begin(), suffix.end());
    return columns;
}

TVector<TString> SecondaryKey(const TKikimrTableMetadata& base, const TIndexDescription& index) {
    NTableIndex::TTableColumns baseColumns;
    baseColumns.Keys = base.KeyColumnNames;
    NTableIndex::TIndexColumns indexColumns{index.KeyColumns, index.DataColumns};
    return NTableIndex::CalcTableImplDescription(NKikimrSchemeOp::EIndexTypeGlobal,
        baseColumns, indexColumns).Keys;
}

TNode ProjectEntry(const TIndexValidation& plan, const TNode& row, const TKikimrTableDescription& table,
    const TVector<TString>& keyColumns, const TVector<TString>& valueColumns,
    const TNode& token = {}, bool checkEmptyToken = false)
{
    TNodes key;
    for (const auto& column : keyColumns) {
        auto member = token && column == TokenColumn ? token : plan.Member(row, column);
        key.push_back(plan.Field(column, plan.Call("AsOptional", {member})));
    }
    TNodes values;
    TNodes comparable;
    for (const auto& column : valueColumns) {
        auto member = plan.Call("AsOptional", {plan.Member(row, column)});
        values.push_back(plan.Field(column, member));
        const auto* type = table.GetColumnType(column);
        YQL_ENSURE(type);
        if (!type->IsEquatable()) {
            member = plan.Call("StablePickle", {member});
        }
        comparable.push_back(plan.Field(column, std::move(member)));
    }
    auto reason = plan.String("");
    if (checkEmptyToken) {
        auto empty = plan.Call("AggrEquals", {plan.Member(row, TokenColumn), reason});
        reason = plan.Call("If", {empty, plan.String("EMPTY_TOKEN_POSTING"), reason});
    }
    return plan.Entry(plan.Call("AsStruct", std::move(key)), plan.Call("AsStruct", std::move(values)),
        plan.Call("AsStruct", std::move(comparable)), std::move(reason));
}

TIndexEntries SecondaryEntries(const TIndexValidation& plan, const TNode& baseRows, const TKikimrTableMetadata& impl,
    const TIndexDescription& index, TStringBuf role, const TVector<TString>& valueColumns)
{
    auto expected = plan.Arg();
    auto actual = plan.Arg();
    auto key = SecondaryKey(*plan.BaseDescription().Metadata, index);
    return {
        role,
        plan.Call("Map", {baseRows, plan.Lambda(expected,
            ProjectEntry(plan, expected, plan.BaseDescription(), key, valueColumns))}),
        plan.Call("Map", {plan.ReadPhysical(impl, AppendColumns(impl.KeyColumnNames, valueColumns)),
            plan.Lambda(actual, ProjectEntry(plan, actual, plan.Describe(impl), impl.KeyColumnNames, valueColumns))}),
    };
}

TVector<TIndexEntries> ValidateSecondary(const TIndexValidation& plan, const TIndexDescription& index,
    const TKikimrTableMetadata& impl)
{
    const auto baseColumns = AppendColumns(SecondaryKey(*plan.BaseDescription().Metadata, index), index.DataColumns);
    return {SecondaryEntries(plan, plan.ReadBase(baseColumns), impl, index, "INDEX", index.DataColumns)};
}

TVector<TIndexEntries> ValidateJson(const TIndexValidation& plan, const TIndexDescription& index,
    const TKikimrTableMetadata& impl)
{
    const auto& base = plan.BaseDescription();
    const auto* options = std::get_if<NKikimrSchemeOp::TFulltextIndexDescription>(&index.SpecializedIndexDescription);
    const bool useRowId = options && options->GetUseRowIdAsDocId();
    const TIndexDescription* mapping = nullptr;
    TKikimrTableMetadataPtr mappingImpl;
    if (useRowId) {
        for (size_t i = 0; i < base.Metadata->Indexes.size(); ++i) {
            const auto& candidate = base.Metadata->Indexes[i];
            if (candidate.Type == TIndexDescription::EType::GlobalSyncUnique
                && candidate.State == TIndexDescription::EIndexState::Ready
                && candidate.KeyColumns.size() == 1 && candidate.KeyColumns.front() == RowIdColumn)
            {
                mapping = &candidate;
                mappingImpl = base.Metadata->ImplTables[i];
                break;
            }
        }
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
    auto postingKey = index.KeyColumns;
    const auto jsonColumn = postingKey.back();
    postingKey.back() = TokenColumn;
    postingKey = AppendColumns(std::move(postingKey), useRowId ? TVector<TString>{RowIdColumn} : base.Metadata->KeyColumnNames);

    auto document = plan.Arg();
    auto token = plan.Arg();
    const auto typeId = base.Metadata->Columns.at(jsonColumn).TypeInfo.GetTypeId();
    const auto mode = typeId == NScheme::NTypeIds::Json
        ? NFulltext::EIndexMode::JsonIndexOverJson : NFulltext::EIndexMode::JsonIndexOverJsonDocument;
    // FulltextAnalyze uses the write-path JSON tokenizers and emits distinct tokens.
    auto tokens = plan.Call("FulltextAnalyze", {plan.Member(document, jsonColumn), plan.String(""),
        plan.Atom(ToString(static_cast<ui32>(mode)))});
    auto expected = plan.Call("FlatMap", {baseRows, plan.Lambda(document, plan.Call("Map", {tokens,
        plan.Lambda(token, ProjectEntry(plan, document, base, postingKey, {}, plan.Member(token, TokenColumn)))}))});
    auto posting = plan.Arg();
    auto actual = plan.Call("Map", {plan.ReadPhysical(impl, impl.KeyColumnNames), plan.Lambda(posting,
        ProjectEntry(plan, posting, plan.Describe(impl), impl.KeyColumnNames, {}, {}, index.KeyColumns.size() > 1))});

    TVector<TIndexEntries> entries{{"POSTINGS", std::move(expected), std::move(actual)}};
    if (mapping) {
        entries.push_back(SecondaryEntries(plan, baseRows, *mappingImpl, *mapping, "ROW_ID_MAPPING", {}));
    }
    return entries;
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
    if (!index) {
        return plan.Error(TIssuesIds::KIKIMR_SCHEME_ERROR, TStringBuilder() << "Index not found: " << indexName);
    }
    if (index->State != TIndexDescription::EIndexState::Ready) {
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
    auto entries = validator(plan, *index, *impl);
    if (entries.empty()) {
        return nullptr;
    }
    TNodes differences;
    for (const auto& role : entries) {
        differences.push_back(plan.Compare(role));
    }
    auto diff = differences.size() == 1 ? differences.front() : plan.Call("Extend", std::move(differences));
    return plan.Report(std::move(diff));
}

} // namespace NYql
