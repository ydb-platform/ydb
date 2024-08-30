#include "read_attributes_utils.h"
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

namespace NYql {

using namespace NNodes;

class TGatheringAttributesVisitor : public IAstAttributesVisitor {
    void VisitRead(TExprNode&, TString cluster, TString tablePath) override {
        CurrentSource = &*Result.try_emplace(std::make_pair(cluster, tablePath)).first;
    };

    void ExitRead() override {
        CurrentSource = nullptr;
    };

    void VisitAttribute(TString key, TString value) override {
        Y_ABORT_UNLESS(CurrentSource, "cannot write %s: %s", key.c_str(), value.c_str());
        CurrentSource->second.try_emplace(key, value);
    };

    void VisitNonAttribute(TExprNode::TPtr) override {}

public:
    THashMap<std::pair<TString, TString>, THashMap<TString, TString>> Result;

private:
    decltype(Result)::pointer CurrentSource = nullptr;
};

class TAttributesReplacingVisitor : public IAstAttributesVisitor {
public:
    TAttributesReplacingVisitor(THashMap<TString, TString>&& attributesBeforeFilter,
                                TStringBuf cluster, TStringBuf tablePath,
                                TKikimrTableMetadataPtr&& metadata,
                                TExprContext& ctx)
        : GatheredAttributes{std::move(attributesBeforeFilter)}
        , Cluster{cluster}, TablePath{tablePath}
        , Ctx{ctx}
        , Metadata{std::move(metadata)}
        , NewAttributes{Metadata->Attributes}
    {}

    void VisitRead(TExprNode& read, TString cluster, TString tablePath) override {
        if (cluster == Cluster && tablePath == TablePath) {
            Read = &read;
        }
    };

    void ExitRead() override {
        if (!Read) {
            return;
        }
        if (!ReplacedUserchema && Metadata && !Metadata->Columns.empty()) {
            Children.push_back(BuildSchemaFromMetadata(Read->Pos(), Ctx, Metadata->Columns));
        }
        for (const auto& [key, value] : NewAttributes) {
            Children.push_back(Ctx.NewList(Read->Pos(), {
                Ctx.NewAtom(Read->Pos(), key),
                Ctx.NewAtom(Read->Pos(), value),
            }));
        }
        Read->Child(4)->ChangeChildrenInplace(std::move(Children));
        Read = nullptr;
    };

    void VisitAttribute(TString key, TString value) override {
        if (!Read) {
            return;
        }
        const bool gotNewAttributes = Metadata && !Metadata->Attributes.empty();

        if (gotNewAttributes && GatheredAttributes.contains(key) && !Metadata->Attributes.contains(key)) {
            return;
        }
        auto mbNewValue = Metadata->Attributes.FindPtr(key);
        NewAttributes.erase(key);

        auto pos = Read->Pos();
        auto attribute = Ctx.NewList(pos, {
            Ctx.NewAtom(pos, key),
            Ctx.NewAtom(pos, mbNewValue ? *mbNewValue : value)
        });
        Children.push_back(std::move(attribute));
    };

    void VisitNonAttribute(TExprNode::TPtr node) override {
        if (!Read) {
            return;
        }
        if (!Metadata || Metadata->Columns.empty()) {
            Children.push_back(std::move(node));
            return;
        }

        auto nodeChildren = node->Children();
        if (!nodeChildren.empty() && nodeChildren[0]->IsAtom()) {
            TCoAtom attrName{nodeChildren[0]};
            if (attrName.StringValue().equal("userschema")) {
                node = BuildSchemaFromMetadata(Read->Pos(), Ctx, Metadata->Columns);
                ReplacedUserchema = true;
            }
        }
        Children.push_back(std::move(node));
    }

private:
    THashMap<TString, TString> GatheredAttributes;
    TStringBuf Cluster;
    TStringBuf TablePath;

    TExprContext& Ctx;
    TKikimrTableMetadataPtr Metadata;
    TExprNode* Read = nullptr;
    std::vector<TExprNode::TPtr> Children;
    THashMap<TString, TString> NewAttributes;
    bool ReplacedUserchema = false;
};

namespace {

std::optional<std::pair<TString, TString>> GetAsTextAttribute(const TExprNode& child) {
    if (!child.IsList() || child.ChildrenSize() != 2) {
        return std::nullopt;
    }
    if (!(child.Child(0)->IsAtom() && child.Child(1)->IsAtom())) {
        return std::nullopt;
    }

    TCoAtom attrKey{child.Child(0)};
    TCoAtom attrVal{child.Child(1)};

    return std::make_optional(std::make_pair(attrKey.StringValue(), attrVal.StringValue()));
}

} // namespace anonymous

void ExtractReadAttributes(IAstAttributesVisitor& visitor, TExprNode& read, TExprContext& ctx) {
    TraverseReadAttributes(visitor, *read.Child(0), ctx);

    TKiDataSource source(read.ChildPtr(1));
    TKikimrKey key{ctx};
    if (!key.Extract(*read.Child(2))) {
        return;
    }
    auto cluster = source.Cluster().StringValue();
    auto tablePath = key.GetTablePath();

    if (read.ChildrenSize() <= 4) {
        return;
    }
    auto& astAttrs = *read.Child(4);
    visitor.VisitRead(read, cluster, tablePath);
    for (const auto& child : astAttrs.Children()) {
        if (auto asAttribute = GetAsTextAttribute(*child)) {
            visitor.VisitAttribute(asAttribute->first, asAttribute->second);
        } else {
            visitor.VisitNonAttribute(child);
        }
    }
    visitor.ExitRead();
}

void TraverseReadAttributes(IAstAttributesVisitor& visitor, TExprNode& node, TExprContext& ctx) {
    if (node.IsCallable(ReadName)) {
        return ExtractReadAttributes(visitor, node, ctx);
    }
    if (node.IsCallable()) {
        if (node.ChildrenSize() == 0) {
            return;
        }
        return TraverseReadAttributes(visitor, *node.Child(0), ctx);
    }
}

THashMap<std::pair<TString, TString>, THashMap<TString, TString>> GatherReadAttributes(TExprNode& node, TExprContext& ctx) {
    TGatheringAttributesVisitor visitor;
    TraverseReadAttributes(visitor, node, ctx);
    return visitor.Result;
}

void ReplaceReadAttributes(TExprNode& node,
                           THashMap<TString, TString> attributesBeforeFilter,
                           TStringBuf cluster, TStringBuf tablePath,
                           TKikimrTableMetadataPtr metadata,
                           TExprContext& ctx) {
    TAttributesReplacingVisitor visitor{std::move(attributesBeforeFilter), cluster, tablePath, std::move(metadata), ctx};
    TraverseReadAttributes(visitor, node, ctx);
}

static Ydb::Type CreateYdbType(const NKikimr::NScheme::TTypeInfo& typeInfo, bool notNull) {
    Ydb::Type ydbType;
    if (typeInfo.GetTypeId() == NKikimr::NScheme::NTypeIds::Pg) {
        auto* typeDesc = typeInfo.GetTypeDesc();
        auto* pg = ydbType.mutable_pg_type();
        pg->set_type_name(NKikimr::NPg::PgTypeNameFromTypeDesc(typeDesc));
        pg->set_oid(NKikimr::NPg::PgTypeIdFromTypeDesc(typeDesc));
    } else {
        auto& item = notNull
            ? ydbType
            : *ydbType.mutable_optional_type()->mutable_item();
        //
        // DECIMAL is PrimitiveType with (22,9) defaults in Scheme
        // and separate (non-primitive) type everywhere else
        //
        // NKikimr::NScheme::NTypeIds::Decimal is omitted in public API intentionally
        //
        if (typeInfo.GetTypeId() == NKikimr::NScheme::NTypeIds::Decimal) {
            auto* decimal = item.mutable_decimal_type();
            decimal->set_precision(NKikimr::NScheme::DECIMAL_PRECISION);
            decimal->set_scale(NKikimr::NScheme::DECIMAL_SCALE);
        } else {
            item.set_type_id((Ydb::Type::PrimitiveTypeId)typeInfo.GetTypeId());
        }
    }
    return ydbType;
}

TExprNode::TPtr BuildSchemaFromMetadata(TPositionHandle pos, TExprContext& ctx, const TMap<TString, NYql::TKikimrColumnMetadata>& columns) {
    TVector<std::pair<TString, const NYql::TTypeAnnotationNode*>> typedColumns;
    typedColumns.reserve(columns.size());
    for (const auto& [n, c] : columns) {
        NYdb::TTypeParser parser(NYdb::TType(CreateYdbType(c.TypeInfo, c.NotNull)));
        auto type = NFq::MakeType(parser, ctx);
        typedColumns.emplace_back(n, type);
    }

    const TString ysonSchema = NYql::NCommon::WriteTypeToYson(NFq::MakeStructType(typedColumns, ctx), NYson::EYsonFormat::Text);
    TExprNode::TListType items;
    auto schema = ctx.NewAtom(pos, ysonSchema);
    auto type = ctx.NewCallable(pos, "SqlTypeFromYson"sv, { schema });
    auto order = ctx.NewCallable(pos, "SqlColumnOrderFromYson"sv, { schema });
    auto userSchema = ctx.NewAtom(pos, "userschema"sv);
    return ctx.NewList(pos, {userSchema, type, order});
}

} // namespace NYql
