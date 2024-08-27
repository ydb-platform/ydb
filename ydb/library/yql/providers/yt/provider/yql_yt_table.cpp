#include "yql_yt_table.h"
#include "yql_yt_key.h"
#include "yql_yt_helpers.h"
#include "yql_yt_op_settings.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/public/udf/tz/udf_tz.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/library/yql/public/decimal/yql_decimal_serialize.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/utils/utf8.h>

#include <library/cpp/yson/node/node_io.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/generic/overloaded.h>
#include <util/generic/strbuf.h>
#include <util/generic/hash_set.h>
#include <util/generic/xrange.h>
#include <util/generic/singleton.h>
#include <util/generic/utility.h>

#include <limits>

namespace NYql {

using namespace NNodes;
using namespace NKikimr;
using namespace NKikimr::NUdf;
using namespace std::string_view_literals;

class TExprDataToYtNodeConverter {
public:
    using TConverter = std::function<NYT::TNode(const TExprNode&)>;

    TExprDataToYtNodeConverter() {
        Converters.emplace(TCoUint8::CallableName(), [](const TExprNode& node) {
            return NYT::TNode((ui64)NYql::FromString<ui8>(*node.Child(0), EDataSlot::Uint8));
        });
        Converters.emplace(TCoInt8::CallableName(), [](const TExprNode& node) {
            return NYT::TNode((i64)NYql::FromString<i8>(*node.Child(0), EDataSlot::Int8));
        });
        Converters.emplace(TCoUint16::CallableName(), [](const TExprNode& node) {
            return NYT::TNode((ui64)NYql::FromString<ui16>(*node.Child(0), EDataSlot::Uint16));
        });
        Converters.emplace(TCoInt16::CallableName(), [](const TExprNode& node) {
            return NYT::TNode((i64)NYql::FromString<i16>(*node.Child(0), EDataSlot::Int16));
        });
        Converters.emplace(TCoInt32::CallableName(), [](const TExprNode& node) {
            return NYT::TNode((i64)NYql::FromString<i32>(*node.Child(0), EDataSlot::Int32));
        });
        Converters.emplace(TCoUint32::CallableName(), [](const TExprNode& node) {
            return NYT::TNode((ui64)NYql::FromString<ui32>(*node.Child(0), EDataSlot::Uint32));
        });
        Converters.emplace(TCoInt64::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(NYql::FromString<i64>(*node.Child(0), EDataSlot::Int64));
        });
        Converters.emplace(TCoUint64::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(NYql::FromString<ui64>(*node.Child(0), EDataSlot::Uint64));
        });
        Converters.emplace(TCoString::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(node.Child(0)->Content());
        });
        Converters.emplace(TCoUtf8::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(node.Child(0)->Content());
        });
        Converters.emplace(TCoJson::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(node.Child(0)->Content());
        });
        Converters.emplace(TCoYson::CallableName(), [](const TExprNode& node) {
            return NYT::NodeFromYsonString(TString{node.Child(0)->Content()});
        });
        Converters.emplace(TCoDecimal::CallableName(), [](const TExprNode& node) {
            char data[sizeof(NDecimal::TInt128)];
            const ui32 size = NDecimal::Serialize(
                NDecimal::FromString(node.Child(0)->Content(),
                    ::FromString<ui8>(node.Child(1)->Content()),
                    ::FromString<ui8>(node.Child(2)->Content())),
                data);
            return NYT::TNode(TStringBuf(data, size));
        });
        Converters.emplace(TCoBool::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(NYql::FromString<bool>(*node.Child(0), EDataSlot::Bool));
        });
        Converters.emplace(TCoFloat::CallableName(), [](const TExprNode& node) {
            return NYT::TNode((double)NYql::FromString<float>(*node.Child(0), EDataSlot::Float));
        });
        Converters.emplace(TCoDouble::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(NYql::FromString<double>(*node.Child(0), EDataSlot::Double));
        });
        Converters.emplace(TCoNull::CallableName(), [](const TExprNode& /*node*/) {
            return NYT::TNode::CreateEntity();
        });
        Converters.emplace(TCoNothing::CallableName(), [](const TExprNode& /*node*/) {
            return NYT::TNode::CreateEntity();
        });
        Converters.emplace(TCoDate::CallableName(), [](const TExprNode& node) {
            return NYT::TNode((ui64)NYql::FromString<ui16>(*node.Child(0), EDataSlot::Date));
        });
        Converters.emplace(TCoDatetime::CallableName(), [](const TExprNode& node) {
            return NYT::TNode((ui64)NYql::FromString<ui32>(*node.Child(0), EDataSlot::Datetime));
        });
        Converters.emplace(TCoTimestamp::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(NYql::FromString<ui64>(*node.Child(0), EDataSlot::Timestamp));
        });
        Converters.emplace(TCoInterval::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(NYql::FromString<i64>(*node.Child(0), EDataSlot::Interval));
        });
        Converters.emplace(TCoDate32::CallableName(), [](const TExprNode& node) {
            return NYT::TNode((i64)NYql::FromString<i32>(*node.Child(0), EDataSlot::Date32));
        });
        Converters.emplace(TCoDatetime64::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(NYql::FromString<i64>(*node.Child(0), EDataSlot::Datetime64));
        });
        Converters.emplace(TCoTimestamp64::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(NYql::FromString<i64>(*node.Child(0), EDataSlot::Timestamp64));
        });
        Converters.emplace(TCoInterval64::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(NYql::FromString<i64>(*node.Child(0), EDataSlot::Interval64));
        });
        Converters.emplace(TCoTzDate::CallableName(), [](const TExprNode& node) {
            TStringBuf tzName = node.Child(0)->Content();
            TStringBuf valueStr;
            GetNext(tzName, ',', valueStr);
            TStringStream out;
            NMiniKQL::SerializeTzDate(::FromString<ui16>(valueStr), NMiniKQL::GetTimezoneId(tzName), out);
            return NYT::TNode(out.Str());
        });
        Converters.emplace(TCoTzDatetime::CallableName(), [](const TExprNode& node) {
            TStringBuf tzName = node.Child(0)->Content();
            TStringBuf valueStr;
            GetNext(tzName, ',', valueStr);
            TStringStream out;
            NMiniKQL::SerializeTzDatetime(::FromString<ui32>(valueStr), NMiniKQL::GetTimezoneId(tzName), out);
            return NYT::TNode(out.Str());
        });
        Converters.emplace(TCoTzTimestamp::CallableName(), [](const TExprNode& node) {
            TStringBuf tzName = node.Child(0)->Content();
            TStringBuf valueStr;
            GetNext(tzName, ',', valueStr);
            TStringStream out;
            NMiniKQL::SerializeTzTimestamp(::FromString<ui64>(valueStr), NMiniKQL::GetTimezoneId(tzName), out);
            return NYT::TNode(out.Str());
        });
        Converters.emplace(TCoTzDate32::CallableName(), [](const TExprNode& node) {
            TStringBuf tzName = node.Child(0)->Content();
            TStringBuf valueStr;
            GetNext(tzName, ',', valueStr);
            TStringStream out;
            NMiniKQL::SerializeTzDate32(::FromString<i32>(valueStr), NMiniKQL::GetTimezoneId(tzName), out);
            return NYT::TNode(out.Str());
        });
        Converters.emplace(TCoTzDatetime64::CallableName(), [](const TExprNode& node) {
            TStringBuf tzName = node.Child(0)->Content();
            TStringBuf valueStr;
            GetNext(tzName, ',', valueStr);
            TStringStream out;
            NMiniKQL::SerializeTzDatetime64(::FromString<i64>(valueStr), NMiniKQL::GetTimezoneId(tzName), out);
            return NYT::TNode(out.Str());
        });
        Converters.emplace(TCoTzTimestamp64::CallableName(), [](const TExprNode& node) {
            TStringBuf tzName = node.Child(0)->Content();
            TStringBuf valueStr;
            GetNext(tzName, ',', valueStr);
            TStringStream out;
            NMiniKQL::SerializeTzTimestamp64(::FromString<i64>(valueStr), NMiniKQL::GetTimezoneId(tzName), out);
            return NYT::TNode(out.Str());
        });
        Converters.emplace(TCoUuid::CallableName(), [](const TExprNode& node) {
            return NYT::TNode(node.Child(0)->Content());
        });
    }

    NYT::TNode Convert(const TExprNode& node) const {
        if (auto p = Converters.FindPtr(node.Content())) {
            return (*p)(node);
        }
        return NYT::TNode();
    }
private:
    THashMap<TStringBuf, TConverter> Converters;
};

// Converts ExprNode representation to YT table format
NYT::TNode ExprNodeToYtNode(const TExprNode& node) {
    return Default<TExprDataToYtNodeConverter>().Convert(node.IsCallable("Just") ? node.Head() : node);
}

TExprNode::TPtr YtNodeToExprNode(const NYT::TNode& node, TExprContext& ctx, TPositionHandle pos) {
    switch (node.GetType()) {
    case NYT::TNode::String:
        return Build<TCoString>(ctx, pos)
            .Literal()
                .Value(node.AsString())
            .Build()
            .Done().Ptr();
    case NYT::TNode::Int64:
        return Build<TCoInt64>(ctx, pos)
            .Literal()
                .Value(ToString(node.AsInt64()))
            .Build()
            .Done().Ptr();
    case NYT::TNode::Uint64:
        return Build<TCoUint64>(ctx, pos)
            .Literal()
            .Value(ToString(node.AsUint64()))
            .Build()
            .Done().Ptr();
    case NYT::TNode::Double:
        return Build<TCoDouble>(ctx, pos)
            .Literal()
                .Value(ToString(node.AsDouble()))
            .Build()
            .Done().Ptr();
    case NYT::TNode::Bool:
        return Build<TCoBool>(ctx, pos)
            .Literal()
                .Value(ToString(node.AsBool()))
            .Build()
            .Done().Ptr();
    case NYT::TNode::Null:
        return Build<TCoNull>(ctx, pos)
            .Done().Ptr();
    case NYT::TNode::Undefined:
        ythrow yexception() << "Cannot convert UNDEFINED TNode to expr node";
    default:
        return ctx.Builder(pos).Callable("Yson").Atom(0, NYT::NodeToYsonString(node)).Seal().Build();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

bool TYtTableStatInfo::Validate(const TExprNode& node, TExprContext& ctx) {
    if (!EnsureCallable(node, ctx)) {
        return false;
    }
    if (!node.IsCallable(TYtStat::CallableName())) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected " << TYtStat::CallableName()
            << " callable, but got " << node.Content()));
        return false;
    }

    for (auto& child: node.Children()) {
        if (!EnsureTupleSize(*child, 2, ctx)) {
            return false;
        }
        const TExprNode* name = child->Child(0);
        const TExprNode* value = child->Child(1);
        if (!EnsureAtom(*name, ctx)) {
            return false;
        }

#define VALIDATE_FIELD(field)                                                   \
        if (name->Content() == TStringBuf(#field)) {                           \
            if (!EnsureAtom(*value, ctx)) {                                     \
                return false;                                                   \
            }                                                                   \
            decltype(TYtTableStatInfo::field) _##field;                         \
            if (!TryFromString(value->Content(), _##field)) {                   \
                ctx.AddError(TIssue(ctx.GetPosition(value->Pos()),              \
                    TStringBuilder() << "Bad value of '" #field "' attribute: " \
                        << value->Content()));                                  \
                return false;                                                   \
            }                                                                   \
        }

        if (name->Content() == "Id") {
            if (!EnsureAtom(*value, ctx)) {
                return false;
            }
            if (value->Content().empty()) {
                ctx.AddError(TIssue(ctx.GetPosition(value->Pos()),
                    TStringBuilder() << "Empty value of 'Id' attribute"));
                return false;
            }
        }
        else
            VALIDATE_FIELD(RecordsCount)
        else
            VALIDATE_FIELD(DataSize)
        else
            VALIDATE_FIELD(ChunkCount)
        else
            VALIDATE_FIELD(ModifyTime)
        else
            VALIDATE_FIELD(Revision)
        else if (name->Content() == "SecurityTags") {
            if (!value->IsList()) {
                ctx.AddError(TIssue(ctx.GetPosition(value->Pos()),
                    TStringBuilder() << "Expected list"));
                return false;
            }
            for (const auto& tagAtom : value->Children()) {
                if (!EnsureAtom(*tagAtom, ctx)) {
                    return false;
                }
            }
        } else {
            ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Unsupported table stat option: " << name->Content()));
            return false;
        }
#undef VALIDATE_FIELD

    }
    return true;
}

void TYtTableStatInfo::Parse(TExprBase node) {
    *this = {};
    FromNode = node.Maybe<TExprBase>();
    for (auto child: node.Cast<TYtStat>()) {
        auto setting = child.Cast<TCoNameValueTuple>();
#define HANDLE_FIELD(field)                                                 \
        if (setting.Name().Value() == TStringBuf(#field)) {                 \
            field = FromString(setting.Value().Cast<TCoAtom>().Value());    \
        }

        if (setting.Name().Value() == "Id") {
            Id = TString{setting.Value().Cast<TCoAtom>().Value()};
        }
        else
            HANDLE_FIELD(RecordsCount)
        else
            HANDLE_FIELD(DataSize)
        else
            HANDLE_FIELD(ChunkCount)
        else
            HANDLE_FIELD(ModifyTime)
        else
            HANDLE_FIELD(Revision)
        else if (setting.Name().Value() == "SecurityTags") {
            SecurityTags = {};
            for (const auto& tagAtom : setting.Value().Cast<TListBase<TCoAtom>>()) {
                SecurityTags.emplace_back(tagAtom.Value());
            }
        } else {
            YQL_ENSURE(false, "Unexpected option " << setting.Name().Value());
        }
#undef HANDLE_FIELD
    }
}

TExprBase TYtTableStatInfo::ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const {
    auto statBuilder = Build<TYtStat>(ctx, pos);

#define ADD_FIELD(field)                \
    .Add()                              \
        .Name()                         \
            .Value(TStringBuf(#field), TNodeFlags::Default) \
        .Build()                        \
        .Value<TCoAtom>()               \
            .Value(ToString(field))     \
        .Build()                        \
    .Build()

    statBuilder
        ADD_FIELD(Id)
        ADD_FIELD(RecordsCount)
        ADD_FIELD(DataSize)
        ADD_FIELD(ChunkCount)
        ADD_FIELD(ModifyTime)
        ADD_FIELD(Revision)
        ;

#undef ADD_FIELD

    if (!SecurityTags.empty()) {
        auto secTagsBuilder = Build<TCoAtomList>(ctx, pos);
        for (const auto& tag : SecurityTags) {
            secTagsBuilder
                .Add<TCoAtom>()
                    .Value(tag)
                .Build();
        }
        auto secTagsListExpr = secTagsBuilder.Done();


        statBuilder
            .Add()
                .Name()
                    .Value("SecurityTags")
                .Build()
                .Value(secTagsListExpr)
            .Build();
    }

    return statBuilder.Done();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

bool TYtTableMetaInfo::Validate(const TExprNode& node, TExprContext& ctx) {
    if (!EnsureCallable(node, ctx)) {
        return false;
    }
    if (!node.IsCallable(TYtMeta::CallableName())) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected " << TYtMeta::CallableName()
            << " callable, but got " << node.Content()));
        return false;
    }

    for (auto& child: node.Children()) {
        if (!EnsureTupleSize(*child, 2, ctx)) {
            return false;
        }
        const TExprNode* name = child->Child(0);
        TExprNode* value = child->Child(1);
        if (!EnsureAtom(*name, ctx)) {
            return false;
        }

#define VALIDATE_FIELD(field)                                                   \
        if (name->Content() == TStringBuf(#field)) {                            \
            if (!EnsureAtom(*value, ctx)) {                                     \
                return false;                                                   \
            }                                                                   \
            decltype(TYtTableMetaInfo::field) _##field;                         \
            if (!TryFromString(value->Content(), _##field)) {                   \
                ctx.AddError(TIssue(ctx.GetPosition(value->Pos()),              \
                    TStringBuilder() << "Bad value of '" #field "' attribute: " \
                        << value->Content()));                                  \
                return false;                                                   \
            }                                                                   \
        }

        if (name->Content() == TStringBuf("Attrs")) {
            if (!EnsureTuple(*value, ctx)) {
                return false;
            }
            for (auto& item: value->Children()) {
                if (!EnsureTupleSize(*item, 2, ctx)) {
                    return false;
                }
                if (!EnsureAtom(*item->Child(0), ctx) || !EnsureAtom(*item->Child(1), ctx)) {
                    return false;
                }
            }
        }
        else
            VALIDATE_FIELD(CanWrite)
        else
            VALIDATE_FIELD(DoesExist)
        else
            VALIDATE_FIELD(YqlCompatibleScheme)
        else
            VALIDATE_FIELD(InferredScheme)
        else
            VALIDATE_FIELD(IsDynamic)
        else
            VALIDATE_FIELD(SqlView)
        else
            VALIDATE_FIELD(SqlViewSyntaxVersion)
        else {
            ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Unsupported table meta option: " << name->Content()));
            return false;
        }
#undef VALIDATE_FIELD

    }
    return true;
}

void TYtTableMetaInfo::Parse(TExprBase node) {
    *this = {};
    FromNode = node.Maybe<TExprBase>();
    for (auto child: node.Cast<TYtMeta>()) {
        auto setting = child.Cast<TCoNameValueTuple>();
#define HANDLE_FIELD(field)                                               \
        if (setting.Name().Value() == TStringBuf(#field)) {                \
            field = FromString(setting.Value().Cast<TCoAtom>().Value());    \
        }

        if (setting.Name().Value() == TStringBuf("Attrs")) {
            for (auto item: setting.Value().Cast<TCoNameValueTupleList>()) {
                Attrs.insert({TString{item.Name().Value()}, TString{item.Value().Cast<TCoAtom>().Value()}});
            }
        }
        else if (setting.Name().Value() == TStringBuf("SqlView")) {
            SqlView = TString{setting.Value().Cast<TCoAtom>().Value()};
        }
        else
            HANDLE_FIELD(SqlViewSyntaxVersion)
        else
            HANDLE_FIELD(CanWrite)
        else
            HANDLE_FIELD(DoesExist)
        else
            HANDLE_FIELD(YqlCompatibleScheme)
        else
            HANDLE_FIELD(InferredScheme)
        else
            HANDLE_FIELD(IsDynamic)
        else {
            YQL_ENSURE(false, "Unexpected option " << setting.Name().Value());
        }
#undef HANDLE_FIELD

    }
}

TExprBase TYtTableMetaInfo::ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const {
    auto metaBuilder = Build<TYtMeta>(ctx, pos);

#define ADD_BOOL_FIELD(field)                                                       \
    .Add()                                                                          \
        .Name()                                                                     \
            .Value(TStringBuf(#field), TNodeFlags::Default)                         \
        .Build()                                                                    \
        .Value<TCoAtom>()                                                           \
            .Value(field ? TStringBuf("1") : TStringBuf("0"), TNodeFlags::Default)  \
        .Build()                                                                    \
    .Build()

    metaBuilder
        ADD_BOOL_FIELD(CanWrite)
        ADD_BOOL_FIELD(DoesExist)
        ADD_BOOL_FIELD(YqlCompatibleScheme)
        ADD_BOOL_FIELD(InferredScheme)
        ADD_BOOL_FIELD(IsDynamic)
        ;

#undef ADD_BOOL_FIELD

    if (SqlView) {
        metaBuilder
            .Add()
                .Name()
                    .Value(TStringBuf("SqlView"), TNodeFlags::Default)
                .Build()
                .Value<TCoAtom>()
                    .Value(SqlView)
                .Build()
            .Build();

        metaBuilder
            .Add()
                .Name()
                    .Value(TStringBuf("SqlViewSyntaxVersion"), TNodeFlags::Default)
                .Build()
                .Value<TCoAtom>()
                    .Value(ToString(SqlViewSyntaxVersion), TNodeFlags::Default)
                .Build()
            .Build();
    }

    if (!Attrs.empty()) {
        auto attrsBuilder = Build<TCoNameValueTupleList>(ctx, pos);
        for (auto& attr: Attrs) {
            attrsBuilder.Add()
                .Name()
                    .Value(attr.first)
                .Build()
                .Value<TCoAtom>()
                    .Value(attr.second)
                .Build()
            .Build();
        }
        metaBuilder
            .Add()
                .Name()
                    .Value(TStringBuf("Attrs"), TNodeFlags::Default)
                .Build()
                .Value(attrsBuilder.Done())
            .Build();
    }

    return metaBuilder.Done();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

bool TEpochInfo::Validate(const TExprNode& node, TExprContext& ctx) {
    if (!node.IsCallable(TEpoch::CallableName())) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected " << TEpoch::CallableName()));
        return false;
    }
    if (!EnsureArgsCount(node, 1, ctx)) {
        return false;
    }

    auto epochValue = node.Child(TEpoch::idx_Value);
    if (!EnsureAtom(*epochValue, ctx)) {
        return false;
    }

    ui32 epoch = 0;
    if (!TryFromString(epochValue->Content(), epoch)) {
        ctx.AddError(TIssue(ctx.GetPosition(epochValue->Pos()), TStringBuilder() << "Bad epoch value: " << epochValue->Content()));
        return false;
    }
    return true;
}

TMaybe<ui32> TEpochInfo::Parse(const TExprNode& node) {
    if (auto maybeEpoch = TMaybeNode<TEpoch>(&node)) {
        return FromString<ui32>(maybeEpoch.Cast().Value().Value());
    }
    return Nothing();
}

TExprBase TEpochInfo::ToExprNode(const TMaybe<ui32>& epoch, TExprContext& ctx, const TPositionHandle& pos) {
    return epoch
        ? Build<TEpoch>(ctx, pos).Value().Value(ToString(*epoch), TNodeFlags::Default).Build().Done().Cast<TExprBase>()
        : Build<TCoVoid>(ctx, pos).Done().Cast<TExprBase>();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

TYtTableBaseInfo::TYtTableBaseInfo(const TYtKey& key, TStringBuf cluster)
    : Name(key.GetPath())
    , Cluster(cluster)
{
}

NYT::TNode TYtTableBaseInfo::GetCodecSpecNode(const NCommon::TStructMemberMapper& mapper) const {
    NYT::TNode res = NYT::TNode::CreateMap();
    if (RowSpec) {
        RowSpec->FillCodecNode(res[YqlRowSpecAttribute], mapper);
    }
    if (Meta) {
        if (auto p = Meta->Attrs.FindPtr(FORMAT_ATTR_NAME)) {
            res[FORMAT_ATTR_NAME] = NYT::NodeFromYsonString(*p);
        }
        if (Meta->IsDynamic) {
            res[YqlDynamicAttribute] = true;
        }
    }
    return res;
}

NYT::TNode TYtTableBaseInfo::GetAttrSpecNode(ui64 nativeTypeCompatibility, bool rowSpecCompactForm) const {
    NYT::TNode res = NYT::TNode::CreateMap();
    if (RowSpec) {
        RowSpec->FillAttrNode(res[YqlRowSpecAttribute], nativeTypeCompatibility, rowSpecCompactForm);
    }
    return res;
}

TYtTableBaseInfo::TPtr TYtTableBaseInfo::Parse(TExprBase node) {
    if (node.Maybe<TYtOutTable>()) {
        return MakeIntrusive<TYtOutTableInfo>(node);
    } else if (auto out = node.Maybe<TYtOutput>()) {
        auto tableWithCluster = GetOutTableWithCluster(node);
        auto res = MakeIntrusive<TYtOutTableInfo>(tableWithCluster.first);
        res->Cluster = tableWithCluster.second;
        res->IsUnordered = IsUnorderedOutput(out.Cast());
        return res;
    } else if (node.Maybe<TYtTable>()) {
        return MakeIntrusive<TYtTableInfo>(node);
    } else {
        ythrow yexception() << "Not a table node " << (node.Raw() ? TString{node.Ref().Content()}.Quote() : TStringBuf("\"null\""));
    }
}

TYtTableMetaInfo::TPtr TYtTableBaseInfo::GetMeta(TExprBase node) {
    TMaybeNode<TExprBase> meta;
    if (auto outTable = node.Maybe<TYtOutTable>()) {
        meta = outTable.Cast().Meta();
    } else if (node.Maybe<TYtOutput>()) {
        meta = GetOutTable(node).Cast<TYtOutTable>().Meta();
    } else if (auto table = node.Maybe<TYtTable>()) {
        meta = table.Cast().Meta();
    } else {
        ythrow yexception() << "Not a table node " << (node.Raw() ? TString{node.Ref().Content()}.Quote() : TStringBuf("\"null\""));
    }

    if (meta.Maybe<TCoVoid>()) {
        return {};
    }
    return MakeIntrusive<TYtTableMetaInfo>(meta.Cast());
}

TYqlRowSpecInfo::TPtr TYtTableBaseInfo::GetRowSpec(TExprBase node) {
    TMaybeNode<TExprBase> rowSpec;
    if (auto outTable = node.Maybe<TYtOutTable>()) {
        rowSpec = outTable.Cast().RowSpec();
    } else if (auto out = node.Maybe<TYtOutput>()) {
        rowSpec = GetOutTable(node).Cast<TYtOutTable>().RowSpec();
    } else if (auto table = node.Maybe<TYtTable>()) {
        rowSpec = table.Cast().RowSpec();
    } else {
        ythrow yexception() << "Not a table node " << (node.Raw() ? TString{node.Ref().Content()}.Quote() : TStringBuf("\"null\""));
    }

    if (rowSpec.Maybe<TCoVoid>()) {
        return {};
    }
    return MakeIntrusive<TYqlRowSpecInfo>(rowSpec.Cast());
}

TYtTableStatInfo::TPtr TYtTableBaseInfo::GetStat(NNodes::TExprBase node) {
    TExprNode::TPtr statNode;
    if (node.Maybe<TYtOutTable>()) {
        statNode = node.Cast<TYtOutTable>().Stat().Ptr();
    } else if (node.Maybe<TYtTable>()) {
        statNode = node.Cast<TYtTable>().Stat().Ptr();
    } else if (node.Maybe<TYtOutput>()) {
        auto tableWithCluster = GetOutTableWithCluster(node);
        statNode = tableWithCluster.first.Cast<TYtOutTable>().Stat().Ptr();
    } else {
        ythrow yexception() << "Not a table node " << (node.Raw() ? TString{node.Ref().Content()}.Quote() : TStringBuf("\"null\""));
    }
    return MakeIntrusive<TYtTableStatInfo>(statNode);
}

TStringBuf TYtTableBaseInfo::GetTableName(NNodes::TExprBase node) {
    TMaybeNode<TYtTableBase> tableBase;
    if (auto outTable = node.Maybe<TYtOutTable>()) {
        tableBase = outTable.Cast();
    } else if (node.Maybe<TYtOutput>()) {
        tableBase = GetOutTable(node).Cast<TYtOutTable>();
    } else if (auto table = node.Maybe<TYtTable>()) {
        tableBase = table.Cast();
    } else {
        ythrow yexception() << "Not a table node " << (node.Raw() ? TString{node.Ref().Content()}.Quote() : TStringBuf("\"null\""));
    }

    return tableBase.Cast().Name().Value();
}

bool TYtTableBaseInfo::RequiresRemap() const {
    return Meta->InferredScheme
        // || !Meta->YqlCompatibleScheme -- Non compatuble schemas do not have RowSpec
        || Meta->Attrs.contains(QB2Premapper)
        || !RowSpec
        || !RowSpec->StrictSchema
        || !RowSpec->DefaultValues.empty();
}

bool TYtTableBaseInfo::HasSameScheme(const TTypeAnnotationNode& scheme) const {
    if (!RowSpec) {
        if (scheme.GetKind() != ETypeAnnotationKind::Struct) {
            return false;
        }
        const TVector<const TItemExprType*>& items = scheme.Cast<TStructExprType>()->GetItems();
        if (YAMR_FIELDS.size() != items.size()) {
            return false;
        }
        for (size_t i: xrange(YAMR_FIELDS.size())) {
            if (items[i]->GetName() != YAMR_FIELDS[i]
                || items[i]->GetItemType()->GetKind() != ETypeAnnotationKind::Data
                || items[i]->GetItemType()->Cast<TDataExprType>()->GetSlot() != EDataSlot::String)
            {
                return false;
            }
        }

        return true;
    } else {
        return IsSameAnnotation(*RowSpec->GetType(), scheme);
    }
}

bool TYtTableBaseInfo::HasSamePhysicalScheme(const TYtTableBaseInfo& info) const {
    if (bool(RowSpec) != bool(info.RowSpec)) {
        return false;
    }
    if (RowSpec) {
        if (!IsSameAnnotation(*RowSpec->GetType(), *info.RowSpec->GetType())) {
            return false;
        }
        return RowSpec->GetAuxColumns() == info.RowSpec->GetAuxColumns();
    }
    else {
        return true;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

TYtTableInfo::TYtTableInfo(const TYtKey& key, TStringBuf cluster)
    : TYtTableBaseInfo(key, cluster)
{
}

bool TYtTableInfo::Validate(const TExprNode& node, EYtSettingTypes accepted, TExprContext& ctx) {
    if (!node.IsCallable(TYtTable::CallableName())) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected " << TYtTable::CallableName()));
        return false;
    }

    if (!EnsureArgsCount(node, 8, ctx)) {
        return false;
    }

    if (!EnsureAtom(*node.Child(TYtTable::idx_Name), ctx)) {
        return false;
    }

    if (node.Child(TYtTable::idx_Name)->Content().empty()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Child(TYtTable::idx_Name)->Pos()), TStringBuilder() << "Expected non-empty table name"));
        return false;
    }

#define VALIDATE_OPT_FIELD(idx, TFunc)                                                   \
    if (!node.Child(idx)->IsCallable(TFunc::CallableName())                              \
        && !node.Child(idx)->IsCallable(TStringBuf("Void"))) {                          \
        ctx.AddError(TIssue(ctx.GetPosition(node.Child(idx)->Pos()), TStringBuilder()    \
            << "Expected " << TFunc::CallableName()                                      \
            << " or Void"));                                                             \
        return false;                                                                    \
    }

    VALIDATE_OPT_FIELD(TYtTable::idx_RowSpec, TYqlRowSpec)
    VALIDATE_OPT_FIELD(TYtTable::idx_Meta, TYtMeta)
    VALIDATE_OPT_FIELD(TYtTable::idx_Stat, TYtStat)
    VALIDATE_OPT_FIELD(TYtTable::idx_Epoch, TEpoch)
    VALIDATE_OPT_FIELD(TYtTable::idx_CommitEpoch, TEpoch)

#undef VALIDATE_OPT_FIELD

    if (!EnsureTuple(*node.Child(TYtTable::idx_Settings), ctx)) {
        return false;
    }

    if (!ValidateSettings(*node.Child(TYtTable::idx_Settings), accepted, ctx)) {
        return false;
    }

    if (!EnsureAtom(*node.Child(TYtTable::idx_Cluster), ctx)) {
        return false;
    }

    if (node.Child(TYtTable::idx_Cluster)->Content().empty()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Child(TYtTable::idx_Cluster)->Pos()), TStringBuilder() << "Expected non-empty cluster name"));
        return false;
    }

    return true;
}

void TYtTableInfo::Parse(TExprBase node, bool useTypes) {
    *this = {};
    FromNode = node.Maybe<TExprBase>();
    TYtTable table = node.Cast<TYtTable>();
    Name = table.Name().Value();
    if (table.RowSpec().Maybe<TYqlRowSpec>()) {
        RowSpec = MakeIntrusive<TYqlRowSpecInfo>(table.RowSpec(), useTypes);
    }
    if (table.Meta().Maybe<TYtMeta>()) {
        Meta = MakeIntrusive<TYtTableMetaInfo>(table.Meta());
    }
    if (table.Stat().Maybe<TYtStat>()) {
        Stat = MakeIntrusive<TYtTableStatInfo>(table.Stat());
    }
    Epoch = TEpochInfo::Parse(table.Epoch().Ref());
    CommitEpoch = TEpochInfo::Parse(table.CommitEpoch().Ref());
    Settings = table.Settings();
    Cluster = table.Cluster().Value();
    IsTemp = IsAnonymous = NYql::HasSetting(table.Settings().Ref(), EYtSettingType::Anonymous);
}

TExprBase TYtTableInfo::ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const {
    auto tableBuilder = Build<TYtTable>(ctx, pos);
    YQL_ENSURE(!Name.empty());
    tableBuilder.Name().Value(Name).Build();
    if (RowSpec) {
        tableBuilder.RowSpec(RowSpec->ToExprNode(ctx, pos));
    } else {
        tableBuilder.RowSpec<TCoVoid>().Build();
    }
    if (Meta) {
        tableBuilder.Meta(Meta->ToExprNode(ctx, pos));
    } else {
        tableBuilder.Meta<TCoVoid>().Build();
    }
    if (Stat) {
        tableBuilder.Stat(Stat->ToExprNode(ctx, pos));
    } else {
        tableBuilder.Stat<TCoVoid>().Build();
    }
    tableBuilder.Epoch(TEpochInfo::ToExprNode(Epoch, ctx, pos));
    tableBuilder.CommitEpoch(TEpochInfo::ToExprNode(CommitEpoch, ctx, pos));
    if (Settings) {
        tableBuilder.Settings(Settings.Cast<TCoNameValueTupleList>());
    } else {
        tableBuilder.Settings().Build();
    }
    tableBuilder.Cluster().Value(Cluster).Build();

    return tableBuilder.Done();
}

TStringBuf TYtTableInfo::GetTableLabel(NNodes::TExprBase node) {
    if (auto ann = NYql::GetSetting(node.Cast<TYtTable>().Settings().Ref(), EYtSettingType::Anonymous)) {
        if (ann->ChildrenSize() == 2) {
            return ann->Child(1)->Content();
        }
    }
    return node.Cast<TYtTable>().Name().Value();
}

bool TYtTableInfo::HasSubstAnonymousLabel(NNodes::TExprBase node) {
    if (auto ann = NYql::GetSetting(node.Cast<TYtTable>().Settings().Ref(), EYtSettingType::Anonymous)) {
        return ann->ChildrenSize() == 2;
    }
    return false;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

TYtOutTableInfo::TYtOutTableInfo(const TStructExprType* type, ui64 nativeYtTypeFlags) {
    RowSpec = MakeIntrusive<TYqlRowSpecInfo>();
    RowSpec->SetType(type, nativeYtTypeFlags);

    Meta = MakeIntrusive<TYtTableMetaInfo>();
    Meta->CanWrite = true;
    Meta->DoesExist = true;
    Meta->YqlCompatibleScheme = true;

    IsTemp = true;
}

bool TYtOutTableInfo::Validate(const TExprNode& node, TExprContext& ctx) {
    if (!node.IsCallable(TYtOutTable::CallableName())) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected " << TYtOutTable::CallableName()));
        return false;
    }
    if (!EnsureArgsCount(node, 5, ctx)) {
        return false;
    }

    if (!EnsureAtom(*node.Child(TYtOutTable::idx_Name), ctx)) {
        return false;
    }

#define VALIDATE_OPT_FIELD(idx, TFunc)                                                   \
    if (!node.Child(idx)->IsCallable(TFunc::CallableName())                              \
        && !node.Child(idx)->IsCallable(TStringBuf("Void"))) {                          \
        ctx.AddError(TIssue(ctx.GetPosition(node.Child(idx)->Pos()), TStringBuilder()    \
            << "Expected " << TFunc::CallableName()                                      \
            << " or Void"));                                                             \
        return false;                                                                    \
    }
#define VALIDATE_REQ_FIELD(idx, TFunc)                                                   \
    if (!node.Child(idx)->IsCallable(TFunc::CallableName())) {                           \
        ctx.AddError(TIssue(ctx.GetPosition(node.Child(idx)->Pos()), TStringBuilder()    \
            << "Expected " << TFunc::CallableName()));                                   \
        return false;                                                                    \
    }

    VALIDATE_REQ_FIELD(TYtOutTable::idx_RowSpec, TYqlRowSpec)
    VALIDATE_REQ_FIELD(TYtOutTable::idx_Meta, TYtMeta)
    VALIDATE_OPT_FIELD(TYtOutTable::idx_Stat, TYtStat)

#undef VALIDATE_OPT_FIELD
#undef VALIDATE_REQ_FIELD

    if (!EnsureTuple(*node.Child(TYtOutTable::idx_Settings), ctx)) {
        return false;
    }

    if (!ValidateSettings(*node.Child(TYtOutTable::idx_Settings), EYtSettingType::UniqueBy | EYtSettingType::OpHash | EYtSettingType::ColumnGroups, ctx)) {
        return false;
    }

    if (auto setting = NYql::GetSetting(*node.Child(TYtOutTable::idx_Settings), EYtSettingType::ColumnGroups)) {
        if (!ValidateColumnGroups(*setting, *node.Child(TYtOutTable::idx_RowSpec)->GetTypeAnn()->Cast<TStructExprType>(), ctx)) {
            return false;
        }
    }

    return true;
}

void TYtOutTableInfo::Parse(TExprBase node) {
    *this = {};
    FromNode = node.Maybe<TExprBase>();
    TYtOutTable table = node.Cast<TYtOutTable>();
    Name = table.Name().Value();
    RowSpec = MakeIntrusive<TYqlRowSpecInfo>(table.RowSpec());
    Meta = MakeIntrusive<TYtTableMetaInfo>(table.Meta());
    if (table.Stat().Maybe<TYtStat>()) {
        Stat = MakeIntrusive<TYtTableStatInfo>(table.Stat());
    }
    Settings = table.Settings();
}

TExprBase TYtOutTableInfo::ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const {
    auto tableBuilder = Build<TYtOutTable>(ctx, pos);
    tableBuilder.Name().Value(Name).Build();
    YQL_ENSURE(RowSpec);
    tableBuilder.RowSpec(RowSpec->ToExprNode(ctx, pos));
    YQL_ENSURE(Meta);
    tableBuilder.Meta(Meta->ToExprNode(ctx, pos));
    if (Stat) {
        tableBuilder.Stat(Stat->ToExprNode(ctx, pos));
    } else {
        tableBuilder.Stat<TCoVoid>().Build();
    }
    if (Settings) {
        tableBuilder.Settings(Settings.Cast<TCoNameValueTupleList>());
    } else {
        tableBuilder.Settings().Build();
    }
    return tableBuilder.Done();
}

TYtOutTableInfo& TYtOutTableInfo::SetUnique(const TDistinctConstraintNode* distinct, const TPositionHandle& pos, TExprContext& ctx) {
    if (distinct) {
        RowSpec->UniqueKeys = !RowSpec->SortMembers.empty() && RowSpec->SortMembers.size() == RowSpec->SortedBy.size()
            && distinct->IsOrderBy(*RowSpec->MakeSortConstraint(ctx));
        if (!Settings) {
            Settings = Build<TCoNameValueTupleList>(ctx, pos).Done();
        }
        if (const auto columns = NYql::GetSettingAsColumnList(Settings.Ref(), EYtSettingType::UniqueBy)) {
            YQL_ENSURE(distinct->ContainsCompleteSet(std::vector<std::string_view>(columns.cbegin(), columns.cend())));
        } else if (const auto simple = distinct->FilterFields(ctx, [](const TPartOfConstraintBase::TPathType& path) { return 1U == path.size(); })) {
            auto content = simple->GetContent();
            if (1U < content.size()) {
                std::unordered_set<std::string_view> sorted(RowSpec->SortMembers.cbegin(), RowSpec->SortMembers.cend());
                if (const auto filtered = simple->FilterFields(ctx, [&sorted](const TPartOfConstraintBase::TPathType& path) { return 1U == path.size() && sorted.contains(path.front()); }))
                    content = filtered->GetContent();
            }
            std::vector<std::string_view> uniques(content.front().size());
            std::transform(content.front().cbegin(), content.front().cend(), uniques.begin(), [&](const TPartOfConstraintBase::TSetType& set) { return set.front().front(); });
            Settings = TExprBase(NYql::AddSetting(Settings.Ref(), EYtSettingType::UniqueBy, ToAtomList(uniques, pos, ctx), ctx));
        }
    }
    return *this;
}

NYT::TNode TYtOutTableInfo::GetColumnGroups() const {
    if (Settings) {
        if (auto setting = NYql::GetSetting(Settings.Ref(), EYtSettingType::ColumnGroups)) {
            return NYT::NodeFromYsonString(setting->Tail().Content());
        }
    }
    return {};
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

bool TYtRangesInfo::Validate(const TExprNode& node, TExprContext& ctx, bool exists, const TYqlRowSpecInfo::TPtr& rowSpec) {
    auto validatKey = [&rowSpec, &ctx] (TExprNode& key) {
        if (!EnsureTupleMaxSize(key, (ui32)rowSpec->SortMembers.size(), ctx)) {
            return false;
        }
        for (size_t i: xrange(key.ChildrenSize())) {
            auto keyChild = key.Child(i);
            bool isOptional = false;
            const TDataExprType* columnType = nullptr;
            if (!EnsureDataOrOptionalOfData(keyChild->Pos(), rowSpec->SortedByTypes[i], isOptional, columnType, ctx)) {
                ctx.AddError(TIssue(ctx.GetPosition(keyChild->Pos()), TStringBuilder()
                    << "Table column " << rowSpec->SortedBy[i] << " is not data or optional of data"));
                return false;
            }
            if (IsNull(*keyChild)) {
                if (!isOptional) {
                    ctx.AddError(TIssue(ctx.GetPosition(keyChild->Pos()), TStringBuilder()
                        << "Column " << rowSpec->SortMembers[i] << " type "
                        << *rowSpec->SortedByTypes[i] << " cannot be compared with Null"));
                    return false;
                }
            } else if (keyChild->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
                if (!IsSameAnnotation(*keyChild->GetTypeAnn(), *rowSpec->SortedByTypes[i])) {
                    ctx.AddError(TIssue(ctx.GetPosition(keyChild->Pos()), TStringBuilder()
                        << "Column " << rowSpec->SortMembers[i] << " type "
                        << *rowSpec->SortedByTypes[i] << " should be equal to compare type: "
                        << *keyChild->GetTypeAnn()));
                    return false;
                }
            } else {
                auto keyType = keyChild->GetTypeAnn()->Cast<TDataExprType>();
                if (keyType->GetSlot() != columnType->GetSlot()
                    && !GetSuperType(keyType->GetSlot(), columnType->GetSlot()))
                {
                    ctx.AddError(TIssue(ctx.GetPosition(keyChild->Pos()), TStringBuilder()
                        << "Column " << rowSpec->SortMembers[i] << " type "
                        << *rowSpec->SortedByTypes[i] << " cannot be compared with "
                        << *keyChild->GetTypeAnn()));
                    return false;
                }
            }
        }
        return true;
    };

    for (auto& child: node.Children()) {
        if (!TYtRangeItemBase::Match(child.Get())) {
            ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder()
                << "Expected one of YtRangeItemBase, but got " << child->Content()));

        }
        if (rowSpec) {
            if (child->IsCallable(TYtKeyExact::CallableName())) {
                if (!rowSpec->IsSorted()) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder()
                        << TYtKeyExact::CallableName() << " cannot be used with unsorted table"));
                    return false;
                }

                if (!validatKey(*child->Child(TYtKeyExact::idx_Key))) {
                    return false;
                }
            }
            else if (child->IsCallable(TYtKeyRange::CallableName())) {
                if (!rowSpec->IsSorted()) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder()
                        << TYtKeyRange::CallableName() << " cannot be used with unsorted table"));
                    return false;
                }
                if (!validatKey(*child->Child(TYtKeyRange::idx_Lower))) {
                    return false;
                }
                if (!validatKey(*child->Child(TYtKeyRange::idx_Upper))) {
                    return false;
                }
            }
        } else if (exists) {
            ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder()
                << child->Content() << " cannot be used with YAMR table"));
            return false;
        }
    }
    return true;
}

void TYtRangesInfo::Parse(TExprBase node) {
    *this = {};

    for (auto item: node.Cast<TExprList>()) {
        if (item.Maybe<TYtRow>()) {
            Ranges.emplace_back(TRowSingle{FromString<ui64>(item.Cast<TYtRow>().Index().Literal().Value())});
        } else if (item.Maybe<TYtRowRange>()) {
            TYtRowRange rangeNode = item.Cast<TYtRowRange>();
            TRowRange range;
            if (!rangeNode.Lower().Maybe<TCoVoid>()) {
                range.Lower = FromString<ui64>(rangeNode.Lower().Cast<TCoUint64>().Literal().Value());
            }
            if (!rangeNode.Upper().Maybe<TCoVoid>()) {
                range.Upper = FromString<ui64>(rangeNode.Upper().Cast<TCoUint64>().Literal().Value());
            }
            Ranges.push_back(std::move(range));
        } else if (item.Maybe<TYtKeyExact>()) {
            TYtKeyExact keyNode = item.Cast<TYtKeyExact>();
            Ranges.emplace_back(TKeySingle{TVector<NNodes::TExprBase>{keyNode.Key().begin(), keyNode.Key().end()}});
        } else if (item.Maybe<TYtKeyRange>()) {
            TYtKeyRange rangeNode = item.Cast<TYtKeyRange>();
            TKeyRange range;
            range.Lower.assign(rangeNode.Lower().begin(), rangeNode.Lower().end());
            range.Upper.assign(rangeNode.Upper().begin(), rangeNode.Upper().end());
            // if useKeyBound setting is missed, useKeyBoundApi should be set to false,
            // irrespective of DEFAULT_USE_KEY_BOUND_API value
            range.UseKeyBoundApi = false;
            if (rangeNode.Flags()) {
                for (auto atom: rangeNode.Flags().Cast()) {
                    if (atom.Value() == TStringBuf("excludeLower")) {
                        range.LowerInclude = false;
                    } else if (atom.Value() == TStringBuf("includeUpper")) {
                        range.UpperInclude = true;
                    } else if (atom.Value() == TStringBuf("useKeyBound")) {
                        range.UseKeyBoundApi = true;
                    }
                }
            }
            Ranges.push_back(std::move(range));
        }
    }
}

void TYtRangesInfo::Parse(const TVector<NYT::TReadRange>& ranges, TExprContext& ctx, const TPositionHandle& pos) {
    *this = {};

    for (const NYT::TReadRange& item: ranges) {
        YQL_ENSURE(!NYT::IsTrivial(item.Exact_) || !NYT::IsTrivial(item.LowerLimit_) || !NYT::IsTrivial(item.UpperLimit_), "Full scan range is not supported");

        if (!NYT::IsTrivial(item.Exact_)) {
            YQL_ENSURE(NYT::IsTrivial(item.LowerLimit_) && NYT::IsTrivial(item.UpperLimit_));
            if (item.Exact_.RowIndex_) {
                YQL_ENSURE(!item.Exact_.Key_ && !item.Exact_.Offset_);
                Ranges.emplace_back(TRowSingle{(ui64)*item.Exact_.RowIndex_});
            } else if (item.Exact_.Key_) {
                YQL_ENSURE(!item.Exact_.Offset_);
                TVector<TExprBase> key;
                for (auto& keyPart: item.Exact_.Key_->Parts_) {
                    key.emplace_back(YtNodeToExprNode(keyPart, ctx, pos));
                }
                Ranges.emplace_back(TKeySingle{std::move(key)});
            }
        } else {
            YQL_ENSURE(!NYT::IsTrivial(item.LowerLimit_) || !NYT::IsTrivial(item.UpperLimit_));
            if (item.LowerLimit_.RowIndex_ || item.UpperLimit_.RowIndex_) {
                TRowRange range;
                if (item.LowerLimit_.RowIndex_) {
                    range.Lower.ConstructInPlace(*item.LowerLimit_.RowIndex_);
                }
                if (item.UpperLimit_.RowIndex_) {
                    range.Upper.ConstructInPlace(*item.UpperLimit_.RowIndex_);
                }
                Ranges.emplace_back(std::move(range));
            } else if (item.LowerLimit_.Key_ || item.UpperLimit_.Key_) {
                TKeyRange range;
                range.UseKeyBoundApi = false;
                if (item.LowerLimit_.Key_ && !item.LowerLimit_.Key_->Parts_.empty()) {
                    size_t count = item.LowerLimit_.Key_->Parts_.size();
                    if (item.LowerLimit_.Key_->Parts_.back().IsEntity() && !item.LowerLimit_.Key_->Parts_.back().HasAttributes()) {
                        range.LowerInclude = false;
                        --count;
                    }
                    for (size_t i = 0; i < count; ++i) {
                        range.Lower.emplace_back(YtNodeToExprNode(item.LowerLimit_.Key_->Parts_[i], ctx, pos));
                    }
                }
                if (item.UpperLimit_.Key_ && !item.UpperLimit_.Key_->Parts_.empty()) {
                    size_t count = item.UpperLimit_.Key_->Parts_.size();
                    if (item.UpperLimit_.Key_->Parts_.back().IsEntity()
                        && item.UpperLimit_.Key_->Parts_.back().HasAttributes()
                        && item.UpperLimit_.Key_->Parts_.back().GetAttributes().AsMap().Value("type", "") == "max") {
                        range.UpperInclude = true;
                        --count;
                    }
                    for (size_t i = 0; i < count; ++i) {
                        range.Upper.emplace_back(YtNodeToExprNode(item.UpperLimit_.Key_->Parts_[i], ctx, pos));
                    }
                }
                Ranges.emplace_back(std::move(range));
            } else {
                YQL_ENSURE(item.LowerLimit_.KeyBound_ || item.UpperLimit_.KeyBound_);
                TKeyRange range;
                range.UseKeyBoundApi = true;
                if (item.LowerLimit_.KeyBound_) {
                    auto relation = item.LowerLimit_.KeyBound_->Relation();
                    YQL_ENSURE(relation == NYT::ERelation::Greater || relation == NYT::ERelation::GreaterOrEqual);
                    range.LowerInclude = relation == NYT::ERelation::GreaterOrEqual;
                    YQL_ENSURE(!item.LowerLimit_.KeyBound_->Key().Parts_.empty());
                    for (auto& key : item.LowerLimit_.KeyBound_->Key().Parts_) {
                        range.Lower.emplace_back(YtNodeToExprNode(key, ctx, pos));
                    }
                }

                if (item.UpperLimit_.KeyBound_) {
                    auto relation = item.UpperLimit_.KeyBound_->Relation();
                    YQL_ENSURE(relation == NYT::ERelation::Less || relation == NYT::ERelation::LessOrEqual);
                    range.UpperInclude = relation == NYT::ERelation::LessOrEqual;
                    YQL_ENSURE(!item.UpperLimit_.KeyBound_->Key().Parts_.empty());
                    for (auto& key : item.UpperLimit_.KeyBound_->Key().Parts_) {
                        range.Upper.emplace_back(YtNodeToExprNode(key, ctx, pos));
                    }
                }
                Ranges.emplace_back(std::move(range));
            }
        }
    }
}

void TYtRangesInfo::AddRowRange(const TRowRange& range) {
    Ranges.push_back(range);
}

void TYtRangesInfo::SetUseKeyBoundApi(bool useKeyBoundApi) {
    for (auto& range: Ranges) {
        if (auto* data = std::get_if<TKeyRange>(&range)) {
            data->UseKeyBoundApi = useKeyBoundApi;
        }
    }
}

TExprBase TYtRangesInfo::ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const {
    auto rangesBuilder = Build<TExprList>(ctx, pos);
    for (auto& range: Ranges) {
        std::visit(TOverloaded{
            [&](const TRowSingle& row) {
                rangesBuilder.Add<TYtRow>()
                    .Index()
                        .Literal()
                            .Value(ToString(row.Offset), TNodeFlags::Default)
                        .Build()
                    .Build()
                .Build();
            },
            [&](const TRowRange& data) {
                auto builder = rangesBuilder.Add<TYtRowRange>();
                if (data.Lower) {
                    builder.Lower<TCoUint64>()
                        .Literal()
                            .Value(ToString(*data.Lower), TNodeFlags::Default)
                        .Build()
                    .Build();
                } else {
                    builder.Lower<TCoVoid>().Build();
                }
                if (data.Upper) {
                    builder.Upper<TCoUint64>()
                        .Literal()
                            .Value(ToString(*data.Upper), TNodeFlags::Default)
                        .Build()
                    .Build();
                } else {
                    builder.Upper<TCoVoid>().Build();
                }
                builder.Build();
            },
            [&](const TKeySingle& key) {
                rangesBuilder.Add<TYtKeyExact>()
                    .Key()
                        .Add(key.Key)
                    .Build()
                .Build();
            },
            [&](const TKeyRange& data) {
                auto builder = rangesBuilder.Add<TYtKeyRange>();
                builder.Lower().Add(data.Lower).Build();
                builder.Upper().Add(data.Upper).Build();
                if (!data.LowerInclude || data.UpperInclude) {
                    auto flagsBuilder = builder.Flags();
                    if (!data.LowerInclude) {
                        flagsBuilder.Add().Value("excludeLower", TNodeFlags::Default).Build();
                    }
                    if (data.UpperInclude) {
                        flagsBuilder.Add().Value("includeUpper", TNodeFlags::Default).Build();
                    }
                    if (data.UseKeyBoundApi) {
                        flagsBuilder.Add().Value("useKeyBound", TNodeFlags::Default).Build();
                    }
                    flagsBuilder.Build();
                }
                builder.Build();
            }
        }, range);
    }
    return rangesBuilder.Done();
}

void TYtRangesInfo::FillRichYPath(NYT::TRichYPath& path, size_t keyColumnsCount) const {
    path.MutableRanges().ConstructInPlace();
    for (auto& range: Ranges) {
        std::visit(TOverloaded{
            [&](const TRowSingle& row) {
                path.AddRange(NYT::TReadRange().Exact(NYT::TReadLimit().RowIndex(row.Offset)));
            },
            [&](const TRowRange& data) {
                NYT::TReadRange ytRange;
                if (data.Lower) {
                    ytRange.LowerLimit(NYT::TReadLimit().RowIndex(*data.Lower));
                }
                if (data.Upper) {
                    ytRange.UpperLimit(NYT::TReadLimit().RowIndex(*data.Upper));
                }
                path.AddRange(ytRange);
            },
            [&](const TKeySingle& data) {
                NYT::TKey key;
                for (auto& node: data.Key) {
                    auto keyPart = ExprNodeToYtNode(node.Ref());
                    YQL_ENSURE(!keyPart.IsUndefined(), "Unsupported range node: " << node.Ref().Content());
                    key.Add(std::move(keyPart));
                }
                path.AddRange(NYT::TReadRange().Exact(NYT::TReadLimit().Key(key)));
            },
            [&](const TKeyRange& data) {
                YQL_ENSURE(keyColumnsCount > 0);
                YQL_ENSURE(data.Lower.size() <= keyColumnsCount);
                YQL_ENSURE(data.Upper.size() <= keyColumnsCount);
                NYT::TReadRange ytRange;
                if (!data.Lower.empty()) {
                    NYT::TKey key;
                    for (auto& node: data.Lower) {
                        auto keyPart = ExprNodeToYtNode(node.Ref());
                        YQL_ENSURE(!keyPart.IsUndefined(), "Unsupported range node: " << node.Ref().Content());
                        key.Add(std::move(keyPart));
                    }
                    if (data.UseKeyBoundApi) {
                        NYT::TKeyBound lower(data.LowerInclude ? NYT::ERelation::GreaterOrEqual : NYT::ERelation::Greater, key);
                        ytRange.LowerLimit(NYT::TReadLimit().KeyBound(lower));
                    } else {
                        if (!data.LowerInclude) {
                            size_t toAddMaxs = keyColumnsCount - data.Lower.size();
                            for (size_t i = 0; i < toAddMaxs; ++i) {
                                auto entity = NYT::TNode::CreateEntity();
                                entity.Attributes().AsMap()[TStringBuf("type")] = TStringBuf("max");
                                key.Add(entity);
                            }
                            if (toAddMaxs == 0) {
                                key.Add(NYT::TNode::CreateEntity());
                            }
                        }
                        ytRange.LowerLimit(NYT::TReadLimit().Key(key));
                    }
                }
                if (!data.Upper.empty()) {
                    NYT::TKey key;
                    for (auto& node: data.Upper) {
                        auto keyPart = ExprNodeToYtNode(node.Ref());
                        YQL_ENSURE(!keyPart.IsUndefined(), "Unsupported range node: " << node.Ref().Content());
                        key.Add(std::move(keyPart));
                    }
                    if (data.UseKeyBoundApi) {
                        NYT::TKeyBound upper(data.UpperInclude ? NYT::ERelation::LessOrEqual : NYT::ERelation::Less, key);
                        ytRange.UpperLimit(NYT::TReadLimit().KeyBound(upper));
                    } else {
                        if (data.UpperInclude) {
                            auto entity = NYT::TNode::CreateEntity();
                            entity.Attributes().AsMap()[TStringBuf("type")] = TStringBuf("max");
                            key.Add(entity);
                        }
                        ytRange.UpperLimit(NYT::TReadLimit().Key(key));
                    }
                }
                path.AddRange(ytRange);
            }
        }, range);
    }
}

TMaybe<ui64> TYtRangesInfo::GetUsedRows(ui64 tableRowCount) const {
    ui64 rows = 0;
    if (tableRowCount) {
        for (auto& range: Ranges) {
            if (std::holds_alternative<TRowSingle>(range)) {
                ++rows;
            } else if (const auto* data = std::get_if<TRowRange>(&range)) {
                ui64 range = tableRowCount;
                if (data->Upper) {
                    range = *data->Upper;
                }
                if (data->Lower) {
                    range -= Min(range, *data->Lower);
                }
                rows += range;
            } else {
                return Nothing();
            }
        }
    }
    return rows;
}

size_t TYtRangesInfo::GetRangesCount() const {
    return Ranges.size();
}

size_t TYtRangesInfo::GetUsedKeyPrefixLength() const {
    size_t used = 0;
    for (auto& range: Ranges) {
        if (const auto* key = std::get_if<TKeySingle>(&range)) {
            used = std::max(used, key->Key.size());
        } else if (const auto* keyRange = std::get_if<TKeyRange>(&range)) {
            used = std::max(used, keyRange->Lower.size());
            used = std::max(used, keyRange->Upper.size());
        }
    }
    return used;
}

TVector<TYtRangesInfo::TRange> TYtRangesInfo::GetRanges() const {
    return Ranges;
}

bool TYtRangesInfo::IsEmpty() const {
    return Ranges.empty() || AllOf(Ranges, [](const TRange& r) {
        if (const TRowRange* data = ::std::get_if<TRowRange>(&r)) {
            return (data->Lower && data->Upper && *data->Lower >= *data->Upper) ||
                   (data->Upper && *data->Upper == 0ul);
        }
        return false;
    });
}

namespace {

template <typename TLayerType>
NUdf::TUnboxedValuePod GetTzValue(TStringBuf atom) {
    TStringBuf valueStr;
    GetNext(atom, ',', valueStr);
    return NUdf::TUnboxedValuePod(::FromString<TLayerType>(valueStr));
}

NUdf::TUnboxedValuePod GetDecimalValue(TCoDecimal decimal) {
    return NUdf::TUnboxedValuePod(NDecimal::FromString(decimal.Literal().Value(),
        ::FromString<ui8>(decimal.Precision().Value()),
        ::FromString<ui8>(decimal.Scale().Value())));
}

NKikimr::NUdf::EDataSlot GetCompareType(TStringBuf type) {
    using namespace NKikimr::NUdf;
    auto cmpSlot = GetDataSlot(type);
    switch (cmpSlot) {
    case EDataSlot::Int8:
    case EDataSlot::Uint8:
    case EDataSlot::Int16:
    case EDataSlot::Uint16:
    case EDataSlot::Int32:
    case EDataSlot::Uint32:
    case EDataSlot::Int64:
        cmpSlot = EDataSlot::Int64;
        break;
    case EDataSlot::Uint64:
        cmpSlot = EDataSlot::Uint64;
        break;
    default:
        break;
    }
    return cmpSlot;
}

int CompareDataNodes(NNodes::TExprBase left, NNodes::TExprBase right)
{
    using namespace NNodes;
    using namespace NKikimr::NUdf;

    if (left.Maybe<TCoNull>()) {
        return right.Maybe<TCoNull>().IsValid() ? 0 : -1;
    } else if (right.Maybe<TCoNull>()) {
        return 1;
    }

    YQL_ENSURE(left.Maybe<TCoDataCtor>().IsValid());
    YQL_ENSURE(right.Maybe<TCoDataCtor>().IsValid());

    auto leftDataCtor = left.Cast<TCoDataCtor>();
    auto rightDataCtor = right.Cast<TCoDataCtor>();

    auto leftType = GetCompareType(left.Ref().Content());
    auto rightType = GetCompareType(right.Ref().Content());
    if (leftType == rightType) {
        switch (leftType) {
        case EDataSlot::Bool:
            return NUdf::CompareValues(leftType,
                TUnboxedValuePod(NYql::FromString<bool>(leftDataCtor.Literal().Ref(), leftType)),
                TUnboxedValuePod(NYql::FromString<bool>(rightDataCtor.Literal().Ref(), leftType)));
        case EDataSlot::Int64:
        case EDataSlot::Interval:
        case EDataSlot::Date:
        case EDataSlot::Datetime:
        case EDataSlot::Date32:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
        case EDataSlot::Interval64:
            return NUdf::CompareValues(leftType,
                TUnboxedValuePod(NYql::FromString<i64>(leftDataCtor.Literal().Ref(), leftType)),
                TUnboxedValuePod(NYql::FromString<i64>(rightDataCtor.Literal().Ref(), leftType)));
        case EDataSlot::Uint64:
        case EDataSlot::Timestamp:
            return NUdf::CompareValues(leftType,
                TUnboxedValuePod(NYql::FromString<ui64>(leftDataCtor.Literal().Ref(), leftType)),
                TUnboxedValuePod(NYql::FromString<ui64>(rightDataCtor.Literal().Ref(), leftType)));
        case EDataSlot::Float:
            return NUdf::CompareValues(leftType,
                TUnboxedValuePod(NYql::FromString<float>(leftDataCtor.Literal().Ref(), leftType)),
                TUnboxedValuePod(NYql::FromString<float>(rightDataCtor.Literal().Ref(), leftType)));
        case EDataSlot::Double:
            return NUdf::CompareValues(leftType,
                TUnboxedValuePod(NYql::FromString<double>(leftDataCtor.Literal().Ref(), leftType)),
                TUnboxedValuePod(NYql::FromString<double>(rightDataCtor.Literal().Ref(), leftType)));
        case EDataSlot::String:
        case EDataSlot::Utf8:
        case EDataSlot::Uuid:
            return leftDataCtor.Literal().Value().compare(rightDataCtor.Literal().Value());
        case EDataSlot::TzDate:
            return NUdf::CompareValues(leftType,
                GetTzValue<ui16>(leftDataCtor.Literal().Value()),
                GetTzValue<ui16>(rightDataCtor.Literal().Value()));
        case EDataSlot::TzDatetime:
            return NUdf::CompareValues(leftType,
                GetTzValue<ui32>(leftDataCtor.Literal().Value()),
                GetTzValue<ui32>(rightDataCtor.Literal().Value()));
        case EDataSlot::TzTimestamp:
            return NUdf::CompareValues(leftType,
                GetTzValue<ui64>(leftDataCtor.Literal().Value()),
                GetTzValue<ui64>(rightDataCtor.Literal().Value()));
        case EDataSlot::Decimal:
            return NUdf::CompareValues(leftType,
                GetDecimalValue(left.Cast<TCoDecimal>()),
                GetDecimalValue(right.Cast<TCoDecimal>()));
        default:
            break;
        }
    }

    YQL_LOG_CTX_THROW yexception() << "Cannot compare " << left.Ref().Content() << " and " << right.Ref().Content();
}

bool AdjacentDataNodes(NNodes::TExprBase left, NNodes::TExprBase right)
{
    using namespace NNodes;
    using namespace NKikimr::NUdf;

    YQL_ENSURE(left.Maybe<TCoDataCtor>().IsValid());
    YQL_ENSURE(right.Maybe<TCoDataCtor>().IsValid());

    auto leftLiteral = left.Cast<TCoDataCtor>().Literal();
    auto rightLiteral = right.Cast<TCoDataCtor>().Literal();

    auto leftType = GetCompareType(left.Ref().Content());
    auto rightType = GetCompareType(right.Ref().Content());
    if (leftType == rightType) {
        switch (leftType) {
        case EDataSlot::Bool:
            return !FromString<bool>(leftLiteral.Ref(), leftType) && FromString<bool>(rightLiteral.Ref(), rightType);
        case EDataSlot::Int64:
            return FromString<i64>(leftLiteral.Ref(), leftType) + 1 == FromString<i64>(rightLiteral.Ref(), rightType);
        case EDataSlot::Uint64:
            return FromString<ui64>(leftLiteral.Ref(), leftType) + 1 == FromString<ui64>(rightLiteral.Ref(), rightType);
        default:
            return false;
        }
    }

    YQL_LOG_CTX_THROW yexception() << "Cannot compare " << left.Ref().Content() << " and " << right.Ref().Content();
}

template <bool UpperBound>
void ScaleDate(ui64& val, bool& includeBound, EDataSlot srcDataSlot, EDataSlot targetDataSlot) {
    switch (srcDataSlot) {
    case EDataSlot::Date:
        switch (targetDataSlot) {
        case EDataSlot::Datetime:
            val *= 86400ull;
            break;
        case EDataSlot::Timestamp:
            val *= 86400000000ull;
            break;
        default:
            break;
        }
        break;
    case EDataSlot::Datetime:
        switch (targetDataSlot) {
        case EDataSlot::Date:
            if (val % 86400ull) {
                includeBound = UpperBound;
            }
            val /= 86400ull;
            break;
        case EDataSlot::Timestamp:
            val *= 1000000ull;
            break;
        default:
            break;
        }
        break;
    case EDataSlot::Timestamp:
        switch (targetDataSlot) {
        case EDataSlot::Date:
            if (val % 86400000000ull) {
                includeBound = UpperBound;
            }
            val /= 86400000000ull;
            break;
        case EDataSlot::Datetime:
            if (val % 1000000ULL) {
                includeBound = UpperBound;
            }
            val /= 1000000ULL;
            break;
        default:
            break;
        }
        break;
    default:
        break;
    }
}

bool AdjustLowerValue(TString& lowerValue, bool& lowerInclude, EDataSlot lowerDataSlot, EDataSlot targetDataSlot) {
    if (EDataSlot::Interval == targetDataSlot
        || (GetDataTypeInfo(targetDataSlot).Features & NUdf::SignedIntegralType))
    {
        // Target is signed integer
        if (GetDataTypeInfo(lowerDataSlot).Features & NUdf::FloatType) {
            double dVal = FromString<double>(lowerValue);
            i64 val = 0;
            if (dVal <= static_cast<double>(std::numeric_limits<i64>::min())) {
                if (dVal < static_cast<double>(std::numeric_limits<i64>::min())) {
                    lowerInclude = true;
                }
                val = std::numeric_limits<i64>::min();
            }
            else if (dVal == static_cast<double>(std::numeric_limits<i64>::max())) {
                val = std::numeric_limits<i64>::max();
            }
            else if (dVal > static_cast<double>(std::numeric_limits<i64>::max())) {
                return false;
            }
            else if (std::ceil(dVal) != dVal) {
                lowerInclude = true;
                val = static_cast<i64>(std::ceil(dVal));
            }
            else {
                val = static_cast<i64>(dVal);
            }

            TMaybe<i64> valMin;
            TMaybe<i64> valMax;
            switch (targetDataSlot) {
            case EDataSlot::Int8:
                valMin = static_cast<i64>(std::numeric_limits<i8>::min());
                valMax = static_cast<i64>(std::numeric_limits<i8>::max());
                break;
            case EDataSlot::Int16:
                valMin = static_cast<i64>(std::numeric_limits<i16>::min());
                valMax = static_cast<i64>(std::numeric_limits<i16>::max());
                break;
            case EDataSlot::Int32:
                valMin = static_cast<i64>(std::numeric_limits<i32>::min());
                valMax = static_cast<i64>(std::numeric_limits<i32>::max());
                break;
            case EDataSlot::Int64:
            case EDataSlot::Interval:
                valMin = static_cast<i64>(std::numeric_limits<i64>::min());
                valMax = static_cast<i64>(std::numeric_limits<i64>::max());
                break;
            default:
                break;
            }

            if (valMax && (val > *valMax || (!lowerInclude && val == *valMax))) {
                return false;
            }

            if (valMin && val < *valMin) {
                lowerInclude = true;
                val = *valMin;
            }

            if (!lowerInclude) {
                lowerInclude = true;
                ++val;
            }

            lowerValue = ToString(val);
        }
        else if (GetDataTypeInfo(lowerDataSlot).Features & NUdf::UnsignedIntegralType) {
            ui64 val = FromString<ui64>(lowerValue);
            TMaybe<ui64> valMax;
            switch (targetDataSlot) {
            case EDataSlot::Int8:
                valMax = static_cast<ui64>(std::numeric_limits<i8>::max());
                break;
            case EDataSlot::Int16:
                valMax = static_cast<ui64>(std::numeric_limits<i16>::max());
                break;
            case EDataSlot::Int32:
                valMax = static_cast<ui64>(std::numeric_limits<i32>::max());
                break;
            case EDataSlot::Int64:
            case EDataSlot::Interval:
                valMax = static_cast<ui64>(std::numeric_limits<i64>::max());
                break;
            default:
                break;
            }

            if (valMax && (val > *valMax || (!lowerInclude && val == *valMax))) {
                return false;
            }

            if (!lowerInclude) {
                lowerInclude = true;
                ++val;
                lowerValue = ToString(val);
            }
        }
        else if (GetDataTypeInfo(lowerDataSlot).Features & NUdf::SignedIntegralType) {
            i64 val = FromString<i64>(lowerValue);

            TMaybe<i64> valMin;
            TMaybe<i64> valMax;
            switch (targetDataSlot) {
            case EDataSlot::Int8:
                valMin = static_cast<i64>(std::numeric_limits<i8>::min());
                valMax = static_cast<i64>(std::numeric_limits<i8>::max());
                break;
            case EDataSlot::Int16:
                valMin = static_cast<i64>(std::numeric_limits<i16>::min());
                valMax = static_cast<i64>(std::numeric_limits<i16>::max());
                break;
            case EDataSlot::Int32:
                valMin = static_cast<i64>(std::numeric_limits<i32>::min());
                valMax = static_cast<i64>(std::numeric_limits<i32>::max());
                break;
            case EDataSlot::Int64:
            case EDataSlot::Interval:
                valMin = static_cast<i64>(std::numeric_limits<i64>::min());
                valMax = static_cast<i64>(std::numeric_limits<i64>::max());
                break;
            default:
                break;
            }

            if (valMax && (val > *valMax || (!lowerInclude && val == *valMax))) {
                return false;
            }

            if (valMin && val < *valMin) {
                lowerInclude = true;
                val = *valMin;
            }

            if (!lowerInclude) {
                lowerInclude = true;
                ++val;
            }

            lowerValue = ToString(val);
        }
    }
    else if (GetDataTypeInfo(targetDataSlot).Features & (NUdf::UnsignedIntegralType | NUdf::DateType)) {
        // Target is unsigned integer
        ui64 val = 0;
        if (GetDataTypeInfo(lowerDataSlot).Features & NUdf::SignedIntegralType) {
            if (FromString<i64>(lowerValue) < 0) {
                lowerInclude = true;
                val = 0;
            }
            else {
                val = FromString<ui64>(lowerValue);
            }
        }
        else if (GetDataTypeInfo(lowerDataSlot).Features & NUdf::FloatType) {
            double dVal = FromString<double>(lowerValue);

            if (dVal > static_cast<double>(std::numeric_limits<ui64>::max())) {
                return false;
            }
            else if (dVal == static_cast<double>(std::numeric_limits<ui64>::max())) {
                val = std::numeric_limits<ui64>::max();
            }
            else if (dVal < 0. || std::ceil(dVal) != dVal) {
                lowerInclude = true;
                val = static_cast<ui64>(std::ceil(Max<double>(0., dVal)));
            }
            else {
                val = static_cast<ui64>(dVal);
            }
        }
        else if (GetDataTypeInfo(lowerDataSlot).Features & NUdf::UnsignedIntegralType) {
            val = FromString<ui64>(lowerValue);
        }
        else if ((GetDataTypeInfo(lowerDataSlot).Features & NUdf::DateType) && (GetDataTypeInfo(targetDataSlot).Features & NUdf::DateType)
            && lowerDataSlot != targetDataSlot) {

            val = FromString<ui64>(lowerValue);
            ScaleDate<false>(val, lowerInclude, lowerDataSlot, targetDataSlot);
        }
        else {
            // Nothing to change for other types
            return true;
        }
        TMaybe<ui64> valMax;
        switch (targetDataSlot) {
        case EDataSlot::Uint8:
            valMax = static_cast<ui64>(std::numeric_limits<ui8>::max());
            break;
        case EDataSlot::Uint16:
            valMax = static_cast<ui64>(std::numeric_limits<ui16>::max());
            break;
        case EDataSlot::Uint32:
            valMax = static_cast<ui64>(std::numeric_limits<ui32>::max());
            break;
        case EDataSlot::Uint64:
            valMax = static_cast<ui64>(std::numeric_limits<ui64>::max());
            break;
        case EDataSlot::Date:
            valMax = static_cast<ui64>(NUdf::MAX_DATE);
            break;
        case EDataSlot::Datetime:
            valMax = static_cast<ui64>(NUdf::MAX_DATETIME);
            break;
        case EDataSlot::Timestamp:
            valMax = static_cast<ui64>(NUdf::MAX_TIMESTAMP);
            break;
        default:
            break;
        }

        if (valMax && (val > *valMax || (!lowerInclude && val == *valMax))) {
            return false;
        }

        if (!lowerInclude) {
            lowerInclude = true;
            ++val;
        }

        lowerValue = ToString(val);
    }
    return true;
}

bool AdjustUpperValue(TString& upperValue, bool& upperInclude, EDataSlot upperDataSlot, EDataSlot targetDataSlot) {
    if (EDataSlot::Interval == targetDataSlot
        || (GetDataTypeInfo(targetDataSlot).Features & NUdf::SignedIntegralType))
    {
        // Target is signed integer
        if (GetDataTypeInfo(upperDataSlot).Features & NUdf::FloatType) {
            double dVal = FromString<double>(upperValue);
            i64 val = 0;
            if (dVal >= static_cast<double>(std::numeric_limits<i64>::max())) {
                if (dVal > static_cast<double>(std::numeric_limits<i64>::max())) {
                    upperInclude = true;
                }
                val = std::numeric_limits<i64>::max();
            }
            else if (dVal < static_cast<double>(std::numeric_limits<i64>::min())) {
                return false;
            }
            else if (dVal == static_cast<double>(std::numeric_limits<i64>::min())) {
                val = std::numeric_limits<i64>::min();
            }
            else if (std::floor(dVal) != dVal) {
                upperInclude = true;
                val = static_cast<ui64>(std::floor(dVal));
            }
            else {
                val = static_cast<i64>(dVal);
            }

            TMaybe<i64> valMin;
            TMaybe<i64> valMax;
            switch (targetDataSlot) {
            case EDataSlot::Int8:
                valMin = static_cast<i64>(std::numeric_limits<i8>::min());
                valMax = static_cast<i64>(std::numeric_limits<i8>::max());
                break;
            case EDataSlot::Int16:
                valMin = static_cast<i64>(std::numeric_limits<i16>::min());
                valMax = static_cast<i64>(std::numeric_limits<i16>::max());
                break;
            case EDataSlot::Int32:
                valMin = static_cast<i64>(std::numeric_limits<i32>::min());
                valMax = static_cast<i64>(std::numeric_limits<i32>::max());
                break;
            case EDataSlot::Int64:
            case EDataSlot::Interval:
                valMin = static_cast<i64>(std::numeric_limits<i64>::min());
                valMax = static_cast<i64>(std::numeric_limits<i64>::max());
                break;
            default:
                break;
            }

            if (valMin && (val < *valMin || (!upperInclude && val == *valMin))) {
                return false;
            }

            if (valMax && val > *valMax) {
                upperInclude = true;
                val = *valMax;
            }
            else if (upperInclude && (!valMax || val < *valMax)) {
                upperInclude = false;
                ++val;
            }

            upperValue = ToString(val);
        }
        else if (GetDataTypeInfo(upperDataSlot).Features & NUdf::UnsignedIntegralType) {
            ui64 val = FromString<ui64>(upperValue);
            TMaybe<ui64> valMax;
            switch (targetDataSlot) {
            case EDataSlot::Int8:
                valMax = static_cast<ui64>(std::numeric_limits<i8>::max());
                break;
            case EDataSlot::Int16:
                valMax = static_cast<ui64>(std::numeric_limits<i16>::max());
                break;
            case EDataSlot::Int32:
                valMax = static_cast<ui64>(std::numeric_limits<i32>::max());
                break;
            case EDataSlot::Int64:
            case EDataSlot::Interval:
                valMax = static_cast<ui64>(std::numeric_limits<i64>::max());
                break;
            default:
                break;
            }

            if (valMax && val > *valMax) {
                upperInclude = true;
                val = *valMax;
                upperValue = ToString(val);
            }
            else if (upperInclude && (!valMax || val < *valMax)) {
                upperInclude = false;
                ++val;
                upperValue = ToString(val);
            }
        }
        else if (GetDataTypeInfo(upperDataSlot).Features & NUdf::SignedIntegralType) {
            i64 val = FromString<i64>(upperValue);

            TMaybe<i64> valMin;
            TMaybe<i64> valMax;
            switch (targetDataSlot) {
            case EDataSlot::Int8:
                valMin = static_cast<i64>(std::numeric_limits<i8>::min());
                valMax = static_cast<i64>(std::numeric_limits<i8>::max());
                break;
            case EDataSlot::Int16:
                valMin = static_cast<i64>(std::numeric_limits<i16>::min());
                valMax = static_cast<i64>(std::numeric_limits<i16>::max());
                break;
            case EDataSlot::Int32:
                valMin = static_cast<i64>(std::numeric_limits<i32>::min());
                valMax = static_cast<i64>(std::numeric_limits<i32>::max());
                break;
            case EDataSlot::Int64:
            case EDataSlot::Interval:
                valMin = static_cast<i64>(std::numeric_limits<i64>::min());
                valMax = static_cast<i64>(std::numeric_limits<i64>::max());
                break;
            default:
                break;
            }

            if (valMin && (val < *valMin || (!upperInclude && val == *valMin))) {
                return false;
            }

            if (valMax && val > *valMax) {
                upperInclude = true;
                val = *valMax;
            }
            else if (upperInclude && (!valMax || val < *valMax)) {
                upperInclude = false;
                ++val;
            }

            upperValue = ToString(val);
        }
    }
    else if (GetDataTypeInfo(targetDataSlot).Features & (NUdf::UnsignedIntegralType | NUdf::DateType)) {
        // Target is unsigned integer
        ui64 val = 0;
        if (GetDataTypeInfo(upperDataSlot).Features & NUdf::SignedIntegralType) {
            if (FromString<i64>(upperValue) < 0) {
                return false;
            }
            else {
                val = FromString<ui64>(upperValue);
            }
        }
        else if (GetDataTypeInfo(upperDataSlot).Features & NUdf::FloatType) {
            double dVal = FromString<double>(upperValue);
            if (dVal < 0.) {
                return false;
            }
            else if (dVal >= static_cast<double>(std::numeric_limits<ui64>::max())) {
                if (dVal > static_cast<double>(std::numeric_limits<ui64>::max())) {
                    upperInclude = true;
                }
                val = std::numeric_limits<ui64>::max();
            }
            else if (std::floor(dVal) != dVal) {
                upperInclude = true;
                val = static_cast<ui64>(std::floor(dVal));
            }
            else {
                val = static_cast<ui64>(dVal);
            }
        }
        else if (GetDataTypeInfo(upperDataSlot).Features & NUdf::UnsignedIntegralType) {
            val = FromString<ui64>(upperValue);
        }
        else if ((GetDataTypeInfo(upperDataSlot).Features & NUdf::DateType) && (GetDataTypeInfo(targetDataSlot).Features & NUdf::DateType)
            && upperDataSlot != targetDataSlot) {

            val = FromString<ui64>(upperValue);
            ScaleDate<true>(val, upperInclude, upperDataSlot, targetDataSlot);
        }
        else {
            // Nothing to change for other types
            return true;
        }

        TMaybe<ui64> valMax;
        switch (targetDataSlot) {
        case EDataSlot::Uint8:
            valMax = static_cast<ui64>(std::numeric_limits<ui8>::max());
            break;
        case EDataSlot::Uint16:
            valMax = static_cast<ui64>(std::numeric_limits<ui16>::max());
            break;
        case EDataSlot::Uint32:
            valMax = static_cast<ui64>(std::numeric_limits<ui32>::max());
            break;
        case EDataSlot::Uint64:
            valMax = static_cast<ui64>(std::numeric_limits<ui64>::max());
            break;
        case EDataSlot::Date:
            valMax = static_cast<ui64>(NUdf::MAX_DATE);
            break;
        case EDataSlot::Datetime:
            valMax = static_cast<ui64>(NUdf::MAX_DATETIME);
            break;
        case EDataSlot::Timestamp:
            valMax = static_cast<ui64>(NUdf::MAX_TIMESTAMP);
            break;
        default:
            break;
        }

        if (valMax && val > *valMax) {
            upperInclude = true;
            val = *valMax;
        }
        else if (upperInclude && (!valMax || val < *valMax)) {
            upperInclude = false;
            ++val;
        }

        upperValue = ToString(val);
    }
    return true;
}


struct TKeyRangeInternal {
    TVector<TExprBase> ExactPart;
    TMaybeNode<TExprBase> LowerLeaf;
    TMaybeNode<TExprBase> UpperLeaf;
    bool LowerInclude = false;
    bool UpperInclude = false;

    bool IsFullScan() const {
        return ExactPart.empty() && !LowerLeaf && !UpperLeaf;
    }

    bool IsExact() const {
        return (!ExactPart.empty() && !LowerLeaf && !UpperLeaf)
            || (LowerLeaf.Maybe<TCoNull>() && UpperLeaf.Maybe<TCoNull>())
            || (LowerLeaf.IsValid() && UpperLeaf.IsValid() && LowerInclude
                && ((UpperInclude && 0 == CompareDataNodes(LowerLeaf.Cast(), UpperLeaf.Cast()))
                    || (!UpperInclude && AdjacentDataNodes(LowerLeaf.Cast(), UpperLeaf.Cast())))
                );
    }

    bool IsLowerInf() const {
        return ExactPart.empty() && !LowerLeaf;
    }

    bool IsUpperInf() const {
        return ExactPart.empty() && !UpperLeaf;
    }

    void NormalizeExact() {
        if (LowerLeaf && UpperLeaf) {
            ExactPart.push_back(LowerLeaf.Cast());
            LowerLeaf = UpperLeaf = {};
        }
    }

    TYtRangesInfo::TKeyRange ToKeyRange() const {
        TYtRangesInfo::TKeyRange range;
        YQL_ENSURE(LowerLeaf || UpperLeaf);
        range.Lower.assign(ExactPart.begin(), ExactPart.end());
        range.Upper.assign(ExactPart.begin(), ExactPart.end());
        if (LowerLeaf) {
            range.Lower.push_back(LowerLeaf.Cast());
        }
        if (UpperLeaf) {
            range.Upper.push_back(UpperLeaf.Cast());
        }
        range.LowerInclude = LowerInclude || !LowerLeaf;
        range.UpperInclude = (UpperInclude && UpperLeaf) || (!UpperLeaf && !ExactPart.empty());
        return range;
    }
};

template <bool leftLower, bool rightLower>
bool CompareKeyRange(const TKeyRangeInternal& l, const TKeyRangeInternal& r) {
    if (leftLower && l.IsLowerInf()) {
        // Left lower is -inf, always less than right
        return !rightLower || !r.IsLowerInf();
    }
    if (!leftLower && l.IsUpperInf()) {
        // Left upper is +inf, always greater than right
        return false;
    }
    if (rightLower && r.IsLowerInf()) {
        // Right lower is -inf, always less than left
        return false;
    }
    if (!rightLower && r.IsUpperInf()) {
        // Right upper is +inf, always greater than left
        return leftLower || !l.IsUpperInf();
    }
    auto& lBound = leftLower ? l.LowerLeaf : l.UpperLeaf;
    auto& rBound = rightLower ? r.LowerLeaf : r.UpperLeaf;
    int cmp = 0;
    for (size_t i = 0; i < Min(l.ExactPart.size(), r.ExactPart.size()) && 0 == cmp; ++i) {
        cmp = CompareDataNodes(l.ExactPart[i], r.ExactPart[i]);
    }
    if (0 == cmp) {
        if (l.ExactPart.size() < r.ExactPart.size() && lBound) {
            cmp = CompareDataNodes(lBound.Cast(), r.ExactPart[l.ExactPart.size()]);
        }
        else if (l.ExactPart.size() > r.ExactPart.size() && rBound) {
            cmp = CompareDataNodes(l.ExactPart[r.ExactPart.size()], rBound.Cast());
        }
        else if (lBound && rBound) {
            cmp = CompareDataNodes(lBound.Cast(), rBound.Cast());
        }
    }
    if (cmp < 0) {
        return true;
    }
    else if (cmp > 0) {
        return false;
    }

    if (l.ExactPart.size() == r.ExactPart.size()) {
        if (leftLower && !l.LowerLeaf) {
            // Left lower is -inf, always less than right
            return !rightLower || r.LowerLeaf;
        }
        if (!leftLower && !l.UpperLeaf) {
            // Left upper is +inf, always greater than right
            return false;
        }
        if (rightLower && !r.LowerLeaf) {
            // Right lower is -inf, always less than left
            return false;
        }
        if (!rightLower && !r.UpperLeaf) {
            // Right upper is +inf, always greater than left
            return leftLower || l.UpperLeaf;
        }
    }

    auto lInclude = leftLower ? !l.LowerInclude : l.UpperInclude;
    auto rInclude = rightLower ? !r.LowerInclude : r.UpperInclude;

    if (rightLower) {
        return lInclude == rInclude
            ? (l.ExactPart.size() + lBound.IsValid()) < (r.ExactPart.size() + rBound.IsValid())
            : lInclude < rInclude;
    } else {
        return lInclude == rInclude
            ? (l.ExactPart.size() + lBound.IsValid()) > (r.ExactPart.size() + rBound.IsValid())
            : lInclude > rInclude;
    }
}

void DeduplicateRanges(TVector<TKeyRangeInternal>& ranges) {
    std::stable_sort(ranges.begin(), ranges.end(), CompareKeyRange<true, true>);

    size_t i = 0;
    while (i + 1 < ranges.size()) {
        TKeyRangeInternal& current = ranges[i];
        TKeyRangeInternal& next = ranges[i + 1];
        if (!CompareKeyRange<false, true>(current, next)) { // current.Upper >= next.Lower
            if (CompareKeyRange<false, false>(current, next)) { // current.Upper < next.Upper
                YQL_ENSURE(current.ExactPart.size() == next.ExactPart.size());
                DoSwap(current.UpperLeaf, next.UpperLeaf);
                DoSwap(current.UpperInclude, next.UpperInclude);
            }
            ranges.erase(ranges.begin() + i + 1);
            if (ranges[i].IsFullScan()) {
                ranges.clear();
                return;
            }
        }
        else {
            ++i;
        }
    }
}

TVector<NNodes::TExprBase> ConvertBoundaryNode(const TExprNode& boundary, bool& isIncluded) {
    isIncluded = false;
    YQL_ENSURE(boundary.IsList());
    TExprNodeList items = boundary.ChildrenList();
    YQL_ENSURE(items.size() >= 2);

    YQL_ENSURE(items.back()->IsCallable("Int32"));
    isIncluded = FromString<i32>(items.back()->Head().Content()) != 0;
    items.pop_back();

    TVector<NNodes::TExprBase> result;
    for (auto item : items) {
        if (item->IsCallable("Nothing")) {
            break;
        }
        YQL_ENSURE(item->IsCallable("Just"));
        item = item->HeadPtr();

        result.push_back(NNodes::TExprBase(std::move(item)));
        YQL_ENSURE(result.back().Maybe<TCoDataCtor>() ||
                   result.back().Maybe<TCoNothing>() ||
                   result.back().Maybe<TCoJust>());
    }

    return result;
}

TExprBase RoundTz(const TExprBase& node, bool down, TExprContext& ctx) {
    if (auto maybeTz = node.Maybe<TCoTzDateBase>()) {
        TStringBuf tzName = maybeTz.Cast().Literal().Value();
        TStringBuf value;
        GetNext(tzName, ',', value);

        const auto& names = NUdf::GetTimezones();
        YQL_ENSURE(!names.empty());

        TStringBuf targetName;
        if (down) {
            targetName = names.front();
        } else {
            for (auto it = names.rbegin(); it != names.rend(); ++it) {
                if (!it->empty()) {
                    targetName = *it;
                    break;
                }
            }
        }
        YQL_ENSURE(!tzName.empty());
        YQL_ENSURE(!targetName.empty());

        if (tzName != targetName) {
            return TExprBase(ctx.Builder(node.Pos())
                .Callable(node.Ref().Content())
                    .Atom(0, TStringBuilder() << value << "," << targetName)
                .Seal()
                .Build());
        }
    }
    return node;
}

TMaybe<TYtRangesInfo::TKeyRange> WidenTzKeys(const TYtRangesInfo::TKeySingle& single, TExprContext& ctx) {
    if (AllOf(single.Key, [](const auto& key){ return !TCoTzDateBase::Match(key.Raw()); })) {
        return {};
    }
    TYtRangesInfo::TKeyRange result;
    result.LowerInclude = result.UpperInclude = true;
    for (auto& key : single.Key) {
        result.Lower.push_back(RoundTz(key, true, ctx));
        result.Upper.push_back(RoundTz(key, false, ctx));
    }
    return result;
}

void WidenTzKeys(TYtRangesInfo::TKeyRange& range, TExprContext& ctx) {
    for (auto& key : range.Lower) {
        key = RoundTz(key, true, ctx);
    }
    for (auto& key : range.Upper) {
        key = RoundTz(key, false, ctx);
    }
}

} // unnamed

TYtRangesInfo::TPtr TYtRangesInfo::ApplyLegacyKeyFilters(const TVector<TExprBase>& keyFilters,
    const TYqlRowSpecInfo::TPtr& rowSpec, TExprContext& ctx)
{
    YQL_ENSURE(rowSpec && rowSpec->IsSorted());
    TVector<TKeyRangeInternal> ranges;
    for (auto andGrp: keyFilters) {
        bool emptyMatch = false;
        TKeyRangeInternal key;
        size_t memberIndex = 0;
        for (auto keyPredicates: andGrp.Cast<TCoNameValueTupleList>()) {
            YQL_ENSURE(memberIndex < rowSpec->SortMembers.size() && rowSpec->SortMembers[memberIndex] == keyPredicates.Name().Value());
            const TTypeAnnotationNode* columnType = rowSpec->SortedByTypes[memberIndex];
            if (columnType->GetKind() == ETypeAnnotationKind::Optional) {
                columnType = columnType->Cast<TOptionalExprType>()->GetItemType();
            }
            auto columnDataSlot = columnType->Cast<TDataExprType>()->GetSlot();
            ++memberIndex;

            key.NormalizeExact();
            key.LowerLeaf = key.UpperLeaf = {};
            key.LowerInclude = false;
            key.UpperInclude = false;

            auto isWithinRange = [&key](TExprBase value, bool includeBounds) -> int {
                if (key.UpperLeaf) {
                    int cmp = CompareDataNodes(value, key.UpperLeaf.Cast());
                    if (cmp > 0 || (cmp == 0 && !key.UpperInclude && includeBounds)) {
                        return 1; // out of range: greater than upper bound
                    }
                }
                if (key.LowerLeaf) {
                    int cmp = CompareDataNodes(key.LowerLeaf.Cast(), value);
                    if (cmp > 0 || (cmp == 0 && !key.LowerInclude && includeBounds)) {
                        return -1; // out of range: less than lower bound
                    }
                }
                return 0;
            };

            for (auto cmp: keyPredicates.Value().Cast<TCoNameValueTupleList>()) {
                auto operation = cmp.Name().Value();
                TExprBase value = cmp.Value().Cast();
                if (value.Maybe<TCoNothing>()) {
                    emptyMatch = true;
                    break;
                }
                if (value.Maybe<TCoNull>()) {
                    if ((key.LowerLeaf.IsValid() && !key.LowerLeaf.Maybe<TCoNull>())
                        || (key.UpperLeaf.IsValid() && !key.UpperLeaf.Maybe<TCoNull>()))
                    {
                        emptyMatch = true;
                        break;
                    }
                    key.LowerLeaf = key.UpperLeaf = value;
                    key.LowerInclude = key.UpperInclude = true;
                }
                else {
                    if (key.LowerLeaf.Maybe<TCoNull>() || key.UpperLeaf.Maybe<TCoNull>()) {
                        emptyMatch = true;
                        break;
                    }

                    if (value.Maybe<TCoJust>()) {
                        value = value.Cast<TCoJust>().Input();
                    }

                    if (operation == TStringBuf("<") || operation == TStringBuf("<=")) {
                        bool includeBound = (operation == TStringBuf("<="));
                        int cmp = isWithinRange(value, includeBound);
                        if (cmp < 0) /* lower than existing lower bound yields an empty match */ {
                            emptyMatch = true;
                            break;
                        }
                        else if (cmp == 0) {
                            key.UpperLeaf = value;
                            key.UpperInclude = includeBound;
                        }
                    }
                    else if (operation == TStringBuf(">") || operation == TStringBuf(">=")) {
                        bool includeBound = (operation == TStringBuf(">="));
                        int cmp = isWithinRange(value, includeBound);
                        if (cmp > 0) /* upper than existing upper bound yields an empty match */ {
                            emptyMatch = true;
                            break;
                        }
                        else if (cmp == 0) {
                            key.LowerLeaf = value;
                            key.LowerInclude = includeBound;
                        }
                    }
                    else if (operation == TStringBuf("==")) {
                        int cmp = isWithinRange(value, true);
                        if (cmp != 0) /* value out of the bounds */ {
                            emptyMatch = true;
                            break;
                        }
                        else {
                            key.UpperLeaf = key.LowerLeaf = value;
                            key.UpperInclude = key.LowerInclude = true;
                        }
                    }
                    else if (operation == TStringBuf("StartsWith")) {
                        int cmp = isWithinRange(value, true);
                        if (cmp != 0) /* value out of the bounds */ {
                            emptyMatch = true;
                            break;
                        }

                        key.LowerLeaf = value;
                        key.LowerInclude = true;
                        YQL_ENSURE(value.Ref().IsCallable({"String", "Utf8"}));
                        if (auto content = TString{key.LowerLeaf.Cast<TCoDataCtor>().Literal().Value()}; !content.empty()) {
                            std::optional<std::string> incremented;
                            if (columnDataSlot == EDataSlot::String) {
                                key.LowerLeaf = value = TExprBase(ctx.RenameNode(value.Ref(), "String"));
                                incremented = NextLexicographicString(content);
                            } else {
                                YQL_ENSURE(columnDataSlot == EDataSlot::Utf8);
                                if (IsUtf8(content)) {
                                    incremented = NextValidUtf8(content);
                                } else {
                                    emptyMatch = true;
                                    break;
                                }
                            }

                            if (incremented) {
                                key.UpperLeaf = ctx.ChangeChildren(value.Ref(), {ctx.NewAtom(value.Ref().Head().Pos(), *incremented)});
                                key.UpperInclude = false;
                            }
                        }
                    }
                }
            }

            if (!emptyMatch && key.LowerLeaf.Maybe<TCoDataCtor>()) {
                auto lowerDataSlot = GetDataSlot(key.LowerLeaf.Cast().Ref().Content());
                if (GetDataTypeInfo(lowerDataSlot).Features & (NUdf::NumericType | NUdf::DateType)) {
                    TString newLowerValue = TString{key.LowerLeaf.Cast<TCoDataCtor>().Literal().Value()};
                    if (!AdjustLowerValue(newLowerValue, key.LowerInclude, lowerDataSlot, columnDataSlot)) {
                        emptyMatch = true;
                    }
                    else if (columnDataSlot != lowerDataSlot
                        || newLowerValue != key.LowerLeaf.Cast<TCoDataCtor>().Literal().Value())
                    {
                        key.LowerLeaf = TExprBase(ctx.Builder(key.LowerLeaf.Cast().Pos())
                            .Callable(GetDataTypeInfo(columnDataSlot).Name)
                                .Atom(0, newLowerValue)
                            .Seal()
                            .Build());
                    }
                }
            }

            if (!emptyMatch && key.UpperLeaf.Maybe<TCoDataCtor>()) {
                auto upperDataSlot = GetDataSlot(key.UpperLeaf.Cast().Ref().Content());
                if (GetDataTypeInfo(upperDataSlot).Features & (NUdf::NumericType | NUdf::DateType)) {
                    TString newUpperValue = TString{key.UpperLeaf.Cast<TCoDataCtor>().Literal().Value()};
                    if (!AdjustUpperValue(newUpperValue, key.UpperInclude, upperDataSlot, columnDataSlot)) {
                        emptyMatch = true;
                    }
                    else if (columnDataSlot != upperDataSlot
                        || newUpperValue != key.UpperLeaf.Cast<TCoDataCtor>().Literal().Value())
                    {
                        key.UpperLeaf = TExprBase(ctx.Builder(key.UpperLeaf.Cast().Pos())
                            .Callable(GetDataTypeInfo(columnDataSlot).Name)
                                .Atom(0, newUpperValue)
                            .Seal()
                            .Build());
                    }
                }
            }

            if (emptyMatch) {
                break;
            }

            if (!key.IsExact()) {
                break;
            }
        }

        if (!emptyMatch) {
            if (key.IsFullScan()) {
                return TYtRangesInfo::TPtr();
            }

            ranges.push_back(std::move(key));
        }
    }

    TYtRangesInfo::TPtr rangesInfo = MakeIntrusive<TYtRangesInfo>();
    if (!ranges.empty()) {
        if (ranges.size() > 1) {
            DeduplicateRanges(ranges);
            if (ranges.empty()) {
                return TYtRangesInfo::TPtr(); // Full scan
            }
        }
        for (TKeyRangeInternal& range: ranges) {
            if (range.IsExact()) {
                range.NormalizeExact();
                TKeySingle single{std::move(range.ExactPart)};
                auto maybeKeyRange = WidenTzKeys(single, ctx);
                if (maybeKeyRange) {
                    rangesInfo->Ranges.emplace_back(*maybeKeyRange);
                } else {
                    rangesInfo->Ranges.emplace_back(std::move(single));
                }
            }
            else {
                TKeyRange r = range.ToKeyRange();
                WidenTzKeys(r, ctx);
                rangesInfo->Ranges.emplace_back(std::move(r));
            }
        }
    }
    return rangesInfo;
}

TYtRangesInfo::TPtr TYtRangesInfo::ApplyKeyFilter(const TExprNode& keyFilter) {
    // keyFilter is List<Tuple<Left, Right>>, Left/Right are key boundaries
    if (keyFilter.IsCallable("List")) {
        return MakeEmptyRange();
    }

    YQL_ENSURE(keyFilter.IsCallable("AsList"));
    auto res = MakeIntrusive<TYtRangesInfo>();
    for (auto& interval : keyFilter.ChildrenList()) {
        YQL_ENSURE(interval->IsList());
        YQL_ENSURE(interval->ChildrenSize() == 2);

        TKeyRange range;
        range.Lower = ConvertBoundaryNode(interval->Head(), range.LowerInclude);
        if (range.Lower.empty()) {
            // just a convention
            range.LowerInclude = true;
        }
        range.Upper = ConvertBoundaryNode(interval->Tail(), range.UpperInclude);

        if (range.Lower.empty() && range.Upper.empty()) {
            // full range
            return {};
        }

        bool sameBounds = range.Lower.size() == range.Upper.size();
        for (size_t i = 0; sameBounds && i < range.Lower.size(); ++i) {
            sameBounds = range.Lower[i].Raw() == range.Upper[i].Raw();
        }

        if (sameBounds) {
            YQL_ENSURE(range.LowerInclude && range.UpperInclude, "Not expecting empty regions here");
            res->Ranges.emplace_back(TKeySingle{std::move(range.Lower)});
        } else {
            res->Ranges.emplace_back(std::move(range));
        }
    }

    return res;
}

TYtRangesInfo::TPtr TYtRangesInfo::MakeEmptyRange() {
    return MakeIntrusive<TYtRangesInfo>();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

bool TYtColumnsInfo::Validate(TExprNode& node, TExprContext& ctx) {
    if (!node.IsCallable(TCoVoid::CallableName())) {
        if (!EnsureTuple(node, ctx)) {
            return false;
        }
        THashSet<TStringBuf> columns;
        THashSet<TStringBuf> renameFrom;
        THashSet<TStringBuf> renameTo;

        for (auto& child: node.Children()) {
            TStringBuf name;
            TStringBuf type;
            TStringBuf originalName;

            if (child->Type() == TExprNode::Atom) {
                name = child->Content();
            } else {
                if (!EnsureTupleMinSize(*child, 2, ctx)) {
                    return false;
                }
                if (!EnsureAtom(*child->Child(0), ctx)) {
                    return false;
                }
                if (!EnsureAtom(*child->Child(1), ctx)) {
                    return false;
                }

                name = child->Child(0)->Content();
                type = child->Child(1)->Content();

                if (type == "weak") {
                    if (!EnsureTupleSize(*child, 2, ctx)) {
                        return false;
                    }
                } else if (type == "rename") {
                    if (!EnsureTupleSize(*child, 3, ctx)) {
                        return false;
                    }
                    originalName = child->Child(2)->Content();
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Unknown column type: " << type));
                    return false;
                }
            }

            if (type != "rename") {
                if (!columns.insert(name).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Duplicate column name: " << name));
                    return false;
                }
            } else {
                if (!renameFrom.insert(originalName).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Duplicate original column name: " << originalName));
                    return false;
                }
                if (renameTo.find(originalName) != renameTo.end()) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Rename chain detected with original column name: " << originalName));
                    return false;
                }

                if (!renameTo.insert(name).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Duplicate target column name: " << name));
                    return false;
                }
                if (renameFrom.find(name) != renameFrom.end()) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Rename chain detected with target column name: " << name));
                    return false;
                }
            }
        }
    }
    return true;
}

void TYtColumnsInfo::Parse(NNodes::TExprBase node) {
    Renames = {};
    Columns = {};
    Others = false;
    if (node.Maybe<TExprList>()) {
        for (auto child: node.Cast<TExprList>()) {
            TColumn column;
            if (child.Maybe<TCoAtom>()) {
                column.Name = child.Cast<TCoAtom>().Value();
                if (!Columns) {
                    Columns.ConstructInPlace();
                }
                Columns->emplace_back(column);
                UpdateOthers(column.Name);
                continue;
            }
            auto tuple = child.Cast<TCoAtomList>();
            column.Name = tuple.Item(0).Value();
            column.Type = tuple.Item(1).Value();
            if (tuple.Ref().ChildrenSize() == 2) {
                if (!Columns) {
                    Columns.ConstructInPlace();
                }
                Columns->emplace_back(column);
                UpdateOthers(column.Name);
                continue;
            }

            auto from = tuple.Item(2).Value();
            if (!Renames) {
                Renames.ConstructInPlace();
            }
            (*Renames)[from] = column.Name;
        }

        if (!Columns && !Renames) {
            Columns.ConstructInPlace();
        }
    }
}

NNodes::TExprBase TYtColumnsInfo::ToExprNode(TExprContext& ctx, const TPositionHandle& pos, const THashSet<TString>* filter) const {
    if (!Columns && !Renames) {
        return Build<TCoVoid>(ctx, pos).Done();
    }

    auto listBuilder = Build<TExprList>(ctx, pos);
    if (Renames) {
        for (auto& r: *Renames) {
            listBuilder
                .Add<TCoAtomList>()
                    .Add()
                        .Value(r.second)
                    .Build()
                    .Add()
                        .Value("rename", TNodeFlags::Default)
                    .Build()
                    .Add()
                        .Value(r.first)
                    .Build()
                .Build();
        }
    }

    if (Columns) {
        for (auto& c: *Columns) {
            if (filter && !filter->contains(c.Name)) {
                continue;
            }
            if (c.Type == "weak") {
                listBuilder
                    .Add<TCoAtomList>()
                        .Add()
                            .Value(c.Name)
                        .Build()
                        .Add()
                            .Value(c.Type, TNodeFlags::Default)
                        .Build()
                    .Build();
            } else {
                listBuilder
                    .Add<TCoAtom>()
                        .Value(c.Name)
                    .Build();
            }
        }
    }

    return listBuilder.Done();
}

void TYtColumnsInfo::UpdateOthers(TStringBuf col) {
    if (!Others) {
        Others = (col == YqlOthersColumnName);
    }
}

void TYtColumnsInfo::FillRichYPath(NYT::TRichYPath& path, bool withColumns) const {
    if (Columns && withColumns) {
        TVector<TString> columns;
        for (auto& c: *Columns) {
            columns.push_back(c.Name);
        }
        path.Columns(columns);
    }

    if (Renames) {
        path.RenameColumns(*Renames);
    }
}


/////////////////////////////////////////////////////////////////////////////////////////////////////////

bool TYtPathInfo::Validate(const TExprNode& node, TExprContext& ctx) {
    if (!node.IsCallable(TYtPath::CallableName())) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected " << TYtPath::CallableName()));
        return false;
    }
    if (!EnsureMinMaxArgsCount(node, 4, 5, ctx)) {
        return false;
    }

    if (!node.Child(TYtPath::idx_Table)->IsCallable(TYtTable::CallableName())
        && !node.Child(TYtPath::idx_Table)->IsCallable(TYtOutput::CallableName())
        && !node.Child(TYtPath::idx_Table)->IsCallable(TYtOutTable::CallableName())) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Child(TYtPath::idx_Table)->Pos()), TStringBuilder() << "Unexpected "
            << node.Child(TYtPath::idx_Table)->Content()));
        return false;
    }

    if (!TYtColumnsInfo::Validate(*node.Child(TYtPath::idx_Columns), ctx)) {
        return false;
    }

    if (!node.Child(TYtPath::idx_Ranges)->IsCallable(TCoVoid::CallableName())) {
        TYqlRowSpecInfo::TPtr rowSpec = TYtTableBaseInfo::GetRowSpec(TExprBase(node.ChildPtr(TYtPath::idx_Table)));
        TYtTableMetaInfo::TPtr meta = TYtTableBaseInfo::GetMeta(TExprBase(node.ChildPtr(TYtPath::idx_Table)));
        if (!TYtRangesInfo::Validate(*node.Child(TYtPath::idx_Ranges), ctx, meta && meta->DoesExist, rowSpec)) {
            return false;
        }
    }

    if (!node.Child(TYtPath::idx_Stat)->IsCallable(TYtStat::CallableName())
        && !node.Child(TYtPath::idx_Stat)->IsCallable(TCoVoid::CallableName())) {

        ctx.AddError(TIssue(ctx.GetPosition(node.Child(TYtPath::idx_Stat)->Pos()), TStringBuilder()
            << "Expected " << TYtStat::CallableName()
            << " or Void"));
        return false;
    }

    if (node.ChildrenSize() > TYtPath::idx_AdditionalAttributes && !EnsureAtom(*node.Child(TYtPath::idx_AdditionalAttributes), ctx)) {
        return false;
    }

    return true;
}

void TYtPathInfo::Parse(TExprBase node) {
    *this = {};
    FromNode = node.Maybe<TExprBase>();
    TYtPath path = node.Cast<TYtPath>();
    Table = TYtTableBaseInfo::Parse(path.Table());

    if (path.Columns().Maybe<TExprList>()) {
        Columns = MakeIntrusive<TYtColumnsInfo>(path.Columns());
    }

    if (path.Ranges().Maybe<TExprList>()) {
        Ranges = MakeIntrusive<TYtRangesInfo>(path.Ranges());
    }

    if (path.Stat().Maybe<TYtStat>()) {
        Stat = MakeIntrusive<TYtTableStatInfo>(path.Stat().Ptr());
    }
    if (path.AdditionalAttributes().Maybe<TCoAtom>()) {
        AdditionalAttributes = path.AdditionalAttributes().Cast().Value();
    }
}

TExprBase TYtPathInfo::ToExprNode(TExprContext& ctx, const TPositionHandle& pos, NNodes::TExprBase table) const {
    auto pathBuilder = Build<TYtPath>(ctx, pos);
    pathBuilder.Table(table);
    if (Columns) {
        pathBuilder.Columns(Columns->ToExprNode(ctx, pos));
    } else {
        pathBuilder.Columns<TCoVoid>().Build();
    }
    if (Ranges) {
        pathBuilder.Ranges(Ranges->ToExprNode(ctx, pos));
    } else {
        pathBuilder.Ranges<TCoVoid>().Build();
    }
    if (Stat) {
        pathBuilder.Stat(Stat->ToExprNode(ctx, pos));
    } else {
        pathBuilder.Stat<TCoVoid>().Build();
    }
    if (AdditionalAttributes) {
        pathBuilder.AdditionalAttributes<TCoAtom>()
            .Value(*AdditionalAttributes, TNodeFlags::MultilineContent)
        .Build();
    }

    return pathBuilder.Done();
}

void TYtPathInfo::FillRichYPath(NYT::TRichYPath& path) const {
    if (AdditionalAttributes) {
        DeserializeRichYPathAttrs(*AdditionalAttributes, path);
    }
    if (Columns) {
        // Should have the same criteria as in TYtPathInfo::GetCodecSpecNode()
        bool useAllColumns = !Table->RowSpec; // Always use all columns for YAMR format
        if (Table->RowSpec && !Table->RowSpec->StrictSchema && Columns->HasOthers()) {
            useAllColumns = true;
        }

        Columns->FillRichYPath(path, /* withColumns = */ !useAllColumns);
    }
    if (Ranges) {
        YQL_ENSURE(Table);
        YQL_ENSURE(Table->RowSpec);
        Ranges->FillRichYPath(path, Table->RowSpec->SortedBy.size());
    }
}

IGraphTransformer::TStatus TYtPathInfo::GetType(const TTypeAnnotationNode*& filtered, TExprNode::TPtr& newFields, TExprContext& ctx, const TPositionHandle& pos) const {
    YQL_ENSURE(Table, "TYtPathInfo::Parse() must be called");

    filtered = nullptr;
    newFields = {};
    const TStructExprType* inputType = Table->FromNode.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    if (!Columns) {
        filtered = inputType;
        return IGraphTransformer::TStatus::Ok;
    } else {
        auto& renames = Columns->GetRenames();
        if (renames) {
            auto inputTypeItems = inputType->GetItems();
            for (auto& r : *renames) {
                auto from = r.first;
                auto to = r.second;

                auto idx = inputType->FindItem(from);
                if (!idx) {
                    ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Unknown original member: " << from));
                    return IGraphTransformer::TStatus::Error;
                }
                inputTypeItems[*idx] = ctx.MakeType<TItemExprType>(to, inputTypeItems[*idx]->GetItemType());
            }
            YQL_ENSURE(!inputTypeItems.empty());
            inputType = ctx.MakeType<TStructExprType>(inputTypeItems);
        }

        auto& columns = Columns->GetColumns();
        if (!columns) {
            filtered = inputType;
            return IGraphTransformer::TStatus::Ok;
        }

        THashSet<TString> auxColumns;
        if (Table->RowSpec) {
            for (auto& aux: Table->RowSpec->GetAuxColumns()) {
                auxColumns.insert(aux.first);
            }
        }
        bool hasMissingColumns = false;
        THashSet<TString> fieldNames;
        for (auto& item: *columns) {
            if (inputType->FindItem(item.Name) || item.Type == "weak") {
                fieldNames.insert(item.Name);
            } else if (!auxColumns.contains(item.Name)) {
                hasMissingColumns = true;
            }
        }

        if (hasMissingColumns) {
            newFields = Columns->ToExprNode(ctx, pos, &fieldNames).Ptr();
            return IGraphTransformer::TStatus::Repeat;
        }

        TVector<const TItemExprType*> items;
        for (auto& item : inputType->GetItems()) {
            if (fieldNames.contains(item->GetName())) {
                items.push_back(item);
            }
        }
        auto weakType = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Yson));
        for (auto& item: *columns) {
            if (!inputType->FindItem(item.Name) && item.Type == "weak") {
                items.push_back(ctx.MakeType<TItemExprType>(item.Name, weakType));
            }
        }

        auto itemType = ctx.MakeType<TStructExprType>(items);
        if (!itemType->Validate(pos, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        filtered = itemType;
        return IGraphTransformer::TStatus::Ok;
    }
}

const NCommon::TStructMemberMapper& TYtPathInfo::GetColumnMapper() {
    if (Mapper_) {
        return *Mapper_;
    }
    THashSet<TStringBuf> filterColumns;
    THashMap<TString, TString> renames;
    if (Columns && Columns->GetRenames()) {
        renames = *Columns->GetRenames();
    }

    bool useAllColumns = true; // Should have the same criteria as in TYtPathInfo::FillRichYPath()
    if (HasColumns()) {
        useAllColumns = !Table->RowSpec; // Always use all columns for YAMR format
        if (Table->RowSpec && !Table->RowSpec->StrictSchema && Columns->HasOthers()) {
            useAllColumns = true;
        }

        if (!useAllColumns) {
            for (auto& c: *Columns->GetColumns()) {
                filterColumns.insert(c.Name);
            }
        }
    }
    if (useAllColumns && renames.empty()) {
        Mapper_.ConstructInPlace();
        return *Mapper_;
    }

    Mapper_ = [renames, filterColumns, useAllColumns, strict = Table->RowSpec->StrictSchema](TStringBuf name) -> TMaybe<TStringBuf> {
        if (auto r = renames.find(name); r != renames.cend()) {
            name = r->second;
        }
        if (strict && !useAllColumns && !filterColumns.contains(name)) {
            return Nothing();
        }
        if (!strict && name == YqlOthersColumnName) {
            return Nothing();
        }
        return MakeMaybe(name);
    };
    return *Mapper_;
}

NYT::TNode TYtPathInfo::GetCodecSpecNode() {
    auto specNode = Table->GetCodecSpecNode(GetColumnMapper());
    if (Table->RowSpec && !Table->RowSpec->StrictSchema && HasColumns()) {
        auto list = NYT::TNode::CreateList();
        for (auto& col: *Columns->GetColumns()) {
            if (col.Type == "weak") {
                list.Add(col.Name);
            }
        }
        if (!list.AsList().empty()) {
            specNode[YqlRowSpecAttribute][RowSpecAttrWeakFields] = list;
        }
    }

    return specNode;
}

bool TYtPathInfo::RequiresRemap() const {
    if (!Table->RequiresRemap()) {
        return false;
    }
    const auto meta = Table->Meta;
    const auto rowSpec = Table->RowSpec;
    if (!meta->Attrs.contains(QB2Premapper) && rowSpec && rowSpec->DefaultValues.empty() && !rowSpec->StrictSchema) {
        if (HasColumns() && !Columns->HasOthers()) {
            return false;
        }
    }

    return true;
}

TString TYtPathInfo::GetCodecSpecStr() {
    return NYT::NodeToCanonicalYsonString(GetCodecSpecNode(), NYson::EYsonFormat::Text);
}

ui64 TYtPathInfo::GetNativeYtTypeFlags() {
    return Table->RowSpec ? Table->RowSpec->GetNativeYtTypeFlags(GetColumnMapper()) : 0;
}

TMaybe<NYT::TNode> TYtPathInfo::GetNativeYtType() {
    return Table->RowSpec ? Table->RowSpec->GetNativeYtType(GetColumnMapper()) : Nothing();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

} // NYql
