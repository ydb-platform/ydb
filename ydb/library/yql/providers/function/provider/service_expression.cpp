#include "service_expression.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>

namespace NYql::NDqFunction {
    namespace {

        TExprNode::TPtr StructType(const std::vector<TServiceFunctionSignature::TField>& fields, TPositionHandle pos, TExprContext& ctx) {
            TExprNode::TListType members;
            for (const auto& field : fields) {
                members.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, field.Name),
                                                    ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, field.Type)})}));
            }
            return ctx.NewCallable(pos, "StructType", std::move(members));
        }

    } // namespace

    TExprNode::TPtr RewriteServiceExpression(const TExprNode::TPtr& apply,
                                             const TDqFunctionGatewayFactory& factory, TExprContext& ctx) {
        if (!apply->IsCallable("Apply") || !apply->ChildrenSize() || !apply->Head().IsCallable("Udf") ||
            !apply->Head().ChildrenSize() || !apply->Head().Head().IsAtom()) {
            return apply;
        }
        const TString name(apply->Head().Head().Content());
        const auto* signature = factory.FindServiceFunction(name);
        if (!signature) {
            return apply;
        }
        const auto fail = [&](TStringBuf message) -> TExprNode::TPtr {
            ctx.AddError(TIssue(ctx.GetPosition(apply->Pos()), TString(message)));
            return {};
        };
        if (apply->ChildrenSize() != signature->Input.size() + 2) {
            return fail("Service method requires its manifest arguments and a constant options struct (connection, optional batch_size)");
        }
        const auto& options = apply->Tail();
        if (!options.IsCallable("AsStruct")) {
            return fail("Service options must be a constant struct");
        }
        TExprNode::TPtr connection, batch;
        THashSet<TString> seen;
        for (const auto& field : options.Children()) {
            if (!field->IsList() || field->ChildrenSize() != 2 || !field->Head().IsAtom() ||
                !seen.emplace(field->Head().Content()).second) {
                return fail("Invalid or duplicate service option");
            }
            const auto& value = field->TailPtr();
            if (field->Head().IsAtom("connection")) {
                if (!value->IsCallable({"String", "Utf8"}) || value->ChildrenSize() != 1 ||
                    !value->Head().IsAtom() || value->Head().Content().empty()) {
                    return fail("Service connection must be a nonempty constant string");
                }
                connection = value;
            } else if (field->Head().IsAtom("batch_size")) {
                ui32 count = 0;
                if (!value->IsCallable("Int32") || value->ChildrenSize() != 1 || !value->Head().IsAtom() ||
                    !TryFromString(value->Head().Content(), count) || count == 0 || count > 64 ||
                    (count > 1 && !signature->Batch)) {
                    return fail("Service batch_size must be a constant Int32 in 1..64 supported by the method");
                }
                batch = value;
            } else {
                return fail("Unknown service option; expected connection or batch_size");
            }
        }
        if (!connection) {
            return fail("Service connection option is required");
        }
        const auto pos = apply->Pos();
        TExprNode::TListType settings{
            ctx.NewList(pos, {ctx.NewAtom(pos, "connection"), connection}),
            ctx.NewList(pos, {ctx.NewAtom(pos, "input_type"), StructType(signature->Input, pos, ctx)}),
            ctx.NewList(pos, {ctx.NewAtom(pos, "output_type"), StructType(signature->Output, pos, ctx)})};
        if (batch) {
            settings.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, "batch_size"), batch}));
        }
        const auto dot = name.rfind('.');
        auto callable = ctx.NewCallable(pos, "SqlExternalFunction", {ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, name.substr(0, dot))}),
                                                                     ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, name.substr(dot + 1))}),
                                                                     ctx.NewList(pos, std::move(settings))});
        TExprNode::TListType arguments;
        for (size_t i = 0; i < signature->Input.size(); ++i) {
            arguments.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, signature->Input[i].Name), apply->ChildPtr(i + 1)}));
        }
        // The occurrence identity prevents CSE from combining separate effectful calls.
        return ctx.NewCallable(pos, "WasmServiceCall", {callable,
                                                        ctx.NewCallable(pos, "AsStruct", std::move(arguments)), ctx.NewAtom(pos, ToString(apply->UniqueId()))});
    }

} // namespace NYql::NDqFunction
