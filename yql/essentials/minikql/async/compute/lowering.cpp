#include "program.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <util/generic/hash_set.h>
#include <util/stream/str.h>

namespace NKikimr::NMiniKQL::NAsync {
namespace {

void CheckValueType(TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::Data:
            return;
        case TType::EKind::Optional:
            return CheckValueType(static_cast<TOptionalType*>(type)->GetItemType());
        case TType::EKind::Struct: {
            auto* s = static_cast<TStructType*>(type);
            for (ui32 i = 0; i < s->GetMembersCount(); ++i) {
                CheckValueType(s->GetMemberType(i));
            }
            return;
        }
        case TType::EKind::Tuple: {
            auto* t = static_cast<TTupleType*>(type);
            for (ui32 i = 0; i < t->GetElementsCount(); ++i) {
                CheckValueType(t->GetElementType(i));
            }
            return;
        }
        default:
            MKQL_ENSURE(false, "Async row expression supports Data/Optional/Struct/Tuple only; Resource, list, block and higher-order contexts require explicit lowering");
    }
}

class TLowering {
public:
    TLowering(TProgramBuilder& pb, TRuntimeNode row, const TCallableRegistry& registry)
        : Pb_(pb)
        , Row_(row)
        , Registry_(registry)
    {
    }

    TRuntimeNode Build(TRuntimeNode source, TRuntimeNode body, ui32 window) {
        MKQL_ENSURE(!Row_.IsImmediate() && static_cast<TCallable*>(Row_.GetNode())->GetType()->GetName() == "Arg", "AsyncMap requires an explicit row argument");
        MKQL_ENSURE(window && window <= 1024, "AsyncMap frame window must be in [1, 1024]");
        const bool flow = source.GetStaticType()->IsFlow();
        if (flow) {
            source = Pb_.FromFlow(source);
        }
        MKQL_ENSURE(source.GetStaticType()->IsStream(), "AsyncMap supports Stream/Flow, not List or higher-order contexts");
        MKQL_ENSURE(static_cast<TStreamType*>(source.GetStaticType())->GetItemType()->IsSameType(*Row_.GetStaticType()), "AsyncMap row type mismatch");
        Nodes_.push_back(source);
        Nodes_.push_back({}); // serialized execution IR
        Program_.MaxFrames = window;
        Program_.Root = Lower(body);
        TStringStream meta;
        Save(&meta, Program_);
        Nodes_[1] = Pb_.NewDataLiteral<NUdf::EDataSlot::String>(meta.Str());
        TCallableBuilder call(Pb_.GetTypeEnvironment(), "AsyncMap", TStreamType::Create(body.GetStaticType(), Pb_.GetTypeEnvironment()), true);
        for (auto node : Nodes_) {
            call.Add(node);
        }
        auto result = TRuntimeNode(call.Build(), false);
        return flow ? Pb_.ToFlow(result, {}) : result;
    }

private:
    ui32 Append(TRuntimeNode node) {
        Nodes_.push_back(node);
        return Nodes_.size() - 1;
    }

    ui32 Lower(TRuntimeNode node) {
        if (auto it = Slots_.find(node.GetNode()); it != Slots_.end()) {
            return it->second;
        }
        CheckValueType(node.GetStaticType());
        TInstruction ins;
        TRuntimeNode fragment = node;
        auto bind = [&](TRuntimeNode child) {
            ins.Dependencies.push_back(Lower(child));
            auto arg = Pb_.Arg(child.GetStaticType());
            ins.Arguments.push_back(Append(arg));
            return arg;
        };
        if (node.GetNode() == Row_.GetNode()) {
            ins.Kind = EInstruction::Input;
            fragment = Pb_.Arg(node.GetStaticType());
        } else if (!node.IsImmediate()) {
            auto* call = static_cast<TCallable*>(node.GetNode());
            MKQL_ENSURE(!call->HasResult(), "Async lowering must precede callable replacement");
            const auto name = call->GetType()->GetName();
            if (name == "AsyncInvoke") {
                MKQL_ENSURE(call->GetInputsCount() == 2, "AsyncInvoke expects name and argument");
                const auto& desc = Registry_.Resolve(AS_VALUE(TDataLiteral, call->GetInput(0))->AsValue().AsStringRef());
                MKQL_ENSURE(call->GetInputsCount() == 2 && desc.InputType->IsSameType(*call->GetInput(1).GetStaticType()) && desc.ResultType->IsSameType(*node.GetStaticType()), "Async callable signature mismatch");
                ins.Kind = EInstruction::Invoke;
                ins.Dependencies.push_back(Lower(call->GetInput(1)));
                ins.Provider = desc.Provider;
                ins.Operation = desc.Operation;
                ins.MaxResponseBytes = desc.MaxResponseBytes;
                fragment = Pb_.Arg(node.GetStaticType()); // carries the result type only
            } else if (name == "If") {
                MKQL_ENSURE(call->GetInputsCount() == 3, "Async If expects three operands");
                ins.Kind = EInstruction::Branch;
                TCallableBuilder copy(Pb_.GetTypeEnvironment(), name, node.GetStaticType(), true);
                for (ui32 i = 0; i != 3; ++i) {
                    copy.Add(bind(call->GetInput(i)));
                }
                fragment = TRuntimeNode(copy.Build(), false);
            } else if (name == "Unwrap") {
                MKQL_ENSURE(call->GetInputsCount() == 5, "Async Unwrap expects five operands");
                ins.Kind = EInstruction::Unwrap;
                TCallableBuilder copy(Pb_.GetTypeEnvironment(), name, node.GetStaticType(), true);
                copy.Add(bind(call->GetInput(0)));
                copy.Add(bind(call->GetInput(1)));
                // Source position is literal metadata inspected by WrapUnwrap.
                for (ui32 i = 2; i != 5; ++i) {
                    copy.Add(call->GetInput(i));
                }
                fragment = TRuntimeNode(copy.Build(), false);
            } else {
                // Only strict row primitives. And/Or/Coalesce and other lazy,
                // higher-order or stateful operators need their own lowering.
                static const THashSet<TStringBuf> strict = {
                    "Add", "Sub", "Mul", "Div", "Mod", "Equals", "NotEquals", "Less", "LessOrEqual", "Greater", "GreaterOrEqual",
                    "Member", "Nth", "Not", "Exists", "Just", "Concat", "Random", "RandomNumber", "RandomUuid", "Now"};
                MKQL_ENSURE(name != "Invoke" || call->GetInputsCount() >= 2, "Invalid strict builtin invocation");
                const TStringBuf primitive = name == "Invoke"
                                                 ? TStringBuf(AS_VALUE(TDataLiteral, call->GetInput(0))->AsValue().AsStringRef())
                                                 : TStringBuf(name);
                MKQL_ENSURE(strict.contains(primitive), "Unsupported callable in async row expression: " << primitive);
                ins.Kind = EInstruction::Sync;
                TCallableBuilder copy(Pb_.GetTypeEnvironment(), name, node.GetStaticType(), true);
                for (ui32 i = 0; i < call->GetInputsCount(); ++i) {
                    // Literal parameters (e.g. Member's index) are inspected by
                    // existing factories; retain them. Dynamic operands are saved slots.
                    auto child = call->GetInput(i);
                    if (child.IsImmediate() && child.GetNode()->GetType()->IsData()) {
                        copy.Add(child);
                    } else {
                        copy.Add(bind(child));
                    }
                }
                fragment = TRuntimeNode(copy.Build(), false);
            }
        } else {
            ins.Kind = EInstruction::Sync;
            switch (node.GetStaticType()->GetKind()) {
                case TType::EKind::Struct: {
                    auto* value = AS_VALUE(TStructLiteral, node);
                    TStructLiteralBuilder copy(Pb_.GetTypeEnvironment());
                    for (ui32 i = 0; i < value->GetValuesCount(); ++i) {
                        copy.Add(value->GetType()->GetMemberName(i), bind(value->GetValue(i)));
                    }
                    fragment = TRuntimeNode(copy.Build(), true);
                    break;
                }
                case TType::EKind::Tuple: {
                    auto* value = AS_VALUE(TTupleLiteral, node);
                    std::vector<TRuntimeNode> fields;
                    for (ui32 i = 0; i < value->GetValuesCount(); ++i) {
                        fields.push_back(bind(value->GetValue(i)));
                    }
                    fragment = Pb_.NewTuple(fields);
                    break;
                }
                case TType::EKind::Optional: {
                    auto* value = AS_VALUE(TOptionalLiteral, node);
                    if (value->HasItem()) {
                        fragment = TRuntimeNode(BuildOptionalLiteral(bind(value->GetItem()), Pb_.GetTypeEnvironment()), true);
                    }
                    break;
                }
                default:
                    break;
            }
        }
        ins.Fragment = Append(fragment);
        const ui32 slot = Program_.Instructions.size();
        Program_.Instructions.push_back(std::move(ins));
        Slots_.emplace(node.GetNode(), slot);
        return slot;
    }

    TProgramBuilder& Pb_;
    TRuntimeNode Row_;
    const TCallableRegistry& Registry_;
    TProgram Program_;
    std::vector<TRuntimeNode> Nodes_;
    THashMap<TNode*, ui32> Slots_;
};

} // namespace

void TCallableRegistry::Register(TCallableDescriptor descriptor) {
    MKQL_ENSURE(!descriptor.Name.empty() && descriptor.InputType && descriptor.ResultType && !descriptor.Provider.empty() && !descriptor.Operation.empty(), "Incomplete async callable descriptor");
    CheckValueType(descriptor.InputType);
    CheckValueType(descriptor.ResultType);
    const auto name = descriptor.Name;
    MKQL_ENSURE(Descriptors_.emplace(name, std::move(descriptor)).second, "Duplicate async callable: " << name);
}
const TCallableDescriptor& TCallableRegistry::Resolve(TStringBuf name) const {
    const auto it = Descriptors_.find(name);
    MKQL_ENSURE(it != Descriptors_.end(), "Unknown async callable: " << name);
    return it->second;
}
TRuntimeNode TCallableRegistry::Call(TProgramBuilder& builder, TStringBuf name, TRuntimeNode argument) const {
    const auto& desc = Resolve(name);
    MKQL_ENSURE(desc.InputType->IsSameType(*argument.GetStaticType()), "Async argument type mismatch: " << name);
    TCallableBuilder call(builder.GetTypeEnvironment(), "AsyncInvoke", desc.ResultType, true);
    call.Add(builder.NewDataLiteral<NUdf::EDataSlot::String>(name));
    call.Add(argument);
    return TRuntimeNode(call.Build(), false);
}
TRuntimeNode BuildAsyncMap(TProgramBuilder& builder, TRuntimeNode input, TRuntimeNode rowArgument,
                           TRuntimeNode body, const TCallableRegistry& registry, ui32 maxFrames) {
    return TLowering(builder, rowArgument, registry).Build(input, body, maxFrames);
}

} // namespace NKikimr::NMiniKQL::NAsync
