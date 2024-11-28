#include "mkql_node_cast.h"

#define MKQL_AS_VALUE(name, suffix)                                      \
    template <>                                                          \
    T##name##suffix*                                                     \
    AsValue(TRuntimeNode node, const TSourceLocation& location) {        \
        MKQL_ENSURE_WITH_LOC(                                            \
                location,                                                \
                node.HasValue() && node.GetStaticType()->Is##name(),     \
                "Expected value of T" #name #suffix <<                   \
                " but got " << node.GetStaticType()->GetKindAsStr());    \
        return static_cast<T##name##suffix*>(node.GetValue());           \
    }

#define MKQL_AS_TYPE(name)                                               \
    template <>                                                          \
    T##name##Type*                                                       \
    AsType(TType* type, const TSourceLocation& location) {               \
        MKQL_ENSURE_WITH_LOC(                                            \
                location,                                                \
                type->Is##name(),                                        \
                   "Expected type of T" #name "Type"                     \
                   " but got " << type->GetKindAsStr());                 \
        return static_cast<T##name##Type*>(type);                        \
    }                                                                    \
    template <>                                                          \
    const T##name##Type*                                                 \
    AsType(const TType* type, const TSourceLocation& location) {         \
        MKQL_ENSURE_WITH_LOC(                                            \
                location,                                                \
                type->Is##name(),                                        \
                   "Expected type of T" #name "Type"                     \
                   " but got " << type->GetKindAsStr());                 \
        return static_cast<const T##name##Type*>(type);                  \
    }

namespace NKikimr {
namespace NMiniKQL {

MKQL_AS_TYPE(Any)
MKQL_AS_TYPE(Callable)
MKQL_AS_TYPE(Data)
MKQL_AS_TYPE(Dict)
MKQL_AS_TYPE(List)
MKQL_AS_TYPE(Optional)
MKQL_AS_TYPE(Struct)
MKQL_AS_TYPE(Tuple)
MKQL_AS_TYPE(Type)
MKQL_AS_TYPE(Void)
MKQL_AS_TYPE(Resource)
MKQL_AS_TYPE(Variant)
MKQL_AS_TYPE(Stream)
MKQL_AS_TYPE(Flow)
MKQL_AS_TYPE(Tagged)
MKQL_AS_TYPE(Block)
MKQL_AS_TYPE(Pg)
MKQL_AS_TYPE(Multi)

MKQL_AS_VALUE(Any, Type)
MKQL_AS_VALUE(Callable, Type)
MKQL_AS_VALUE(Data, Type)
MKQL_AS_VALUE(Dict, Type)
MKQL_AS_VALUE(List, Type)
MKQL_AS_VALUE(Optional, Type)
MKQL_AS_VALUE(Struct, Type)
MKQL_AS_VALUE(Tuple, Type)
MKQL_AS_VALUE(Type, Type)
MKQL_AS_VALUE(Void, Type)
MKQL_AS_VALUE(Variant, Type)
MKQL_AS_VALUE(Stream, Type)
MKQL_AS_VALUE(Flow, Type)

MKQL_AS_VALUE(Data, Literal)
MKQL_AS_VALUE(Dict, Literal)
MKQL_AS_VALUE(List, Literal)
MKQL_AS_VALUE(Optional, Literal)
MKQL_AS_VALUE(Struct, Literal)
MKQL_AS_VALUE(Tuple, Literal)
MKQL_AS_VALUE(Variant, Literal)

TCallable* AsCallable(
        const TStringBuf& name,
        TRuntimeNode node,
        const TSourceLocation& location)
{
    MKQL_ENSURE_WITH_LOC(location,
            !node.IsImmediate() && node.GetNode()->GetType()->IsCallable(),
            "Expected callable " << name);

    auto callable = static_cast<TCallable*>(node.GetNode());
    MKQL_ENSURE_WITH_LOC(location,
            callable->GetType()->GetName() == name,
            "Expected callable " << name);

    return callable;
}

} // namespace NMiniKQL
} // namespace NKikimr
