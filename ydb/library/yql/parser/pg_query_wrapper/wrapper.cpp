#include "wrapper.h"
extern "C" {
#include "contrib/pg_query.h"
#include "contrib/src/pg_query_internal.h"
}

#include <util/string/builder.h>
#include <util/generic/noncopyable.h>
#include <util/generic/scope.h>
#include <util/generic/stack.h>
#include <util/system/tls.h>

extern "C" {

#define OUT_TYPE(typename, typename_c) PgQuery__##typename_c*

struct RawStmt;
void _outRawStmt(OUT_TYPE(RawStmt, RawStmt) out_node, const RawStmt *node);

void _outNodeImpl(PgQuery__Node* out, const void *obj);

protobuf_c_boolean field_is_zeroish(const ProtobufCFieldDescriptor *field, const void *member);
size_t sizeof_elt_in_repeated_array(ProtobufCType type);

typedef void(*out_func)(void*, void*);

}

namespace NYql {

namespace {

struct TOutNodeStack {
    TStack<std::pair<PgQuery__Node*, const void*>> Stack;
    TStack<std::tuple<void*, void*, out_func>> StackSpecific;
};

Y_POD_THREAD(TOutNodeStack*) OutNodeStack = nullptr;

}

void PGParse(const TString& input, IPGParseEvents& events) {
    MemoryContext ctx = NULL;
    PgQueryInternalParsetreeAndError parsetree_and_error;

    ctx = pg_query_enter_memory_context();
    Y_DEFER {
        pg_query_exit_memory_context(ctx);
    };

    parsetree_and_error = pg_query_raw_parse(input.c_str());
    Y_DEFER {
        if (parsetree_and_error.error) {
            pg_query_free_error(parsetree_and_error.error);
        }

        free(parsetree_and_error.stderr_buffer);
    };

    if (parsetree_and_error.error) {
        TPosition position(1, 1);
        TTextWalker walker(position);
        size_t distance = Min(size_t(parsetree_and_error.error->cursorpos), input.Size());
        for (size_t i = 0; i < distance; ++i) {
            walker.Advance(input[i]);
        }

        events.OnError(TIssue(position, TString(parsetree_and_error.error->message)));
    } else {
        PgQuery__ParseResult parse_result = PG_QUERY__PARSE_RESULT__INIT;
        const ListCell *lc;
        int i = 0;

        parse_result.version = PG_VERSION_NUM;

        TOutNodeStack tls;
        OutNodeStack = &tls;
        Y_DEFER {
            OutNodeStack = nullptr;
        };

        if (parsetree_and_error.tree == NULL) {
            parse_result.n_stmts = 0;
            parse_result.stmts = NULL;
        } else {
            parse_result.n_stmts = list_length(parsetree_and_error.tree);
            parse_result.stmts = (PgQuery__RawStmt **)palloc(sizeof(PgQuery__RawStmt*) * parse_result.n_stmts);
            foreach(lc, parsetree_and_error.tree)
            {
                parse_result.stmts[i] = (PgQuery__RawStmt *)palloc(sizeof(PgQuery__RawStmt));
                pg_query__raw_stmt__init(parse_result.stmts[i]);
                _outRawStmt(parse_result.stmts[i], (const RawStmt*)lfirst(lc));
                i++;
            }
        }

        for (;;) {
            if (!tls.Stack.empty()) {
                auto top = tls.Stack.top();
                tls.Stack.pop();
                _outNodeImpl(top.first, top.second);
                continue;
            }

            if (!tls.StackSpecific.empty()) {
                auto top = tls.StackSpecific.top();
                tls.StackSpecific.pop();
                std::get<2>(top)(std::get<0>(top), std::get<1>(top));
                continue;
            }

            break;
        }

        events.OnResult(&parse_result);
    }
}

#define ASSERT_IS_MESSAGE_DESCRIPTOR(desc) \
    assert((desc)->magic == PROTOBUF_C__MESSAGE_DESCRIPTOR_MAGIC)

#define ASSERT_IS_MESSAGE(message) \
    ASSERT_IS_MESSAGE_DESCRIPTOR((message)->descriptor)

struct TMessageContext {
    const ProtobufCMessage * Message = nullptr;
    ui32 Field = 0;
    ui32 RepeatIndex = 0;
    ui64 Id = 0;
};

using TMessageContextStack = TStack<TMessageContext>;

bool PrintRequiredField(TMessageContextStack& stack, const ProtobufCFieldDescriptor *field,
    const void *member, IOutputStream& out, ui64& nextId) {
    if (field->type != PROTOBUF_C_TYPE_MESSAGE || !stack.top().Id) {
        out << field->name << ": ";
    }

    switch (field->type) {
    case PROTOBUF_C_TYPE_SINT32:
        out << *(const int32_t *)member;
        break;
    case PROTOBUF_C_TYPE_ENUM: {
        auto value = *(const int32_t *)member;
        out << protobuf_c_enum_descriptor_get_value((const ProtobufCEnumDescriptor *)field->descriptor, value)->name;
        break;
    }
    case PROTOBUF_C_TYPE_INT32:
        out << *(const int32_t *)member;
        break;
    case PROTOBUF_C_TYPE_UINT32:
        out << *(const uint32_t *)member;
        break;
    case PROTOBUF_C_TYPE_SINT64:
        out << *(const int64_t *)member;
        break;
    case PROTOBUF_C_TYPE_INT64:
    case PROTOBUF_C_TYPE_UINT64:
        out << *(const uint64_t *)member;
        break;
    case PROTOBUF_C_TYPE_SFIXED32:
    case PROTOBUF_C_TYPE_FIXED32:
        out << *(const uint32_t *)member;
        break;
    case PROTOBUF_C_TYPE_FLOAT:
        out << *(const float *)member;
        break;
    case PROTOBUF_C_TYPE_SFIXED64:
    case PROTOBUF_C_TYPE_FIXED64:
        out << *(const uint64_t *)member;
        break;
    case PROTOBUF_C_TYPE_DOUBLE:
        out << *(const double *)member;
        break;
    case PROTOBUF_C_TYPE_BOOL:
        out << (*(const protobuf_c_boolean *)member ? "true" : "false");
        break;
    case PROTOBUF_C_TYPE_STRING: {
        auto str = *(char *const *)member;
        out << (str ? str : "");
        break;
    }
    case PROTOBUF_C_TYPE_BYTES: {
        auto bd = (const ProtobufCBinaryData *)member;
        out.Write(bd->data, bd->len);
        break;
    }
    case PROTOBUF_C_TYPE_MESSAGE: {
        if (!stack.top().Id) {
            stack.top().Id = ++nextId;
            out << "> #" << stack.top().Id << "\n";
            stack.push({ *(ProtobufCMessage* const *)member, 0, 0, 0 });
            return false;
        } else {
            out << "< #" << stack.top().Id << "\n";
            stack.top().Id = 0;
            return true;
        }
    }
    default:
        Y_UNREACHABLE();
    }

    out << "\n";
    return true;
}

bool PrintOptionalField(TMessageContextStack& stack, const ProtobufCFieldDescriptor *field, const protobuf_c_boolean has, const void *member, IOutputStream& out, ui64& nextId) {
    if (field->type == PROTOBUF_C_TYPE_MESSAGE ||
        field->type == PROTOBUF_C_TYPE_STRING) {
        const void *ptr = *(const void * const *)member;
        if (ptr == NULL || ptr == field->default_value)
            return true;
    }
    else {
        if (!has)
            return true;
    }

    return PrintRequiredField(stack, field, member, out, nextId);
}

bool PrintUnlabeledField(TMessageContextStack& stack, const ProtobufCFieldDescriptor *field, const void *member, IOutputStream& out, ui64& nextId) {
    if (field_is_zeroish(field, member)) {
        return true;
    }

    return PrintRequiredField(stack, field, member, out, nextId);
}

bool PrintRepeatedField(TMessageContextStack& stack, const ProtobufCFieldDescriptor *field, size_t count, const void *member, IOutputStream& out, ui64& nextId) {
    void *array = *(void * const *)member;
    if (count == 0)
        return true;

    unsigned siz = sizeof_elt_in_repeated_array(field->type);

    array = (char *)array + siz * stack.top().RepeatIndex;
    while (stack.top().RepeatIndex < count) {
        if (!PrintRequiredField(stack, field, array, out, nextId)) {
            return false;
        }

        array = (char *)array + siz;
        ++stack.top().RepeatIndex;
        stack.top().Id = 0;
    }

    return true;
}

bool PrintOneOf(TMessageContextStack& stack, const ProtobufCFieldDescriptor *field, uint32_t oneof_case, const void *member, IOutputStream& out, ui64& nextId) {
    if (oneof_case != field->id) {
        return true;
    }

    if (field->type == PROTOBUF_C_TYPE_MESSAGE ||
        field->type == PROTOBUF_C_TYPE_STRING)
    {
        const void *ptr = *(const void * const *)member;
        if (ptr == NULL || ptr == field->default_value)
            return true;
    }

    return PrintRequiredField(stack, field, member, out, nextId);
}

bool PrintCProtoImpl(TMessageContextStack& stack, IOutputStream& out, ui64& nextId) {
    auto message = stack.top().Message;
    ASSERT_IS_MESSAGE(message);
    while (stack.top().Field < message->descriptor->n_fields) {
        const ProtobufCFieldDescriptor *field = message->descriptor->fields + stack.top().Field;
        const void *member = ((const char *) message) + field->offset;

        /*
        * It doesn't hurt to compute qmember (a pointer to the
        * quantifier field of the structure), but the pointer is only
        * valid if the field is:
        *  - a repeated field, or
        *  - a field that is part of a oneof
        *  - an optional field that isn't a pointer type
        * (Meaning: not a message or a string).
        */
        const void *qmember =
        ((const char *) message) + field->quantifier_offset;

        if (field->label == PROTOBUF_C_LABEL_REQUIRED) {
            if (!PrintRequiredField(stack, field, member, out, nextId)) {
                return false;
            }
        } else if ((field->label == PROTOBUF_C_LABEL_OPTIONAL ||
            field->label == PROTOBUF_C_LABEL_NONE) &&
            (0 != (field->flags & PROTOBUF_C_FIELD_FLAG_ONEOF))) {
            if (!PrintOneOf(stack, field, *(const uint32_t *)qmember, member, out, nextId)) {
                return false;
            }
        } else if (field->label == PROTOBUF_C_LABEL_OPTIONAL) {
            if (!PrintOptionalField(stack, field, *(const protobuf_c_boolean *)qmember, member, out, nextId)) {
                return false;
            }
        } else if (field->label == PROTOBUF_C_LABEL_NONE) {
            if (!PrintUnlabeledField(stack, field, member, out, nextId)) {
                return false;
            }
        } else {
            if (!PrintRepeatedField(stack, field, *(const size_t *)qmember, member, out, nextId)) {
                return false;
            }
        }

        stack.top().Field++;
        stack.top().RepeatIndex = 0;
        stack.top().Id = 0;
    }

    return true;
}

void PrintCProto(const ProtobufCMessage *message, IOutputStream& out) {
    ui64 nextId = 0;
    TMessageContextStack stack;
    stack.push({ message, 0, 0, 0 });
    while (!stack.empty()) {
        if (PrintCProtoImpl(stack, out, nextId)) {
            stack.pop();
        }
    }
}

}

extern "C" {

int register_out_node(PgQuery__Node* out, const void *obj) {
    using namespace NYql;

    if (!OutNodeStack) {
        return 0;
    }

    OutNodeStack->Stack.push({ out, obj });
    return 1;
}

int register_specific_node(void* out, void* node, out_func func) {
    using namespace NYql;

    if (!OutNodeStack) {
        return 0;
    }

    OutNodeStack->StackSpecific.push({ out, node, func });
    return 1;
}

}

