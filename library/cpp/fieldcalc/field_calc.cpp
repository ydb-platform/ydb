#include <cstdio>

#include <util/str_stl.h>
#include <util/string/subst.h>
#include <util/string/util.h>
#include <util/string/cast.h>
#include <util/stream/printf.h>

#include "field_calc_int.h"

using namespace std;

enum Operators {
    OP_ADD,
    OP_SUBSTRACT,
    OP_MULTIPLY,
    OP_DIVIDE,
    OP_MODULUS,
    OP_REGEXP,
    OP_REGEXP_NOT,
    OP_LEFT_SHIFT,
    OP_RIGHT_SHIFT,
    OP_EQUAL,
    OP_NOT_EQUAL,
    OP_LESS,
    OP_LESS_OR_EQUAL,
    OP_GREATER,
    OP_GREATER_OR_EQUAL,
    OP_XOR,
    OP_BITWISE_OR,
    OP_BITWISE_AND,
    OP_LOGICAL_OR,
    OP_LOGICAL_AND,
    OP_UNARY_NOT,
    OP_UNARY_COMPLEMENT,
    OP_UNARY_MINUS,
    OP_LOG,
    OP_LOG10,
    OP_ROUND,
    OP_ASSIGN,
    OP_QUESTION,
    OP_COLON,

    OP_UNKNOWN,
};

struct calc_op;

struct calc_elem {
    dump_item item;
    char oper;
    int op_prio;
};

struct calc_op {
    dump_item Left, Right;
    char Oper;
    bool force_long;
    bool unary;
    bool is_variable;
    bool string_op; // TODO -> bitop

    // for local vars
    mutable bool calculated;
    mutable eval_res_type result;

    calc_op(calc_elem& left, calc_elem& right)
        : Left(left.item)
        , Right(right.item)
        , Oper(right.oper)
        , is_variable(false)
        , calculated(false)
        , result(false)
    {
        force_long = Oper == OP_XOR || Oper == OP_BITWISE_OR || Oper == OP_BITWISE_AND ||
                     Oper == OP_LOGICAL_OR || Oper == OP_LOGICAL_AND || Oper == OP_UNARY_NOT ||
                     Oper == OP_UNARY_COMPLEMENT || Oper == OP_LEFT_SHIFT || Oper == OP_RIGHT_SHIFT ||
                     Oper == OP_MODULUS;
        unary = Oper == OP_UNARY_NOT || Oper == OP_UNARY_COMPLEMENT || Oper == OP_UNARY_MINUS ||
                Oper == OP_LOG || Oper == OP_LOG10 || Oper == OP_ROUND;
        string_op = IsStringType(Left.type) && IsStringType(Right.type) &&
                    (Oper == OP_REGEXP || Oper == OP_REGEXP_NOT || Oper == OP_EQUAL || Oper == OP_NOT_EQUAL ||
                     Oper == OP_LESS || Oper == OP_LESS_OR_EQUAL || Oper == OP_GREATER || Oper == OP_GREATER_OR_EQUAL);
        if (Oper == OP_REGEXP || Oper == OP_REGEXP_NOT) {
            if (!string_op)
                ythrow yexception() << "calc-expr: regexp requested for non-strings";
            ythrow yexception() << "calc-expr: regexps currently not supported";
        }
    }

    Y_FORCE_INLINE void eval(const char** dd) const {
        if (is_variable) {
            if (!calculated) {
                do_eval(dd);
                calculated = true;
            }
        } else {
            do_eval(dd);
        }
    }

private:
    Y_FORCE_INLINE void do_eval(const char** dd) const;
};

void calc_op::do_eval(const char** dd) const {
    eval_res_type left1 = unary ? (eval_res_type) false : Left.eval(dd);
    if (Oper == OP_QUESTION) {
        left1.to_long();
        if (left1.res_long) {
            result = Right.eval(dd);
        } else {
            result = eval_res_type(); // null
        }
        return;
    } else if (Oper == OP_COLON) {
        if (left1.is_null()) {
            result = Right.eval(dd);
        } else {
            result = left1;
        }
        return;
    }

    if (Y_UNLIKELY(string_op)) {
        TStringBuf left2 = Left.GetStrBuf(dd);
        TStringBuf right2 = Right.GetStrBuf(dd);
        switch (Oper) {
            case OP_REGEXP:
                result = false;
                break;
            case OP_REGEXP_NOT:
                result = false;
                break;
            case OP_EQUAL:
                result = left2 == right2;
                break;
            case OP_NOT_EQUAL:
                result = left2 != right2;
                break;
            case OP_LESS:
                result = left2 < right2;
                break;
            case OP_LESS_OR_EQUAL:
                result = left2 <= right2;
                break;
            case OP_GREATER:
                result = left2 > right2;
                break;
            case OP_GREATER_OR_EQUAL:
                result = left2 >= right2;
                break;
            default:
                assert(false);
        }
        return;
    }

    eval_res_type right1 = Right.eval(dd);
    if (force_long) { // logical ops will be all long
        left1.to_long();
        right1.to_long();
    }
    switch (Oper) {
        case OP_ADD:
            result = left1 + right1;
            break;
        case OP_SUBSTRACT:
            result = left1 - right1;
            break;
        case OP_MULTIPLY:
            result = left1 * right1;
            break;
        case OP_DIVIDE:
            result = left1 / right1;
            break;
        case OP_MODULUS:
            result = left1.res_long ? left1.res_long % right1.res_long : 0;
            break;
        case OP_LEFT_SHIFT:
            result = left1.res_long << right1.res_long;
            break;
        case OP_RIGHT_SHIFT:
            result = left1.res_long >> right1.res_long;
            break;
        case OP_EQUAL:
            result = left1 == right1;
            break;
        case OP_NOT_EQUAL:
            result = !(left1 == right1);
            break;
        case OP_LESS:
            result = left1 < right1;
            break;
        case OP_LESS_OR_EQUAL:
            result = !(right1 < left1);
            break; // <=
        case OP_GREATER:
            result = right1 < left1;
            break;
        case OP_GREATER_OR_EQUAL:
            result = !(left1 < right1);
            break; // >=
        case OP_XOR:
            result = left1.res_long ^ right1.res_long;
            break;
        case OP_BITWISE_OR:
            result = left1.res_long | right1.res_long;
            break;
        case OP_BITWISE_AND:
            result = left1.res_long & right1.res_long;
            break;
        case OP_LOGICAL_OR:
            result = left1.res_long || right1.res_long;
            break;
        case OP_LOGICAL_AND:
            result = left1.res_long && right1.res_long;
            break;
        case OP_UNARY_NOT:
            result = !right1.res_long;
            break;
        case OP_UNARY_COMPLEMENT:
            result = ~right1.res_long;
            break;
        case OP_UNARY_MINUS:
            result = Minus(right1);
            break;
        case OP_LOG:
            result = Log(right1);
            break;
        case OP_LOG10:
            result = Log10(right1);
            break;
        case OP_ROUND:
            result = Round(right1);
            break;
        default:
            assert(false);
    }
}

namespace {
    // copy-paste of fcat(TString)
    // we don't want it to be too slow, yet we don't want do slow down our
    // main functionality, libc fprintf, even a little
    size_t Y_PRINTF_FORMAT(2, 3) fprintf(TString* s, const char* c, ...) {
        TStringOutput so(*s);

        va_list params;
        va_start(params, c);
        const size_t ret = Printf(so, c, params);
        va_end(params);

        return ret;
    }
    size_t Y_PRINTF_FORMAT(2, 3) fprintf(IOutputStream* s, const char* c, ...) {
        va_list params;
        va_start(params, c);
        const size_t ret = Printf(*s, c, params);
        va_end(params);

        return ret;
    }
}

template <class TOut>
void dump_item::print(TOut* p, const char** dd) const {
    const char* d = dd[pack_id];
    const fake* f = reinterpret_cast<const fake*>(d);

    switch (type) {
        case DIT_FAKE_ITEM:
            assert(false);
            break;
        case DIT_MATH_RESULT:
            assert(false);
            break; // must call eval instead
        case DIT_NAME:
            assert(false);
            break; // no op

        case DIT_BOOL_FIELD:
            fprintf(p, *(bool*)(d + field_offset) ? "true" : "false");
            break;
        case DIT_UI8_FIELD:
            fprintf(p, "%u", *(ui8*)(d + field_offset));
            break;
        case DIT_UI16_FIELD:
            fprintf(p, "%u", *(ui16*)(d + field_offset));
            break;
        case DIT_UI32_FIELD:
            fprintf(p, "%u", *(ui32*)(d + field_offset));
            break;
        case DIT_I64_FIELD:
            fprintf(p, "%" PRId64, *(i64*)(d + field_offset));
            break;
        case DIT_UI64_FIELD:
            fprintf(p, "%" PRIu64, *(ui64*)(d + field_offset));
            break;
        case DIT_FLOAT_FIELD:
            fprintf(p, "%.4f", *(float*)(d + field_offset));
            break;
        case DIT_DOUBLE_FIELD:
            fprintf(p, "%.7f", *(double*)(d + field_offset));
            break;
        case DIT_TIME_T32_FIELD:
            fprintf(p, "%ld", (long)*(time_t32*)(d + field_offset));
            break;
        case DIT_PF16UI32_FIELD:
            fprintf(p, "%u", (ui32) * (pf16ui32*)(d + field_offset));
            break;
        case DIT_PF16FLOAT_FIELD:
            fprintf(p, "%.4f", (float)*(pf16float*)(d + field_offset));
            break;
        case DIT_SF16FLOAT_FIELD:
            fprintf(p, "%.4f", (float)*(sf16float*)(d + field_offset));
            break;
        case DIT_STRING_FIELD:
            fprintf(p, "%s", (d + field_offset));
            break;

        case DIT_LONG_CONST:
            fprintf(p, "%ld", long_const);
            break;
        case DIT_FLOAT_CONST:
            fprintf(p, "%.4f", float_const);
            break;
        case DIT_STR_CONST:
            fprintf(p, "%.*s", (int)the_buf.size(), the_buf.data());
            break;

        case DIT_INT_FUNCTION:
            fprintf(p, "%d", (f->*int_fn)());
            break;
        case DIT_FLOAT_FUNCTION:
            fprintf(p, "%.4f", (f->*float_fn)());
            break;
        case DIT_BOOL_FUNCTION:
            fprintf(p, "%d", (f->*bool_fn)());
            break;
        case DIT_STR_FUNCTION:
            fprintf(p, "%s", (f->*str_fn)());
            break;
        case DIT_STRBUF_FUNCTION:
            the_buf.clear();
            fprintf(p, "%s", (f->*strbuf_2_fn)(the_buf, nullptr));
            break;

        case DIT_UI8_EXT_FUNCTION:
            fprintf(p, "%u", (*ui8_ext_fn)(f));
            break;
        case DIT_UI16_EXT_FUNCTION:
            fprintf(p, "%u", (*ui16_ext_fn)(f));
            break;
        case DIT_UI32_EXT_FUNCTION:
            fprintf(p, "%u", (*ui32_ext_fn)(f));
            break;
        case DIT_UI64_EXT_FUNCTION:
            fprintf(p, "%" PRIu64, (*ui64_ext_fn)(f));
            break;

        case DIT_UI8_ENUM_EQ:
            fprintf(p, "%d", *(ui8*)(d + field_offset) == enum_val);
            break;
        case DIT_UI8_ENUM_SET:
            fprintf(p, "%d", !!(*(ui8*)(d + field_offset) & enum_val));
            break;

        case DIT_UI16_ENUM_EQ:
            fprintf(p, "%d", *(ui16*)(d + field_offset) == enum_val);
            break;
        case DIT_UI16_ENUM_SET:
            fprintf(p, "%d", !!(*(ui16*)(d + field_offset) & enum_val));
            break;

        case DIT_UI32_ENUM_EQ:
            fprintf(p, "%d", *(ui32*)(d + field_offset) == enum_val);
            break;
        case DIT_UI32_ENUM_SET:
            fprintf(p, "%d", !!(*(ui32*)(d + field_offset) & enum_val));
            break;

        case DIT_INT_ENUM_FUNCTION_EQ:
            fprintf(p, "%d", (ui32)(f->*int_enum_fn)() == enum_val);
            break;
        case DIT_INT_ENUM_FUNCTION_SET:
            fprintf(p, "%d", !!(ui32)((f->*int_enum_fn)() & enum_val));
            break;

        case DIT_BOOL_FUNC_FIXED_STR:
            fprintf(p, "%u", (ui32)(f->*bool_strbuf_fn)(the_buf));
            break;
        case DIT_UI8_FUNC_FIXED_STR:
            fprintf(p, "%u", (ui32)(f->*ui8_strbuf_fn)(the_buf));
            break;
        case DIT_UI16_FUNC_FIXED_STR:
            fprintf(p, "%u", (ui32)(f->*ui16_strbuf_fn)(the_buf));
            break;
        case DIT_UI32_FUNC_FIXED_STR:
            fprintf(p, "%u", (f->*ui32_strbuf_fn)(the_buf));
            break;
        case DIT_I64_FUNC_FIXED_STR:
            fprintf(p, "%" PRId64, (f->*i64_strbuf_fn)(the_buf));
            break;
        case DIT_UI64_FUNC_FIXED_STR:
            fprintf(p, "%" PRIu64, (f->*ui64_strbuf_fn)(the_buf));
            break;
        case DIT_FLOAT_FUNC_FIXED_STR:
            fprintf(p, "%.4f", (f->*float_strbuf_fn)(the_buf));
            break;
        case DIT_DOUBLE_FUNC_FIXED_STR:
            fprintf(p, "%.7f", (f->*double_strbuf_fn)(the_buf));
            break;

        case DIT_RESOLVE_BY_NAME:
            fprintf(p, "%s", (f->*resolve_fn)(the_buf).data());
            break;

        default:
            assert(false);
            break;
    }
}

// instantiate, just for a case
template void dump_item::print<FILE>(FILE* p, const char** dd) const;
template void dump_item::print<TString>(TString* p, const char** dd) const;
template void dump_item::print<IOutputStream>(IOutputStream* p, const char** dd) const;

TStringBuf dump_item::GetStrBuf(const char** dd) const {
    const char* d = dd[pack_id];
    const fake* f = reinterpret_cast<const fake*>(d);
    switch (type) {
        case DIT_STRING_FIELD:
            return d + field_offset;
        case DIT_STR_CONST:
            return the_buf;
        case DIT_STR_FUNCTION:
            return (f->*str_fn)();
        case DIT_STRBUF_FUNCTION:
            the_buf.clear();
            return (f->*strbuf_2_fn)(the_buf, nullptr);
        case DIT_RESOLVE_BY_NAME:
            return (f->*resolve_fn)(the_buf);
        default:
            assert(false);
            return TStringBuf();
    }
}

// recursive
eval_res_type dump_item::eval(const char** dd) const {
    const char* d = dd[pack_id];
    const fake* f = reinterpret_cast<const fake*>(d);

    switch (type) {
        case DIT_FAKE_ITEM:
            assert(false);
            return (long int)0;
        case DIT_MATH_RESULT:
            this->op->eval(dd);
            return this->op->result;
        case DIT_NAME:
            assert(false);
            return (long int)0;

        case DIT_BOOL_FIELD:
            return (ui32) * (bool*)(d + field_offset);
        case DIT_UI8_FIELD:
            return (ui32) * (ui8*)(d + field_offset);
        case DIT_UI16_FIELD:
            return (ui32) * (ui16*)(d + field_offset);
        case DIT_UI32_FIELD:
            return (ui32) * (ui32*)(d + field_offset);
        case DIT_I64_FIELD:
            return (long)*(i64*)(d + field_offset); // TODO: 64 bit support in calculator?
        case DIT_UI64_FIELD:
            return (long)*(ui64*)(d + field_offset); // TODO: 64 bit support in calculator?
        case DIT_FLOAT_FIELD:
            return (float)*(float*)(d + field_offset);
        case DIT_DOUBLE_FIELD:
            return *(double*)(d + field_offset);
        case DIT_TIME_T32_FIELD:
            return (long)*(time_t32*)(d + field_offset);
        case DIT_PF16UI32_FIELD:
            return (ui32) * (pf16ui32*)(d + field_offset);
        case DIT_PF16FLOAT_FIELD:
            return (float)*(pf16float*)(d + field_offset);
        case DIT_SF16FLOAT_FIELD:
            return (float)*(sf16float*)(d + field_offset);
        case DIT_STRING_FIELD:
            return !!d[field_offset]; // we don't have any string functions, just 0 if empty

        case DIT_LONG_CONST:
            return long_const;
        case DIT_FLOAT_CONST:
            return float_const;
        case DIT_STR_CONST:
            return !!the_buf;

        case DIT_INT_FUNCTION:
            return (long)(f->*int_fn)();
        case DIT_FLOAT_FUNCTION:
            return (float)(f->*float_fn)();
        case DIT_BOOL_FUNCTION:
            return (long)(f->*bool_fn)();
        case DIT_STR_FUNCTION:
            return !!*(f->*str_fn)(); // string -> int
        case DIT_STRBUF_FUNCTION:
            the_buf.clear();
            return !!*(f->*strbuf_2_fn)(the_buf, nullptr); // string -> 0/1

        case DIT_UI8_EXT_FUNCTION:
            return (ui32)(*ui8_ext_fn)(f);
        case DIT_UI16_EXT_FUNCTION:
            return (ui32)(*ui16_ext_fn)(f);
        case DIT_UI32_EXT_FUNCTION:
            return (ui32)(*ui32_ext_fn)(f);
        case DIT_UI64_EXT_FUNCTION:
            return (long)(*ui64_ext_fn)(f); // TODO: 64 bit support in calculator?

        case DIT_UI8_ENUM_EQ:
            return (ui32)(*(ui8*)(d + field_offset) == enum_val);
        case DIT_UI8_ENUM_SET:
            return !!(ui32)(*(ui8*)(d + field_offset) & enum_val);

        case DIT_UI16_ENUM_EQ:
            return (ui32)(*(ui16*)(d + field_offset) == enum_val);
        case DIT_UI16_ENUM_SET:
            return !!(ui32)(*(ui16*)(d + field_offset) & enum_val);

        case DIT_UI32_ENUM_EQ:
            return (ui32)(*(ui32*)(d + field_offset) == enum_val);
        case DIT_UI32_ENUM_SET:
            return !!(ui32)(*(ui32*)(d + field_offset) & enum_val);

        case DIT_INT_ENUM_FUNCTION_EQ:
            return (ui32)((ui32)(f->*int_enum_fn)() == enum_val);
        case DIT_INT_ENUM_FUNCTION_SET:
            return !!(ui32)((ui32)(f->*int_enum_fn)() & enum_val);

        case DIT_BOOL_FUNC_FIXED_STR:
            return (ui32)(f->*bool_strbuf_fn)(the_buf);
        case DIT_UI8_FUNC_FIXED_STR:
            return (ui32)(f->*ui8_strbuf_fn)(the_buf);
        case DIT_UI16_FUNC_FIXED_STR:
            return (ui32)(f->*ui16_strbuf_fn)(the_buf);
        case DIT_UI32_FUNC_FIXED_STR:
            return (ui32)(f->*ui32_strbuf_fn)(the_buf);
        case DIT_I64_FUNC_FIXED_STR:
            return (long)(f->*i64_strbuf_fn)(the_buf);
        case DIT_UI64_FUNC_FIXED_STR:
            return (long)(f->*ui64_strbuf_fn)(the_buf);
        case DIT_FLOAT_FUNC_FIXED_STR:
            return (float)(f->*float_strbuf_fn)(the_buf);
        case DIT_DOUBLE_FUNC_FIXED_STR:
            return (double)(f->*double_strbuf_fn)(the_buf);

        case DIT_RESOLVE_BY_NAME:
            return !!(f->*resolve_fn)(the_buf);

        default:
            assert(false);
            break;
    }

    // unreached
    return eval_res_type(false);
}

void dump_item::set_arrind(int arrind) {
    switch (type) {
        case DIT_BOOL_FIELD:
            field_offset += arrind * sizeof(bool);
            break;
        case DIT_UI8_FIELD:
            field_offset += arrind * sizeof(ui8);
            break;
        case DIT_UI16_FIELD:
            field_offset += arrind * sizeof(ui16);
            break;
        case DIT_UI32_FIELD:
            field_offset += arrind * sizeof(ui32);
            break;
        case DIT_I64_FIELD:
            field_offset += arrind * sizeof(i64);
            break;
        case DIT_UI64_FIELD:
            field_offset += arrind * sizeof(ui64);
            break;
        case DIT_FLOAT_FIELD:
            field_offset += arrind * sizeof(float);
            break;
        case DIT_DOUBLE_FIELD:
            field_offset += arrind * sizeof(double);
            break;
        case DIT_TIME_T32_FIELD:
            field_offset += arrind * sizeof(time_t32);
            break;
        case DIT_PF16UI32_FIELD:
            field_offset += arrind * sizeof(pf16ui32);
            break;
        case DIT_PF16FLOAT_FIELD:
            field_offset += arrind * sizeof(pf16float);
            break;
        case DIT_SF16FLOAT_FIELD:
            field_offset += arrind * sizeof(sf16float);
            break;
        default:
            break;
    }
}

static str_spn FieldNameChars("a-zA-Z0-9_$", true);
static str_spn MathOpChars("-+=*%/&|<>()!~^?:#", true);
static str_spn SpaceChars("\t\n\r ", true);

TFieldCalculatorBase::TFieldCalculatorBase() {
}

TFieldCalculatorBase::~TFieldCalculatorBase() = default;

bool TFieldCalculatorBase::item_by_name(dump_item& it, const char* name) const {
    for (size_t i = 0; i < named_dump_items.size(); i++) {
        const named_dump_item* list = named_dump_items[i].first;
        size_t sz = named_dump_items[i].second;
        for (unsigned int n = 0; n < sz; n++) {
            if (!stricmp(name, list[n].name)) {
                it = list[n].item;
                it.pack_id = i;
                return true;
            }
        }
    }
    return false;
}

bool TFieldCalculatorBase::get_local_var(dump_item& dst, char* var_name) {
    TMap<const char*, dump_item>::const_iterator it = local_vars.find(var_name);
    if (it == local_vars.end()) {
        // New local variable
        dst.type = DIT_LOCAL_VARIABLE;
        dst.local_var_name = pool.append(var_name);
        return false;
    } else {
        dst = it->second;
        return true;
    }
}

char* TFieldCalculatorBase::get_field(dump_item& dst, char* s) {
    if (!stricmp(s, "name")) {
        dst.type = DIT_NAME;
        return s + 4; // leave there 0
    }

    if (*s == '"' || *s == '\'') {
        char* end = strchr(s + 1, *s);
        bool hasEsc = false;
        while (end && end > s + 1 && end[-1] == '\\') {
            end = strchr(end + 1, *s);
            hasEsc = true;
        }
        if (!end)
            ythrow yexception() << "calc-expr: unterminated string constant at " << s;
        dst.type = DIT_STR_CONST;
        dst.the_buf.assign(s + 1, end);
        if (hasEsc)
            SubstGlobal(dst.the_buf, *s == '"' ? "\\\"" : "\\'", *s == '"' ? "\"" : "'");
        dst.set_arrind(0); // just for a case
        return end + 1;
    }

    bool is_number = isdigit((ui8)*s) || (*s == '+' || *s == '-') && isdigit((ui8)s[1]), is_float = false;
    char* end = FieldNameChars.cbrk(s + is_number);
    if (is_number && *end == '.') {
        is_float = true;
        end = FieldNameChars.cbrk(end + 1);
    }
    char* next = SpaceChars.cbrk(end);
    int arr_index = 0;
    bool has_arr_index = false;
    if (*next == '[') {
        arr_index = atoi(next + 1);
        has_arr_index = true;
        next = strchr(next, ']');
        if (!next)
            ythrow yexception() << "calc-expr: No closing ']' for '" << s << "'";
        next = SpaceChars.cbrk(next + 1);
    }
    char end_sav = *end;
    *end = 0;

    if (!item_by_name(dst, s)) {
        if (!is_number) {
            get_local_var(dst, s);
        } else if (is_float) {
            dst = (float)strtod(s, nullptr);
        } else
            dst = strtol(s, nullptr, 10);

        dst.pack_id = 0;
        *end = end_sav;
        return next;
    }

    // check array/not array
    if (has_arr_index && !dst.is_array_field())
        ythrow yexception() << "calc-expr: field " << s << " is not an array";

    //if (!has_arr_index && dst.is_array_field())
    //    yexception("calc-expr: field %s is array, index required", s);

    if (has_arr_index && (arr_index < 0 || arr_index >= dst.arr_length))
        ythrow yexception() << "calc-expr: array index [" << arr_index << "] is out of range for field " << s << " (length is " << dst.arr_length << ")";

    *end = end_sav;
    dst.set_arrind(arr_index);
    return next;
}

// BEGIN Stack calculator functions
inline char* skipspace(char* c, int& bracket_depth) {
    while ((ui8)*c <= ' ' && *c || *c == '(' || *c == ')') {
        if (*c == '(')
            bracket_depth++;
        else if (*c == ')')
            bracket_depth--;
        c++;
    }
    return c;
}

void ensure_defined(const dump_item& item) {
    if (item.type == DIT_LOCAL_VARIABLE) {
        ythrow yexception() << "Usage of non-defined field or local variable '" << item.local_var_name << "'";
    }
}

void TFieldCalculatorBase::emit_op(TVector<calc_op>& ops, calc_elem& left, calc_elem& right) {
    int out_op = ops.size();
    char oper = right.oper;
    ensure_defined(right.item);
    if (oper == OP_ASSIGN) {
        if (left.item.type != DIT_LOCAL_VARIABLE) {
            ythrow yexception() << "Assignment only to local variables is allowed";
        }
        if (local_vars.find(left.item.local_var_name) != local_vars.end()) {
            ythrow yexception() << "Reassignment to the local variable " << left.item.local_var_name << " is not allowed";
        }
        local_vars[left.item.local_var_name] = right.item;
        if (right.item.type == DIT_MATH_RESULT) {
            calc_ops[right.item.arr_ind].is_variable = true;
        }
        left = right;
    } else {
        ensure_defined(left.item);
        ops.push_back(calc_op(left, right));
        left.item.type = DIT_MATH_RESULT;
        left.item.arr_ind = out_op;
    }
}

inline int get_op_prio(char c) {
    switch (c) {
        case OP_ASSIGN:
            return 1;
        case OP_QUESTION:
        case OP_COLON:
            return 2;
        case OP_LOGICAL_OR:
            return 3;
        case OP_LOGICAL_AND:
            return 4;
        case OP_BITWISE_OR:
            return 5;
        case OP_XOR:
            return 6;
        case OP_BITWISE_AND:
            return 7;
        case OP_EQUAL:
        case OP_NOT_EQUAL:
            return 8;
        case OP_LESS:
        case OP_LESS_OR_EQUAL:
        case OP_GREATER:
        case OP_GREATER_OR_EQUAL:
            return 9;
        case OP_LEFT_SHIFT:
        case OP_RIGHT_SHIFT:
            return 10;
        case OP_ADD:
        case OP_SUBSTRACT:
            return 11;
        case OP_MULTIPLY:
        case OP_DIVIDE:
        case OP_MODULUS:
            return 12;
        case OP_REGEXP:
        case OP_REGEXP_NOT:
            return 13;
        case OP_UNARY_NOT:
        case OP_UNARY_COMPLEMENT:
        case OP_UNARY_MINUS:
        case OP_LOG:
        case OP_LOG10:
        case OP_ROUND:
            return 14;
        default:
            return 0;
    }
}

Operators get_oper(char*& c, bool unary_op_near) {
    Operators cur_oper = OP_UNKNOWN;
    switch (*c++) {
        case '&':
            if (*c == '&')
                cur_oper = OP_LOGICAL_AND, c++;
            else
                cur_oper = OP_BITWISE_AND;
            break;
        case '|':
            if (*c == '|')
                cur_oper = OP_LOGICAL_OR, c++;
            else
                cur_oper = OP_BITWISE_OR;
            break;
        case '<':
            if (*c == '=')
                cur_oper = OP_LESS_OR_EQUAL, c++;
            else if (*c == '<')
                cur_oper = OP_LEFT_SHIFT, c++;
            else
                cur_oper = OP_LESS;
            break;
        case '>':
            if (*c == '=')
                cur_oper = OP_GREATER_OR_EQUAL, c++;
            else if (*c == '>')
                cur_oper = OP_RIGHT_SHIFT, c++;
            else
                cur_oper = OP_GREATER;
            break;
        case '!':
            if (*c == '=')
                cur_oper = OP_NOT_EQUAL, c++;
            else if (*c == '~')
                cur_oper = OP_REGEXP_NOT, c++;
            else
                cur_oper = OP_UNARY_NOT;
            break;
        case '=':
            if (*c == '=')
                cur_oper = OP_EQUAL, c++;
            else if (*c == '~')
                cur_oper = OP_REGEXP, c++;
            else
                cur_oper = OP_ASSIGN;
            break;
        case '-':
            if (unary_op_near)
                cur_oper = OP_UNARY_MINUS;
            else
                cur_oper = OP_SUBSTRACT;
            break;
        case '#':
            if (!strncmp(c, "LOG#", 4)) {
                cur_oper = OP_LOG;
                c += 4;
            } else if (!strncmp(c, "LOG10#", 6)) {
                cur_oper = OP_LOG10;
                c += 6;
            } else if (!strncmp(c, "ROUND#", 6)) {
                cur_oper = OP_ROUND;
                c += 6;
            }
            break;
        case '+':
            cur_oper = OP_ADD;
            break;
        case '*':
            cur_oper = OP_MULTIPLY;
            break;
        case '/':
            cur_oper = OP_DIVIDE;
            break;
        case '%':
            cur_oper = OP_MODULUS;
            break;
        case '^':
            cur_oper = OP_XOR;
            break;
        case '~':
            cur_oper = OP_UNARY_COMPLEMENT;
            break;
        case '?':
            cur_oper = OP_QUESTION;
            break;
        case ':':
            cur_oper = OP_COLON;
            break;
    }
    return cur_oper;
}
// END Stack calculator functions

void TFieldCalculatorBase::Compile(char** field_names, int field_count) {
    out_el = 0, out_cond = 0;
    autoarray<dump_item>(field_count).swap(printouts);
    autoarray<dump_item>(field_count).swap(conditions);
    local_vars.clear();

    // parse arguments into calculator's "pseudo-code"
    for (int el = 0; el < field_count; el++) {
        char* c = field_names[el];
        bool is_expr = !!*MathOpChars.brk(c), is_cond = *c == '?';
        if (is_cond)
            c++;
        if (!is_expr && !is_cond) {
            get_field(printouts[out_el], c);
            ensure_defined(printouts[out_el]);
            ++out_el;
            continue;
        } else { // Stack Calculator
            const int maxstack = 64;
            calc_elem fstack[maxstack]; // calculator's stack
            int bdepth = 0;             // brackets depth
            int stack_cur = -1;
            bool unary_op_near = false; // indicates that the next operator in unary
            bool had_assignment_out_of_brackets = false;
            int uop_seq = 0; // maintains right-to left order for unary operators
            while (*(c = skipspace(c, bdepth))) {
                /** https://wiki.yandex.ru/JandeksPoisk/Antispam/OwnersData/attselect#calc */
                //printf("1.%i c = '%s'\n", unary_op_near, c);
                Operators cur_oper = OP_UNKNOWN;
                int op_prio = 0;
                if (stack_cur >= 0) {
                    cur_oper = get_oper(c, unary_op_near);
                    op_prio = get_op_prio(cur_oper);
                    if (!op_prio)
                        ythrow yexception() << "calc-expr: Unsupported operator '" << c[-1] << "'";
                    op_prio += bdepth * 256 + uop_seq;
                    if (unary_op_near)
                        uop_seq += 20;
                    while (op_prio <= fstack[stack_cur].op_prio && stack_cur > 0) {
                        emit_op(calc_ops, fstack[stack_cur - 1], fstack[stack_cur]);
                        stack_cur--;
                    }
                }
                //printf("2.%i c = '%s'\n", unary_op_near, c);
                had_assignment_out_of_brackets |= (bdepth == 0 && cur_oper == OP_ASSIGN);
                c = skipspace(c, bdepth);
                unary_op_near = *c == '-' && !isdigit((ui8)c[1]) || *c == '~' || (*c == '!' && c[1] != '=') ||
                                !strncmp(c, "#LOG#", 5) || !strncmp(c, "#LOG10#", 7) || !strncmp(c, "#ROUND#", 7);
                if (!unary_op_near)
                    uop_seq = 0;
                if (stack_cur >= maxstack - 1)
                    ythrow yexception() << "calc-expr: Math eval stack overflow!\n";
                stack_cur++;
                fstack[stack_cur].oper = cur_oper;
                fstack[stack_cur].op_prio = op_prio;
                //printf("3.%i c = '%s'\n", unary_op_near, c);
                if (unary_op_near)
                    fstack[stack_cur].item = dump_item();
                else
                    c = get_field(fstack[stack_cur].item, c);
            }
            while (stack_cur > 0) {
                emit_op(calc_ops, fstack[stack_cur - 1], fstack[stack_cur]);
                stack_cur--;
            }
            ensure_defined(fstack[0].item);
            if (is_cond) {
                if (had_assignment_out_of_brackets)
                    ythrow yexception() << "Assignment in condition. (Did you mean '==' instead of '='?)";
                if (fstack[0].item.type != DIT_FAKE_ITEM) // Skip empty conditions: "?()".
                    conditions[out_cond++] = fstack[0].item;
            } else if (!had_assignment_out_of_brackets) {
                printouts[out_el++] = fstack[0].item;
            }
        }
    }
    // calc_ops will not grow any more, so arr_ind -> op
    for (int n = 0; n < out_cond; n++)
        conditions[n].rewrite_op(calc_ops.data());
    for (int n = 0; n < out_el; n++)
        printouts[n].rewrite_op(calc_ops.data());
    for (auto& local_var : local_vars) {
        local_var.second.rewrite_op(calc_ops.data());
    }
    for (int n = 0; n < (int)calc_ops.size(); n++) {
        calc_ops[n].Left.rewrite_op(calc_ops.data());
        calc_ops[n].Right.rewrite_op(calc_ops.data());
    }
}

void dump_item::rewrite_op(const calc_op* ops) {
    if (type == DIT_MATH_RESULT)
        op = ops + arr_ind;
}

void TFieldCalculatorBase::MarkLocalVarsAsUncalculated() {
    for (auto& local_var : local_vars) {
        if (local_var.second.type == DIT_MATH_RESULT) {
            local_var.second.op->calculated = false;
        }
    }
}

bool TFieldCalculatorBase::Cond(const char** d) {
    MarkLocalVarsAsUncalculated();
    for (int n = 0; n < out_cond; n++) {
        /** https://wiki.yandex.ru/JandeksPoisk/Antispam/OwnersData/attselect#conditions */
        eval_res_type res = conditions[n].eval(d);
        bool is_true = res.type == 0 ? !!res.res_ui32 : res.type == 1 ? !!res.res_long : !!res.res_dbl;
        if (!is_true)
            return false;
    }
    return true;
}

bool TFieldCalculatorBase::CondById(const char** d, int condNumber) {
    MarkLocalVarsAsUncalculated();
    if (condNumber >= out_cond)
        return false;
    eval_res_type res = conditions[condNumber].eval(d);
    bool is_true = res.type == 0 ? !!res.res_ui32 : res.type == 1 ? !!res.res_long : !!res.res_dbl;
    if (!is_true)
        return false;
    return true;
}

void TFieldCalculatorBase::Print(FILE* p, const char** d, const char* Name) {
    for (int n = 0; n < out_el; n++) {
        if (printouts[n].type == DIT_NAME) {
            fprintf(p, "%s", Name);
        } else if (printouts[n].type == DIT_MATH_RESULT) { // calculate
            eval_res_type res = printouts[n].eval(d);
            switch (res.type) {
                case 0:
                    fprintf(p, "%u", res.res_ui32);
                    break;
                case 1:
                    fprintf(p, "%ld", res.res_long);
                    break;
                case 2:
                    fprintf(p, "%f", res.res_dbl);
                    break;
            }
        } else {
            printouts[n].print(p, d);
        }
        fprintf(p, n != out_el - 1 ? "\t" : "\n");
    }
}

void TFieldCalculatorBase::CalcAll(const char** d, TVector<float>& result) const {
    result.clear();
    for (int n = 0; n < out_el; ++n) {
        if (printouts[n].type == DIT_MATH_RESULT || printouts[n].type == DIT_FLOAT_FIELD) {
            eval_res_type res = printouts[n].eval(d);
            result.push_back(res.res_dbl);
        }
    }
}

void TFieldCalculatorBase::SelfTest() {
    if (out_el < 1)
        ythrow yexception() << "Please specify conditions for test mode";
    const char* dummy = "";
    eval_res_type res = printouts[0].eval(&dummy);
    switch (res.type) {
        case 0:
            printf("%u\n", res.res_ui32);
            break;
        case 1:
            printf("%ld\n", res.res_long);
            break;
        case 2:
            printf("%f\n", res.res_dbl);
            break;
    }
}

void TFieldCalculatorBase::PrintDiff(const char* rec1, const char* rec2) {
    for (size_t n = 0; n < named_dump_items[0].second; n++) {
        const dump_item& field = named_dump_items[0].first[n].item;
        if (!field.is_field())
            continue; // not really a field
        for (int ind = 0, arrsz = field.is_array_field() ? field.arr_length : 1; ind < arrsz; ind++) {
            intptr_t sav_field_offset = field.field_offset;
            const_cast<dump_item&>(field).set_arrind(ind);
            if (field.eval(&rec1) == field.eval(&rec2)) {
                const_cast<dump_item&>(field).field_offset = sav_field_offset;
                continue;
            }
            if (field.is_array_field())
                printf("\t%s[%i]: ", named_dump_items[0].first[n].name, ind);
            else
                printf("\t%s: ", named_dump_items[0].first[n].name);
            field.print(stdout, &rec1);
            printf(" -> ");
            field.print(stdout, &rec2);
            const_cast<dump_item&>(field).field_offset = sav_field_offset;
        }
    }
}

void TFieldCalculatorBase::DumpAll(IOutputStream& s, const char** d, const TStringBuf& delim) {
    bool firstPrinted = false;
    for (size_t k = 0; k < named_dump_items.size(); k++) {
        const named_dump_item* fields = named_dump_items[k].first;
        size_t numFields = named_dump_items[k].second;
        const char* obj = d[k];
        for (size_t n = 0; n < numFields; n++) {
            const dump_item& field = fields[n].item;
            if (!field.is_field())
                continue;
            for (int ind = 0, arrsz = field.is_array_field() ? field.arr_length : 1; ind < arrsz; ind++) {
                if (firstPrinted)
                    s << delim;
                else
                    firstPrinted = true;
                s << fields[n].name;
                if (field.is_array_field())
                    Printf(s, "[%i]", ind);
                s << "=";
                intptr_t sav_field_offset = field.field_offset;
                const_cast<dump_item&>(field).set_arrind(ind);
                field.print(&s, &obj);
                const_cast<dump_item&>(field).field_offset = sav_field_offset;
            }
        }
    }
}
