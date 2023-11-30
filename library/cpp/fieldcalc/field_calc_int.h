#pragma once

#include <cmath>

#include <util/system/defaults.h>
#include <util/system/yassert.h>
#include <util/memory/alloc.h>
#include <util/generic/yexception.h>

#include "lossy_types.h"
#include "field_calc.h"

// eval_res_type
struct eval_res_type {
    union {
        ui32 res_ui32;
        long res_long;
        double res_dbl;
    };
    int type;
    eval_res_type(ui32 v)
        : res_ui32(v)
        , type(0)
    {
    }
    eval_res_type(long v)
        : res_long(v)
        , type(1)
    {
    }
    eval_res_type(bool v)
        : res_long(v)
        , type(1)
    {
    }
    eval_res_type(double v)
        : res_dbl(v)
        , type(2)
    {
    }
    // a special null value for ternary operator
    explicit eval_res_type()
        : type(3)
    {
    }
    operator ui32() const;
    operator long() const;
    operator double() const;
    void to_long();
    bool is_null() const;
};

inline bool eval_res_type::is_null() const {
    return type == 3;
}

inline void eval_res_type::to_long() {
    if (type == 0)
        res_long = res_ui32;
    else if (type == 2)
        res_long = (long)res_dbl;
    type = 1;
}

inline eval_res_type::operator ui32() const {
    assert(type == 0);
    return res_ui32;
}

inline eval_res_type::operator long() const {
    assert(type == 0 || type == 1);
    return type == 1 ? res_long : res_ui32;
}

inline eval_res_type::operator double() const {
    return type == 2 ? res_dbl : type == 1 ? (double)res_long : (double)res_ui32;
}

inline eval_res_type operator+(const eval_res_type& a, const eval_res_type& b) {
    switch (std::max(a.type, b.type)) {
        case 0:
            return (ui32)a + (ui32)b;
        case 1:
            return (long)a + (long)b;
        /*case 2*/ default:
            return (double)a + (double)b;
    }
}

inline eval_res_type operator-(const eval_res_type& a, const eval_res_type& b) {
    switch (std::max(a.type, b.type)) {
        case 0:
        case 1:
            return (long)a - (long)b;
        /*case 2*/ default:
            return (double)a - (double)b;
    }
}

inline eval_res_type Minus(const eval_res_type& a) {
    switch (a.type) {
        case 0:
            return -(long)a.res_ui32;
        case 1:
            return -a.res_long;
        /*case 2*/ default:
            return -a.res_dbl;
    }
}

inline eval_res_type Log(const eval_res_type& a) {
    switch (a.type) {
        case 0:
            return log(a.res_ui32);
        case 1:
            return log(a.res_long);
        /*case 2*/ default:
            return log(a.res_dbl);
    }
}

inline eval_res_type Log10(const eval_res_type& a) {
    switch (a.type) {
        case 0:
            return log10(a.res_ui32);
        case 1:
            return log10(a.res_long);
        /*case 2*/ default:
            return log10(a.res_dbl);
    }
}

inline eval_res_type Round(const eval_res_type& a) {
    switch (a.type) {
        case 0:
            return a.res_ui32;
        case 1:
            return a.res_long;
        /*case 2*/ default:
            return round(a.res_dbl);
    }
}

inline bool operator==(const eval_res_type& a, const eval_res_type& b) {
    switch (std::max(a.type, b.type)) {
        case 0:
            return (ui32)a == (ui32)b;
        case 1:
            return (long)a == (long)b;
        /*case 2*/ default:
            return (double)a == (double)b;
    }
}

inline bool operator<(const eval_res_type& a, const eval_res_type& b) {
    switch (std::max(a.type, b.type)) {
        case 0:
            return (ui32)a < (ui32)b;
        case 1:
            return (long)a < (long)b;
        /*case 2*/ default:
            return (double)a < (double)b;
    }
}

inline eval_res_type operator*(const eval_res_type& a, const eval_res_type& b) {
    switch (std::max(a.type, b.type)) {
        case 0:
            return (ui32)a * (ui32)b;
        case 1:
            return (long)a * (long)b;
        /*case 2*/ default:
            return (double)a * (double)b;
    }
}

inline double operator/(const eval_res_type& a, const eval_res_type& b) {
    double a1 = a, b1 = b;
    if (b1 == 0) {
        if (a1 == 0)
            return 0.;                             // assume that a should be 0
        ythrow yexception() << "Division by zero"; // TODO: show parameter names
    }
    return a1 / b1;
}

// dump_item
enum EDumpItemType {
    DIT_FAKE_ITEM,   // fake item - value never used
    DIT_MATH_RESULT, // eval result
    DIT_NAME,

    DIT_FIELDS_START, // Start of item types for real fields

    DIT_BOOL_FIELD,
    DIT_UI8_FIELD,
    DIT_UI16_FIELD,
    DIT_UI32_FIELD,
    DIT_I64_FIELD,
    DIT_UI64_FIELD,
    DIT_FLOAT_FIELD,
    DIT_DOUBLE_FIELD,
    DIT_TIME_T32_FIELD,
    DIT_PF16UI32_FIELD,
    DIT_PF16FLOAT_FIELD,
    DIT_SF16FLOAT_FIELD,
    DIT_STRING_FIELD, // new

    DIT_FIELDS_END, // End of item types for real fields

    DIT_LONG_CONST,
    DIT_FLOAT_CONST,
    DIT_STR_CONST,

    DIT_INT_FUNCTION,
    DIT_FLOAT_FUNCTION,
    DIT_BOOL_FUNCTION,
    DIT_STR_FUNCTION,    // new
    DIT_STRBUF_FUNCTION, // new

    DIT_UI8_EXT_FUNCTION,
    DIT_UI16_EXT_FUNCTION,
    DIT_UI32_EXT_FUNCTION,
    DIT_UI64_EXT_FUNCTION,

    DIT_UI8_ENUM_EQ,
    DIT_UI8_ENUM_SET,
    DIT_UI16_ENUM_EQ,
    DIT_UI16_ENUM_SET,
    DIT_UI32_ENUM_EQ,
    DIT_UI32_ENUM_SET,
    DIT_INT_ENUM_FUNCTION_EQ,
    DIT_INT_ENUM_FUNCTION_SET,

    DIT_BOOL_FUNC_FIXED_STR,
    DIT_UI8_FUNC_FIXED_STR,
    DIT_UI16_FUNC_FIXED_STR,
    DIT_UI32_FUNC_FIXED_STR,
    DIT_I64_FUNC_FIXED_STR,
    DIT_UI64_FUNC_FIXED_STR,
    DIT_FLOAT_FUNC_FIXED_STR,
    DIT_DOUBLE_FUNC_FIXED_STR,

    DIT_RESOLVE_BY_NAME, //new - for external functions

    DIT_LOCAL_VARIABLE
};

inline bool IsStringType(EDumpItemType type) {
    return type == DIT_STRING_FIELD || type == DIT_STR_CONST || type == DIT_STR_FUNCTION || type == DIT_STRBUF_FUNCTION || type == DIT_RESOLVE_BY_NAME;
}

struct fake {};

struct calc_op;

typedef int (fake::*int_fn_t)() const;
typedef float (fake::*float_fn_t)() const;
typedef bool (fake::*bool_fn_t)() const;
typedef ui16 (fake::*ui16_fn_t)() const;
typedef ui32 (fake::*ui32_fn_t)() const;
typedef bool (fake::*bool_strbuf_fn_t)(const TStringBuf&) const;     // string -> bool
typedef ui8 (fake::*ui8_strbuf_fn_t)(const TStringBuf&) const;       // string -> ui8
typedef ui16 (fake::*ui16_strbuf_fn_t)(const TStringBuf&) const;     // string -> ui16
typedef ui32 (fake::*ui32_strbuf_fn_t)(const TStringBuf&) const;     // string -> ui32
typedef i64 (fake::*i64_strbuf_fn_t)(const TStringBuf&) const;       // string -> i64
typedef ui64 (fake::*ui64_strbuf_fn_t)(const TStringBuf&) const;     // string -> ui64
typedef float (fake::*float_strbuf_fn_t)(const TStringBuf&) const;   // string -> float
typedef double (fake::*double_strbuf_fn_t)(const TStringBuf&) const; // string -> double
typedef const char* (fake::*str_fn_t)() const;
typedef const char* (fake::*strbuf_2_fn_t)(TString& buf, const char* nul) const;
typedef TStringBuf (fake::*resolve_fn_t)(const TStringBuf&) const; // string -> string, $var -> "value"

// note: we can not reuse the above signatures, calling conventions may differ
typedef ui8 (*ui8_ext_fn_t)(const fake*);
typedef ui16 (*ui16_ext_fn_t)(const fake*);
typedef ui32 (*ui32_ext_fn_t)(const fake*);
typedef ui64 (*ui64_ext_fn_t)(const fake*);

struct dump_item {
    EDumpItemType type;
    int pack_id = 0;

    union {
        // fields
        intptr_t field_offset;

        // constants
        long long_const;
        float float_const;

        // functions
        int_fn_t int_fn;
        float_fn_t float_fn;
        bool_fn_t bool_fn;
        str_fn_t str_fn;
        strbuf_2_fn_t strbuf_2_fn;
        resolve_fn_t resolve_fn;

        bool_strbuf_fn_t bool_strbuf_fn;
        ui8_strbuf_fn_t ui8_strbuf_fn;
        ui16_strbuf_fn_t ui16_strbuf_fn;
        ui32_strbuf_fn_t ui32_strbuf_fn;
        i64_strbuf_fn_t i64_strbuf_fn;
        ui64_strbuf_fn_t ui64_strbuf_fn;
        float_strbuf_fn_t float_strbuf_fn;
        double_strbuf_fn_t double_strbuf_fn;

        ui8_ext_fn_t ui8_ext_fn;
        ui16_ext_fn_t ui16_ext_fn;
        ui32_ext_fn_t ui32_ext_fn;
        ui64_ext_fn_t ui64_ext_fn;

        // enum
        int_fn_t int_enum_fn;

        // for DIT_MATH_RESULT
        const calc_op* op;
    };

    // for enum
    ui32 enum_val;

    // for local vars, also used to mark accessor functions to use them in dump
    const char* local_var_name = nullptr;

    int arr_ind; // externally initialized!
    int arr_length;

    mutable TString the_buf; // buffer for string function, string constants also here

    // Ctors
    dump_item()
        : type(DIT_FAKE_ITEM)
        , field_offset(0)
    {
    }

    dump_item(bool* ptr, int arrlen = 0)
        : type(DIT_BOOL_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }
    dump_item(ui8* ptr, int arrlen = 0)
        : type(DIT_UI8_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }
    dump_item(ui16* ptr, int arrlen = 0)
        : type(DIT_UI16_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }
    dump_item(ui32* ptr, int arrlen = 0)
        : type(DIT_UI32_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }
    dump_item(i64* ptr, int arrlen = 0)
        : type(DIT_I64_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }
    dump_item(ui64* ptr, int arrlen = 0)
        : type(DIT_UI64_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }
    dump_item(float* ptr, int arrlen = 0)
        : type(DIT_FLOAT_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }
    dump_item(double* ptr, int arrlen = 0)
        : type(DIT_DOUBLE_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }
    dump_item(time_t32* ptr, int arrlen = 0)
        : type(DIT_TIME_T32_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }
    dump_item(pf16ui32* ptr, int arrlen = 0)
        : type(DIT_PF16UI32_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }
    dump_item(pf16float* ptr, int arrlen = 0)
        : type(DIT_PF16FLOAT_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }
    dump_item(sf16float* ptr, int arrlen = 0)
        : type(DIT_SF16FLOAT_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }
    dump_item(char* ptr, int arrlen = 0)
        : type(DIT_STRING_FIELD)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , arr_length(arrlen)
    {
    }

    dump_item(long val)
        : type(DIT_LONG_CONST)
        , long_const(val)
    {
    }
    dump_item(float val)
        : type(DIT_FLOAT_CONST)
        , float_const(val)
    {
    }
    dump_item(TString& val)
        : type(DIT_STR_CONST)
        , the_buf(val)
    {
    }

    dump_item(int_fn_t fn)
        : type(DIT_INT_FUNCTION)
        , int_fn(fn)
    {
    }
    dump_item(float_fn_t fn)
        : type(DIT_FLOAT_FUNCTION)
        , float_fn(fn)
    {
    }
    dump_item(bool_fn_t fn)
        : type(DIT_BOOL_FUNCTION)
        , bool_fn(fn)
    {
    }
    dump_item(bool_strbuf_fn_t fn, const char* name)
        : type(DIT_BOOL_FUNC_FIXED_STR)
        , bool_strbuf_fn(fn)
        , the_buf(name)
    {
    }
    dump_item(ui8_strbuf_fn_t fn, const char* name)
        : type(DIT_UI8_FUNC_FIXED_STR)
        , ui8_strbuf_fn(fn)
        , the_buf(name)
    {
    }
    dump_item(ui16_strbuf_fn_t fn, const char* name)
        : type(DIT_UI16_FUNC_FIXED_STR)
        , ui16_strbuf_fn(fn)
        , the_buf(name)
    {
    }
    dump_item(ui32_strbuf_fn_t fn, const char* name)
        : type(DIT_UI32_FUNC_FIXED_STR)
        , ui32_strbuf_fn(fn)
        , the_buf(name)
    {
    }
    dump_item(i64_strbuf_fn_t fn, const char* name)
        : type(DIT_I64_FUNC_FIXED_STR)
        , i64_strbuf_fn(fn)
        , the_buf(name)
    {
    }
    dump_item(ui64_strbuf_fn_t fn, const char* name)
        : type(DIT_UI64_FUNC_FIXED_STR)
        , ui64_strbuf_fn(fn)
        , the_buf(name)
    {
    }
    dump_item(float_strbuf_fn_t fn, const char* name)
        : type(DIT_FLOAT_FUNC_FIXED_STR)
        , float_strbuf_fn(fn)
        , the_buf(name)
    {
    }
    dump_item(double_strbuf_fn_t fn, const char* name)
        : type(DIT_DOUBLE_FUNC_FIXED_STR)
        , double_strbuf_fn(fn)
        , the_buf(name)
    {
    }
    dump_item(str_fn_t fn)
        : type(DIT_STR_FUNCTION)
        , str_fn(fn)
    {
    }
    dump_item(strbuf_2_fn_t fn)
        : type(DIT_STRBUF_FUNCTION)
        , strbuf_2_fn(fn)
    {
    }

    dump_item(ui8_ext_fn_t fn, const char* lvn = nullptr)
        : type(DIT_UI8_EXT_FUNCTION)
        , ui8_ext_fn(fn)
        , local_var_name(lvn)
    {
    }
    dump_item(ui16_ext_fn_t fn, const char* lvn = nullptr)
        : type(DIT_UI16_EXT_FUNCTION)
        , ui16_ext_fn(fn)
        , local_var_name(lvn)
    {
    }
    dump_item(ui32_ext_fn_t fn, const char* lvn = nullptr)
        : type(DIT_UI32_EXT_FUNCTION)
        , ui32_ext_fn(fn)
        , local_var_name(lvn)
    {
    }
    dump_item(ui64_ext_fn_t fn, const char* lvn = nullptr)
        : type(DIT_UI64_EXT_FUNCTION)
        , ui64_ext_fn(fn)
        , local_var_name(lvn)
    {
    }

    dump_item(ui8* ptr, ui32 val, bool bitset)
        : type(bitset ? DIT_UI8_ENUM_SET : DIT_UI8_ENUM_EQ)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , enum_val(val)
    {
    }

    dump_item(ui16* ptr, ui32 val, bool bitset)
        : type(bitset ? DIT_UI16_ENUM_SET : DIT_UI16_ENUM_EQ)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , enum_val(val)
    {
    }

    dump_item(ui32* ptr, ui32 val, bool bitset)
        : type(bitset ? DIT_UI32_ENUM_SET : DIT_UI32_ENUM_EQ)
        , field_offset(reinterpret_cast<intptr_t>(ptr))
        , enum_val(val)
    {
    }

    dump_item(int_fn_t fn, ui32 val, bool bitset)
        : type(bitset ? DIT_INT_ENUM_FUNCTION_SET : DIT_INT_ENUM_FUNCTION_EQ)
        , int_enum_fn(fn)
        , enum_val(val)
    {
    }

    dump_item(resolve_fn_t fn, const char* name)
        : type(DIT_RESOLVE_BY_NAME)
        , resolve_fn(fn)
        , the_buf(name)
    {
    } //name of variable saved in the_buf

    // Functions
    template <class TOut> // implemented for FILE*, TString* (appends) and IOutputStream*
    void print(TOut* p, const char** dd) const;
    TStringBuf GetStrBuf(const char** dd) const; // for char-types only!
    eval_res_type eval(const char** dd) const;
    void set_arrind(int arrind);
    void rewrite_op(const calc_op* ops);

    bool is_accessor_func() const {
        return type >= DIT_INT_FUNCTION && type <= DIT_UI64_EXT_FUNCTION && local_var_name;
    }

    bool is_field() const {
        return type > DIT_FIELDS_START && type < DIT_FIELDS_END || is_accessor_func();
    }

    bool is_array_field() const {
        return is_field() && arr_length > 0;
    }
};

// named_dump_item
struct named_dump_item {
    const char* name;
    dump_item item;
};
