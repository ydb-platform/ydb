#include "spark_functions.h"

#include <util/generic/hash.h>
#include <util/generic/singleton.h>
#include <util/string/cast.h>

namespace NYql::NSpark {
namespace {

class TFunctionRegistry {
public:
    const TSparkFunction* Find(const TString& name) const {
        return Functions_.FindPtr(name);
    }

    void Enumerate(const std::function<void(const TString& name, const TString& bindingName)>& callback) const {
        for (const auto& [name, bindingName] : Functions_) {
            callback(name, bindingName.BindingName);
        }
    }

private:
    const THashMap<TString, TSparkFunction> Functions_ = {
        {"concat", {.BindingName = "", .MinArgs = 2, .MaxArgs = Max<ui32>()}},
        {"raise_error", {.BindingName = "", .MinArgs = 1, .MaxArgs = 1}},
        {"nullif", {.BindingName = "nullif", .MinArgs = 2, .MaxArgs = 2}},
        {"isnull", {.BindingName = "isnull", .MinArgs = 1, .MaxArgs = 1}},
        {"isnotnull", {.BindingName = "isnotnull", .MinArgs = 1, .MaxArgs = 1}},
        {"ifnull", {.BindingName = "ifnull", .MinArgs = 2, .MaxArgs = 2}},
        {"nvl", {.BindingName = "ifnull", .MinArgs = 2, .MaxArgs = 2}}, // ifnull
        {"nvl2", {.BindingName = "nvl2", .MinArgs = 3, .MaxArgs = 3}},
        {"startswith", {.BindingName = "startswith", .MinArgs = 2, .MaxArgs = 2}},
        {"endswith", {.BindingName = "endswith", .MinArgs = 2, .MaxArgs = 2}},
        {"contains", {.BindingName = "contains", .MinArgs = 2, .MaxArgs = 2}},
        {"instr", {.BindingName = "instr", .MinArgs = 2, .MaxArgs = 2}},
        {"locate", {.BindingName = "locate", .MinArgs = 2, .MaxArgs = 3}},
        {"levenshtein", {.BindingName = "levenshtein", .MinArgs = 2, .MaxArgs = 3}},
        {"base64", {.BindingName = "base64", .MinArgs = 1, .MaxArgs = 1}},
        {"hex", {.BindingName = "hex", .MinArgs = 1, .MaxArgs = 1}},
        {"bin", {.BindingName = "bin", .MinArgs = 1, .MaxArgs = 1}},
        {"chr", {.BindingName = "chr", .MinArgs = 1, .MaxArgs = 1}},
        {"unhex", {.BindingName = "unhex", .MinArgs = 1, .MaxArgs = 1}},
        {"md5", {.BindingName = "md5", .MinArgs = 1, .MaxArgs = 1}},
        {"sha1", {.BindingName = "sha1", .MinArgs = 1, .MaxArgs = 1}},
        {"unbase64", {.BindingName = "unbase64", .MinArgs = 1, .MaxArgs = 1}},
        {"reverse", {.BindingName = "reverse", .MinArgs = 1, .MaxArgs = 1}},
        {"substring", {.BindingName = "substring", .MinArgs = 2, .MaxArgs = 3}},
        {"left", {.BindingName = "left", .MinArgs = 2, .MaxArgs = 2}},
        {"right", {.BindingName = "right", .MinArgs = 2, .MaxArgs = 2}},
        {"substr", {.BindingName = "substring", .MinArgs = 2, .MaxArgs = 3}}, // substring
        {"lpad", {.BindingName = "lpad", .MinArgs = 2, .MaxArgs = 3}},
        {"rpad", {.BindingName = "rpad", .MinArgs = 2, .MaxArgs = 3}},
        {"replace", {.BindingName = "replace", .MinArgs = 2, .MaxArgs = 3}},
        {"translate", {.BindingName = "translate", .MinArgs = 3, .MaxArgs = 3}},
        {"trim", {.BindingName = "trim", .MinArgs = 1, .MaxArgs = 2}},
        {"ltrim", {.BindingName = "ltrim", .MinArgs = 1, .MaxArgs = 2}},
        {"rtrim", {.BindingName = "rtrim", .MinArgs = 1, .MaxArgs = 2}},
        {"repeat", {.BindingName = "repeat", .MinArgs = 2, .MaxArgs = 2}},
        {"space", {.BindingName = "space", .MinArgs = 1, .MaxArgs = 1}},
        {"split_part", {.BindingName = "split_part", .MinArgs = 3, .MaxArgs = 3}},
        {"find_in_set", {.BindingName = "find_in_set", .MinArgs = 2, .MaxArgs = 2}},
        {"lower", {.BindingName = "lower", .MinArgs = 1, .MaxArgs = 1}},
        {"lcase", {.BindingName = "lower", .MinArgs = 1, .MaxArgs = 1}}, // lower
        {"upper", {.BindingName = "upper", .MinArgs = 1, .MaxArgs = 1}},
        {"ucase", {.BindingName = "upper", .MinArgs = 1, .MaxArgs = 1}}, // upper
        {"length", {.BindingName = "length", .MinArgs = 1, .MaxArgs = 1}},
        {"len", {.BindingName = "length", .MinArgs = 1, .MaxArgs = 1}},              // length
        {"char_length", {.BindingName = "length", .MinArgs = 1, .MaxArgs = 1}},      // length
        {"character_length", {.BindingName = "length", .MinArgs = 1, .MaxArgs = 1}}, // length
        {"octet_length", {.BindingName = "octet_length", .MinArgs = 1, .MaxArgs = 1}},
        {"bit_length", {.BindingName = "bit_length", .MinArgs = 1, .MaxArgs = 1}},
        {"abs", {.BindingName = "abs", .MinArgs = 1, .MaxArgs = 1}},
        {"isnan", {.BindingName = "isnan", .MinArgs = 1, .MaxArgs = 1}},
        {"nanvl", {.BindingName = "nanvl", .MinArgs = 2, .MaxArgs = 2}},
        {"bit_count", {.BindingName = "bit_count", .MinArgs = 1, .MaxArgs = 1}},
        {"bit_get", {.BindingName = "bit_get", .MinArgs = 2, .MaxArgs = 2}},
        {"getbit", {.BindingName = "bit_get", .MinArgs = 2, .MaxArgs = 2}}, // bit_get
        {"factorial", {.BindingName = "factorial", .MinArgs = 1, .MaxArgs = 1}},
        {"positive", {.BindingName = "positive", .MinArgs = 1, .MaxArgs = 1}},
        {"negative", {.BindingName = "negative", .MinArgs = 1, .MaxArgs = 1}},
        {"try_mod", {.BindingName = "try_mod", .MinArgs = 2, .MaxArgs = 2}},
        {"mod", {.BindingName = "mod", .MinArgs = 2, .MaxArgs = 2}},
        {"sqrt", {.BindingName = "sqrt", .MinArgs = 1, .MaxArgs = 1}},
        {"ceil", {.BindingName = "ceil", .MinArgs = 1, .MaxArgs = 1}},
        {"ceiling", {.BindingName = "ceil", .MinArgs = 1, .MaxArgs = 1}}, // ceil
        {"floor", {.BindingName = "floor", .MinArgs = 1, .MaxArgs = 1}},
        {"round", {.BindingName = "round", .MinArgs = 1, .MaxArgs = 2}},
        {"cbrt", {.BindingName = "cbrt", .MinArgs = 1, .MaxArgs = 1}},
        {"cot", {.BindingName = "cot", .MinArgs = 1, .MaxArgs = 1}},
        {"sec", {.BindingName = "sec", .MinArgs = 1, .MaxArgs = 1}},
        {"csc", {.BindingName = "csc", .MinArgs = 1, .MaxArgs = 1}},
        {"acos", {.BindingName = "acos", .MinArgs = 1, .MaxArgs = 1}},
        {"acosh", {.BindingName = "acosh", .MinArgs = 1, .MaxArgs = 1}},
        {"asin", {.BindingName = "asin", .MinArgs = 1, .MaxArgs = 1}},
        {"atan", {.BindingName = "atan", .MinArgs = 1, .MaxArgs = 1}},
        {"cosh", {.BindingName = "cosh", .MinArgs = 1, .MaxArgs = 1}},
        {"sinh", {.BindingName = "sinh", .MinArgs = 1, .MaxArgs = 1}},
        {"tanh", {.BindingName = "tanh", .MinArgs = 1, .MaxArgs = 1}},
        {"sin", {.BindingName = "sin", .MinArgs = 1, .MaxArgs = 1}},
        {"cos", {.BindingName = "cos", .MinArgs = 1, .MaxArgs = 1}},
        {"tan", {.BindingName = "tan", .MinArgs = 1, .MaxArgs = 1}},
        {"exp", {.BindingName = "exp", .MinArgs = 1, .MaxArgs = 1}},
        {"expm1", {.BindingName = "expm1", .MinArgs = 1, .MaxArgs = 1}},
        {"e", {.BindingName = "e", .MinArgs = 0, .MaxArgs = 0}},
        {"pi", {.BindingName = "pi", .MinArgs = 0, .MaxArgs = 0}},
        {"log1p", {.BindingName = "log1p", .MinArgs = 1, .MaxArgs = 1}},
        {"log", {.BindingName = "log", .MinArgs = 1, .MaxArgs = 2}},
        {"ln", {.BindingName = "ln", .MinArgs = 1, .MaxArgs = 1}}, // log
        {"log10", {.BindingName = "log10", .MinArgs = 1, .MaxArgs = 1}},
        {"log2", {.BindingName = "log2", .MinArgs = 1, .MaxArgs = 1}},
        {"asinh", {.BindingName = "asinh", .MinArgs = 1, .MaxArgs = 1}},
        {"atanh", {.BindingName = "atanh", .MinArgs = 1, .MaxArgs = 1}},
        {"rint", {.BindingName = "rint", .MinArgs = 1, .MaxArgs = 1}},
        {"sign", {.BindingName = "sign", .MinArgs = 1, .MaxArgs = 1}},
        {"signum", {.BindingName = "sign", .MinArgs = 1, .MaxArgs = 1}}, // sign
        {"degrees", {.BindingName = "degrees", .MinArgs = 1, .MaxArgs = 1}},
        {"radians", {.BindingName = "radians", .MinArgs = 1, .MaxArgs = 1}},
        {"atan2", {.BindingName = "atan2", .MinArgs = 2, .MaxArgs = 2}},
        {"hypot", {.BindingName = "hypot", .MinArgs = 2, .MaxArgs = 2}},
        {"pow", {.BindingName = "pow", .MinArgs = 2, .MaxArgs = 2}},
        {"power", {.BindingName = "pow", .MinArgs = 2, .MaxArgs = 2}}, // pow
        {"pmod", {.BindingName = "pmod", .MinArgs = 2, .MaxArgs = 2}},
    };
};

} // namespace

const TSparkFunction* FindFunction(const TString& name) {
    return Singleton<TFunctionRegistry>()->Find(name);
}

TString TSparkFunction::GetBindingName(ui32 argumentCount) const {
    TString bindingName = BindingName;
    if (MinArgs != MaxArgs) {
        bindingName += '_';
        bindingName += ToString(argumentCount);
    }
    return bindingName;
}

void EnumerateFunctions(const std::function<void(const TString& name, const TString& bindingName)>& callback) {
    Singleton<TFunctionRegistry>()->Enumerate(callback);
}

} // namespace NYql::NSpark
