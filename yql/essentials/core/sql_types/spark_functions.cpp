#include "spark_functions.h"

#include <util/generic/hash.h>
#include <util/generic/singleton.h>

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
        {"nvl", {.BindingName = "ifnull", .MinArgs = 2, .MaxArgs = 2}},
        {"nvl2", {.BindingName = "nvl2", .MinArgs = 3, .MaxArgs = 3}},
        {"startswith", {.BindingName = "startswith", .MinArgs = 2, .MaxArgs = 2}},
        {"endswith", {.BindingName = "endswith", .MinArgs = 2, .MaxArgs = 2}},
        {"contains", {.BindingName = "contains", .MinArgs = 2, .MaxArgs = 2}},
        {"instr", {.BindingName = "instr", .MinArgs = 2, .MaxArgs = 2}},
        {"base64", {.BindingName = "base64", .MinArgs = 1, .MaxArgs = 1}},
        {"hex", {.BindingName = "hex", .MinArgs = 1, .MaxArgs = 1}},
        {"unhex", {.BindingName = "unhex", .MinArgs = 1, .MaxArgs = 1}},
        {"md5", {.BindingName = "md5", .MinArgs = 1, .MaxArgs = 1}},
        {"sha1", {.BindingName = "sha1", .MinArgs = 1, .MaxArgs = 1}},
        {"unbase64", {.BindingName = "unbase64", .MinArgs = 1, .MaxArgs = 1}},
        {"reverse", {.BindingName = "reverse", .MinArgs = 1, .MaxArgs = 1}},
        {"substring", {.BindingName = "substring", .MinArgs = 2, .MaxArgs = 3}},
        {"left", {.BindingName = "left", .MinArgs = 2, .MaxArgs = 2}},
        {"right", {.BindingName = "right", .MinArgs = 2, .MaxArgs = 2}},
        {"substr", {.BindingName = "substring", .MinArgs = 2, .MaxArgs = 3}},
        {"lpad", {.BindingName = "lpad", .MinArgs = 2, .MaxArgs = 3}},
        {"rpad", {.BindingName = "rpad", .MinArgs = 2, .MaxArgs = 3}},
        {"replace", {.BindingName = "replace", .MinArgs = 2, .MaxArgs = 3}},
        {"lower", {.BindingName = "lower", .MinArgs = 1, .MaxArgs = 1}},
        {"lcase", {.BindingName = "lower", .MinArgs = 1, .MaxArgs = 1}},
        {"upper", {.BindingName = "upper", .MinArgs = 1, .MaxArgs = 1}},
        {"ucase", {.BindingName = "upper", .MinArgs = 1, .MaxArgs = 1}},
        {"length", {.BindingName = "length", .MinArgs = 1, .MaxArgs = 1}},
        {"len", {.BindingName = "length", .MinArgs = 1, .MaxArgs = 1}},
        {"char_length", {.BindingName = "length", .MinArgs = 1, .MaxArgs = 1}},
        {"character_length", {.BindingName = "length", .MinArgs = 1, .MaxArgs = 1}},
        {"octet_length", {.BindingName = "octet_length", .MinArgs = 1, .MaxArgs = 1}},
        {"bit_length", {.BindingName = "bit_length", .MinArgs = 1, .MaxArgs = 1}},
        {"sqrt", {.BindingName = "sqrt", .MinArgs = 1, .MaxArgs = 1}},
        {"cbrt", {.BindingName = "cbrt", .MinArgs = 1, .MaxArgs = 1}},
        {"acos", {.BindingName = "acos", .MinArgs = 1, .MaxArgs = 1}},
        {"asin", {.BindingName = "asin", .MinArgs = 1, .MaxArgs = 1}},
        {"atan", {.BindingName = "atan", .MinArgs = 1, .MaxArgs = 1}},
        {"cosh", {.BindingName = "cosh", .MinArgs = 1, .MaxArgs = 1}},
        {"sinh", {.BindingName = "sinh", .MinArgs = 1, .MaxArgs = 1}},
        {"tanh", {.BindingName = "tanh", .MinArgs = 1, .MaxArgs = 1}},
        {"sin", {.BindingName = "sin", .MinArgs = 1, .MaxArgs = 1}},
        {"cos", {.BindingName = "cos", .MinArgs = 1, .MaxArgs = 1}},
        {"tan", {.BindingName = "tan", .MinArgs = 1, .MaxArgs = 1}},
        {"exp", {.BindingName = "exp", .MinArgs = 1, .MaxArgs = 1}},
        {"e", {.BindingName = "e", .MinArgs = 0, .MaxArgs = 0}},
        {"pi", {.BindingName = "pi", .MinArgs = 0, .MaxArgs = 0}},
        {"log", {.BindingName = "log", .MinArgs = 1, .MaxArgs = 1}},
        {"ln", {.BindingName = "log", .MinArgs = 1, .MaxArgs = 1}},
        {"log10", {.BindingName = "log10", .MinArgs = 1, .MaxArgs = 1}},
        {"log2", {.BindingName = "log2", .MinArgs = 1, .MaxArgs = 1}},
        {"asinh", {.BindingName = "asinh", .MinArgs = 1, .MaxArgs = 1}},
        {"rint", {.BindingName = "rint", .MinArgs = 1, .MaxArgs = 1}},
        {"sign", {.BindingName = "sign", .MinArgs = 1, .MaxArgs = 1}},
        {"signum", {.BindingName = "sign", .MinArgs = 1, .MaxArgs = 1}},
        {"degrees", {.BindingName = "degrees", .MinArgs = 1, .MaxArgs = 1}},
        {"radians", {.BindingName = "radians", .MinArgs = 1, .MaxArgs = 1}},
        {"atan2", {.BindingName = "atan2", .MinArgs = 2, .MaxArgs = 2}},
        {"hypot", {.BindingName = "hypot", .MinArgs = 2, .MaxArgs = 2}},
        {"pow", {.BindingName = "pow", .MinArgs = 2, .MaxArgs = 2}},
        {"power", {.BindingName = "pow", .MinArgs = 2, .MaxArgs = 2}},
    };
};

} // namespace

const TSparkFunction* FindFunction(const TString& name) {
    return Singleton<TFunctionRegistry>()->Find(name);
}

void EnumerateFunctions(const std::function<void(const TString& name, const TString& bindingName)>& callback) {
    Singleton<TFunctionRegistry>()->Enumerate(callback);
}

} // namespace NYql::NSpark
