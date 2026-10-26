#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <vector>

namespace NYql::NDqFunction {

    using TDqFunctionType = TString;

    // Public SQL signature only. Connection resolution (including ACL/credentials)
    // still goes through ResolveFunction, even when the signature is already known.
    struct TServiceFunctionSignature {
        struct TField {
            TString Name;
            TString Type;
        };
        std::vector<TField> Input;
        std::vector<TField> Output;
        bool Batch = false;
        ui32 MaxBatchRows = 1;
    };

    struct TDqFunctionDescription {
        TDqFunctionType Type;
        TString FunctionName;
        TString Connection;

        TString InvokeUrl;
        TString Status;
        // Optional internal transform implementation, independent of the public function type.
        TString TransformType;
    };

    struct TDqFunctionDescriptionHash {
        size_t operator()(const TDqFunctionDescription& key) const {
            std::hash<TString> Hash;
            auto initHash = CombineHashes(Hash(key.Type), Hash(key.FunctionName));
            if (!key.Connection.empty()) {
                initHash = CombineHashes(Hash(key.Connection), initHash);
            }
            return initHash;
        }
    };

    struct TDqFunctionDescriptionEq {
        bool operator()(const TDqFunctionDescription& a, const TDqFunctionDescription& b) const {
            return a.Type == b.Type && a.FunctionName == b.FunctionName && a.Connection == b.Connection;
        }
    };

    using TDqFunctionsSet = THashSet<TDqFunctionDescription, TDqFunctionDescriptionHash, TDqFunctionDescriptionEq>;

    class TDqFunctionResolver: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<TDqFunctionResolver>;

        TDqFunctionResolver();

        TDqFunctionDescription AddFunction(const TDqFunctionType& type, const TString& functionName, const TString& connection);
        std::vector<TDqFunctionDescription> FunctionsToResolve();

    private:
        TDqFunctionsSet FunctionsDescription;
    };

} // namespace NYql::NDqFunction
