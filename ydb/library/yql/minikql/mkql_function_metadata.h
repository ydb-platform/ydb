#pragma once

#include <ydb/library/yql/public/udf/udf_value.h>

namespace NKikimr {

namespace NMiniKQL {

using TFunctionPtr = NUdf::TUnboxedValuePod (*)(const NUdf::TUnboxedValuePod* args);

struct TFunctionParamMetadata {
    enum EFlags : ui16 {
        FlagIsNullable = 0x01,
    };

    TFunctionParamMetadata() = default;

    TFunctionParamMetadata(NUdf::TDataTypeId schemeType, ui32 flags)
        : SchemeType(schemeType)
        , Flags(flags)
    {}

    bool IsNullable() const {
        return Flags & FlagIsNullable;
    }

    NUdf::TDataTypeId SchemeType = 0;
    ui16 Flags = 0;
};

struct TFunctionDescriptor {
    TFunctionDescriptor() = default;

    TFunctionDescriptor(const TFunctionParamMetadata* resultAndArgs, TFunctionPtr function, void* generator = nullptr)
        : ResultAndArgs(resultAndArgs)
        , Function(function)
        , Generator(generator)
    {}

    const TFunctionParamMetadata* ResultAndArgs = nullptr; // ends with SchemeType zero
    TFunctionPtr Function = nullptr;
    void *Generator = nullptr;
};

using TFunctionParamMetadataList = std::vector<TFunctionParamMetadata>;
using TArgType = std::pair<NUdf::TDataTypeId, bool>; // type with optional flag
using TDescriptionList = std::vector<TFunctionDescriptor>;
using TFunctionsMap = std::unordered_map<TString, TDescriptionList>;

class IBuiltinFunctionRegistry: public TThrRefBase, private TNonCopyable
{
public:
    typedef TIntrusivePtr<IBuiltinFunctionRegistry> TPtr;

    virtual ui64 GetMetadataEtag() const = 0;

    virtual void PrintInfoTo(IOutputStream& out) const = 0;

    virtual void Register(const std::string_view& name, const TFunctionDescriptor& description) = 0;

    virtual bool HasBuiltin(const std::string_view& name) const = 0;

    virtual void RegisterAll(TFunctionsMap&& functions, TFunctionParamMetadataList&& arguments) = 0;

    virtual const TFunctionsMap& GetFunctions() const = 0;

    virtual TFunctionDescriptor GetBuiltin(const std::string_view& name, const std::pair<NUdf::TDataTypeId, bool>* argTypes, size_t argTypesCount) const = 0;
};

}
}
