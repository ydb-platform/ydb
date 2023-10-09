#include "functions_metadata.h"

#include <ydb/library/yql/minikql/mkql_function_metadata.h>
#include <ydb/core/protos/scheme_type_operation.pb.h>


namespace NKikimr {
using namespace NMiniKQL;

void SerializeMetadata(const IBuiltinFunctionRegistry& funcRegistry, TString* out)
{
    NKikimrSchemeTypeOperation::TMetadata metadata;
    for (const auto& op : funcRegistry.GetFunctions()) {
        auto protoOp = metadata.AddOperation();
        protoOp->SetName(op.first);
        for (const auto& desc : op.second) {
            auto protoDesc = protoOp->AddDescription();
            for (const auto* arg = desc.ResultAndArgs; arg->SchemeType; ++arg) {
                auto protoArg = protoDesc->AddArg();
                protoArg->SetSchemeType(arg->SchemeType);
                protoArg->SetFlags(arg->Flags);
            }
        }
    }

    Y_PROTOBUF_SUPPRESS_NODISCARD metadata.SerializeToString(out);
}

void DeserializeMetadata(TStringBuf buffer, IBuiltinFunctionRegistry& funcRegistry)
{
    NKikimrSchemeTypeOperation::TMetadata metadata;
    Y_ABORT_UNLESS(metadata.ParseFromArray(buffer.data(), buffer.size()));
    size_t totalArgsToAllocate = 0;
    for (const auto& protoOp : metadata.GetOperation()) {
        for (const auto& protoDesc : protoOp.GetDescription()) {
            totalArgsToAllocate += protoDesc.ArgSize() + 1;
        }
    }

    TFunctionParamMetadataList arguments;
    arguments.resize(totalArgsToAllocate);

    TFunctionsMap functions;
    functions.reserve(metadata.OperationSize());

    size_t argPosition = 0;
    for (const auto& protoOp : metadata.GetOperation()) {
        auto& desc = functions[protoOp.GetName()];
        Y_ABORT_UNLESS(desc.empty());
        desc.reserve(protoOp.DescriptionSize());
        for (const auto& protoDesc : protoOp.GetDescription()) {
            const size_t firstArg = argPosition;
            for (const auto& protoArg : protoDesc.GetArg()) {
                auto& arg = arguments[argPosition++];
                arg.SchemeType = protoArg.GetSchemeType();
                arg.Flags = protoArg.GetFlags();
            }

            ++argPosition; // terminating arg

            desc.push_back(TFunctionDescriptor(&arguments[firstArg], nullptr));
        }
    }

    Y_ABORT_UNLESS(argPosition == arguments.size());

    funcRegistry.RegisterAll(std::move(functions), std::move(arguments));
}

} // namespace NKikimr
