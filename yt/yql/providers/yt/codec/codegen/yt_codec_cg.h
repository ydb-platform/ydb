#pragma once
#include <util/generic/string.h>

namespace llvm {
    class Function;
}

namespace NKikimr {
namespace NMiniKQL {
    class TType;
    class THolderFactory;
}

}

namespace NYql {

namespace NUdf {
class TUnboxedValue;
class TUnboxedValuePod;
}

namespace NCodegen {
    class ICodegen;
}

namespace NCommon {
    class TInputBuf;
    class TOutputBuf;
}

TString GetYtCodecBitCode();
void YtCodecAddMappings(NCodegen::ICodegen& codegen);

class IYtCodecCgWriter {
public:
    virtual ~IYtCodecCgWriter() = default;
    virtual void AddField(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags) = 0;
    virtual llvm::Function* Build() = 0;
};

template<bool Flat>
THolder<IYtCodecCgWriter> MakeYtCodecCgWriter(const std::unique_ptr<NCodegen::ICodegen>& codegen,
    const void* cookie = nullptr);

class IYtCodecCgReader {
public:
    virtual ~IYtCodecCgReader() = default;
    virtual void AddField(NKikimr::NMiniKQL::TType* type, const NYql::NUdf::TUnboxedValuePod& defValue, ui64 nativeYtTypeFlags) = 0;
    virtual void SkipField(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags) = 0;
    virtual void SkipOther() = 0;
    virtual void SkipVirtual() = 0;
    virtual llvm::Function* Build() = 0;
};

THolder<IYtCodecCgReader> MakeYtCodecCgReader(const std::unique_ptr<NCodegen::ICodegen>& codegen,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, const void* cookie = nullptr);

extern "C" void ThrowBadDecimal();

}
