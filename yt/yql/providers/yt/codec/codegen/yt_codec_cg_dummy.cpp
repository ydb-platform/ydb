#include "yt_codec_cg.h"

#include <util/generic/yexception.h>

namespace NYql {

TString GetYtCodecBitCode() {
    throw yexception() << "Codegen is not available";
}

void YtCodecAddMappings(NCodegen::ICodegen& codegen) {
    Y_UNUSED(codegen);
    throw yexception() << "Codegen is not available";
}

template<bool Flat>
THolder<IYtCodecCgWriter> MakeYtCodecCgWriter(const std::unique_ptr<NCodegen::ICodegen>& codegen, const void* cookie) {
    Y_UNUSED(codegen);
    Y_UNUSED(cookie);
    throw yexception() << "Codegen is not available";
}

template THolder<IYtCodecCgWriter> MakeYtCodecCgWriter<true>(const std::unique_ptr<NCodegen::ICodegen>& codegen, const void* cookie);
template THolder<IYtCodecCgWriter> MakeYtCodecCgWriter<false>(const std::unique_ptr<NCodegen::ICodegen>& codegen, const void* cookie);

THolder<IYtCodecCgReader> MakeYtCodecCgReader(const std::unique_ptr<NCodegen::ICodegen>& codegen,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, const void* cookie) {
    Y_UNUSED(codegen);
    Y_UNUSED(holderFactory);
    Y_UNUSED(cookie);
    throw yexception() << "Codegen is not available";
}

extern "C" void ThrowBadDecimal() {
    throw yexception() << "Codegen is not available";
}

}
