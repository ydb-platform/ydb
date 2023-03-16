#pragma once
#include <util/generic/string.h>

namespace NKikimr::NMiniKQL {

class TNode;
struct TRuntimeNode;

} // namespace NKikimr::NMiniKQL

namespace NFq {

TString PrettyPrintMkqlProgram(const NKikimr::NMiniKQL::TNode* node, size_t initialIndentChars = 0);
TString PrettyPrintMkqlProgram(const NKikimr::NMiniKQL::TRuntimeNode& node, size_t initialIndentChars = 0);
TString PrettyPrintMkqlProgram(const TString& rawProgram, size_t initialIndentChars = 0);

} // namespace NFq
