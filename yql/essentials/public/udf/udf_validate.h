#pragma once
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

namespace NYql {
namespace NUdf {

#define UDF_VALIDATE_MODE(XX) \
    XX(None, 0)                       \
    XX(Lazy, 1)                       \
    XX(Greedy, 2)                     \
    XX(Max, 3)                        \

#define UDF_VALIDATE_POLICY(XX) \
    XX(Fail, 0)                       \
    XX(Exception, 1)                  \
    XX(Max, 2)                        \

enum class EValidateMode : ui8 {
    UDF_VALIDATE_MODE(ENUM_VALUE_GEN)
};

enum class EValidatePolicy : ui8 {
    UDF_VALIDATE_POLICY(ENUM_VALUE_GEN)
};

TString ValidateModeAvailables();
TStringBuf ValidateModeAsStr(EValidateMode validateMode);
EValidateMode ValidateModeByStr(const TString& verifyModeStr);
TStringBuf ValidatePolicyAsStr(EValidatePolicy verifyPolicy);
EValidatePolicy ValidatePolicyByStr(const TString& verifyPolicy);

} // namspace NUdf
} // namspace NYql
