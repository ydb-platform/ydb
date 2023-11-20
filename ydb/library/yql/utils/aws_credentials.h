#pragma once

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/string.h>

namespace NYql {

TString ConvertBasicToAwsToken(const TString& accessKey, const TString& accessSecret);

TString PrepareAwsHeader(const TString& token);

TString ExtractAwsCredentials(TSmallVec<TString>& headers);

}
