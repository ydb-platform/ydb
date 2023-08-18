#pragma once

#include <util/generic/string.h>

namespace LdapMock {

TString EncodeInt(int num);
TString EncodeEnum(int num);
TString EncodeSize(size_t size);
TString EncodeString(const TString& str);
TString EncodeSequence(const TString& sequenceBody);

}
