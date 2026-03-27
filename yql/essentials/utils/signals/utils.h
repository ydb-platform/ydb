#pragma once

#include <util/generic/fwd.h>

namespace google::protobuf { // NOLINT(readability-identifier-naming)
class Message;
} // namespace google::protobuf

namespace NYql {

void ProcTitleInit(int argc, const char** argv);
void SetProcTitle(const char* title);
void AddProcTitleSuffix(const char* suffix);
const char* GetProcTitle();

TString PbMessageToStr(const google::protobuf::Message& msg);
TString Proto2Yson(const google::protobuf::Message& proto);

} // namespace NYql
