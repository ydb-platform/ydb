#pragma once

#include <util/generic/fwd.h>

namespace google {
namespace protobuf {
    class Message;
} // namespace protobuf
} // namespace google

namespace NYql {

void ProcTitleInit(int argc, const char* argv[]);
void SetProcTitle(const char* title);
void AddProcTitleSuffix(const char* suffix);
const char* GetProcTitle();

TString PbMessageToStr(const google::protobuf::Message& msg);
TString Proto2Yson(const google::protobuf::Message& proto);

} // namespace NYql
