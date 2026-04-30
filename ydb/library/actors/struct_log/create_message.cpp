#include "create_message.h"

namespace NKikimr::NStructuredLog {

namespace {
    thread_local std::vector<TStructuredMessage> BuildMessageStack;
}

TStructuredMessage& TCreateMessageArg::PushBuildMessage() {
    BuildMessageStack.push_back(TStructuredMessage());
    return GetBuildMessage();
}

TStructuredMessage& TCreateMessageArg::GetBuildMessage() {
    return BuildMessageStack.back();
}

TStructuredMessage TCreateMessageArg::PopBuildMessage() {
    auto result = std::move(GetBuildMessage());
    BuildMessageStack.pop_back();
    return result;
}

}
