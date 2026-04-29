#pragma once
#include "create_message.h"
#include "structured_message.h"

namespace NKikimr::NStructuredLog {

class TLogStack {

public:
    static TStructuredMessage& GetTop();
    static void Push();
    static void Pop();

    class TLogGuard {
    public:
        TLogGuard() {
            Push();
        }

        TLogGuard(const TLogGuard&) = delete;
        TLogGuard(TLogGuard&&) = delete;
        TLogGuard& operator=(const TLogGuard&) = delete;
        TLogGuard& operator=(TLogGuard&&) = delete;

        void* operator new(std::size_t sz) = delete;
        void* operator new[](std::size_t sz) = delete;

        ~TLogGuard() {
            Pop();
        }
    };
};

#define YDBLOG_UPDATE_CONTEXT(...) YDBLOG_UPDATE_MESSAGE(TLogStack::GetTop(), __VA_ARGS__)
#define YDBLOG_REMOVE_CONTEXT(...) TLogStack::GetTop().RemoveValues({__VA_ARGS__})

}
