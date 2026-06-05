#pragma once
#include "create_message.h"
#include "structured_message.h"

namespace NActors::NStructuredLog {

class TLogStack {
public:
    static TStructuredMessage& GetTop();
    static void Push();
    static void Pop();

    class TLogGuard {
    public:
        TLogGuard() { Push(); }

        TLogGuard(const TLogGuard&) = delete;
        TLogGuard(TLogGuard&&) = delete;
        TLogGuard& operator=(const TLogGuard&) = delete;
        TLogGuard& operator=(TLogGuard&&) = delete;

        void* operator new(std::size_t sz) = delete;
        void* operator new[](std::size_t sz) = delete;

        ~TLogGuard() { Pop(); }
    };
};

#define YDB_LOG_UPDATE_CONTEXT(...) YDB_LOG_UPDATE_MESSAGE(TLogStack::GetTop(), __VA_ARGS__)
#define YDB_LOG_REMOVE_CONTEXT(...) TLogStack::GetTop().RemoveValues({__VA_ARGS__})

}  // namespace NActors::NStructuredLog
