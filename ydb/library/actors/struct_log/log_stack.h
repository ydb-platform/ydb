#pragma once
#include "create_message.h"
#include "structured_message.h"

#include <ydb/library/actors/core/log_iface.h>

#include <optional>

namespace NActors::NStructuredLog {

class TLogStack {
public:
    static TStructuredMessage& GetTop();
    static void Push(const std::optional<NActors::NLog::EComponent>& component = {});
    static void Pop();
    static NActors::NLog::EComponent GetComponent(NActors::NLog::EComponent defaultComponent = 0);

    class TLogGuard {
    public:
        TLogGuard(const std::optional<NActors::NLog::EComponent>& component = {}) {
            Push(component);
        }

        TLogGuard(const TLogGuard&) = delete;
        TLogGuard(TLogGuard&&) = delete;
        TLogGuard& operator=(const TLogGuard&) = delete;
        TLogGuard& operator=(TLogGuard&&) = delete;

        void* operator new(std::size_t sz) = delete;
        void* operator new[](std::size_t sz) = delete;

        ~TLogGuard() { Pop(); }
    };
};

#define YDB_LOG_CREATE_CONTEXT(...) \
    ::NActors::NStructuredLog::TLogStack::TLogGuard ydblogContextGuard; \
    YDB_LOG_UPDATE_MESSAGE(::NActors::NStructuredLog::TLogStack::GetTop(), __VA_ARGS__)

#define YDB_LOG_CREATE_CONTEXT_COMP(COMP, ...) \
    ::NActors::NStructuredLog::TLogStack::TLogGuard ydblogContextGuard(COMP); \
    YDB_LOG_UPDATE_MESSAGE(::NActors::NStructuredLog::TLogStack::GetTop(), __VA_ARGS__)

#define YDB_LOG_UPDATE_CONTEXT(...) YDB_LOG_UPDATE_MESSAGE(::NActors::NStructuredLog::TLogStack::GetTop(), __VA_ARGS__)
#define YDB_LOG_REMOVE_CONTEXT(...) ::NActors::NStructuredLog::TLogStack::GetTop().RemoveValues({__VA_ARGS__})

}  // namespace NActors::NStructuredLog
