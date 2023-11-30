#pragma once

#include "actor.h"
#include "executor_thread.h"

#include <util/system/defaults.h>

#define HFuncCtx(TEvType, HandleFunc, Ctx)                                          \
    case TEvType::EventType: {                                                      \
        typename TEvType::TPtr* x = reinterpret_cast<typename TEvType::TPtr*>(&ev); \
        HandleFunc(*x, Ctx);                                                        \
        break;                                                                      \
    }

#define HFunc(TEvType, HandleFunc)                                                  \
    case TEvType::EventType: {                                                      \
        typename TEvType::TPtr* x = reinterpret_cast<typename TEvType::TPtr*>(&ev); \
        HandleFunc(*x, this->ActorContext()); \
        break;                                                                      \
    }

#define hFunc(TEvType, HandleFunc)                                                  \
    case TEvType::EventType: {                                                      \
        typename TEvType::TPtr* x = reinterpret_cast<typename TEvType::TPtr*>(&ev); \
        HandleFunc(*x);                                                             \
        break;                                                                      \
    }

#define HFuncTraced(TEvType, HandleFunc)                          \
    case TEvType::EventType: {                                    \
        TRACE_EVENT_TYPE(Y_STRINGIZE(TEvType));                      \
        TEvType::TPtr* x = reinterpret_cast<TEvType::TPtr*>(&ev); \
        HandleFunc(*x, this->ActorContext());             \
        break;                                                    \
    }

#define hFuncTraced(TEvType, HandleFunc)                                            \
    case TEvType::EventType: {                                                      \
        TRACE_EVENT_TYPE(Y_STRINGIZE(TEvType));                                        \
        typename TEvType::TPtr* x = reinterpret_cast<typename TEvType::TPtr*>(&ev); \
        HandleFunc(*x);                                                             \
        break;                                                                      \
    }

#define HTemplFunc(TEvType, HandleFunc)                                             \
    case TEvType::EventType: {                                                      \
        typename TEvType::TPtr* x = reinterpret_cast<typename TEvType::TPtr*>(&ev); \
        HandleFunc(*x, this->ActorContext());              \
        break;                                                                      \
    }

#define hTemplFunc(TEvType, HandleFunc)                                             \
    case TEvType::EventType: {                                                      \
        typename TEvType::TPtr* x = reinterpret_cast<typename TEvType::TPtr*>(&ev); \
        HandleFunc(*x);                                                             \
        break;                                                                      \
    }

#define SFunc(TEvType, HandleFunc) \
    case TEvType::EventType:       \
        HandleFunc(this->ActorContext()); \
        break;

#define sFunc(TEvType, HandleFunc) \
    case TEvType::EventType:       \
        HandleFunc();              \
        break;

#define CFunc(TEventType, HandleFunc) \
    case TEventType:                  \
        HandleFunc(this->ActorContext()); \
        break;

#define CFuncCtx(TEventType, HandleFunc, ctx) \
    case TEventType:                  \
        HandleFunc(ctx); \
        break;

#define cFunc(TEventType, HandleFunc) \
    case TEventType:                  \
        HandleFunc();                 \
        break;

#define FFunc(TEventType, HandleFunc) \
    case TEventType:                  \
        HandleFunc(ev, this->ActorContext()); \
        break;

#define fFunc(TEventType, HandleFunc) \
    case TEventType:                  \
        HandleFunc(ev);               \
        break;

#define IgnoreFunc(TEvType)  \
    case TEvType::EventType: \
        break;

#define ExceptionFunc(ExceptionType, HandleFunc)    \
    catch (const ExceptionType& exception) {        \
        HandleFunc(exception);                      \
    }

#define ExceptionFuncEv(ExceptionType, HandleFunc)    \
    catch (const ExceptionType& exception) {          \
        HandleFunc(exception, ev);                    \
    }

#define AnyExceptionFunc(HandleFunc)                \
    catch (...) {                                   \
        HandleFunc();                               \
    }

#define AnyExceptionFuncEv(HandleFunc)                \
    catch (...) {                                     \
        HandleFunc(ev);                               \
    }
