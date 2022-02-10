#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/messagebus/remote_client_connection.h>
#include <library/cpp/messagebus/remote_client_session.h>
#include <library/cpp/messagebus/remote_server_connection.h>
#include <library/cpp/messagebus/remote_server_session.h>
#include <library/cpp/messagebus/ybus.h>
#include <library/cpp/messagebus/oldmodule/module.h>
#include <library/cpp/messagebus/scheduler/scheduler.h>

#include <util/generic/object_counter.h>
#include <util/system/type_name.h>
#include <util/stream/output.h>

#include <typeinfo>

struct TObjectCountCheck {
    bool Enabled;

    template <typename T>
    struct TReset {
        TObjectCountCheck* const Thiz;

        TReset(TObjectCountCheck* thiz)
            : Thiz(thiz)
        {
        }

        void operator()() {
            long oldValue = TObjectCounter<T>::ResetObjectCount();
            if (oldValue != 0) {
                Cerr << "warning: previous counter: " << oldValue << " for " << TypeName<T>() << Endl;
                Cerr << "won't check in this test" << Endl;
                Thiz->Enabled = false;
            }
        }
    };

    TObjectCountCheck() {
        Enabled = true;
        DoForAllCounters<TReset>();
    }

    template <typename T>
    struct TCheckZero {
        TCheckZero(TObjectCountCheck*) {
        }

        void operator()() {
            UNIT_ASSERT_VALUES_EQUAL_C(0L, TObjectCounter<T>::ObjectCount(), TypeName<T>());
        }
    };

    ~TObjectCountCheck() {
        if (Enabled) {
            DoForAllCounters<TCheckZero>();
        }
    }

    template <template <typename> class TOp>
    void DoForAllCounters() {
        TOp< ::NBus::NPrivate::TRemoteClientConnection>(this)();
        TOp< ::NBus::NPrivate::TRemoteServerConnection>(this)();
        TOp< ::NBus::NPrivate::TRemoteClientSession>(this)();
        TOp< ::NBus::NPrivate::TRemoteServerSession>(this)();
        TOp< ::NBus::NPrivate::TScheduler>(this)();
        TOp< ::NEventLoop::TEventLoop>(this)();
        TOp< ::NEventLoop::TChannel>(this)();
        TOp< ::NBus::TBusModule>(this)();
        TOp< ::NBus::TBusJob>(this)();
    }
};
