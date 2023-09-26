#pragma once

#include <util/generic/ptr.h>
#include <functional>

struct TEventHandler {
    virtual ~TEventHandler() {
    }
    virtual int Handle(int signum) = 0;
};

void SetAsyncSignalHandler(int signum, THolder<TEventHandler> handler);
void SetAsyncSignalHandler(int signum, void (*handler)(int));
void SetAsyncSignalFunction(int signum, std::function<void(int)> func);
