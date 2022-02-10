#pragma once

const char* PushWhatThreadDoes(const char* what);
void PopWhatThreadDoes(const char* prev);
const char** WhatThreadDoesLocation();

struct TWhatThreadDoesPushPop {
private:
    const char* Prev;

public:
    TWhatThreadDoesPushPop(const char* what) {
        Prev = PushWhatThreadDoes(what);
    }

    ~TWhatThreadDoesPushPop() {
        PopWhatThreadDoes(Prev);
    }
};

#ifdef __GNUC__
#define WHAT_THREAD_DOES_FUNCTION __PRETTY_FUNCTION__
#else
#define WHAT_THREAD_DOES_FUNCTION __FUNCTION__
#endif

#define WHAT_THREAD_DOES_PUSH_POP_CURRENT_FUNC() \
    TWhatThreadDoesPushPop whatThreadDoesPushPopCurrentFunc(WHAT_THREAD_DOES_FUNCTION)
