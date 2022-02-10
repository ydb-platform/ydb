#include "what_thread_does.h"

#include "thread_extra.h"

#include <util/system/tls.h>

Y_POD_STATIC_THREAD(const char*)
WhatThreadDoes;

const char* PushWhatThreadDoes(const char* what) {
    const char* r = NTSAN::RelaxedLoad(&WhatThreadDoes);
    NTSAN::RelaxedStore(&WhatThreadDoes, what);
    return r;
}

void PopWhatThreadDoes(const char* prev) {
    NTSAN::RelaxedStore(&WhatThreadDoes, prev);
}

const char** WhatThreadDoesLocation() {
    return &WhatThreadDoes;
}
