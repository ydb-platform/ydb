#pragma once

#include "glob_compat.h"

#include <util/generic/noncopyable.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>

class TGlobPaths : TNonCopyable {
public:
    TGlobPaths(const char* pattern) {
        Impl.gl_pathc = 0;
        int result = glob(pattern, 0, nullptr, &Impl);
        Y_ENSURE(result == 0 || result == GLOB_NOMATCH, "glob failed");
    }

    TGlobPaths(const TString& pattern)
        : TGlobPaths(pattern.data())
    {
    }

    ~TGlobPaths() {
        globfree(&Impl);
    }

    const char** begin() {
        return const_cast<const char**>(Impl.gl_pathv);
    }

    const char** end() {
        return const_cast<const char**>(Impl.gl_pathv + Impl.gl_pathc);
    }

private:
    glob_t Impl;
};
