#pragma once

#include <util/generic/refcount.h>

class TUseCountChecker {
private:
    TAtomicCounter Counter;

public:
    TUseCountChecker();
    ~TUseCountChecker();
    void Inc();
    void Dec();
};

class TUseCountHolder {
private:
    TUseCountChecker* CurrentChecker;

public:
    TUseCountHolder();
    explicit TUseCountHolder(TUseCountChecker* currentChecker);
    TUseCountHolder& operator=(TUseCountHolder that);
    ~TUseCountHolder();
    void Swap(TUseCountHolder&);
    void Reset();
};
