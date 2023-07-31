#pragma once

#include "address.h"

namespace NIPREG {

class TChecker {
public:
    virtual ~TChecker() {}

    virtual bool CheckNext(const TAddress& first, const TAddress& last) = 0;

    void CheckNextFatal(const TAddress& first, const TAddress& last);
};

class TFlatChecker: public TChecker {
private:
    TAddress PrevLast;
    bool HasState;

public:
    TFlatChecker();
    virtual bool CheckNext(const TAddress& first, const TAddress& last);
};

class TIntersectingChecker: public TChecker {
private:
    TAddress PrevFirst;
    TAddress PrevLast;
    bool HasState;

public:
    TIntersectingChecker();
    virtual bool CheckNext(const TAddress& first, const TAddress& last);
};

}
