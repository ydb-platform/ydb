#pragma once

#include <library/cpp/timezone_conversion/civil.h>

class TCronExpression {
public:
    TCronExpression(const TStringBuf cronUnparsedExpr);
    ~TCronExpression();

    NDatetime::TCivilSecond CronNext(NDatetime::TCivilSecond date);
    NDatetime::TCivilSecond CronPrev(NDatetime::TCivilSecond date);

private:
    class TImpl;
    THolder<TImpl> Impl;
};
