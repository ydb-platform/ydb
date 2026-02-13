#pragma once
#include <library/cpp/yson/node/node.h>
#include <util/generic/fwd.h>
#include <library/cpp/testing/unittest/registar.h>


class TSkipIndexTestFixture: public NUnitTest::TBaseFixture {
    struct Impl;


    NYT::TNode Select(TString query);
    void SchemeSetup(TString query);
private:
    THolder<Impl> Impl_;
};