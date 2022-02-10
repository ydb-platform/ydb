#pragma once

#include "mpsc_read_as_filled.h"
#include "mpsc_htswap.h"
#include "mpsc_vinfarr_obstructive.h"
#include "mpsc_intrusive_unordered.h"
#include "mpmc_unordered_ring.h"

struct TBasicHTSwap: public NThreading::THTSwapQueue<> {
};

struct TBasicReadAsFilled: public NThreading::TReadAsFilledQueue<> {
};

struct TBasicObstructiveConsumer
   : public NThreading::TObstructiveConsumerQueue<> {
};

struct TBasicMPSCIntrusiveUnordered
   : public NThreading::TMPSCIntrusiveUnordered {
};

struct TIntrusiveLink: public NThreading::TIntrusiveNode {
};

struct TMPMCUnorderedRing: public NThreading::TMPMCUnorderedRing {
    TMPMCUnorderedRing()
        : NThreading::TMPMCUnorderedRing(10000000)
    {
    }
};

#define REGISTER_TESTS_FOR_ALL_ORDERED_QUEUES(TestTemplate)         \
    UNIT_TEST_SUITE_REGISTRATION(TestTemplate<TBasicHTSwap>);       \
    UNIT_TEST_SUITE_REGISTRATION(TestTemplate<TBasicReadAsFilled>); \
    UNIT_TEST_SUITE_REGISTRATION(TestTemplate<TBasicObstructiveConsumer>)

#define REGISTER_TESTS_FOR_ALL_UNORDERED_QUEUES(TestTemplate)                 \
    UNIT_TEST_SUITE_REGISTRATION(TestTemplate<TBasicMPSCIntrusiveUnordered>); \
    UNIT_TEST_SUITE_REGISTRATION(TestTemplate<TMPMCUnorderedRing>);
