#include "common.h"

#include <library/cpp/tvmauth/client/logger.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NTvmAuth;

Y_UNIT_TEST_SUITE(ClientLogger) { 
    int i = 0;

    Y_UNIT_TEST(Debug) { 
        TLogger l;
        l.Debug("qwerty");
        UNIT_ASSERT_VALUES_EQUAL("7: qwerty\n", l.Stream.Str());
    }

    Y_UNIT_TEST(Info) { 
        TLogger l;
        l.Info("qwerty");
        UNIT_ASSERT_VALUES_EQUAL("6: qwerty\n", l.Stream.Str());
    }

    Y_UNIT_TEST(Warning) { 
        TLogger l;
        l.Warning("qwerty");
        UNIT_ASSERT_VALUES_EQUAL("4: qwerty\n", l.Stream.Str());
    }

    Y_UNIT_TEST(Error) { 
        TLogger l;
        l.Error("qwerty");
        UNIT_ASSERT_VALUES_EQUAL("3: qwerty\n", l.Stream.Str());
    }

#ifdef _unix_
    Y_UNIT_TEST(Cerr_) { 
        TCerrLogger l(5);
        l.Error("hit");
        l.Debug("miss");
    }
#endif
}
