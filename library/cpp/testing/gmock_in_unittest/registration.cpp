#include "events.h"

#include <gmock/gmock.h>

#include <library/cpp/testing/unittest/plugin.h>

namespace {
    class TGMockUnittestPlugin: public NUnitTest::NPlugin::IPlugin {
    public:
        void OnStartMain(int argc, char* argv[]) override {
            testing::InitGoogleMock(&argc, argv);
            testing::TestEventListeners& listeners = testing::UnitTest::GetInstance()->listeners();
            delete listeners.Release(listeners.default_result_printer());
            listeners.Append(new TGMockTestEventListener());
        }
    };

    NUnitTest::NPlugin::TPluginRegistrator registerGMock(new TGMockUnittestPlugin());

}
