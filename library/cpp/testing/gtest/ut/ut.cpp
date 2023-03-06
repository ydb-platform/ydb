#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/testing/hook/hook.h>

static int HookOrder = 0;
static int PreInitHook1 = 0;
static int PreInitHook2 = 0;
static int PreRunHook1 = 0;
static int PreRunHook2 = 0;
static int PostRunHook1 = 0;
static int PostRunHook2 = 0;

Y_TEST_HOOK_BEFORE_INIT(PreInit1) {
    PreInitHook1 = ++HookOrder;
}

Y_TEST_HOOK_BEFORE_INIT(PreInit2) {
    PreInitHook2 = ++HookOrder;
}

Y_TEST_HOOK_BEFORE_RUN(PreRun1) {
    PreRunHook1 = ++HookOrder;
}

Y_TEST_HOOK_BEFORE_RUN(PreRun2) {
    PreRunHook2 = ++HookOrder;
}

Y_TEST_HOOK_AFTER_RUN(PostRun1) {
    PostRunHook1 = ++HookOrder;
    Cerr << "PostRunHook1" << Endl;
}

Y_TEST_HOOK_AFTER_RUN(PostRun2) {
    PostRunHook2 = ++HookOrder;
    Cerr << "PostRunHook2" << Endl;
}

TEST(Gtest, HookOrder) {
    EXPECT_NEAR(PreInitHook1, PreInitHook2, 1);
    EXPECT_NEAR(PreRunHook1, PreRunHook2, 1);
    EXPECT_LE(std::max(PreInitHook1, PreInitHook2), std::min(PreRunHook1, PreRunHook2));
    EXPECT_EQ(PostRunHook1, 0);
    EXPECT_EQ(PostRunHook2, 0);
}

TEST(Gtest, Metrics) {
    RecordProperty("Metric1", 10);
}
