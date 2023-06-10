#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/codecs/tls_cache.h>

Y_UNIT_TEST_SUITE(CodecsBufferFactoryTest){
    void AssignToBuffer(TBuffer & buf, TStringBuf val){
        buf.Assign(val.data(), val.size());
}

TStringBuf AsStringBuf(const TBuffer& b) {
    return TStringBuf(b.Data(), b.Size());
}

Y_UNIT_TEST(TestAcquireReleaseReuse) {
    NCodecs::TBufferTlsCache factory;
    // acquiring the first buffer
    auto buf1 = factory.Item();
    AssignToBuffer(buf1.Get(), "Buffer_01");
    {
        // acquiring the second buffer
        auto buf2 = factory.Item();
        AssignToBuffer(buf2.Get(), "Buffer_02");
    }
    // the first buffer should stay intact
    UNIT_ASSERT_EQUAL(AsStringBuf(buf1.Get()), "Buffer_01");
    {
        // reacquiring the last released buffer
        // expecting it zero sized but having the same memory
        auto buf2 = factory.Item();
        UNIT_ASSERT_VALUES_EQUAL(buf2.Get().Size(), 0u);
        buf2.Get().Resize(TStringBuf("Buffer_02").Size());
        UNIT_ASSERT_EQUAL(AsStringBuf(buf2.Get()), "Buffer_02");
    }
    // when the factory dies we should see no leaks
}
}
