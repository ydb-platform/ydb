#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cstdio>
#include <cstdlib>
#include <memory>

using namespace NYdb::NTopic;

namespace {

class TExitSensitiveCodec final : public ICodec {
public:
    ~TExitSensitiveCodec() override {
        std::fputs("A registered codec was destroyed during process teardown.\n", stderr);
        std::_Exit(EXIT_FAILURE);
    }

    std::string Decompress(const std::string& data) const override {
        return data;
    }

    std::unique_ptr<IOutputStream> CreateCoder(TBuffer&, int) const override {
        std::abort();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(CodecRegistryLifetime) {
    Y_UNIT_TEST(RegisteredCodecSurvivesProcessTeardown) {
        // This target forks subtests. Its child returns from main normally, so a
        // finite registry would destroy this codec and fail the child at exit.
        constexpr auto codecId = static_cast<uint32_t>(ECodec::CUSTOM);
        auto& registry = TCodecMap::GetTheCodecMap();
        registry.Set(codecId, std::make_unique<TExitSensitiveCodec>());
        UNIT_ASSERT_VALUES_EQUAL(registry.GetOrThrow(codecId)->Decompress("payload"), "payload");
    }
}
