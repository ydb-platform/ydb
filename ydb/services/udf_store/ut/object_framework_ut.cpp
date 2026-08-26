#include <ydb/services/udf_store/wasm/object_framework/object_framework.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cstdlib>
#include <cstring>
#include <string>

namespace {

struct TEcho {
    char* Data = nullptr;
    size_t Len = 0;
};

void EchoInit(void* self, const void* blob, size_t blobLen) {
    auto* echo = static_cast<TEcho*>(self);
    if (blobLen == 0) {
        return;
    }
    echo->Data = static_cast<char*>(malloc(blobLen));
    UNIT_ASSERT(echo->Data);
    memcpy(echo->Data, blob, blobLen);
    echo->Len = blobLen;
}

void EchoDestroy(void* self) {
    auto* echo = static_cast<TEcho*>(self);
    free(echo->Data);
    echo->Data = nullptr;
    echo->Len = 0;
}

const TObjectType EchoType = {
    "Echo",
    sizeof(TEcho),
    &EchoInit,
    &EchoDestroy,
};

} // namespace

Y_UNIT_TEST_SUITE(TObjectFrameworkTest) {
    Y_UNIT_TEST(CreateGetDestroyTwoObjects) {
        const std::string a = "alpha";
        const std::string b = "beta";

        const TObjectHandle ha = ObjectFrameworkCreate(&EchoType, a.data(), a.size());
        const TObjectHandle hb = ObjectFrameworkCreate(&EchoType, b.data(), b.size());
        UNIT_ASSERT(ha != 0);
        UNIT_ASSERT(hb != 0);
        UNIT_ASSERT(ha != hb);

        auto* ea = static_cast<TEcho*>(ObjectFrameworkGet(ha, &EchoType));
        auto* eb = static_cast<TEcho*>(ObjectFrameworkGet(hb, &EchoType));
        UNIT_ASSERT(ea);
        UNIT_ASSERT(eb);
        UNIT_ASSERT_VALUES_EQUAL(std::string(ea->Data, ea->Len), a);
        UNIT_ASSERT_VALUES_EQUAL(std::string(eb->Data, eb->Len), b);

        ObjectFrameworkDestroy(ha);
        UNIT_ASSERT(!ObjectFrameworkGet(ha, &EchoType));
        UNIT_ASSERT(ObjectFrameworkGet(hb, &EchoType));

        ObjectFrameworkDestroy(hb);
        UNIT_ASSERT(!ObjectFrameworkGet(hb, &EchoType));
    }

    Y_UNIT_TEST(RejectBadType) {
        UNIT_ASSERT_VALUES_EQUAL(ObjectFrameworkCreate(nullptr, nullptr, 0), 0u);

        TObjectType bad = EchoType;
        bad.InstanceSize = 0;
        UNIT_ASSERT_VALUES_EQUAL(ObjectFrameworkCreate(&bad, nullptr, 0), 0u);
    }
}
