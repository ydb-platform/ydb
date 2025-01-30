#include <ydb/library/grpc/server/grpc_response.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/duration.pb.h>
#include <grpcpp/impl/codegen/proto_utils.h>
#include <grpcpp/impl/grpc_library.h>

using namespace NYdbGrpc;

using google::protobuf::Duration;

Y_UNIT_TEST_SUITE(ResponseTest) {

    template <typename T>
    grpc::ByteBuffer Serialize(T resp) {
        grpc::ByteBuffer buf;
        bool ownBuf = false;
        grpc::Status status = grpc::SerializationTraits<T>::Serialize(resp, &buf, &ownBuf);
        UNIT_ASSERT(status.ok());
        return buf;
    }

    template <typename T>
    T Deserialize(grpc::ByteBuffer* buf) {
        T message;
        auto status = grpc::SerializationTraits<T>::Deserialize(buf, &message);
        UNIT_ASSERT(status.ok());
        return message;
    }

    Y_UNIT_TEST(UniversalResponseMsg) {
        Duration d1;
        d1.set_seconds(12345);
        d1.set_nanos(67890);

        auto buf = Serialize(TUniversalResponse<Duration>(&d1));
        Duration d2 = Deserialize<Duration>(&buf);

        UNIT_ASSERT_VALUES_EQUAL(d2.seconds(), 12345);
        UNIT_ASSERT_VALUES_EQUAL(d2.nanos(), 67890);
    }

    Y_UNIT_TEST(UniversalResponseBuf) {
        Duration d1;
        d1.set_seconds(123);
        d1.set_nanos(456);

        TString data = d1.SerializeAsString();
        grpc::Slice dataSlice{data.data(), data.size()};
        grpc::ByteBuffer dataBuf{&dataSlice, 1};

        auto buf = Serialize(TUniversalResponse<Duration>(&dataBuf));
        Duration d2 = Deserialize<Duration>(&buf);

        UNIT_ASSERT_VALUES_EQUAL(d2.seconds(), 123);
        UNIT_ASSERT_VALUES_EQUAL(d2.nanos(), 456);
    }

    Y_UNIT_TEST(UniversalResponseRefMsg) {
        Duration d1;
        d1.set_seconds(12345);
        d1.set_nanos(67890);

        auto buf = Serialize(TUniversalResponseRef<Duration>(&d1));
        Duration d2 = Deserialize<Duration>(&buf);

        UNIT_ASSERT_VALUES_EQUAL(d2.seconds(), 12345);
        UNIT_ASSERT_VALUES_EQUAL(d2.nanos(), 67890);
    }

    Y_UNIT_TEST(UniversalResponseRefBuf) {
        Duration d1;
        d1.set_seconds(123);
        d1.set_nanos(456);

        TString data = d1.SerializeAsString();
        grpc::Slice dataSlice{data.data(), data.size()};
        grpc::ByteBuffer dataBuf{&dataSlice, 1};

        auto buf = Serialize(TUniversalResponseRef<Duration>(&dataBuf));
        Duration d2 = Deserialize<Duration>(&buf);

        UNIT_ASSERT_VALUES_EQUAL(d2.seconds(), 123);
        UNIT_ASSERT_VALUES_EQUAL(d2.nanos(), 456);
    }
}
