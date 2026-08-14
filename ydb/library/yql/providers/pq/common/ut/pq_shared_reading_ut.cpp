#include <ydb/library/yql/providers/pq/common/pq_shared_reading.h>

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/any.pb.h>


namespace NYql {

Y_UNIT_TEST_SUITE(THasSharedReading) {

    Y_UNIT_TEST(EmptyAnyHasNoSharedReading) {
        google::protobuf::Any empty;
        UNIT_ASSERT(!HasSharedReading(empty));
    }

    Y_UNIT_TEST(WrongMessageTypeHasNoSharedReading) {
        google::protobuf::Any any;
        // Pack an unrelated message so UnpackTo(TDqPqTopicSource) fails.
        NPq::NProto::TDqPqTopicSink sink;
        any.PackFrom(sink);
        UNIT_ASSERT(!HasSharedReading(any));
    }

    Y_UNIT_TEST(SharedReadingFalse) {
        google::protobuf::Any any;
        NPq::NProto::TDqPqTopicSource source;
        source.SetSharedReading(false);
        any.PackFrom(source);
        UNIT_ASSERT(!HasSharedReading(any));
    }

    Y_UNIT_TEST(SharedReadingTrue) {
        google::protobuf::Any any;
        NPq::NProto::TDqPqTopicSource source;
        source.SetSharedReading(true);
        any.PackFrom(source);
        UNIT_ASSERT(HasSharedReading(any));
    }

}

} // namespace NYql
