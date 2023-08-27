#include <ydb/core/fq/libs/compute/common/config.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

Y_UNIT_TEST_SUITE(Config) {
    Y_UNIT_TEST(IncludeScope) {
        NFq::NConfig::TComputeConfig proto;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
            DefaultCompute: IN_PLACE
            ComputeMapping {
                QueryType: ANALYTICS
                Compute: YDB
                Activation {
                    IncludeScopes: "oss://test1"
                }
            }
            Ydb {
                Enable: true
                ControlPlane {
                    Enable: true
                    Cms {
                    }
                }
                PinTenantName: "/root/cp"
                SynchronizationService {
                    Enable: True
                }
            }
        )", &proto));
        NFq::TComputeConfig config(proto);
        UNIT_ASSERT(config.YdbComputeControlPlaneEnabled("oss://test1"));
        UNIT_ASSERT(!config.YdbComputeControlPlaneEnabled("oss://test2"));
    }

    Y_UNIT_TEST(ExcludeScope) {
        NFq::NConfig::TComputeConfig proto;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
            DefaultCompute: IN_PLACE
            ComputeMapping {
                QueryType: ANALYTICS
                Compute: YDB
                Activation {
                    Percentage: 100
                    ExcludeScopes: "oss://test1"
                }
            }
            Ydb {
                Enable: true
                ControlPlane {
                    Enable: true
                    Cms {
                    }
                }
                PinTenantName: "/root/cp"
                SynchronizationService {
                    Enable: True
                }
            }
        )", &proto));
        NFq::TComputeConfig config(proto);
        UNIT_ASSERT(!config.YdbComputeControlPlaneEnabled("oss://test1"));
        UNIT_ASSERT(config.YdbComputeControlPlaneEnabled("oss://test2"));
    }
}
