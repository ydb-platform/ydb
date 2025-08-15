#include "audit_config.h"

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

namespace NKikimr::NAudit {

static TAuditConfig FromProtoText(const TString& text) {
    NKikimrConfig::TAuditConfig proto;
    UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(text, &proto));
    TAuditConfig cfg = proto;
    return cfg;
}

Y_UNIT_TEST_SUITE(AuditConfigTest) {
    Y_UNIT_TEST(Processing) {
        using TLogClassConfig = NKikimrConfig::TAuditConfig::TLogClassConfig;
        {
            TAuditConfig cfg = FromProtoText(R"(
                LogClassConfig {
                    LogClass: ClusterAdmin
                    EnableLogging: true
                    ExcludeAccountType: Service
                }
                LogClassConfig {
                    LogClass: Dml
                    EnableLogging: false
                }
                LogClassConfig {
                    LogClass: Default
                    EnableLogging: true
                }
            )");

            UNIT_ASSERT(cfg.EnableLogging(TLogClassConfig::Login, NACLibProto::SUBJECT_TYPE_USER));
            UNIT_ASSERT(cfg.EnableLogging(TLogClassConfig::Login, NACLibProto::SUBJECT_TYPE_SERVICE));
            UNIT_ASSERT(cfg.EnableLogging(TLogClassConfig::ClusterAdmin, NACLibProto::SUBJECT_TYPE_USER));
            UNIT_ASSERT(!cfg.EnableLogging(TLogClassConfig::ClusterAdmin, NACLibProto::SUBJECT_TYPE_SERVICE));
            UNIT_ASSERT(!cfg.EnableLogging(TLogClassConfig::Dml, NACLibProto::SUBJECT_TYPE_SERVICE_IMPERSONATED_FROM_USER));
            UNIT_ASSERT(!cfg.EnableLogging(TLogClassConfig::Dml, NACLibProto::SUBJECT_TYPE_USER));
        }

        {
            TAuditConfig cfg = FromProtoText(R"(
                LogClassConfig {
                    LogClass: Ddl
                    EnableLogging: true
                    ExcludeAccountType: ServiceImpersonatedFromUser
                    ExcludeAccountType: Service
                }
            )");

            UNIT_ASSERT(!cfg.EnableLogging(TLogClassConfig::Login, NACLibProto::SUBJECT_TYPE_USER));
            UNIT_ASSERT(!cfg.EnableLogging(TLogClassConfig::Dml, NACLibProto::SUBJECT_TYPE_SERVICE));
            UNIT_ASSERT(!cfg.EnableLogging(TLogClassConfig::Default, NACLibProto::SUBJECT_TYPE_ANONYMOUS));
            UNIT_ASSERT(cfg.EnableLogging(TLogClassConfig::Ddl, NACLibProto::SUBJECT_TYPE_USER));
            UNIT_ASSERT(cfg.EnableLogging(TLogClassConfig::Ddl, NACLibProto::SUBJECT_TYPE_ANONYMOUS));
            UNIT_ASSERT(!cfg.EnableLogging(TLogClassConfig::Ddl, NACLibProto::SUBJECT_TYPE_SERVICE_IMPERSONATED_FROM_USER));
            UNIT_ASSERT(!cfg.EnableLogging(TLogClassConfig::Ddl, NACLibProto::SUBJECT_TYPE_SERVICE));
        }
    }

    Y_UNIT_TEST(IncorrectConfig) {
        UNIT_ASSERT_EXCEPTION(FromProtoText(R"(
            LogClassConfig {
                LogClass: ClusterAdmin
                EnableLogging: true
            }
            LogClassConfig {
                LogClass: ClusterAdmin
                EnableLogging: true
            }
        )"), yexception);

        UNIT_ASSERT_EXCEPTION(FromProtoText(R"(
            LogClassConfig {
                EnableLogging: true
            }
        )"), yexception);
    }

    Y_UNIT_TEST(DefaultInitialization) {
        using TLogClassConfig = NKikimrConfig::TAuditConfig::TLogClassConfig;
        TAuditConfig cfg;
        UNIT_ASSERT(!cfg.EnableLogging(TLogClassConfig::Login, NACLibProto::SUBJECT_TYPE_USER));
    }
}

} // namespace NKikimr::NAudit
