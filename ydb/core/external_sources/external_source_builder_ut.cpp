#include "external_source_builder.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr {

namespace {

class TTestFixture : public NUnitTest::TBaseFixture {
public:
    TTestFixture()
        : Builder("Test")
        , Props(*Proto.MutableProperties()->MutableProperties())
    {
    }

public:
    void SetUp(NUnitTest::TTestContext& context) override {
        NUnitTest::TBaseFixture::SetUp(context);
    }

protected:
    NExternalSource::TExternalSourceBuilder Builder;
    NKikimrSchemeOp::TExternalDataSourceDescription Proto;
    ::google::protobuf::Map<TProtoStringType, TProtoStringType>& Props;
};

}

Y_UNIT_TEST_SUITE(ExternalSourceBuilderTest) {

    Y_UNIT_TEST_F(ValidateName, TTestFixture) {
        auto source = Builder.Build();
        UNIT_ASSERT_VALUES_EQUAL(source->GetName(), "Test");
    }

    // Test returned auth methods when conditions are not set  
    Y_UNIT_TEST_F(ValidateAuthWithoutCondition, TTestFixture) {
        auto source = Builder
            .Auth({"auth1", "auth2"})
            .Build();

        const auto authMethods = source->GetAuthMethods();

        UNIT_ASSERT_VALUES_EQUAL(authMethods.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(authMethods[0], "auth1");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[1], "auth2");
    }

    // Test returned auth methods when conditions are set  
    Y_UNIT_TEST_F(ValidateAuthWithCondition, TTestFixture) {
        auto source = Builder
            .Auth(
                {"auth1", "auth2"}, 
                // check that ddl has "property1" equals to "value"
                NExternalSource::GetHasSettingCondition("property1", "value")
            )
            .Auth(
                {"auth3", "auth4"}, 
                // check that ddl has "property2" equals to "value"
                NExternalSource::GetHasSettingCondition("property2", "value")
            )
            .Build();
        
        // ddl without any value     
        auto authMethods = source->GetAuthMethods();

        UNIT_ASSERT_VALUES_EQUAL(authMethods.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(authMethods[0], "auth1");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[1], "auth2");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[2], "auth3");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[3], "auth4");
    }

    // Test validation when ddl has property which is not supported by source
    // i.e. source does not contain this property in a list of available properties 
    Y_UNIT_TEST_F(ValidateUnsupportedField, TTestFixture) {
        // source has property "field"
        auto source = Builder
            .Property("field")
            .Build();

        // ddl with "field1"
        Props["field1"] = "value";
        
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->ValidateExternalDataSource(Proto.SerializeAsString()), 
            NExternalSource::TExternalSourceException,
            "Unsupported property: field1"
        );

        // ddl with "field"
        Props.clear();
        Props["field"] = "value";
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(Proto.SerializeAsString()));
    }

    // Test validation for non required property 
    Y_UNIT_TEST_F(ValidateNonRequiredField, TTestFixture) {
        auto source = Builder
            .Property("field")
            .Build();
        
        // ddl without "field"
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(Proto.SerializeAsString()));

        // ddl with "field"
        Props["field"] = "value";
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(Proto.SerializeAsString()));
    }
    
    // Test validation for required property
    Y_UNIT_TEST_F(ValidateRequiredField, TTestFixture) {
        auto source = Builder
            .Property("field", NExternalSource::GetRequiredValidator())
            .Build();
        
        // ddl without "field"
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->ValidateExternalDataSource(Proto.SerializeAsString()), 
            NExternalSource::TExternalSourceException,
            "required property: field is not set"
        );

        // ddl with "field"
        Props["field"] = "value";
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(Proto.SerializeAsString()));
    }

    // Test validation for non required property with allowed list of values
    Y_UNIT_TEST_F(ValidateNonRequiredFieldValues, TTestFixture) {
        auto source = Builder
            .Property("field", NExternalSource::GetIsInListValidator({"v1", "v2", "v3"}, false))
            .Build();
        
        // ddl without "field"
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(""));
    }

    // Test validation for required property with allowed list of values
    Y_UNIT_TEST_F(ValidateRequiredFieldValues, TTestFixture) {
        auto source = Builder
            .Property("field", NExternalSource::GetIsInListValidator({"v1", "v2", "v3"}, true))
            .Build();
        
        // ddl without "field"
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->ValidateExternalDataSource(Proto.SerializeAsString()), 
            NExternalSource::TExternalSourceException,
            "required property: field is not set"
        );

        // ddl with "field" equals to value not in allowed list 
        Props["field"] = "value";
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->ValidateExternalDataSource(Proto.SerializeAsString()), 
            NExternalSource::TExternalSourceException,
            "property: field has wrong value: value allowed values: v3, v2, v1"
        );

        // ddl with "field" equals to "v1"
        Props["field"] = "v1";
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(Proto.SerializeAsString()));

        // ddl with "field" equals to "v2"
        Props["field"] = "v2";
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(Proto.SerializeAsString()));

        // ddl with "field" equals to "v3"
        Props["field"] = "v3";
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(Proto.SerializeAsString()));
    }

    // Test validation for required property with condition 
    Y_UNIT_TEST_F(ValidateRequiredFieldOnCondition, TTestFixture) {
        auto source = Builder
            .Property("field1")
            .Property(
                "field", 
                NExternalSource::GetRequiredValidator(), 
                // apply validator if ddl has "field1" equals to "v"
                NExternalSource::GetHasSettingCondition("field1", "v")
            )
            .Build();
        
        // ddl without "field1"   
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(Proto.SerializeAsString()));

        // ddl with "field1" but without "field"
        Props["field1"] = "v";

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->ValidateExternalDataSource(Proto.SerializeAsString()), 
            NExternalSource::TExternalSourceException,
            "required property: field is not set"
        );

        // ddl with "field1" and "field"
        Props["field"] = "q";
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(Proto.SerializeAsString()));
    }
}

} // NKikimr
