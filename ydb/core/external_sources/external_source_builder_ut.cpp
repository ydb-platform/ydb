#include "external_source_builder.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(ExternalSourceBuilderTest) {

    Y_UNIT_TEST(ValidateName) {
        auto source = NExternalSource::TExternalSourceBuilder("Test")
            .Build();
            
        UNIT_ASSERT_VALUES_EQUAL(source->GetName(), "Test");
    }

    // Test returned auth methods when conditions are not set  
    Y_UNIT_TEST(ValidateAuthWithoutCondition) {
        auto source = NExternalSource::TExternalSourceBuilder("Test")
            .Auth({"auth1", "auth2"})
            .Build();

        const auto authMethods = source->GetAuthMethods("");

        UNIT_ASSERT_VALUES_EQUAL(authMethods.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(authMethods[0], "auth1");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[1], "auth2");
    }

    // Test returned auth methods when conditions are set  
    Y_UNIT_TEST(ValidateAuthWithCondition1) {
        auto source = NExternalSource::TExternalSourceBuilder("Test")
            .Auth(
                {"auth1", "auth2"}, 
                // check that ddl has "property1" equals to "value"
                NExternalSource::HasSettingCondition("property1", "value")
            )
            .Auth(
                {"auth3", "auth4"}, 
                // check that ddl has "property2" equals to "value"
                NExternalSource::HasSettingCondition("property2", "value")
            )
            .Build();
        
        // ddl without any value     
        auto authMethods = source->GetAuthMethods("");
        UNIT_ASSERT_VALUES_EQUAL(authMethods.size(), 0);

        // ddl with "property1" equals to "value"    
        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        proto.MutableProperties()->MutableProperties()->insert({"property1", "value"});            
        authMethods = source->GetAuthMethods(proto.SerializeAsString());

        UNIT_ASSERT_VALUES_EQUAL(authMethods.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(authMethods[0], "auth1");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[1], "auth2");

        // ddl with "property2" equals to "value"    
        proto.MutableProperties()->MutableProperties()->insert({"property2", "value"});            
        authMethods = source->GetAuthMethods(proto.SerializeAsString());

        UNIT_ASSERT_VALUES_EQUAL(authMethods.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(authMethods[0], "auth1");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[1], "auth2");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[2], "auth3");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[3], "auth4");
    }

    // Test validation when ddl has property which is not supported by source
    // i.e. source does not contain this property in a list of available properties 
    Y_UNIT_TEST(ValidateUnsupportedField) {
        // source has property "field"
        auto source = NExternalSource::TExternalSourceBuilder("Test")
            .Property("field")
            .Build();

        // ddl with "field1"
        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        proto.MutableProperties()->MutableProperties()->insert({"field1", "value"});
        
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->ValidateExternalDataSource(proto.SerializeAsString()), 
            NExternalSource::TExternalSourceException,
            "Unsupported property: field1"
        );

        // ddl with "field"
        proto.MutableProperties()->MutableProperties()->clear();
        proto.MutableProperties()->MutableProperties()->insert({"field", "value"});
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));
    }

    // Test validation for non required property 
    Y_UNIT_TEST(ValidateNonRequiredField) {
        auto source = NExternalSource::TExternalSourceBuilder("Test")
            .Property("field")
            .Build();
        
        // ddl without "field"
        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        // ddl with "field"
        proto.MutableProperties()->MutableProperties()->insert({"field", "value"});
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));
    }
    
    // Test validation for required property
    Y_UNIT_TEST(ValidateRequiredField) {
        auto source = NExternalSource::TExternalSourceBuilder("Test")
            .Property("field", NExternalSource::RequiredValidator())
            .Build();
        
        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        
        // ddl without "field"
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->ValidateExternalDataSource(proto.SerializeAsString()), 
            NExternalSource::TExternalSourceException,
            "required property: field is not set"
        );

        // ddl with "field"
        proto.MutableProperties()->MutableProperties()->insert({"field", "value"});
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));
    }

    // Test validation for non required property with allowed list of values
    Y_UNIT_TEST(ValidateNonRequiredFieldValues) {
        auto source = NExternalSource::TExternalSourceBuilder("Test")
            .Property("field", NExternalSource::IsInListValidator({"v1", "v2", "v3"}, false))
            .Build();
        
        // ddl without "field"
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(""));
    }

    // Test validation for required property with allowed list of values
    Y_UNIT_TEST(ValidateRequiredFieldValues) {
        auto source = NExternalSource::TExternalSourceBuilder("Test")
            .Property("field", NExternalSource::IsInListValidator({"v1", "v2", "v3"}, true))
            .Build();
        
        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        
        // ddl without "field"
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->ValidateExternalDataSource(proto.SerializeAsString()), 
            NExternalSource::TExternalSourceException,
            "required property: field is not set"
        );
        
        // ddl with "field" equals to value not in allowed list 
        proto.MutableProperties()->MutableProperties()->insert({"field", "value"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->ValidateExternalDataSource(proto.SerializeAsString()), 
            NExternalSource::TExternalSourceException,
            "property: field has wrong value: value allowed values: v3,v2,v1"
        );

        // ddl with "field" equals to "v1"
        proto.MutableProperties()->MutableProperties()->clear();
        proto.MutableProperties()->MutableProperties()->insert({"field", "v1"});
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        // ddl with "field" equals to "v2"
        proto.MutableProperties()->MutableProperties()->clear();
        proto.MutableProperties()->MutableProperties()->insert({"field", "v2"});
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        // ddl with "field" equals to "v3"
        proto.MutableProperties()->MutableProperties()->clear();
        proto.MutableProperties()->MutableProperties()->insert({"field", "v3"});
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

    }

    // Test validation for required property with condition 
    Y_UNIT_TEST(ValidateRequiredFieldOnCondition) {
        auto source = NExternalSource::TExternalSourceBuilder("Test")
            .Property("field1")
            .Property(
                "field", 
                NExternalSource::RequiredValidator(), 
                // apply validator if ddl has "field1" equals to "v"
                NExternalSource::HasSettingCondition("field1", "v")
            )
            .Build();
        
        // ddl without "field1"   
        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        // ddl with "field1" but without "field"
        proto.MutableProperties()->MutableProperties()->insert({"field1", "v"});

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->ValidateExternalDataSource(proto.SerializeAsString()), 
            NExternalSource::TExternalSourceException,
            "required property: field is not set"
        );

        // ddl with "field1" and "field"
        proto.MutableProperties()->MutableProperties()->insert({"field", "q"});
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));
    }
}

} // NKikimr
