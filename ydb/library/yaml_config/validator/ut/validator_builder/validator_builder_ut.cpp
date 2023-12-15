#include <util/stream/str.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yaml_config/validator/validator_builder.h>

namespace NKikimr {

using namespace NYamlConfig::NValidator;

Y_UNIT_TEST_SUITE(ValidatorBuilder) {
    Y_UNIT_TEST(CanCreateAllTypesOfNodes) {
        TGenericBuilder v0;
        TMapBuilder v1;
        TArrayBuilder v2;
        TInt64Builder v3;
        NYamlConfig::NValidator::TStringBuilder v4;
        TBoolBuilder v5;
    }

    Y_UNIT_TEST(BuildSimpleValidator) {
        TMapBuilder dimensions([](auto& b){
            auto greaterThanZero = [](TInt64Builder& b) {
                b.Min(0);
            };
            b.Int64("length", greaterThanZero);
            b.Int64("width", greaterThanZero);
            b.Int64("height", greaterThanZero);
        });

        auto tagsConfigurator = [](auto& b){
            b.Description("Tags for the product")
            .Optional()
            .Unique()
            .StringItem();
        };

        auto map = TMapBuilder()
            .Description("Map desription")
            .Int64("productId", [](auto& b) {
                b.Description("TString description");
            })
            .String("productName", [](auto& b) {
                b.Description("Name of the product");
            })
            .Int64("price", [](auto& b) {
                b.Description("The price of the product")
                .Min(0);
            })
            .Array("tags", tagsConfigurator)
            .Field("dimensions", dimensions);
        
        map.MapAt("dimensions").Description("product size");
    }

    Y_UNIT_TEST(CanHaveMultipleType) {
        TGenericBuilder m;
        m.CanBeMap();
        m.CanBeBool();
        m.CanBeInt64();
        TGenericBuilder m2;
        m2.CanBeInt64();
        m2.CanBeBool();
        m.CanBe(m2);
    }

    Y_UNIT_TEST(CanHaveDuplicateType) {
        TGenericBuilder m;
        m.CanBeMap([](auto& b){ b.Int64("int_field"); });
        m.CanBeMap([](auto& b){ b.Int64("int_field2"); });
        m.CanBeBool();
    }

    Y_UNIT_TEST(CreateMultitypeNode) {
        TGenericBuilder b1;
        b1.CanBeMap();
        b1.CanBeInt64();
        b1.CanBeString();
        TGenericBuilder b2;
        b2.CanBeInt64();
        b2.CanBeString();
        b2.CanBeArray();
        TMapBuilder b3;
        b3
            .Int64("int_f")
            .String("string_f")
            .Map("map");
        TGenericBuilder final_b;
        final_b.CanBeInt64();
        final_b.CanBe(b1);
        final_b.CanBe(b2);
        final_b.CanBe(b3);
    }
}

} // namespace NKikimr
