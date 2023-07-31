#include "fake_spec.h"

namespace NYql {
    namespace NPureCalc {
        NYT::TNode MakeFakeSchema() {
            auto itemType = NYT::TNode::CreateList();
            itemType.Add("DataType");
            itemType.Add("Int32");

            auto itemNode = NYT::TNode::CreateList();
            itemNode.Add("Name");
            itemNode.Add(std::move(itemType));

            auto items = NYT::TNode::CreateList();
            items.Add(std::move(itemNode));

            auto schema = NYT::TNode::CreateList();
            schema.Add("StructType");
            schema.Add(std::move(items));

            return schema;
        }

        TFakeInputSpec FakeIS(ui32 inputsNumber) {
            auto spec = TFakeInputSpec();
            spec.Schemas = TVector<NYT::TNode>(inputsNumber, MakeFakeSchema());
            return spec;
        }

        TFakeOutputSpec FakeOS() {
            auto spec = TFakeOutputSpec();
            spec.Schema = MakeFakeSchema();
            return spec;
        }
    }
}
