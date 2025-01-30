#include "fake_spec.h"

namespace NYql {
    namespace NPureCalc {
        NYT::TNode MakeFakeSchema(bool pg) {
            auto itemType = NYT::TNode::CreateList();
            itemType.Add(pg ? "PgType" : "DataType");
            itemType.Add(pg ? "int4" : "Int32");

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

        TFakeInputSpec FakeIS(ui32 inputsNumber, bool pg) {
            auto spec = TFakeInputSpec();
            spec.Schemas = TVector<NYT::TNode>(inputsNumber, MakeFakeSchema(pg));
            return spec;
        }

        TFakeOutputSpec FakeOS(bool pg) {
            auto spec = TFakeOutputSpec();
            spec.Schema = MakeFakeSchema(pg);
            return spec;
        }
    }
}
