#include "fake_spec.h"

namespace NYql::NPureCalc {
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

NYT::TNode CreateTypeNode(const TString& fieldType) {
    return NYT::TNode::CreateList()
        .Add("DataType")
        .Add(fieldType);
}

NYT::TNode CreateOptionalTypeNode(const TString& fieldType) {
    return NYT::TNode::CreateList()
        .Add("OptionalType")
        .Add(CreateTypeNode(fieldType));
}

void AddField(NYT::TNode& node, const TString& fieldName, const TString& fieldType) {
    node.Add(
        NYT::TNode::CreateList()
            .Add(fieldName)
            .Add(CreateOptionalTypeNode(fieldType)));
}

NYT::TNode MakeFakeStructSchema() {
    auto structMembers = NYT::TNode::CreateList();
    AddField(structMembers, "Id", "Uint32");
    AddField(structMembers, "Name", "Utf8");
    AddField(structMembers, "Body", "String");

    auto rootMembers = NYT::TNode::CreateList();
    rootMembers.Add(
        NYT::TNode::CreateList()
            .Add("_r")
            .Add(NYT::TNode::CreateList()
                     .Add("StructType")
                     .Add(std::move(structMembers))));

    return NYT::TNode::CreateList()
        .Add("StructType")
        .Add(std::move(rootMembers));
}

TFakeOutputSpec FakeStructOS() {
    auto spec = TFakeOutputSpec();
    spec.Schema = MakeFakeStructSchema();
    return spec;
}
} // namespace NYql::NPureCalc
