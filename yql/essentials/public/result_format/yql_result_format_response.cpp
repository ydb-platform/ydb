#include "yql_result_format_response.h"
#include "yql_result_format_impl.h"

namespace NYql::NResult {

TVector<TResult> ParseResponse(const NYT::TNode& responseNode) {
    CHECK(responseNode.IsList());
    TVector<TResult> v;
    for (const auto& resultNode : responseNode.AsList()) {
        TResult res;
        CHECK(resultNode.IsMap());
        if (resultNode.HasKey("Label")) {
            const auto& labelNode = resultNode["Label"];
            CHECK(labelNode.IsString());
            res.Label = labelNode.AsString();
        }

        if (resultNode.HasKey("Position")) {
            TPosition pos;
            const auto& positionNode = resultNode["Position"];
            CHECK(positionNode.IsMap());
            CHECK(positionNode.HasKey("File"));
            const auto& fileNode = positionNode["File"];
            CHECK(fileNode.IsString());
            pos.File = fileNode.AsString();
            CHECK(positionNode.HasKey("Row"));
            const auto& rowNode = positionNode["Row"];
            CHECK(rowNode.IsInt64());
            pos.Row = rowNode.AsInt64();
            CHECK(positionNode.HasKey("Column"));
            const auto& columnNode = positionNode["Column"];
            CHECK(columnNode.IsInt64());
            pos.Column = columnNode.AsInt64();
            res.Position = pos;
        }

        CHECK(resultNode.HasKey("Write"));
        const auto& writeNodeList = resultNode["Write"];
        CHECK(writeNodeList.IsList());
        for (const auto& writeNode : writeNodeList.AsList()) {
            CHECK(writeNode.IsMap());
            TWrite write;
            if (writeNode.HasKey("Type")) {
                write.Type = &writeNode["Type"];
            }

            if (writeNode.HasKey("Data")) {
                write.Data = &writeNode["Data"];
            }

            if (writeNode.HasKey("Truncated")) {
                const auto& truncatedNode = writeNode["Truncated"];
                CHECK(truncatedNode.IsBool());
                write.IsTruncated = truncatedNode.AsBool();
            }

            if (writeNode.HasKey("Ref")) {
                const auto& refsNodeList = writeNode["Ref"];
                CHECK(refsNodeList.IsList());
                for (const auto& refNode : refsNodeList.AsList()) {
                    CHECK(refNode.IsMap());
                    CHECK(refNode.HasKey("Reference"));
                    const auto& referenceNodeList = refNode["Reference"];
                    CHECK(referenceNodeList.IsList());
                    TFullResultRef ref;
                    for (const auto& node : referenceNodeList.AsList()) {
                        CHECK(node.IsString());
                        ref.Reference.push_back(node.AsString());
                    }

                    if (refNode.HasKey("Columns")) {
                        const auto& columnsNode = refNode["Columns"];
                        CHECK(columnsNode.IsList());
                        ref.Columns.ConstructInPlace();
                        for (const auto& node : columnsNode.AsList()) {
                            CHECK(node.IsString());
                            ref.Columns->push_back(node.AsString());
                        }
                    }

                    CHECK(refNode.HasKey("Remove"));
                    const auto& removeNode = refNode["Remove"];
                    CHECK(removeNode.IsBool());
                    ref.Remove = removeNode.AsBool();

                    write.Refs.push_back(ref);
                }
            }

            res.Writes.push_back(write);
        }

        if (resultNode.HasKey("Truncated")) {
            const auto& truncatedNode = resultNode["Truncated"];
            CHECK(truncatedNode.IsBool());
            res.IsTruncated = truncatedNode.AsBool();
        }

        if (resultNode.HasKey("Unordered")) {
            const auto& unorderedNode = resultNode["Unordered"];
            CHECK(unorderedNode.IsBool());
            res.IsUnordered = unorderedNode.AsBool();
        }

        v.push_back(res);
    }

    return v;
}

}
