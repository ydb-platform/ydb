#include "helpers.h"

#include <library/cpp/yson/writer.h>

#include <library/cpp/yson/node/node_visitor.h>

#include <util/string/ascii.h>
#include <util/generic/hash_set.h>


namespace NYql {
    namespace NPureCalc {
        namespace NPrivate {
            NYT::TNode GetSchema(
                const TVector<TString>& fields,
                const TVector<TString>& optionalFields
            ) {
                THashSet<TString> optionalFilter {
                    optionalFields.begin(), optionalFields.end()
                };

                NYT::TNode members {NYT::TNode::CreateList()};

                auto addField = [&] (const TString& name, const TString& type) {
                    auto typeNode = NYT::TNode::CreateList()
                        .Add("DataType")
                        .Add(type);

                    if (optionalFilter.contains(name)) {
                        typeNode = NYT::TNode::CreateList()
                            .Add("OptionalType")
                            .Add(typeNode);
                    }

                    members.Add(NYT::TNode::CreateList()
                        .Add(name)
                        .Add(typeNode)
                    );
                };

                for (const auto& field: fields) {
                    TString type {field};
                    type[0] = AsciiToUpper(type[0]);
                    addField(field, type);
                }

                NYT::TNode schema = NYT::TNode::CreateList()
                    .Add("StructType")
                    .Add(members);

                return schema;
            }
        }
    }
}
