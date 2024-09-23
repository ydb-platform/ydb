#include "read_attributes_utils.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>

namespace {
using namespace NYql;

struct TAstHelper {
    TExprNode::TPtr CreateReadNode(TStringBuf cluster, TStringBuf path, TExprNode::TListType attributes, TExprNode::TPtr world = nullptr) {
        if (!world) {
            world = Ctx.NewWorld(Pos);
        }
        return Ctx.NewCallable(Pos, ReadName, {
            std::move(world),
            Ctx.NewCallable(Pos, "DataSource", {
                Ctx.NewAtom(Pos, "kikimr"),
                Ctx.NewAtom(Pos, cluster),
            }),
            Ctx.NewCallable(Pos, "Key", {
                Ctx.NewList(Pos, {
                    Ctx.NewAtom(Pos, "table"),
                    Ctx.NewCallable(Pos, "String", {
                        Ctx.NewAtom(Pos, path)
                    }),
                }),
            }),
            Ctx.NewCallable(Pos, "Void", {}),
            Ctx.NewList(Pos, std::move(attributes)),
        });
    }

    TExprNode::TPtr NewAttribute(TStringBuf key, TStringBuf value) {
        return NewList({
            NewAtom(key),
            NewAtom(value),
        });
    }

    TExprNode::TPtr NewAtom(TStringBuf value) {
        return Ctx.NewAtom(Pos, value);
    }

    TExprNode::TPtr NewCallable(TStringBuf name, TExprNode::TListType arguments) {
        return Ctx.NewCallable(Pos, name, std::move(arguments));
    }

    TExprNode::TPtr NewList(TExprNode::TListType children) {
        return Ctx.NewList(Pos, std::move(children));
    }

    TExprContext Ctx;
    TPosition Pos;
};

void CheckAttributes(const THashMap<std::pair<TString, TString>, THashMap<TString, TString>>& allAttributes,
                     TStringBuf cluster, TStringBuf path, THashMap<TString, TString> expected) {
    auto attributes = allAttributes.FindPtr(std::make_pair(cluster, path));
    UNIT_ASSERT(attributes);
    ASSERT_THAT(*attributes, testing::ContainerEq(expected));
}
}

Y_UNIT_TEST_SUITE(ReadAttributesUtils) {
    using namespace NYql;

    Y_UNIT_TEST(AttributesGatheringEmpry) {
        TAstHelper helper;
        auto node = helper.CreateReadNode("cluster", "path/to/file", {});
        auto allAttributes = GatherReadAttributes(*node, helper.Ctx);
        UNIT_ASSERT_EQUAL(allAttributes.size(), 1);

        CheckAttributes(allAttributes, "cluster", "path/to/file", {});
    }

    Y_UNIT_TEST(AttributesGatheringFilter) {
        TAstHelper helper;
        auto node = helper.CreateReadNode("cluster", "path/to/file", {
            helper.NewList({
                helper.NewAtom("should ignore"),
                helper.NewAtom("because has"),
                helper.NewAtom("three atoms"),
            }),
            helper.NewAttribute("good", "attribute"),
            helper.NewList({
                helper.NewAtom("second is not atom, thus should be ignored"),
                helper.NewCallable("callable", {}),
            }),
            helper.NewAttribute("also good", "attribute"),
            helper.NewAtom("no children - not an attribute"),
            helper.NewCallable("callable instead of list", {
                helper.NewAtom("should"),
                helper.NewAtom("ignore"),
            })
        });
        auto allAttributes = GatherReadAttributes(*node, helper.Ctx);
        UNIT_ASSERT_EQUAL(allAttributes.size(), 1);

        CheckAttributes(allAttributes, "cluster", "path/to/file", {{"good", "attribute"}, {"also good", "attribute"}});
    }

    Y_UNIT_TEST(AttributesGatheringRecursive) {
        TAstHelper helper;
        auto node = helper.CreateReadNode("cluster", "path/1", {
            helper.NewAttribute("key11", "val11"),
        }, helper.NewCallable("Left!", {
            helper.CreateReadNode("cluster", "path/2", {
                helper.NewAttribute("key21", "val21"),
                helper.NewAttribute("key22", "val22"),
                helper.NewAttribute("key23", "val23"),
            }),
        }));
        auto allAttributes = GatherReadAttributes(*node, helper.Ctx);
        UNIT_ASSERT_EQUAL(allAttributes.size(), 2);

        CheckAttributes(allAttributes, "cluster", "path/1", {{"key11", "val11"}});
        CheckAttributes(allAttributes, "cluster", "path/2", {{"key21", "val21"}, {"key22", "val22"}, {"key23", "val23"}});
    }

    Y_UNIT_TEST(ReplaceAttributesEmpty) {
        TAstHelper helper;
        auto node = helper.CreateReadNode("cluster", "path", {
            helper.NewAttribute("key1", "val1"),
            helper.NewAttribute("key2", "val2"),
        });
        auto metadata = MakeIntrusive<TKikimrTableMetadata>();
        // empty attributes in metadata should not change anything
        ReplaceReadAttributes(*node, {{"key", "val"}}, "cluster", "path", metadata, helper.Ctx);

        auto allAttributes = GatherReadAttributes(*node, helper.Ctx);
        CheckAttributes(allAttributes, "cluster", "path", {{"key1", "val1"}, {"key2", "val2"}});
    }

    Y_UNIT_TEST(ReplaceAttributesFilter) {
        TAstHelper helper;
        auto node = helper.CreateReadNode("cluster", "path/to/file", {
            helper.NewList({
                helper.NewAtom("should ignore"),
                helper.NewAtom("because has"),
                helper.NewAtom("three atoms"),
            }),
            helper.NewAttribute("good", "attribute"),
            helper.NewList({
                helper.NewAtom("second is not atom, thus should be ignored"),
                helper.NewCallable("callable", {}),
            }),
            helper.NewAttribute("also good", "attribute"),
            helper.NewAtom("no children - not an attribute"),
            helper.NewCallable("callable instead of list", {
                helper.NewAtom("should"),
                helper.NewAtom("ignore"),
            })
        });
        auto metadata = MakeIntrusive<TKikimrTableMetadata>();
        // NB! changes to attributes from metadata, will result into replacing AST metadata
        metadata->Attributes = {{"good", "still good"}, {"new key", "new value"}};
        ReplaceReadAttributes(*node, {{"good", "attribute"}, {"also good", "attribute"}}, "cluster", "path/to/file", metadata, helper.Ctx);

        auto allAttributes = GatherReadAttributes(*node, helper.Ctx);
        CheckAttributes(allAttributes, "cluster", "path/to/file", metadata->Attributes);
    }
}