#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/dnf.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;

TEST(TDnfTest, Conjunctions)
{
    TConjunctiveClause clause;
    EXPECT_TRUE(clause.IsSatisfiedBy(std::vector<TString>()));
    EXPECT_TRUE(clause.IsSatisfiedBy(std::vector<TString>({TString("aaa")})));

    clause = TConjunctiveClause({"aaa"}, {});
    EXPECT_FALSE(clause.IsSatisfiedBy(std::vector<TString>()));
    EXPECT_TRUE(clause.IsSatisfiedBy(std::vector<TString>({TString("aaa")})));

    clause = TConjunctiveClause({"aaa", "bbb"}, {});
    EXPECT_FALSE(clause.IsSatisfiedBy(std::vector<TString>()));
    EXPECT_FALSE(clause.IsSatisfiedBy(std::vector<TString>({TString("aaa")})));
    EXPECT_TRUE(clause.IsSatisfiedBy(std::vector<TString>({TString("aaa"), TString("bbb")})));

    clause = TConjunctiveClause({"aaa", "bbb"}, {"ccc"});
    EXPECT_FALSE(clause.IsSatisfiedBy(std::vector<TString>()));
    EXPECT_FALSE(clause.IsSatisfiedBy(std::vector<TString>({TString("aaa")})));
    EXPECT_TRUE(clause.IsSatisfiedBy(std::vector<TString>({TString("aaa"), TString("bbb")})));
    EXPECT_FALSE(clause.IsSatisfiedBy(std::vector<TString>({TString("aaa"), TString("bbb"), TString("ccc")})));
}

TEST(TDnfTest, Dnf)
{
    TDnfFormula dnf;

    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<TString>()));
    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<TString>({TString("aaa")})));

    dnf.Clauses().push_back(TConjunctiveClause({"aaa", "bbb"}, {"ccc"}));

    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<TString>()));
    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<TString>({TString("aaa")})));
    EXPECT_TRUE(dnf.IsSatisfiedBy(std::vector<TString>({TString("aaa"), TString("bbb")})));
    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<TString>({TString("aaa"), TString("bbb"), TString("ccc")})));

    dnf.Clauses().push_back(TConjunctiveClause({"ccc"}, {}));

    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<TString>()));
    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<TString>({TString("aaa")})));
    EXPECT_TRUE(dnf.IsSatisfiedBy(std::vector<TString>({TString("aaa"), TString("bbb")})));
    EXPECT_TRUE(dnf.IsSatisfiedBy(std::vector<TString>({TString("ccc")})));
    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<TString>({TString("bbb")})));
}

TEST(TDnfTest, Serialization)
{
    auto clause = TConjunctiveClause({"aaa", "bbb"}, {"ccc"});
    auto conjunctionString = ConvertToYsonString(clause, EYsonFormat::Text);
    EXPECT_EQ("{\"include\"=[\"aaa\";\"bbb\";];\"exclude\"=[\"ccc\";];}", conjunctionString.AsStringBuf());
    EXPECT_EQ(clause, ConvertTo<TConjunctiveClause>(conjunctionString));

    auto dnf = TDnfFormula({
        TConjunctiveClause({"aaa", "bbb"}, {"ccc"}),
        TConjunctiveClause({"ccc"}, {})});
    auto dnfString = ConvertToYsonString(dnf, EYsonFormat::Text);
    EXPECT_EQ("[{\"include\"=[\"aaa\";\"bbb\";];\"exclude\"=[\"ccc\";];};{\"include\"=[\"ccc\";];\"exclude\"=[];};]", dnfString.AsStringBuf());
    EXPECT_EQ(dnf, ConvertTo<TDnfFormula>(dnfString));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
