#pragma once
#include <library/cpp/yson/node/node.h>
#include <yql/essentials/public/langver/yql_langver.h>

namespace NYql {

class TConfigException: public yexception {};

class IPolyArgs {
public:
    virtual ~IPolyArgs() = default;

    using TArgs = THashMap<TString, NYT::TNode>;

    struct TUnresolvedInput {
        TMaybe<TVector<NYT::TNode>> UserTypeArgs;
        TLangVersion LangVer = MinLangVersion;
    };

    virtual ui32 GetPredicatesCount() const = 0;
    virtual TMaybe<TUnresolvedInput> GetUnresolvedInput(ui32 index) const = 0;

    struct TMatchResult {
        ui32 Index = Max<ui32>();
        TMaybe<NYT::TNode> CallableType;
        TMaybe<NYT::TNode> RunConfigType;
        TMaybe<TString> Error;
    };

    // may fill callable type if specified, otherwise resolved callable type should be used
    // returns index of first matched predicate
    virtual TMatchResult Match(const TArgs& args, TLangVersion version) const = 0;
};

std::unique_ptr<IPolyArgs> ParsePolyArgs(const NYT::TNode& config);

} // namespace NYql
