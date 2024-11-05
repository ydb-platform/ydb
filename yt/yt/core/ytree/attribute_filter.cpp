#include "attribute_filter.h"

#include "node.h"
#include "convert.h"
#include "fluent.h"
#include "ypath_client.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/async_writer.h>
#include <yt/yt/core/yson/async_consumer.h>
#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/string_filter.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

namespace NYT::NYTree {

using namespace NYPath;
using namespace NYson;
using namespace NLogging;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

//! Used only for YT_LOG_ALERT.
YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "AttributeFilter");

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

void CanonizeAndValidatePath(TYPath& path)
{
    TYPath result;
    result.reserve(path.size());

    try {
        if (path.empty()) {
            THROW_ERROR_EXCEPTION("Empty paths are not allowed");
        }

        NYPath::TTokenizer tokenizer(path);
        tokenizer.Expect(NYPath::ETokenType::StartOfStream);
        tokenizer.Advance();
        while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
            tokenizer.Expect(NYPath::ETokenType::Slash);
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);
            result += "/" + ToYPathLiteral(tokenizer.GetLiteralValue());
            tokenizer.Advance();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(TError("Error validating attribute path %Qv", path) << ex);
    }

    path = std::move(result);
}

////////////////////////////////////////////////////////////////////////////////

//! Create a consumer that filters incoming YSON by given path filter and
//! feeds the result into target consumer upon destruction.
//!
//! Parameter #sync controls whether the filtering consumer is to be used in
//! solely synchronous mode (i.e. no calls of IAsyncYsonConsumer::OnRaw(TFuture<TYsonString>)
//! are allowed). If set to true, the use of #targetConsumer is also restricted
//! to synchronous methods.

struct IHeterogenousFilterConsumer
    : public TAttributeFilter::IFilteringConsumer
    , public TAttributeFilter::IAsyncFilteringConsumer
{ };

std::unique_ptr<IHeterogenousFilterConsumer> CreateFilteringConsumerImpl(
    IAsyncYsonConsumer* targetConsumer,
    const TAttributeFilter::TPathFilter& pathFilter,
    bool sync,
    std::any targetConsumerHolder = {})
{
    class TFilterConsumer
        : public IHeterogenousFilterConsumer
    {
    public:
        TFilterConsumer(
            IAsyncYsonConsumer* targetConsumer,
            const std::vector<TYPath>& paths,
            bool sync,
            std::any targetConsumerHolder)
            : TargetConsumer_(targetConsumer)
            , Paths_(paths)
            , Sync_(sync)
            , TargetConsumerHolder_(std::move(targetConsumerHolder))
        { }

        IYsonConsumer* GetConsumer() override
        {
            YT_VERIFY(Sync_);
            return &AsyncWriter_;
        }

        IAsyncYsonConsumer* GetAsyncConsumer() override
        {
            YT_VERIFY(!Sync_);
            return &AsyncWriter_;
        }

        void Finish() override
        {
            // First, get a future for a single YSON string.
            const auto& asyncSegments = AsyncWriter_.GetSegments();
            TFuture<TYsonString> asyncYson;

            if (asyncSegments.size() != 1) {
                // I am not sure whether such scenario happens in real life.
                // The only place I found that uses async OnRaw is object_detail.cpp,
                // and there may be either only sync calls or only one async call of consumer.
                // But just in case, let async writer do the job on concatenating these segments.
                asyncYson = AsyncWriter_.Finish();
            } else {
                asyncYson = asyncSegments.front().ApplyUnique(BIND([] (std::pair<TYsonString, bool>&& pair) {
                    return std::move(pair.first);
                }));
            }

            // Second, perform actual filtration.
            auto asyncFilteredYson = asyncYson.ApplyUnique(BIND([paths = std::move(Paths_), sync = Sync_] (TYsonString&& yson) {
                // Note the special case when there are no matches. Ideally we would like to not emit
                // our attribute at all, but the possibility to do so depends on whether we are in sync or async case.
                //
                // If we are in sync case, we may ask FilterYsonString to run in a mode, in which it may return
                // the null YSON string which has a special meaning of "no matches". If we see the null YSON string, we
                // do not call TargetConsumer_ at all. In typical case TargetConsumer_ is an instance of
                // TAttributeValueConsumer which does not emit an attribute key in such case, and our goal is fulfilled.
                //
                // In contrary to the synchronous case, asynchronous TAttributeValueConsumer::OnRaw always emits
                // an attribute key. Therefore, we must ensure we get some meaningful value. We request a fallback
                // implementation of filtration routine that always returns an empty list, map or a scalar value.
                return FilterYsonString(paths, yson, /*allowNullResult*/ sync);
            }));

            // Finally, feed this asynchronous yson to the target consumer. And now goes the tricky part.
            //
            // If sync flag is set, we must use target consumer synchronously. This contract is important for
            // synchronous overload of TAttributeFilter::CreateFilteringConsumer to work properly. Failure to do so
            // would result in YT_ABORT() in case when TAsyncYsonConsumerAdapter is used as a target (refer to
            // the implementation of synchronous CreateFilteringConsumer overload).
            //
            // Now note that in synchronous case asyncSegments contains a single set future, therefore
            // asyncYson is also a set future, and asyncFilteredYson is also set.
            if (Sync_) {
                // This could be YT_VERIFY if this code was not so heavily used in master. Hope trace id
                // of the log message below will help in investigation.
                YT_LOG_ALERT_UNLESS(asyncFilteredYson.IsSet(), "Unexpected unset future in synchronous attribute filtering");
                if (!asyncFilteredYson.IsSet()) {
                    THROW_ERROR_EXCEPTION("Unexpected unset future in synchronous attribute filtering");
                }

                auto&& filteredYsonOrError = asyncFilteredYson.Get();
                filteredYsonOrError.ThrowOnError();

                auto filteredYson = std::move(filteredYsonOrError.Value());

                // Handle the null YSON string case (see comment around asyncFilteredYson definition).
                if (filteredYson) {
                    TargetConsumer_->OnRaw(filteredYson);
                }
            } else {
                TargetConsumer_->OnRaw(std::move(asyncFilteredYson));
            }
        }

    private:
        IAsyncYsonConsumer* const TargetConsumer_;
        const std::vector<TYPath> Paths_;
        const bool Sync_;
        const std::any TargetConsumerHolder_;

        TAsyncYsonWriter AsyncWriter_;
    };

    return std::make_unique<TFilterConsumer>(targetConsumer, *pathFilter, sync, std::move(targetConsumerHolder));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TAttributeFilter::TAttributeFilter(std::vector<TString> keys, std::vector<TYPath> paths)
    : Keys(std::move(keys))
    , Paths(std::move(paths))
    , Universal(false)
{ }

TAttributeFilter& TAttributeFilter::operator =(std::vector<TString> keys)
{
    Keys = std::move(keys);
    Paths = {};
    Universal = false;

    return *this;
}

TAttributeFilter::operator bool() const
{
    return !Universal;
}

void TAttributeFilter::ValidateKeysOnly(TStringBuf context) const
{
    if (!Paths.empty()) {
        THROW_ERROR_EXCEPTION("Filtering attributes by path is not implemented for %v", context);
    }
}

bool TAttributeFilter::IsEmpty() const
{
    return !Universal && Keys.empty() && Paths.empty();
}

bool TAttributeFilter::AdmitsKeySlow(TStringBuf key) const
{
    if (!*this) {
        return true;
    }
    return std::find(Keys.begin(), Keys.end(), key) != Keys.end() ||
        std::find(Paths.begin(), Paths.end(), "/" + ToYPathLiteral(key)) != Paths.end();
}

TAttributeFilter::TKeyToFilter TAttributeFilter::Normalize() const
{
    YT_VERIFY(*this);
    if (Paths.empty()) {
        // Fast path for key-only case.
        TKeyToFilter result;
        result.reserve(Keys.size());
        for (const auto& key : Keys) {
            result[key] = std::nullopt;
        }
        return result;
    }

    // As a first step, prepare a combined vector of paths: canonize all paths
    // and transform all keys to paths of form /<ToYPathLiteral(key)> (which is
    // already a canonical form).
    std::vector<TYPath> paths = Paths;
    for (auto& path : paths) {
        NDetail::CanonizeAndValidatePath(path);
    }
    paths.reserve(paths.size() + Keys.size());
    for (const auto& key : Keys) {
        paths.emplace_back("/" + ToYPathLiteral(key));
    }

    YT_VERIFY(!paths.empty());

    // Then, sort all paths lexicographically and perform a unique-like procedure. Note that
    // the lexicographical order for YPaths corresponds to the preorder traversal of a YTree.
    std::sort(paths.begin(), paths.end());
    {
        // A pointer to the last taken path.
        auto lastIt = paths.begin();
        for (auto it = std::next(paths.begin()); it != paths.end(); ++it) {
            // Check if the current path is in a subtree of that last taken path.
            if (!HasPrefix(*it, *lastIt)) {
                (++lastIt)->swap(*it);
            }
        }
        paths.erase(++lastIt, paths.end());
    }

    // Finally, group remaining paths by the first token in path.

    //! Split a path into a first key value and a remaining suffix.
    auto splitPath = [] (const TYPath& path) -> std::pair<TString, TYPath> {
        NYPath::TTokenizer tokenizer(path);
        tokenizer.Expect(NYPath::ETokenType::StartOfStream);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();
        auto suffix = TYPath(tokenizer.GetSuffix());
        return {std::move(key), std::move(suffix)};
    };

    TKeyToFilter result;

    TString firstKey;
    TYPath firstSuffix;
    std::tie(firstKey, firstSuffix) = splitPath(paths.front());
    std::vector<TYPath> subpaths = {std::move(firstSuffix)};

    //! Finishes current group.
    auto flushGroup = [&] {
        result[firstKey] = std::move(subpaths);
    };

    for (auto firstIt = paths.begin(); firstIt != paths.end(); ) {
        auto lastIt = std::next(firstIt);
        // Extract a contiguous segment of paths starting from #firstKey.
        while (true) {
            // We have reached the end, flush the group.
            if (lastIt == paths.end()) {
                flushGroup();
                break;
            }
            auto [lastKey, lastSuffix] = splitPath(*lastIt);
            // A new group started; flush current group and setup firstKey for the next group.
            if (lastKey != firstKey) {
                flushGroup();
                // Micro-optimization: re-use result of last splitting in order to perform as
                // little splittings as possible.
                firstKey = std::move(lastKey);
                subpaths = {std::move(lastSuffix)};
                break;
            }
            subpaths.emplace_back(std::move(lastSuffix));
            ++lastIt;
        }
        firstIt = lastIt;
    }

    return result;
}

std::unique_ptr<TAttributeFilter::IFilteringConsumer> TAttributeFilter::CreateFilteringConsumer(
    IYsonConsumer* targetConsumer,
    const TPathFilter& pathFilter)
{
    if (!pathFilter) {
        class TBypassFilteringConsumer
            : public TAttributeFilter::IFilteringConsumer
        {
        public:
            explicit TBypassFilteringConsumer(IYsonConsumer* targetConsumer)
                : TargetConsumer_(targetConsumer)
            { }

            IYsonConsumer* GetConsumer() override
            {
                return TargetConsumer_;
            }

            void Finish() override
            { }

        private:
            IYsonConsumer* TargetConsumer_;
        };

        return std::make_unique<TBypassFilteringConsumer>(targetConsumer);
    }

    // Implementation of CreateFilteringConsumerImpl with sync = true has an important property.
    // Provided that async filtering consumer is used solely in synchronous manner,
    // it invokes only synchronous part of the target consumer interface. Therefore,
    // it is safe to implement this overload using TAsyncYsonConsumerAdapter which
    // invokes YT_ABORT() whenever asynchronous interface is used on it, and return
    // IAsyncYsonConsumer* upcasted to IYsonConsumer*.

    auto asyncTargetConsumer = std::make_shared<TAsyncYsonConsumerAdapter>(targetConsumer);
    // Do not forget to pass ownership for the adapter to a resulting filtering consumer.
    return NDetail::CreateFilteringConsumerImpl(asyncTargetConsumer.get(), pathFilter, /*sync*/ true, /*targetConsumerHolder*/ asyncTargetConsumer);
}

std::unique_ptr<TAttributeFilter::IAsyncFilteringConsumer> TAttributeFilter::CreateAsyncFilteringConsumer(
    IAsyncYsonConsumer* targetConsumer,
    const TPathFilter& pathFilter)
{
    if (!pathFilter) {
        class TBypassAsyncFilteringConsumer
            : public TAttributeFilter::IAsyncFilteringConsumer
        {
        public:
            explicit TBypassAsyncFilteringConsumer(IAsyncYsonConsumer* targetConsumer)
                : TargetConsumer_(targetConsumer)
            { }

            IAsyncYsonConsumer* GetAsyncConsumer() override
            {
                return TargetConsumer_;
            }

            void Finish() override
            { }

        private:
            IAsyncYsonConsumer* TargetConsumer_;
        };

        return std::make_unique<TBypassAsyncFilteringConsumer>(targetConsumer);
    }

    return NDetail::CreateFilteringConsumerImpl(targetConsumer, pathFilter, /*sync*/ false);
}

////////////////////////////////////////////////////////////////////////////////

// NB: universal filter is represented as an absent protobuf value.

void ToProto(NProto::TAttributeFilter* protoFilter, const TAttributeFilter& filter)
{
    YT_VERIFY(filter);

    ToProto(protoFilter->mutable_keys(), filter.Keys);
    ToProto(protoFilter->mutable_paths(), filter.Paths);
}

void FromProto(TAttributeFilter* filter, const NProto::TAttributeFilter& protoFilter)
{
    filter->Universal = false;
    FromProto(&filter->Keys, protoFilter.keys());
    FromProto(&filter->Paths, protoFilter.paths());
}

void Serialize(const TAttributeFilter& filter, IYsonConsumer* consumer)
{
    if (filter) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("keys").Value(filter.Keys)
                .Item("paths").Value(filter.Paths)
            .EndMap();
    } else {
        BuildYsonFluently(consumer)
            .Entity();
    }
}

void Deserialize(TAttributeFilter& filter, const INodePtr& node)
{
    switch (node->GetType()) {
        case ENodeType::Map: {
            auto mapNode = node->AsMap();

            filter.Universal = false;
            filter.Keys.clear();
            if (auto keysNode = mapNode->FindChild("keys")) {
                filter.Keys = ConvertTo<std::vector<TString>>(keysNode);
            }

            filter.Paths.clear();
            if (auto pathsNode = mapNode->FindChild("paths")) {
                filter.Paths = ConvertTo<std::vector<TString>>(pathsNode);
            }

            break;
        }
        case ENodeType::List: {
            // Compatibility mode with HTTP clients that specify attribute keys as string lists.
            filter.Universal = false;
            filter.Keys = ConvertTo<std::vector<TString>>(node);
            filter.Paths = {};
            break;
        }
        case ENodeType::Entity: {
            filter.Universal = true;
            filter.Keys = {};
            filter.Paths = {};
            break;
        }
        default:
            THROW_ERROR_EXCEPTION("Unexpected attribute filter type: expected \"map\", \"list\" or \"entity\", got %Qlv", node->GetType());
    }
}

void Deserialize(TAttributeFilter& attributeFilter, TYsonPullParserCursor* cursor)
{
    Deserialize(attributeFilter, ExtractTo<NYTree::INodePtr>(cursor));
}

void FormatValue(
    TStringBuilderBase* builder,
    const TAttributeFilter& attributeFilter,
    TStringBuf /*spec*/)
{
    if (attributeFilter) {
        builder->AppendFormat("{Keys: %v, Paths: %v}", attributeFilter.Keys, attributeFilter.Paths);
    } else {
        builder->AppendString("(universal)");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
