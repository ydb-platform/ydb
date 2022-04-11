#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/url_builder.h>

#include <util/generic/size_literals.h>

#include <contrib/libs/re2/re2/re2.h>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/xml/document/xml-document.h>
#include <library/cpp/retry/retry_policy.h>

namespace NYql {

namespace {

using namespace NNodes;

std::array<TExprNode::TPtr, 2U> GetSchema(const TExprNode& settings) {
    for (auto i = 0U; i < settings.ChildrenSize(); ++i)
        if (settings.Child(i)->Head().IsAtom("userschema"))
            return {settings.Child(i)->ChildPtr(1), settings.Child(i)->ChildrenSize() > 2 ? settings.Child(i)->TailPtr() : TExprNode::TPtr()};

    return {};
}

using TItemsMap = std::map<TString, std::size_t>;
using TPendingBuckets = std::unordered_map<std::tuple<TString, TString, TString>, std::tuple<TNodeSet, TItemsMap, TIssues>, THash<std::tuple<TString, TString, TString>>>;

ERetryErrorClass RetryS3SlowDown(long httpResponseCode) {
    return httpResponseCode == 503 ? ERetryErrorClass::LongRetry : ERetryErrorClass::NoRetry; // S3 Slow Down == 503
}

void OnDiscovery(
    IHTTPGateway::TWeakPtr gateway,
    TPosition pos,
    IHTTPGateway::TResult&& result,
    const TPendingBuckets::key_type& keys,
    TPendingBuckets::mapped_type& output,
    NThreading::TPromise<void> promise,
    std::weak_ptr<TPendingBuckets> pendingBucketsWPtr,
    int promiseInd,
    const IRetryPolicy<long>::TPtr& retryPolicy) {
    auto pendingBuckets = pendingBucketsWPtr.lock(); // keys and output could be used only when TPendingBuckets is alive
    if (!pendingBuckets) {
        return;
    }
    TString logMsg = TStringBuilder() << "promise #" << promiseInd << ": ";
    switch (result.index()) {
    case 0U: try {
        logMsg += "Result received";
        const NXml::TDocument xml(std::get<IHTTPGateway::TContent>(std::move(result)).Extract(), NXml::TDocument::String);
        if (const auto& root = xml.Root(); root.Name() == "Error") {
            const auto& code = root.Node("Code", true).Value<TString>();
            const auto& message = root.Node("Message", true).Value<TString>();
            std::get<TIssues>(output) = {TIssue(pos, TStringBuilder() << message << ", error: code: " << code)};
            break;
        } else if (root.Name() != "ListBucketResult") {
            std::get<TIssues>(output) = { TIssue(pos, TStringBuilder() << "Unexpected response '" << root.Name() << "' on discovery.") };
            break;
        } else if (const NXml::TNamespacesForXPath nss(1U, {"s3", "http://s3.amazonaws.com/doc/2006-03-01/"});
            root.Node("s3:KeyCount", false, nss).Value<unsigned>() > 0U) {
            const auto& contents = root.XPath("s3:Contents", false, nss);
            auto& items = std::get<TItemsMap>(output);
            if (items.size() + contents.size() > 9000ULL) {
                std::get<TIssues>(output) = { TIssue(pos, TStringBuilder() << "It's over nine thousand items under '" << std::get<0U>(keys) << std::get<1U>(keys) << "'!")};
                break;
            }

            for (const auto& content : contents) {
                items.emplace(content.Node("s3:Key", false, nss).Value<TString>(), content.Node("s3:Size", false, nss).Value<unsigned>());
            }

            if (root.Node("s3:IsTruncated", false, nss).Value<bool>()) {
                if (const auto g = gateway.lock()) {
                    const auto& next = root.Node("s3:NextContinuationToken", false, nss).Value<TString>();
                    const auto& maxKeys = root.Node("s3:MaxKeys", false, nss).Value<TString>();

                    IHTTPGateway::THeaders headers;
                    if (const auto& token = std::get<2U>(keys); !token.empty())
                        headers.emplace_back(token);

                    TString prefix(std::get<1U>(keys));
                    TUrlBuilder urlBuilder(std::get<0U>(keys));
                    auto url = urlBuilder.AddUrlParam("list-type", "2")
                                         .AddUrlParam("prefix", prefix)
                                         .AddUrlParam("continuation-token", next)
                                         .AddUrlParam("max-keys", maxKeys)
                                         .Build();

                    return g->Download(
                        url,
                        std::move(headers),
                        0U,
                        std::bind(&OnDiscovery, gateway, pos, std::placeholders::_1, std::cref(keys), std::ref(output), std::move(promise), pendingBucketsWPtr, promiseInd, retryPolicy),
                        /*data=*/"",
                        retryPolicy);
                }
                YQL_CLOG(INFO, ProviderS3) << "Gateway disappeared.";
            }
        }

        break;
    } catch (const std::exception& ex) {
        logMsg += TStringBuilder() << "Exception occurred: " << ex.what();
        std::get<TIssues>(output) = {TIssue(pos, TStringBuilder() << "Error '" << ex.what() << "' on parse discovery response.")};
        break;
    }
    case 1U:
        logMsg += TStringBuilder() << "Issues occurred: " << std::get<TIssues>(result).ToString();
        std::get<TIssues>(output) = std::get<TIssues>(std::move(result));
        break;
    default:
        logMsg += TStringBuilder() << "Undefined variant index: " << result.index();
        std::get<TIssues>(output) = {TIssue(pos, TStringBuilder() << "Unexpected variant index " << result.index() << " on discovery.")};
        break;
    }

    YQL_CLOG(DEBUG, ProviderS3) << "Set promise with log message: " << logMsg;
    promise.SetValue();
}

TString RegexFromWildcards(const std::string_view& pattern) {
    const auto& escaped = RE2::QuoteMeta(re2::StringPiece(pattern));
    TStringBuilder result;
    result << "(?s)";
    bool slash = false;
    bool group = false;

    for (const char& c : escaped) {
        switch (c) {
            case '{':
                result << '(';
                group = true;
                slash = false;
                break;
            case '}':
                result << ')';
                group = false;
                slash = false;
                break;
            case ',':
                if (group)
                    result << '|';
                else
                    result << "\\,";
                slash = false;
                break;
            case '\\':
                if (slash)
                    result << "\\\\";
                slash = !slash;
                break;
            case '*':
                result << ".*";
                slash = false;
                break;
            case '?':
                result << '.';
                slash = false;
                break;
            default:
                if (slash)
                    result << '\\';
                result << c;
                slash = false;
                break;
        }
    }
    return result;
}

class TS3IODiscoveryTransformer : public TGraphTransformerBase {
public:
    TS3IODiscoveryTransformer(TS3State::TPtr state, IHTTPGateway::TPtr gateway)
        : State_(std::move(state)), Gateway_(std::move(gateway))
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        if (ctx.Step.IsDone(TExprStep::DiscoveryIO)) {
            return TStatus::Ok;
        }

        if (auto reads = FindNodes(input, [&](const TExprNode::TPtr& node) {
            if (const auto maybeRead = TMaybeNode<TS3Read>(node)) {
                if (maybeRead.DataSource()) {
                    return maybeRead.Cast().Arg(2).Ref().IsCallable("MrObject");
                }
            }
            return false;
        }); !reads.empty()) {
            for (auto& r : reads) {
                const TS3Read read(std::move(r));
                const auto& object = read.Arg(2).Ref();
                const std::string_view& path = object.Head().Content();
                const auto& prefix = path.substr(0U, path.find_first_of("*?{"));
                const auto& connect = State_->Configuration->Clusters.at(read.DataSource().Cluster().StringValue());
                const auto& token = State_->Configuration->Tokens.at(read.DataSource().Cluster().StringValue());
                const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(State_->CredentialsFactory, token);
                const auto authToken = credentialsProviderFactory->CreateProvider()->GetAuthInfo();

                std::get<TNodeSet>((*PendingBuckets_)[std::make_tuple(connect.Url, TString(prefix), authToken.empty() ? TString() : TString("X-YaCloud-SubjectToken:") += authToken)]).emplace(read.Raw());
            }
        }

        std::vector<NThreading::TFuture<void>> handles;
        handles.reserve(PendingBuckets_->size());

        int i = 0;
        const auto retryPolicy = IRetryPolicy<long>::GetExponentialBackoffPolicy(RetryS3SlowDown);
        for (auto& bucket : *PendingBuckets_) {
            auto promise = NThreading::NewPromise();
            handles.emplace_back(promise.GetFuture());
            IHTTPGateway::THeaders headers;
            if (const auto& token = std::get<2U>(bucket.first); !token.empty())
                headers.emplace_back(token);
            std::weak_ptr<TPendingBuckets> pendingBucketsWPtr = PendingBuckets_;
            TString prefix(std::get<1U>(bucket.first));
            TUrlBuilder urlBuilder(std::get<0U>(bucket.first));
            const auto url = urlBuilder.AddUrlParam("list-type", "2")
                                       .AddUrlParam("prefix", prefix)
                                       .Build();
            Gateway_->Download(
                url,
                headers,
                0U,
                std::bind(&OnDiscovery,
                    IHTTPGateway::TWeakPtr(Gateway_), ctx.GetPosition((*std::get<TNodeSet>(bucket.second).cbegin())->Pos()), std::placeholders::_1,
                    std::cref(bucket.first), std::ref(bucket.second), std::move(promise), pendingBucketsWPtr, i++, retryPolicy),
                /*data=*/"",
                retryPolicy
            );
            YQL_CLOG(INFO, ProviderS3) << "Enumerate items in " << std::get<0U>(bucket.first) << std::get<1U>(bucket.first);
        }

        if (handles.empty()) {
            return TStatus::Ok;
        }

        AllFuture_ = NThreading::WaitExceptionOrAll(handles);
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) final {
        return AllFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        // Raise errors if any
        AllFuture_.GetValue();

        TNodeOnNodeOwnedMap replaces(PendingBuckets_->size());
        auto buckets = std::move(*PendingBuckets_);
        auto count = 0ULL;
        auto readSize = 0ULL;
        for (auto& bucket : buckets) {
            if (const auto issues = std::move(std::get<TIssues>(bucket.second))) {
                YQL_CLOG(INFO, ProviderS3) << "Discovery " << std::get<0U>(bucket.first) << std::get<1U>(bucket.first) << " error " << issues.ToString();
                std::for_each(issues.begin(), issues.end(), std::bind(&TExprContext::AddError, std::ref(ctx), std::placeholders::_1));
                return TStatus::Error;
            }

            const auto nodes = std::move(std::get<TNodeSet>(bucket.second));
            for (const auto r : nodes) {
                const TS3Read read(r);
                const auto& object = read.Arg(2).Ref();
                const std::string_view& path = object.Head().Content();
                const auto& items = std::get<TItemsMap>(bucket.second);
                YQL_CLOG(INFO, ProviderS3) << "Discovered " << items.size() << " items in " << std::get<0U>(bucket.first) << std::get<1U>(bucket.first);

                TExprNode::TListType paths;
                if (std::string_view::npos != path.find_first_of("?*{")) {
                    const RE2 re(re2::StringPiece(RegexFromWildcards(path)), RE2::Options());
                    paths.reserve(items.size());
                    auto total = 0ULL;
                    for (const auto& item : items) {
                        if (const re2::StringPiece piece(item.first); re.Match(piece, 0, item.first.size(), RE2::ANCHOR_BOTH, nullptr, 0)) {
                            if (item.second > State_->Configuration->FileSizeLimit) {
                                ctx.AddError(TIssue(ctx.GetPosition(object.Pos()), TStringBuilder() << "Object " <<  item.first << " size " << item.second << " is too large."));
                                return TStatus::Error;
                            }

                            total += item.second;
                            ++count;
                            paths.emplace_back(
                                ctx.Builder(object.Pos())
                                    .List()
                                        .Atom(0, item.first)
                                        .Atom(1, ToString(item.second), TNodeFlags::Default)
                                    .Seal()
                                .Build()
                            );
                        }
                    }

                    readSize += total;

                    if (paths.empty()) {
                        ctx.AddError(TIssue(ctx.GetPosition(object.Pos()), TStringBuilder() << "Object " <<  path << " has no items."));
                        return TStatus::Error;
                    }
                    YQL_CLOG(INFO, ProviderS3) << "Object " << path << " has " << paths.size() << " items with total size " << total;
                } else if (const auto f = items.find(TString(path)); items.cend() == f) {
                    ctx.AddError(TIssue(ctx.GetPosition(object.Pos()), TStringBuilder() << "Object " <<  path << " doesn't exist."));
                    return TStatus::Error;
                } else if (const auto size = f->second; size > State_->Configuration->FileSizeLimit) {
                    ctx.AddError(TIssue(ctx.GetPosition(object.Pos()), TStringBuilder() << "Object " <<  path << " size " << size << " is too large."));
                    return TStatus::Error;
                } else {
                    YQL_CLOG(INFO, ProviderS3) << "Object " << path << " size is " <<  size;
                    readSize += size;
                    ++count;
                    paths.emplace_back(
                        ctx.Builder(object.Pos())
                            .List()
                                .Add(0, object.HeadPtr())
                                .Atom(1, ToString(size), TNodeFlags::Default)
                            .Seal()
                        .Build()
                    );
                }

                auto children = object.ChildrenList();
                children.front() = ctx.NewList(object.Pos(), std::move(paths));
                auto s3Object = ctx.NewCallable(object.Pos(), TS3Object::CallableName(), std::move(children));
                auto userSchema = GetSchema(*read.Ref().Child(4));

                replaces.emplace(r, userSchema.back() ?
                    Build<TS3ReadObject>(ctx, read.Pos())
                        .World(read.World())
                        .DataSource(read.DataSource())
                        .Object(std::move(s3Object))
                        .RowType(std::move(userSchema.front()))
                        .ColumnOrder(std::move(userSchema.back()))
                    .Done().Ptr():
                    Build<TS3ReadObject>(ctx, read.Pos())
                        .World(read.World())
                        .DataSource(read.DataSource())
                        .Object(std::move(s3Object))
                        .RowType(std::move(userSchema.front()))
                    .Done().Ptr());
            }
        }

        YQL_CLOG(INFO, ProviderS3) << "Read " << count << " objects with total size is " <<  readSize;

        if (count > State_->Configuration->MaxFilesPerQuery) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Too many objects to read: " <<  count << ", but limit is " << State_->Configuration->MaxFilesPerQuery));
            return TStatus::Error;
        }

        if (readSize > State_->Configuration->MaxReadSizePerQuery) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Too large objects to read: " <<  readSize << ", but limit is " << State_->Configuration->MaxReadSizePerQuery));
            return TStatus::Error;
        }

        return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings(nullptr));
    }
private:
    const TS3State::TPtr State_;
    const IHTTPGateway::TPtr Gateway_;

    const std::shared_ptr<TPendingBuckets> PendingBuckets_ = std::make_shared<TPendingBuckets>();

    NThreading::TFuture<void> AllFuture_;
};

}

THolder<IGraphTransformer> CreateS3IODiscoveryTransformer(TS3State::TPtr state, IHTTPGateway::TPtr gateway) {
    return THolder(new TS3IODiscoveryTransformer(std::move(state), std::move(gateway)));
}

}
