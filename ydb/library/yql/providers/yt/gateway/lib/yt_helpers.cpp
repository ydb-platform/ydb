#include "yt_helpers.h"

#include <ydb/library/yql/providers/yt/lib/res_pull/table_limiter.h>
#include <ydb/library/yql/providers/yt/lib/res_pull/res_or_pull.h>
#include <ydb/library/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec.h>
#include <ydb/library/yql/providers/common/gateway/yql_provider_gateway.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/minikql/aligned_page_pool.h>
#include <ydb/library/yql/utils/log/log.h>

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/error_codes.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/threading/future/future.h>

#include <util/string/split.h>
#include <util/system/env.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/generic/algorithm.h>

namespace NYql {

namespace {
EYqlIssueCode IssueCodeForYtError(const NYT::TYtError& error) {
    if (error.ContainsErrorCode(NYT::NClusterErrorCodes::NSecurityClient::AuthorizationError) ||
        error.ContainsErrorCode(NYT::NClusterErrorCodes::NSecurityClient::AuthenticationError)) {
        return TIssuesIds::YT_ACCESS_DENIED;
    }

    if (error.ContainsErrorCode(NYT::NClusterErrorCodes::NChunkPools::MaxDataWeightPerJobExceeded)) {
        return TIssuesIds::YT_MAX_DATAWEIGHT_PER_JOB_EXCEEDED;
    }

    return TIssuesIds::DEFAULT_ERROR;
}
}

TMaybe<ui64> GetUsedRows(const NYT::TRichYPath& table, ui64 tableRowCount) {
    TMaybe<ui64> rows;
    if (auto ranges = table.GetRanges()) {
        rows = 0;
        for (const NYT::TReadRange& readRange: *ranges) {
            if (readRange.Exact_.RowIndex_) {
                rows = rows.GetOrElse(0) + 1;
            } else if (readRange.LowerLimit_.RowIndex_ || readRange.UpperLimit_.RowIndex_) {
                ui64 range = tableRowCount;
                if (readRange.UpperLimit_.RowIndex_) {
                    range = *readRange.UpperLimit_.RowIndex_;
                }
                if (readRange.LowerLimit_.RowIndex_) {
                    range -= Min<ui64>(range, *readRange.LowerLimit_.RowIndex_);
                }
                rows = rows.GetOrElse(0) + range;
            } else {
                return Nothing();
            }
        }
    }
    return rows;
}

TMaybe<ui64> GetUncompressedFileSize(NYT::ITransactionPtr tx, const TString& path) {
    if (!tx->Exists(path)) {
        return 0;
    }

    NYT::TNode attrs = tx->Get(path + "/@", NYT::TGetOptions().AttributeFilter(
        NYT::TAttributeFilter()
            .AddAttribute(TString("uncompressed_data_size"))
    ));

    return GetDataWeight(attrs);
}

TString TransformPath(TStringBuf tmpFolder, TStringBuf name, bool isTempTable, TStringBuf userName) {
    TString path;
    path.assign(name);
    if (isTempTable && tmpFolder) {
        path = tmpFolder;
        if (!tmpFolder.EndsWith('/')) {
            path += '/';
        }
        path += name;
    }
    if (path.StartsWith("//")) {
        return path.substr(2);
    }

    if (isTempTable && !tmpFolder && path.StartsWith("tmp/")) {
        TStringBuilder builder;
        builder << "tmp/yql/";
        if (userName) {
            builder << userName << '/';
        }
        builder << path.substr(4);
        path = builder;
    }

    return path;
}

namespace {

THashSet<TStringBuf> DEPRECATED_YQL_ATTRS = {
    TStringBuf("_yql_key_meta"),
    TStringBuf("_yql_subkey_meta"),
    TStringBuf("_yql_value_meta"),
};

THashSet<TStringBuf> TEST_YQL_ATTRS = {
    YqlDynamicAttribute,
};

THashSet<TStringBuf> SERVICE_YQL_ATTRS = {
    TStringBuf("_yql_runner"),
    TStringBuf("_yql_op_id"),
    TStringBuf("_yql_op_title"),
    TStringBuf("_yql_query_name"),
};

THashSet<TString> SUPPORTED_RICH_YPATH_ATTRS = {
    "timestamp"
};

}

TMaybe<TString> SerializeRichYPathAttrs(const NYT::TRichYPath& richPath) {
    NYT::TNode pathNode;
    NYT::TNodeBuilder builder(&pathNode);
    NYT::Serialize(richPath, &builder);
    if (!pathNode.HasAttributes() || pathNode.GetAttributes().Empty()) {
        return Nothing();
    }
    auto attrMap = pathNode.GetAttributes().AsMap();
    attrMap.erase("columns");
    attrMap.erase("ranges");
    for (const auto& [attr, _] : attrMap) {
        if (!SUPPORTED_RICH_YPATH_ATTRS.contains(attr)) {
            throw yexception() << "Unsupported YPath attribute: '" << attr << "'";
        }
    }
    pathNode.Attributes() = attrMap;
    return NYT::NodeToYsonString(pathNode.GetAttributes());
}

void DeserializeRichYPathAttrs(const TString& serializedAttrs, NYT::TRichYPath& richPath) {
    NYT::TNode pathNode;
    NYT::TNodeBuilder pathNodeBuilder(&pathNode);
    NYT::Serialize(richPath, &pathNodeBuilder);
    NYT::MergeNodes(pathNode.Attributes(), NYT::NodeFromYsonString(serializedAttrs));
    NYT::Deserialize(richPath, pathNode);
}

IYtGateway::TCanonizedPath CanonizedPath(const TString& path) {
    NYT::TRichYPath richYPath(path);
    if (path.StartsWith('<')) {
        NYT::Deserialize(richYPath, NYT::NodeFromYsonString(path));
    }
    const auto additionalAttrs = SerializeRichYPathAttrs(richYPath);
    size_t pos = 0;
    if ((pos = richYPath.Path_.find('{')) != TString::npos) {
        size_t end = richYPath.Path_.find('}');
        YQL_ENSURE(end != TString::npos && end > pos);
        TVector<TString> columns;
        StringSplitter(richYPath.Path_.substr(pos + 1, end - pos - 1)).Split(',').AddTo(&columns);
        richYPath.Columns(columns);
        richYPath.Path_ = richYPath.Path_.substr(0, pos);
    }

    if ((pos = richYPath.Path_.find('[')) != TString::npos) {
        size_t end = richYPath.Path_.find(']');
        YQL_ENSURE(end != TString::npos && end > pos);
        TString rangeString = richYPath.Path_.substr(pos + 1, end - pos - 1);
        richYPath.Path_ = richYPath.Path_.substr(0, pos);
        TVector<TString> ranges;
        size_t startPos = 0;
        int insideParens = 0;
        for (size_t i = 0; i < rangeString.length(); ++i) {
            switch (rangeString.at(i)) {
            case '(':
                ++insideParens;
                break;
            case ')':
                --insideParens;
                break;
            case ',':
                if (0 == insideParens) {
                    ranges.push_back(rangeString.substr(startPos, i - startPos));
                    startPos = i + 1;
                }
                break;
            }
        }
        if (startPos < rangeString.length()) {
            ranges.push_back(rangeString.substr(startPos));
        }
        auto toReadLimit = [] (const TString& s) -> NYT::TReadLimit {
            if (s.StartsWith('#')) {
                return NYT::TReadLimit().RowIndex(FromString<i64>(s.substr(1)));
            } else if (s.StartsWith('(')) {
                YQL_ENSURE(s.EndsWith(')'));
                TVector<TString> keys;
                StringSplitter(s.substr(1, s.length() - 2)).Split(',').AddTo(&keys);
                NYT::TKey complexKey;
                for (auto& key: keys) {
                    complexKey.Add(NYT::NodeFromYsonString(key));
                }
                return NYT::TReadLimit().Key(complexKey);
            } else {
                return NYT::TReadLimit().Key(NYT::NodeFromYsonString(s));
            }
        };

        richYPath.MutableRanges().ConstructInPlace();
        for (TString& r: ranges) {
            if ((pos = r.find(':')) != TString::npos) {
                NYT::TReadRange range;
                if (TString lower = r.substr(0, pos)) {
                    range.LowerLimit(toReadLimit(lower));
                }
                if (TString upper = r.substr(pos + 1)) {
                    range.UpperLimit(toReadLimit(upper));
                }
                richYPath.AddRange(range);
            } else {
                richYPath.AddRange(NYT::TReadRange().Exact(toReadLimit(r)));
            }
        }
    }
    return {
        richYPath.Path_,
        richYPath.Columns_.Defined() ? richYPath.Columns_->Parts_ : TMaybe<TVector<TString>>(),
        richYPath.GetRanges(),
        additionalAttrs
    };
};

NYT::TNode GetUserAttributes(NYT::ITransactionPtr tx, TString path, TMaybe<bool> includeYqlAttrs) {
    path.append("/@");
    NYT::TNode attrs = tx->Get(path, NYT::TGetOptions()
        .AttributeFilter(NYT::TAttributeFilter()
            .AddAttribute("user_attribute_keys")
        )
    );
    if (attrs.HasKey("user_attribute_keys")) {
        NYT::TAttributeFilter filter;
        for (auto key: attrs["user_attribute_keys"].AsList()) {
            if (key.AsString().StartsWith("_yql") && includeYqlAttrs) {
                if (!*includeYqlAttrs
                    || TEST_YQL_ATTRS.contains(key.AsString())
                    || DEPRECATED_YQL_ATTRS.contains(key.AsString())
                    || SERVICE_YQL_ATTRS.contains(key.AsString())) {
                    continue;
                }
            }
            filter.AddAttribute(key.AsString());
        }
        if (!filter.Attributes_.empty()) {
            return tx->Get(path, NYT::TGetOptions().AttributeFilter(filter));
        }
    }
    return NYT::TNode::CreateMap();
}

void TransferTableAttributes(const NYT::TNode& attributes, const std::function<void(const TString&,const TString&)>& receiver)
{
    for (const auto& attr : attributes.AsMap()) {
        const TString& attrName = attr.first;
        const NYT::TNode& attrValue = attr.second;

        if (attrName == FORMAT_ATTR_NAME) {
            receiver(attrName, NYT::NodeToYsonString(attrValue));
        }
        else if (attrName.StartsWith(TStringBuf("_yql"))) {
            if (attrName == YqlRowSpecAttribute) {
                receiver(attrName, NYT::NodeToYsonString(attrValue));
            } else if (!TEST_YQL_ATTRS.contains(attrName)
                       && !DEPRECATED_YQL_ATTRS.contains(attrName)
                       && !SERVICE_YQL_ATTRS.contains(attr.first)) {
                try {
                    receiver(attrName, attrValue.ConvertTo<TString>());
                } catch (const NYT::TNode::TTypeError&) {
                    throw yexception() << "Unexpected value of '" << attrName << "' attribute: " << NYT::NodeToYsonString(attrValue);
                }
            }
        }
    }
}

NYT::TNode FilterYqlAttributes(const NYT::TNode& attributes)
{
    NYT::TNode res = NYT::TNode::CreateMap();
    for (const auto& attr : attributes.AsMap()) {
        if (attr.first.StartsWith(TStringBuf("_yql"))
            && !TEST_YQL_ATTRS.contains(attr.first)
            && !DEPRECATED_YQL_ATTRS.contains(attr.first)
            && !SERVICE_YQL_ATTRS.contains(attr.first)) {

            res[attr.first] = attr.second;
        }
    }
    return res;
}

template <bool YAMRED_DSV>
static bool IterateRows(NYT::ITransactionPtr tx,
    NYT::TRichYPath path,
    ui32 tableIndex,
    TMkqlIOCache& specsCache,
    IExecuteResOrPull& exec,
    const TTableLimiter& limiter,
    const TMaybe<TSampleParams>& sampling)
{
    const ui64 startRecordInTable = limiter.GetTableStart();
    const ui64 endRecordInTable = limiter.GetTableZEnd(); // 0 means the entire table usage

    if (startRecordInTable || endRecordInTable) {
        YQL_ENSURE(path.GetRanges().Empty());
        NYT::TReadRange readRange;
        if (startRecordInTable) {
            readRange.LowerLimit(NYT::TReadLimit().RowIndex(startRecordInTable));
        }
        if (endRecordInTable) {
            readRange.UpperLimit(NYT::TReadLimit().RowIndex(endRecordInTable));
        }
        path.AddRange(readRange);
    }

    NYT::TTableReaderOptions readerOptions;
    if (sampling && sampling->Mode == EYtSampleMode::Bernoulli) {
        NYT::TNode spec = NYT::TNode::CreateMap();
        spec["sampling_rate"] = sampling->Percentage / 100.;
        if (sampling->Repeat) {
            spec["sampling_seed"] = static_cast<i64>(sampling->Repeat);
        }
        readerOptions.Config(spec);
    }

    if (!YAMRED_DSV && exec.GetColumns()) {
        if (!specsCache.GetSpecs().Inputs[tableIndex]->OthersStructIndex) {
            path.Columns(*exec.GetColumns());
        }
    }

    if (YAMRED_DSV) {
        path.Columns_.Clear();
        auto reader = tx->CreateTableReader<NYT::TYaMRRow>(path, readerOptions);
        for (; reader->IsValid(); reader->Next()) {
            if (!exec.WriteNext(specsCache, reader->GetRow(), tableIndex)) {
                return true;
            }
        }
    } else {
        auto format = specsCache.GetSpecs().MakeInputFormat(tableIndex);
        auto rawReader = tx->CreateRawReader(path, format, readerOptions);
        TMkqlReaderImpl reader(*rawReader, 0, 4 << 10, tableIndex);
        reader.SetSpecs(specsCache.GetSpecs(), specsCache.GetHolderFactory());

        for (reader.Next(); reader.IsValid(); reader.Next()) {
            if (!exec.WriteNext(specsCache, reader.GetRow(), tableIndex)) {
                return true;
            }
        }
    }

    return false;
}

bool IterateYamredRows(NYT::ITransactionPtr tx,
    const NYT::TRichYPath& table,
    ui32 tableIndex,
    TMkqlIOCache& specsCache,
    IExecuteResOrPull& exec,
    const TTableLimiter& limiter,
    const TMaybe<TSampleParams>& sampling)
{
    return IterateRows<true>(tx, table, tableIndex, specsCache, exec, limiter, sampling);
}

bool IterateYsonRows(NYT::ITransactionPtr tx,
    const NYT::TRichYPath& table,
    ui32 tableIndex,
    TMkqlIOCache& specsCache,
    IExecuteResOrPull& exec,
    const TTableLimiter& limiter,
    const TMaybe<TSampleParams>& sampling)
{
    return IterateRows<false>(tx, table, tableIndex, specsCache, exec, limiter, sampling);
}

bool SelectRows(NYT::IClientPtr client,
    const TString& table,
    ui32 tableIndex,
    TMkqlIOCache& specsCache,
    IExecuteResOrPull& exec,
    TTableLimiter& limiter)
{
    ui64 startRecordInTable = limiter.GetTableStart();
    const ui64 endRecordInTable = limiter.GetTableZEnd(); // 0 means the entire table usage
    TStringStream sqlBuilder;
    const auto& columns = exec.GetColumns();
    if (columns) {
        bool isFirstColumn = true;
        for (auto& x : *columns) {
            if (!isFirstColumn) {
                sqlBuilder << ", ";
            }

            isFirstColumn = false;
            sqlBuilder << "[" << x << "]";
        }
    } else {
        sqlBuilder << "*";
    }

    sqlBuilder << " FROM [";
    sqlBuilder << NYT::AddPathPrefix(table, NYT::TConfig::Get()->Prefix);
    sqlBuilder << "]";
    if (exec.GetRowsLimit()) {
        ui64 effectiveLimit = endRecordInTable;
        if (!effectiveLimit) {
            effectiveLimit = startRecordInTable + *exec.GetRowsLimit() + 1;
        } else {
            effectiveLimit = Min(effectiveLimit, *exec.GetRowsLimit() + 1);
        }

        sqlBuilder << " LIMIT " << effectiveLimit;
    }

    ui64 processed = 0;
    bool ret = false;
    auto rows = client->SelectRows(sqlBuilder.Str());
    for (const auto& row : rows) {
        ++processed;
        if (processed <= startRecordInTable) {
            continue;
        }

        if (!exec.WriteNext(specsCache, row, tableIndex)) {
            ret = true;
            break;
        }

        if (endRecordInTable) {
            if (processed >= endRecordInTable) {
                break;
            }
        }
    }

    limiter.Skip(processed);
    return ret;
}

NYT::TNode YqlOpOptionsToSpec(const TYqlOperationOptions& opOpts, const TString& userName, const TVector<std::pair<TString, TString>>& code)
{
    NYT::TNode spec = NYT::TNode::CreateMap();

    if (auto title = opOpts.Title.GetOrElse(TString())) {
        spec["title"] = title;
    } else {
        TStringBuilder titleBuilder;
        titleBuilder << "YQL operation (";
        if (opOpts.QueryName) {
            titleBuilder << *opOpts.QueryName;
        }
        if (opOpts.Id) {
            if (opOpts.QueryName) {
                titleBuilder << ", ";
            }
            titleBuilder << *opOpts.Id;
        }
        titleBuilder << " by " << userName << ')';
        spec["title"] = titleBuilder;
    }

    NYT::TNode& description = spec["description"];
    description["yql_runner"] = opOpts.Runner;

    if (auto id = opOpts.Id.GetOrElse(TString())) {
        description["yql_op_id"] = id;
    }

    if (auto url = opOpts.Url.GetOrElse(TString())) {
        NYT::TNode& urlNode = description["yql_op_url"];
        urlNode = url;
        // Mark as URL for YT UI (see https://clubs.at.yandex-team.ru/yt/2364)
        urlNode.Attributes()["_type_tag"] = "url";
    }

    if (auto title = opOpts.Title.GetOrElse(TString())) {
        description["yql_op_title"] = title;
    }

    if (auto name = opOpts.QueryName.GetOrElse(TString())) {
        description["yql_query_name"] = name;
    }

    static constexpr size_t OP_CODE_LIMIT = 1ul << 17; // 128Kb

    if (!code.empty()) {
        size_t remaining = OP_CODE_LIMIT;
        NYT::TNode& codeNode = description["yql_op_code"];
        for (auto& c: code) {
            TString snippet = c.second;
            if (!remaining) {
                snippet = "__truncated__";
            } else if (snippet.length() > remaining) {
                // Keep the end part of the code as more interesting
                snippet = TStringBuilder() << "__truncated__\n" << TStringBuf(snippet).Last(remaining) << "\n__truncated__";
            }
            codeNode[c.first] = snippet;
            remaining -= Min(remaining, snippet.length());
        }
    }

    if (auto attrs = opOpts.AttrsYson.GetOrElse(TString())) {
        NYT::TNode userAttrs = NYT::NodeFromYsonString(attrs);
        for (const auto& item: userAttrs.AsMap()) {
            const TString& key = item.first;
            const NYT::TNode& value = item.second;

            if (key != TStringBuf("runner") &&
                key != TStringBuf("op_id") &&
                key != TStringBuf("op_url") &&
                key != TStringBuf("op_title") &&
                key != TStringBuf("query_name") &&
                key != TStringBuf("op_code"))
            {
                // do not allow to override specific attrs
                description[TString("yql_") + key] = value;
            }
        }
    }

    return spec;
}

NYT::TNode YqlOpOptionsToAttrs(const TYqlOperationOptions& opOpts) {
    NYT::TNode attrs = NYT::TNode::CreateMap();

    attrs["_yql_runner"] = opOpts.Runner;
    if (auto id = opOpts.Id.GetOrElse(TString())) {
        attrs["_yql_op_id"] = id;
    }
    if (auto title = opOpts.Title.GetOrElse(TString())) {
        attrs["_yql_op_title"] = title;
    }
    if (auto name = opOpts.QueryName.GetOrElse(TString())) {
        attrs["_yql_query_name"] = name;
    }
    return attrs;
}

void CreateParents(const TVector<TString>& tables, NYT::IClientBasePtr tx) {
    auto batchExists = tx->CreateBatchRequest();
    TVector<NThreading::TFuture<void>> batchExistsRes;

    THashSet<TString> uniqFolders;
    auto batchCreateParent = tx->CreateBatchRequest();
    TVector<NThreading::TFuture<NYT::TLockId>> batchCreateParentRes;

    for (auto& table: tables) {
        auto slash = table.rfind('/');
        if (TString::npos != slash) {
            TString folder = table.substr(0, slash);
            if (uniqFolders.insert(folder).second) {
                batchExistsRes.push_back(
                    batchExists->Exists(folder).Apply([&batchCreateParentRes, &batchCreateParent, folder](const NThreading::TFuture<bool>& f) {
                        if (!f.GetValue()) {
                            batchCreateParentRes.push_back(batchCreateParent->Create(folder, NYT::NT_MAP,
                                NYT::TCreateOptions().Recursive(true).IgnoreExisting(true)));
                        }
                    })
                );
            }
        }
    }

    batchExists->ExecuteBatch();
    ForEach(batchExistsRes.begin(), batchExistsRes.end(), [] (const NThreading::TFuture<void>& f) {
        f.GetValue();
    });

    if (!batchCreateParentRes.empty()) {
        batchCreateParent->ExecuteBatch();
        ForEach(batchCreateParentRes.begin(), batchCreateParentRes.end(), [] (const NThreading::TFuture<NYT::TLockId>& f) {
            f.GetValue();
        });
    }
}

TIssue MakeIssueFromYtError(const NYT::TYtError& e, TStringBuf what, TPosition pos, bool shortErrors) {
    TString errMsg = shortErrors || GetEnv("YQL_DETERMINISTIC_MODE") ? e.ShortDescription() : TString(what);
    EYqlIssueCode rootIssueCode = IssueCodeForYtError(e);
    return YqlIssue(pos, rootIssueCode, errMsg);
}

namespace {

void FillResultFromOperationError(NCommon::TOperationResult& result, const NYT::TOperationFailedError& e, TPosition pos, bool shortErrors) {
    TIssue rootIssue = MakeIssueFromYtError(e.GetError(), e.what(), pos, shortErrors);

    if (!e.GetFailedJobInfo().empty()) {
        TSet<TString> uniqueErrors;
        for (auto& failedJob: e.GetFailedJobInfo()) {
            TStringBuf message = failedJob.Stderr;
            auto parsedPos = TryParseTerminationMessage(message);
            if (message.size() < failedJob.Stderr.size()) {
                if (uniqueErrors.emplace(message).second) {
                    rootIssue.AddSubIssue(MakeIntrusive<TIssue>(YqlIssue(parsedPos.GetOrElse(pos), TIssuesIds::DEFAULT_ERROR, TString{message})));
                }
            } else {
                TString errorDescription = failedJob.Error.ShortDescription();
                if (uniqueErrors.insert(errorDescription).second) {
                    rootIssue.AddSubIssue(MakeIntrusive<TIssue>(YqlIssue(pos, TIssuesIds::UNEXPECTED, errorDescription)));
                }
            }
        }
    }

    result.SetStatus(TIssuesIds::DEFAULT_ERROR);
    result.AddIssue(rootIssue);
}

void FillResultFromErrorResponse(NCommon::TOperationResult& result, const NYT::TErrorResponse& e, TPosition pos, bool shortErrors) {
    TIssue rootIssue = MakeIssueFromYtError(e.GetError(), e.what(), pos, shortErrors);

    result.SetStatus(TIssuesIds::DEFAULT_ERROR);
    result.AddIssue(rootIssue);
}

} // unnamed

void FillResultFromCurrentException(NCommon::TOperationResult& result, TPosition pos, bool shortErrors) {
    try {
        throw;
    } catch (const NYT::TOperationFailedError& e) {
        FillResultFromOperationError(result, e, pos, shortErrors);
    } catch (const NYT::TErrorResponse& e) {
        FillResultFromErrorResponse(result, e, pos, shortErrors);
    } catch (const std::exception& e) {
        result.SetException(e, pos);
    } catch (const NKikimr::TMemoryLimitExceededException&) {
        result.SetStatus(TIssuesIds::UNEXPECTED);
        result.AddIssue(TIssue(pos, "Memory limit exceeded in MKQL runtime"));
    } catch (...) {
        result.SetStatus(TIssuesIds::UNEXPECTED);
        result.AddIssue(TIssue(pos, CurrentExceptionMessage()));
    }
}

void EnsureSpecDoesntUseNativeYtTypes(const NYT::TNode& spec, TStringBuf tableName, bool read) {
    if (spec.HasKey(YqlRowSpecAttribute)) {
        const auto& rowSpec = spec[YqlRowSpecAttribute];
        bool useNativeYtTypes = false;
        if (rowSpec.HasKey(RowSpecAttrUseNativeYtTypes)) {
            useNativeYtTypes = rowSpec[RowSpecAttrUseNativeYtTypes].AsBool();
        } else if (rowSpec.HasKey(RowSpecAttrUseTypeV2)) {
            useNativeYtTypes = rowSpec[RowSpecAttrUseTypeV2].AsBool();
        } else if (rowSpec.HasKey(RowSpecAttrNativeYtTypeFlags)) {
            useNativeYtTypes = rowSpec[RowSpecAttrNativeYtTypeFlags].AsUint64() > 0;
        }
        if (useNativeYtTypes) {
            throw yexception() << "Cannot " << (read ? "read" : "modify") << " table \"" << tableName << "\" with type_v3 schema using yson codec";
        }
    }
}

} // NYql
