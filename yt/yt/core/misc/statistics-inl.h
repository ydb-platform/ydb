#ifndef STATISTICS_INL_H_
#error "Direct inclusion of this file is not allowed, include statistics.h"
// For the sake of sane code completion.
#include "statistics.h"
#endif

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TStatistics::AddSample(const NYPath::TYPath& path, const T& sample)
{
    AddSample(path, NYTree::ConvertToNode(sample));
}

template <class T>
void TStatistics::ReplacePathWithSample(const NYPath::TYPath& path, const T& sample)
{
    ReplacePathWithSample(path, NYTree::ConvertToNode(sample));
}

////////////////////////////////////////////////////////////////////////////////

template <class TTags>
void TTaggedStatistics<TTags>::AppendStatistics(const TStatistics& statistics, TTags tags)
{
    for (const auto& [path, summary] : statistics.Data()) {
        auto& pathSummaries = Data_[path];
        auto it = pathSummaries.find(tags);
        if (it == pathSummaries.end()) {
            pathSummaries.emplace(tags, summary);
        } else {
            it->second.Merge(summary);
        }
    }
}

template <class TTags>
void TTaggedStatistics<TTags>::AppendTaggedSummary(const NYPath::TYPath& path, const TTaggedStatistics<TTags>::TTaggedSummaries& taggedSummaries)
{
    auto taggedSummariesIt = Data_.find(path);
    if (taggedSummariesIt == Data_.end()) {
        Data_[path] = taggedSummaries;
        return;
    }

    auto& currentTaggedSummaries = taggedSummariesIt->second;
    for (const auto& [tags, summary] : taggedSummaries) {
        if (auto summaryIt = currentTaggedSummaries.find(tags); summaryIt == currentTaggedSummaries.end()) {
            currentTaggedSummaries.insert(std::pair(tags, summary));
        } else {
            summaryIt->second.Merge(summary);
        }
    }
}

template <class TTags>
const THashMap<TTags, TSummary>* TTaggedStatistics<TTags>::FindTaggedSummaries(const NYPath::TYPath& path) const
{
    auto it = Data_.find(path);
    if (it != Data_.end()) {
        return &it->second;
    }
    return nullptr;
}

template <class TTags>
const typename TTaggedStatistics<TTags>::TSummaryMap& TTaggedStatistics<TTags>::GetData() const
{
    return Data_;
}

template <class TTags>
void TTaggedStatistics<TTags>::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Data_);
}

////////////////////////////////////////////////////////////////////////////////

template <class TTags>
void Serialize(const TTaggedStatistics<TTags>& statistics, NYson::IYsonConsumer* consumer)
{
    SerializeYsonPathsMap<THashMap<TTags, TSummary>>(
        statistics.GetData(),
        consumer,
        [] (const THashMap<TTags, TSummary>& summaries, NYson::IYsonConsumer* consumer) {
            NYTree::BuildYsonFluently(consumer)
                .DoListFor(summaries, [] (NYTree::TFluentList fluentList, const auto& pair) {
                    fluentList.Item()
                        .BeginMap()
                            .Item("tags").Value(pair.first)
                            .Item("summary").Value(pair.second)
                        .EndMap();
                });
        });
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE int SkipEqualTokens(NYPath::TTokenizer& first, NYPath::TTokenizer& second)
{
    int commonDepth = 0;
    while (true) {
        first.Advance();
        second.Advance();
        // Note that both tokenizers can't reach EndOfStream token, because it would mean that
        // currentKey is prefix of prefixKey or vice versa that is prohibited in TStatistics.
        first.Expect(NYPath::ETokenType::Slash);
        second.Expect(NYPath::ETokenType::Slash);

        first.Advance();
        second.Advance();
        first.Expect(NYPath::ETokenType::Literal);
        second.Expect(NYPath::ETokenType::Literal);
        if (first.GetLiteralValue() == second.GetLiteralValue()) {
            ++commonDepth;
        } else {
            break;
        }
    }

    return commonDepth;
}

template <class TMapValue>
void SerializeYsonPathsMap(
    const std::map<NYTree::TYPath, TMapValue>& map,
    NYson::IYsonConsumer* consumer,
    const std::function<void(const TMapValue&, NYson::IYsonConsumer*)>& valueSerializer)
{
    using NYT::Serialize;

    consumer->OnBeginMap();

    // Depth of the previous key defined as a number of nested maps before the summary itself.
    int previousDepth = 0;
    NYPath::TYPath previousPath;
    for (const auto& [currentPath, value] : map) {
        NYPath::TTokenizer previousTokenizer(previousPath);
        NYPath::TTokenizer currentTokenizer(currentPath);

        // The depth of the common part of two keys, needed to determine the number of maps to close.
        int commonDepth = 0;

        if (previousPath) {
            // First we find the position in which current key is different from the
            // previous one in order to close necessary number of maps.
            commonDepth = SkipEqualTokens(currentTokenizer, previousTokenizer);

            // Close all redundant maps.
            while (previousDepth > commonDepth) {
                consumer->OnEndMap();
                --previousDepth;
            }
        } else {
            currentTokenizer.Advance();
            currentTokenizer.Expect(NYPath::ETokenType::Slash);
            currentTokenizer.Advance();
            currentTokenizer.Expect(NYPath::ETokenType::Literal);
        }

        int currentDepth = commonDepth;
        // Open all newly appeared maps.
        while (true) {
            consumer->OnKeyedItem(currentTokenizer.GetLiteralValue());
            currentTokenizer.Advance();
            if (currentTokenizer.GetType() == NYPath::ETokenType::Slash) {
                consumer->OnBeginMap();
                ++currentDepth;
                currentTokenizer.Advance();
                currentTokenizer.Expect(NYPath::ETokenType::Literal);
            } else if (currentTokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
                break;
            } else {
                THROW_ERROR_EXCEPTION("Wrong token type in statistics path")
                    << TErrorAttribute("token_type", currentTokenizer.GetType())
                    << TErrorAttribute("statistics_path", currentPath);
            }
        }
        // Serialize value.
        valueSerializer(value, consumer);

        previousDepth = currentDepth;
        previousPath = currentPath;
    }
    while (previousDepth > 0) {
        consumer->OnEndMap();
        --previousDepth;
    }

    // This OnEndMap is complementary to the OnBeginMap before the main loop.
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
