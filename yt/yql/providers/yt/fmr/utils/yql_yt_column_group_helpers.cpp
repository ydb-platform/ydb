#include "yql_yt_column_group_helpers.h"
#include "yql_yt_parser_fragment_list_index.h"
#include <library/cpp/yson/detail.h>
#include <library/cpp/yson/node/node_io.h>
#include <util/generic/hash_set.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

bool TParsedColumnGroupSpec::IsEmpty() const {
    return ColumnGroups.empty() && DefaultColumnGroupName.empty();
}

TParsedColumnGroupSpec GetColumnGroupsFromSpec(const TString& serializedColumnGroupsSpec) {
    TParsedColumnGroupSpec parsedColumnGroupSpec{};
    if (serializedColumnGroupsSpec.empty()) {
        return parsedColumnGroupSpec;
    }
    auto columnGroupSpec = NYT::NodeFromYsonString(serializedColumnGroupsSpec);
    YQL_ENSURE(!columnGroupSpec.IsUndefined());
    for (const auto& group: columnGroupSpec.AsMap()) {
        if (!group.second.IsEntity()) {
            for (const auto& col: group.second.AsList()) {
                TString column = col.AsString();
                parsedColumnGroupSpec.ColumnGroups[group.first].emplace(column);
            }
        } else {
            parsedColumnGroupSpec.DefaultColumnGroupName = group.first;
        }
    }
    return parsedColumnGroupSpec;
}

TSplittedYsonByColumnGroups SplitYsonByColumnGroups(const TString& tableContent, const TParsedColumnGroupSpec& columnGroupsSpec) {
    ui64 totalColumns = 0;
    for (const auto& [groupName, cols] : columnGroupsSpec.ColumnGroups) {
        totalColumns += cols.size();
    }
    const ui64 estimatedGroupsCount = columnGroupsSpec.ColumnGroups.size() + 1;

    THashMap<TString, ui64> columnGroups; // Column name -> column group index (for non-default groups)
    columnGroups.reserve(totalColumns);
    THashMap<ui64, TString> columnGroupIndexes; // Column group index -> column group name
    columnGroupIndexes.reserve(estimatedGroupsCount);
    std::unordered_map<TString, TString> splittedYsonByColumnGroups;
    splittedYsonByColumnGroups.reserve(estimatedGroupsCount);

    ui64 columnGroupIndex = 0;
    for (const auto& [groupName, cols]: columnGroupsSpec.ColumnGroups) {
        for (const auto& col: cols) {
            columnGroups[col] = columnGroupIndex;
        }
        columnGroupIndexes[columnGroupIndex] = groupName;
        splittedYsonByColumnGroups[groupName] = TString();
        ++columnGroupIndex;
    }

    const TString& defaultColumnGroupName = columnGroupsSpec.DefaultColumnGroupName;
    columnGroupIndexes[columnGroupIndex] = defaultColumnGroupName;
    splittedYsonByColumnGroups[defaultColumnGroupName] = TString();
    ++columnGroupIndex;

    const ui64 columnGroupsNum = columnGroupIndex;

    std::unordered_map<TString, TStringStream> outputYsonStreams;
    outputYsonStreams.reserve(estimatedGroupsCount);
    std::unordered_map<TString, THolder<TBinaryYsonWriter>> binaryYsonWriters;
    binaryYsonWriters.reserve(estimatedGroupsCount);
    std::vector<NYT::NYson::IYsonConsumer*> consumers;
    consumers.reserve(columnGroupsNum);

    for (auto& [groupName, ysonValue]: splittedYsonByColumnGroups) {
        outputYsonStreams[groupName] = TStringStream();
        binaryYsonWriters[groupName] = MakeHolder<TBinaryYsonWriter>(&outputYsonStreams[groupName], ::NYson::EYsonType::ListFragment);
    }

    for (ui64 i = 0; i < columnGroupsNum; ++i) {
        consumers.emplace_back(binaryYsonWriters[columnGroupIndexes[i]].Get());
    }

    ui64 recordsCount = 0;
    TColumnGroupSplitterYsonConsumer splitConsumer(consumers, columnGroups, recordsCount);
    TStringStream inputStream(tableContent);
    NYson::TYsonParser parser(&splitConsumer, &inputStream, ::NYson::EYsonType::ListFragment);
    parser.Parse();
    for (auto& [groupName, ysonContent]: outputYsonStreams) {
        splittedYsonByColumnGroups[groupName] = ysonContent.Str();
    }
    return TSplittedYsonByColumnGroups{.SplittedYsonByColumnGroups = std::move(splittedYsonByColumnGroups), .RecordsCount = recordsCount};
}

TSplittedYsonByColumnGroups SplitYsonByColumnGroupsRaw(TStringBuf tableContent, const TParsedColumnGroupSpec& columnGroupsSpec) {
    ui64 totalColumns = 0;
    for (const auto& [groupName, cols] : columnGroupsSpec.ColumnGroups) {
        totalColumns += cols.size();
    }

    const ui64 estimatedGroupsCount = columnGroupsSpec.ColumnGroups.size() + 1;

    THashMap<TString, ui64> columnToGroupIndex;
    columnToGroupIndex.reserve(totalColumns);
    THashMap<ui64, TString> groupIndexToName;
    groupIndexToName.reserve(estimatedGroupsCount);
    ui64 groupCount = 0;

    for (const auto& [groupName, cols] : columnGroupsSpec.ColumnGroups) {
        for (const auto& col : cols) {
            columnToGroupIndex[col] = groupCount;
        }
        groupIndexToName[groupCount] = groupName;
        ++groupCount;
    }

    TString defaultColumnGroupName = columnGroupsSpec.DefaultColumnGroupName;
    ui64 defaultGroupIndex = groupCount;
    groupIndexToName[groupCount] = defaultColumnGroupName;
    ++groupCount;

    TParserFragmentListIndex parser(tableContent);
    parser.Parse();
    const auto& rows = parser.GetAllColumnsRows();
    const ui64 numRows = rows.size();

    if (numRows > 0) {
        std::vector<TString> firstRowCols;
        for (const auto& col : rows[0].Columns) {
            firstRowCols.push_back(TString(col.Name));
        }
    }

    // Resolve group index for each column (one hash lookup per column),
    // compute exact byte sizes per group buffer.
    // Cache resolved group indices to avoid rehashing.
    struct TResolvedColumn {
        ui64 GroupIdx;
        ui64 KvStartOffset;
        ui64 KvLength;
    };
    std::vector<std::vector<TResolvedColumn>> resolvedRows(numRows);

    std::vector<ui64> groupSizes(groupCount, 0);

    // '{' + '}' + ';' = 3 bytes per group minimum
    const ui64 structuralBytesPerGroup = numRows * 3;
    for (ui64 g = 0; g < groupCount; ++g) {
        groupSizes[g] = structuralBytesPerGroup;
    }

    std::vector<ui64> groupColumnCount(groupCount, 0);
    for (ui64 rowIdx = 0; rowIdx < numRows; ++rowIdx) {
        const auto& row = rows[rowIdx];
        auto& resolved = resolvedRows[rowIdx];
        resolved.reserve(row.Columns.size());

        if (rowIdx) {
            std::fill(groupColumnCount.begin(), groupColumnCount.end(), 0);
        }

        for (const auto& col : row.Columns) {
            ui64 groupIdx;
            auto it = columnToGroupIndex.find(col.Name);
            if (it != columnToGroupIndex.end()) {
                groupIdx = it->second;
            } else {
                groupIdx = defaultGroupIndex;
            }
            if (groupIdx >= groupCount) {
                continue;
            }
            const auto& kvRange = col.KeyValueRange;
            const ui64 kvLength = kvRange.EndOffset - kvRange.StartOffset;
            groupSizes[groupIdx] += kvLength;
            ++groupColumnCount[groupIdx];
            resolved.push_back(TResolvedColumn{groupIdx, kvRange.StartOffset, kvLength});
        }
        for (ui64 g = 0; g < groupCount; ++g) {
            if (groupColumnCount[g] > 1) {
                groupSizes[g] += groupColumnCount[g] - 1;
            }
        }
    }

    std::vector<TString> groupBuffers(groupCount);
    for (ui64 i = 0; i < groupCount; ++i) {
        groupBuffers[i].reserve(groupSizes[i]);
    }

    std::vector<bool> isFirstColumnInGroup(groupCount, true);

    for (ui64 rowIdx = 0; rowIdx < numRows; ++rowIdx) {
        std::fill(isFirstColumnInGroup.begin(), isFirstColumnInGroup.end(), true);

        for (ui64 g = 0; g < groupCount; ++g) {
            groupBuffers[g].append(1, NYson::NDetail::BeginMapSymbol);
        }

        for (const auto& rc : resolvedRows[rowIdx]) {
            auto& buf = groupBuffers[rc.GroupIdx];
            if (!isFirstColumnInGroup[rc.GroupIdx]) {
                buf.append(1, NYson::NDetail::KeyedItemSeparatorSymbol);
            }
            isFirstColumnInGroup[rc.GroupIdx] = false;
            buf.append(tableContent.data() + rc.KvStartOffset, rc.KvLength);
        }

        for (ui64 g = 0; g < groupCount; ++g) {
            groupBuffers[g].append(1, NYson::NDetail::EndMapSymbol);
            groupBuffers[g].append(1, NYson::NDetail::ListItemSeparatorSymbol);
        }
    }

    std::unordered_map<TString, TString> result;
    result.reserve(groupCount);
    for (ui64 g = 0; g < groupCount; ++g) {
        result[groupIndexToName[g]] = std::move(groupBuffers[g]);
    }

    return TSplittedYsonByColumnGroups{
        .SplittedYsonByColumnGroups = std::move(result),
        .RecordsCount = rows.size()
    };
}

TString GetYsonUnionRaw(const std::vector<TString>& ysonInputs, const std::vector<TString>& neededColumns) {
    if (ysonInputs.empty()) {
        return {};
    }
    if (ysonInputs.size() == 1 && neededColumns.empty()) {
        return ysonInputs[0];
    }

    THashSet<TStringBuf> columnsToKeep(neededColumns.begin(), neededColumns.end());
    const bool filterColumns = !columnsToKeep.empty();

    const ui64 inputsNum = ysonInputs.size();
    std::vector<std::vector<TRowFullMarkup>> parsedInputs(inputsNum);

    for (ui64 g = 0; g < inputsNum; ++g) {
        TParserFragmentListIndex parser(ysonInputs[g]);
        parser.Parse();
        parsedInputs[g] = parser.MoveAllColumnsRows();
    }

    const ui64 numRows = parsedInputs[0].size();

    ui64 totalInputSize = 0;
    for (const auto& input : ysonInputs) {
        totalInputSize += input.size();
    }
    TString result;
    result.reserve(totalInputSize + numRows * 4);

    for (ui64 row = 0; row < numRows; ++row) {
        result.append(1, NYson::NDetail::BeginMapSymbol);
        bool firstColumn = true;

        for (ui64 g = 0; g < inputsNum; ++g) {
            const auto& rowMarkup = parsedInputs[g][row];
            const auto& data = ysonInputs[g];

            for (const auto& col : rowMarkup.Columns) {
                if (filterColumns && !columnsToKeep.contains(col.Name)) {
                    continue;
                }

                if (!firstColumn) {
                    result.append(1, NYson::NDetail::KeyedItemSeparatorSymbol);
                }
                firstColumn = false;

                const auto& kvRange = col.KeyValueRange;
                result.append(data.data() + kvRange.StartOffset, kvRange.EndOffset - kvRange.StartOffset);
            }
        }

        result.append(1, NYson::NDetail::EndMapSymbol);
        result.append(1, NYson::NDetail::ListItemSeparatorSymbol);
    }

    return result;
}

TString GetNeededColumnsFromYsonData(const TString& ysonInputs, const std::vector<TString>& neededColumns) {
    TStringStream inputYsonStream(ysonInputs);
    TStringStream outputStream;
    TBinaryYsonWriter writer(&outputStream, ::NYson::EYsonType::ListFragment);
    NYT::NYson::IYsonConsumer* consumer = &writer;
    TSet<TStringBuf> columnsToKeep(neededColumns.begin(), neededColumns.end());
    TColumnFilteringConsumer columnFilteringConsumer(consumer, columnsToKeep, Nothing());
    NYson::TYsonParser parser(&columnFilteringConsumer, &inputYsonStream, ::NYson::EYsonType::ListFragment);
    parser.Parse();
    return outputStream.ReadAll();
}

TString GetYsonUnion(const std::vector<TString>& ysonInputs, const std::vector<TString>& neededColumns) {
    if (ysonInputs.empty()) {
        return {};
    }
    if (ysonInputs.size() == 1 && neededColumns.empty()) {
        return ysonInputs[0];
    }

    TStringStream unionYsonStream;
    TBinaryYsonWriter writer(&unionYsonStream, NYson::EYsonType::ListFragment);
    NYT::NYson::IYsonConsumer* outputConsumer = &writer;

    ui64 inputsNum = ysonInputs.size(), readerIndex = 0, mapDepth = 0, listDepth = 0, finishedReadersNum = 0;
    bool isCurrentColumnNeeeded = true;
    TSet<TStringBuf> columnsToKeep(neededColumns.begin(), neededColumns.end());

    std::vector<THolder<NYsonPull::TReader>> ysonReaders;
    for (auto& ysonInput: ysonInputs) {
        auto ysonStream = NYsonPull::NInput::FromMemory(ysonInput);
        ysonReaders.emplace_back(
            MakeHolder<NYsonPull::TReader>(std::move(ysonStream), NYsonPull::EStreamType::ListFragment)
        );
    }

    while (finishedReadersNum != inputsNum) {
        const auto& event = ysonReaders[readerIndex]->NextEvent();
        switch (event.Type()) {

        case NYsonPull::EEventType::BeginList:
            if (isCurrentColumnNeeeded) {
                outputConsumer->OnBeginList();
                ++listDepth;
            }
            break;

        case NYsonPull::EEventType::EndList:
            if (isCurrentColumnNeeeded) {
                outputConsumer->OnEndList();
                --listDepth;
            }
            break;

        case NYsonPull::EEventType::BeginMap:
            if ((mapDepth > 0 && isCurrentColumnNeeeded) || readerIndex == 0) {
                outputConsumer->OnBeginMap();
            }
            ++mapDepth;
            break;

        case NYsonPull::EEventType::EndMap:
            --mapDepth;
            if (mapDepth > 0) {
                if (isCurrentColumnNeeeded) {
                    outputConsumer->OnEndMap();
                }
            } else {
                if (readerIndex == inputsNum - 1) {
                    readerIndex = 0;
                    outputConsumer->OnEndMap();
                } else {
                    ++readerIndex;
                }
            }
            break;

        case NYsonPull::EEventType::Key:
            if (mapDepth == 1) {
                isCurrentColumnNeeeded = columnsToKeep.contains(event.AsString()) || columnsToKeep.empty();
            }
            if (isCurrentColumnNeeeded) {
                outputConsumer->OnKeyedItem(event.AsString());
            }
            break;

        case NYsonPull::EEventType::Scalar: {
            if (!isCurrentColumnNeeeded) {
                break;
            }
            const auto& scalar = event.AsScalar();
            if (listDepth > 0) {
                outputConsumer->OnListItem();
            }
            switch (scalar.Type()) {
            case NYsonPull::EScalarType::Entity:
                outputConsumer->OnEntity();
                break;

            case NYsonPull::EScalarType::Boolean:
                outputConsumer->OnBooleanScalar(scalar.AsBoolean());
                break;

            case NYsonPull::EScalarType::Int64:
                outputConsumer->OnInt64Scalar(scalar.AsInt64());
                break;

            case NYsonPull::EScalarType::UInt64:
                outputConsumer->OnUint64Scalar(scalar.AsUInt64());
                break;

            case NYsonPull::EScalarType::Float64:
                outputConsumer->OnDoubleScalar(scalar.AsFloat64());
                break;

            case NYsonPull::EScalarType::String:
                outputConsumer->OnStringScalar(scalar.AsString());
                break;
            }
            break;
        }

        case NYsonPull::EEventType::EndStream:
            ++finishedReadersNum;
            ++readerIndex;
            break;

        case NYsonPull::EEventType::BeginAttributes:
        case NYsonPull::EEventType::EndAttributes:
        case NYsonPull::EEventType::BeginStream:
            break;
        }
    }
    return unionYsonStream.ReadAll();
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

TColumnGroupSplitterYsonConsumer::TColumnGroupSplitterYsonConsumer(
    const std::vector<NYT::NYson::IYsonConsumer*>& columnGroupConsumers,
    const THashMap<TString, ui64>& columnGroups,
    ui64& recordsCount
)
    : ColumnGroupConsumers_(columnGroupConsumers)
    , ColumnGroups_(columnGroups)
    , GroupsNum_(columnGroupConsumers.size())
    , RecordsCount_(recordsCount)
{
}

void TColumnGroupSplitterYsonConsumer::OnStringScalar(TStringBuf value) {
    ColumnGroupConsumers_[ConsumerIndex_]->OnStringScalar(value);
}

void TColumnGroupSplitterYsonConsumer::OnInt64Scalar(i64 value) {
    ColumnGroupConsumers_[ConsumerIndex_]->OnInt64Scalar(value);
}

void TColumnGroupSplitterYsonConsumer::OnUint64Scalar(ui64 value) {
    ColumnGroupConsumers_[ConsumerIndex_]->OnUint64Scalar(value);
}

void TColumnGroupSplitterYsonConsumer::OnDoubleScalar(double value) {
    ColumnGroupConsumers_[ConsumerIndex_]->OnDoubleScalar(value);
}

void TColumnGroupSplitterYsonConsumer::OnBooleanScalar(bool value) {
    ColumnGroupConsumers_[ConsumerIndex_]->OnBooleanScalar(value);
}

void TColumnGroupSplitterYsonConsumer::OnEntity() {
    ColumnGroupConsumers_[ConsumerIndex_]->OnEntity();
}

void TColumnGroupSplitterYsonConsumer::OnBeginList() {
    ColumnGroupConsumers_[ConsumerIndex_]->OnBeginList();
}

void TColumnGroupSplitterYsonConsumer::OnListItem() {
    if (MapDepth_ > 0) {
        ColumnGroupConsumers_[ConsumerIndex_]->OnListItem();
    }
}

void TColumnGroupSplitterYsonConsumer::OnEndList() {
    ColumnGroupConsumers_[ConsumerIndex_]->OnEndList();
}

void TColumnGroupSplitterYsonConsumer::OnBeginMap() {
    if (MapDepth_ == 0) {
        for (ui64 i = 0; i < GroupsNum_; ++i) {
            ColumnGroupConsumers_[i]->OnBeginMap();
        }
        ++RecordsCount_;
    } else {
        ColumnGroupConsumers_[ConsumerIndex_]->OnBeginMap();
    }
    ++MapDepth_;
}

void TColumnGroupSplitterYsonConsumer::OnKeyedItem(TStringBuf key) {
    if (MapDepth_ == 1) {
        const auto it = ColumnGroups_.find(key);
        ConsumerIndex_ = (it == ColumnGroups_.end()) ? (GroupsNum_ - 1) : it->second;
    }
    ColumnGroupConsumers_[ConsumerIndex_]->OnKeyedItem(key);
}

void TColumnGroupSplitterYsonConsumer::OnEndMap() {
    --MapDepth_;
    if (MapDepth_ == 0) {
        for (ui64 i = 0; i < GroupsNum_; ++i) {
            ColumnGroupConsumers_[i]->OnEndMap();
        }
    } else {
        ColumnGroupConsumers_[ConsumerIndex_]->OnEndMap();
    }
}

void TColumnGroupSplitterYsonConsumer::OnBeginAttributes() {
    ColumnGroupConsumers_[ConsumerIndex_]->OnBeginAttributes();
}

void TColumnGroupSplitterYsonConsumer::OnEndAttributes() {
    ColumnGroupConsumers_[ConsumerIndex_]->OnEndAttributes();
}

void TColumnGroupSplitterYsonConsumer::OnRaw(TStringBuf yson, NYson::EYsonType type) {
    ColumnGroupConsumers_[ConsumerIndex_]->OnRaw(yson, type);
}

} // namespace NYql::NFmr
