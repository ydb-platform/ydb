#include "yql_yt_column_group_helpers.h"
#include <library/cpp/yson/node/node_io.h>
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
    std::unordered_map<TString, TString> splittedYsonByColumnGroups; // columnGroupName -> All data in yson row corresponding to it

    THashMap<TString, ui64> columnGroups; // Column name -> column group index (for non-default groups)
    THashMap<ui64, TString> columnGroupIndexes; // Column group index -> column group name
    ui64 columnGroupIndex = 0;
    for (auto& [groupName, cols]: columnGroupsSpec.ColumnGroups) {
        for (auto& col: cols) {
            columnGroups[col] = columnGroupIndex;
        }
        columnGroupIndexes[columnGroupIndex] = groupName;
        splittedYsonByColumnGroups[groupName] = TString();
        ++columnGroupIndex;
    }

    TString defaultColumnGroupName = columnGroupsSpec.DefaultColumnGroupName;
    // don't use default column group (#) if all columns are filled with explicit group names.
    if (!(defaultColumnGroupName.empty() && columnGroupIndex > 0)) {
        columnGroupIndexes[columnGroupIndex] = defaultColumnGroupName;
        splittedYsonByColumnGroups[defaultColumnGroupName] = TString();
        ++columnGroupIndex;
    }

    ui64 columnGroupsNum = columnGroupIndex;
    columnGroupIndex = 0;

    std::vector<NYT::NYson::IYsonConsumer*> consumers;
    std::unordered_map<TString, THolder<TBinaryYsonWriter>> binaryYsonWriters;
    std::unordered_map<TString, TStringStream> outputYsonStreams;

    for (auto& [groupName, ysonValue]: splittedYsonByColumnGroups) {
        outputYsonStreams[groupName] = TStringStream();
        binaryYsonWriters[groupName] = MakeHolder<TBinaryYsonWriter>(&outputYsonStreams[groupName], ::NYson::EYsonType::ListFragment);
        ++columnGroupIndex;
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
    return TSplittedYsonByColumnGroups{.SplittedYsonByColumnGroups = splittedYsonByColumnGroups, .RecordsCount = recordsCount};
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
                break;
            }

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
                isCurrentColumnNeeeded = columnsToKeep.contains(event.AsString()) || columnsToKeep.empty(); // If columns to keep is not set, we add all columns.
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
                // YsonPull doesn't have OnListItem() method needed for sax parser, so call it manually.
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

        case NYsonPull::EEventType::EndStream: {
            ++finishedReadersNum;
            ++readerIndex;
        }

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
        if (!ColumnGroups_.contains(key)) {
            // if key is not in column groups map, it must be in default column group with index groupsNum - 1.
            ConsumerIndex_ = GroupsNum_ - 1;
        } else {
            ConsumerIndex_ = ColumnGroups_.at(key);
        }
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
