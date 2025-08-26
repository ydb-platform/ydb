#pragma once

#include <library/cpp/yson/parser.h>
#include <library/cpp/yson_pull/reader.h>
#include <yt/yql/providers/yt/lib/yson_helpers/yson_helpers.h>

namespace NYql::NFmr {

struct TParsedColumnGroupSpec {
    std::unordered_map<TString, TSet<TString>> ColumnGroups; // ColumnGroupName -> List of columns in it
    TString DefaultColumnGroupName;

    bool IsEmpty() const;
};

TParsedColumnGroupSpec GetColumnGroupsFromSpec(const TString& serializedColumnGroupsSpec);

std::unordered_map<TString, TString> SplitYsonByColumnGroups(const TString& ysonInputs, const TParsedColumnGroupSpec& columnGroupsSpec);

TString GetYsonUnion(const std::vector<TString>& ysonRows);

//////////////////////////////////////////////////////////////////////////////////////////////////////////

class TColumnGroupSplitterYsonConsumer: public NYT::NYson::IYsonConsumer {
public:
    TColumnGroupSplitterYsonConsumer(
        const std::vector<NYT::NYson::IYsonConsumer*>& columnGroupConsumers,
        const THashMap<TString, ui64>& columnGroups,
        ui64 groupsNum
    );

    virtual void OnStringScalar(TStringBuf value) final;
    virtual void OnInt64Scalar(i64 value) final;
    virtual void OnUint64Scalar(ui64 value) final;
    virtual void OnDoubleScalar(double value) final;
    virtual void OnBooleanScalar(bool value) final;
    virtual void OnEntity() final;
    virtual void OnBeginList() final;
    virtual void OnListItem() final;
    virtual void OnEndList() final;
    virtual void OnBeginMap() final;
    virtual void OnKeyedItem(TStringBuf key) final;
    virtual void OnEndMap() final;
    virtual void OnBeginAttributes() final;
    virtual void OnEndAttributes() final;
    virtual void OnRaw(TStringBuf yson, NYson::EYsonType type) final;
private:

    std::vector<NYT::NYson::IYsonConsumer*> ColumnGroupConsumers_;
    const THashMap<TString, ui64> ColumnGroups_;
    ui64 MapDepth_ = 0;
    ui64 ConsumerIndex_ = 0;
    const ui64 GroupsNum_;
};

} // namespace NYql::NFmr
