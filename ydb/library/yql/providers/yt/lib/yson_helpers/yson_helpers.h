#pragma once

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/writer.h>

#include <util/generic/fwd.h>
#include <util/generic/strbuf.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>

namespace NYql {

//////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBinaryYsonWriter : public NYson::TYsonWriter {
public:
    TBinaryYsonWriter(IOutputStream* stream, NYson::EYsonType type = ::NYson::EYsonType::Node);

    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;

    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;

    void OnBeginAttributes() override;
    void OnEndAttributes() override;

private:
    TVector<bool> Stack_;
};

class TColumnFilteringConsumer: public NYT::NYson::IYsonConsumer {
public:
    TColumnFilteringConsumer(NYT::NYson::IYsonConsumer* parent, const TMaybe<TSet<TStringBuf>>& columns,
                             const TMaybe<THashMap<TStringBuf, TStringBuf>>& renameColumns);

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
    NYT::NYson::IYsonConsumer* Parent_;
    bool Enable_ = true;
    TVector<bool> Stack_;
    TMaybe<TSet<TStringBuf>> Columns_;
    TMaybe<THashMap<TStringBuf, TStringBuf>> RenameColumns_;
};

class TDoubleHighPrecisionYsonWriter: public NYson::TYsonWriter {
public:
    TDoubleHighPrecisionYsonWriter(IOutputStream* stream, NYson::EYsonType type = ::NYson::EYsonType::Node, bool enableRaw = false);
    void OnDoubleScalar(double value);
};

void WriteTableReference(NYson::TYsonWriter& writer, TStringBuf provider, TStringBuf cluster, TStringBuf table,
    bool remove, const TVector<TString>& columns);

bool HasYqlRowSpec(const NYT::TNode& inAttrs);
bool HasYqlRowSpec(const TString& inputAttrs);

bool HasStrictSchema(const NYT::TNode& attrs);

TMaybe<ui64> GetDataWeight(const NYT::TNode& inAttrs);
ui64 GetTableRowCount(const NYT::TNode& tableAttrs);
ui64 GetContentRevision(const NYT::TNode& tableAttrs);

} // NYql
