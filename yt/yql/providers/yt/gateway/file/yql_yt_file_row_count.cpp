#include "yql_yt_file_row_count.h"

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_visitor.h>
#include <library/cpp/yson/writer.h>

#include <util/stream/file.h>
#include <util/system/fs.h>

namespace NYql::NFile {

namespace {

class TRowCountingYsonConsumer: public NYson::TYsonConsumerBase {
public:
    TRowCountingYsonConsumer(NYT::NYson::IYsonConsumer* inner, ui64& rowCount)
        : Inner_(inner), RowCount_(rowCount) {}

    void OnStringScalar(TStringBuf v)  override { Inner_->OnStringScalar(v); }
    void OnInt64Scalar(i64 v)          override { Inner_->OnInt64Scalar(v); }
    void OnUint64Scalar(ui64 v)        override { Inner_->OnUint64Scalar(v); }
    void OnDoubleScalar(double v)      override { Inner_->OnDoubleScalar(v); }
    void OnBooleanScalar(bool v)       override { Inner_->OnBooleanScalar(v); }
    void OnEntity()                    override { Inner_->OnEntity(); }
    void OnBeginMap()                  override { Inner_->OnBeginMap(); }
    void OnKeyedItem(TStringBuf k)     override { Inner_->OnKeyedItem(k); }
    void OnEndMap()                    override { Inner_->OnEndMap(); }
    void OnBeginAttributes()           override { Inner_->OnBeginAttributes(); }
    void OnEndAttributes()             override { Inner_->OnEndAttributes(); }

    void OnBeginList() override { ++Depth_; Inner_->OnBeginList(); }
    void OnEndList()   override { --Depth_; Inner_->OnEndList(); }
    // In a list fragment the parser emits OnListItem at depth 0 for each row;
    // nested lists inside cell values are wrapped in OnBeginList/OnEndList.
    void OnListItem()  override { if (Depth_ == 0) ++RowCount_; Inner_->OnListItem(); }

private:
    NYT::NYson::IYsonConsumer* const Inner_;
    ui64& RowCount_;
    int Depth_ = 0;
};

} // namespace

std::unique_ptr<NYson::TYsonConsumerBase> MakeRowCountingYsonConsumer(
    NYT::NYson::IYsonConsumer* inner,
    ui64& rowCount
) {
    return std::make_unique<TRowCountingYsonConsumer>(inner, rowCount);
}

void SaveRowCountToAttr(const TString& dataFilePath, ui64 rowCount) {
    NYT::TNode attrs;
    const TString attrPath = dataFilePath + ".attr";
    if (NFs::Exists(attrPath)) {
        attrs = NYT::NodeFromYsonString(TIFStream(attrPath).ReadAll());
    } else {
        attrs = NYT::TNode::CreateMap();
    }
    attrs["row_count"] = static_cast<i64>(rowCount);
    TOFStream ofAttr(attrPath);
    NYson::TYsonWriter writer(&ofAttr, NYson::EYsonFormat::Pretty, ::NYson::EYsonType::Node);
    NYT::TNodeVisitor visitor(&writer);
    visitor.Visit(attrs);
}

} // namespace NYql::NFile
