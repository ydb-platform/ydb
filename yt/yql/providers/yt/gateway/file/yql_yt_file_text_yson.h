#pragma once
#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/mapreduce/interface/common.h>

namespace NYql::NFile {

struct TColumnsInfo {
    TMaybe<NYT::TSortColumns> Columns;
    TMaybe<NYT::TRichYPath::TRenameColumnsDescriptor> RenameColumns;
};

TVector<NYT::TRawTableReaderPtr> MakeTextYsonInputs(const TVector<std::pair<TString, TColumnsInfo>>& files);

}//namespace NYql::NFile