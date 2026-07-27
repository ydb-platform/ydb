#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/mapreduce/interface/common.h>

namespace NYql::NFile {

struct TColumnsInfo {
    TMaybe<NYT::TSortColumns> Columns;
    TMaybe<NYT::TRichYPath::TRenameColumnsDescriptor> RenameColumns;
};

TVector<NYT::TRawTableReaderPtr> MakeTextYsonInputs(const TVector<std::pair<TString, TColumnsInfo>>& files, bool addRowIndex = true);

// Returns splitted=N from the .attr file or 0 if no such atttribute exists
i64 ReadSplittedPartsCount(const TString& filePath);

} // namespace NYql::NFile
