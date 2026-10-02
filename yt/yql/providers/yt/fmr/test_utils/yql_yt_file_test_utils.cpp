#include "yql_yt_file_test_utils.h"

#include <library/cpp/yson/node/node_io.h>

#include <util/stream/file.h>
#include <util/system/fs.h>

namespace NYql::NFmr {

TString ReadFileWithSplittedSupport(const TString& path) {
    const TString attrPath = path + ".attr";
    i64 numParts = 0;
    if (NFs::Exists(attrPath)) {
        const NYT::TNode attrs = NYT::NodeFromYsonString(TFileInput(attrPath).ReadAll());
        if (attrs.HasKey("splitted")) {
            numParts = attrs["splitted"].AsInt64();
        }
    }
    TString result;
    if (numParts > 0) {
        for (i64 p = 0; p < numParts; ++p) {
            result += TFileInput(path + ".part." + ToString(p)).ReadAll();
        }
    } else {
        result = TFileInput(path).ReadAll();
    }
    return result;
}

} // namespace NYql::NFmr
