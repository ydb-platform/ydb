#include "parse_records.h"

using namespace NYql;

namespace NYql::NFmr {

void ParseRecords(NYT::TRawTableReader& reader, NYT::TRawTableWriter& writer, ui64 blockCount, ui64 blockSize) {
    auto blockReader = MakeBlockReader(reader, blockCount, blockSize);
    NCommon::TInputBuf inputBuf(*blockReader, nullptr);
    TVector<char> curYsonRow;
    char cmd;
    while (inputBuf.TryRead(cmd)) {
        curYsonRow.clear();
        CopyYson(cmd, inputBuf, curYsonRow);
        bool needBreak = false;
        if (!inputBuf.TryRead(cmd)) {
            needBreak = true;
        } else {
            YQL_ENSURE(cmd == ';');
            curYsonRow.emplace_back(cmd);
        }
        writer.Write(curYsonRow.data(), curYsonRow.size());
        writer.NotifyRowEnd();
        if (needBreak) {
            break;
        }
    }
}

} // namespace NYql::NFmr
