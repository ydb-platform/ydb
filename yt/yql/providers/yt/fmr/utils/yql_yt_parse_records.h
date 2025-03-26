#include <yt/cpp/mapreduce/interface/io.h>

#include <yt/yql/providers/yt/codec/yt_codec_io.h>

namespace NYql::NFmr {

void ParseRecords(NYT::TRawTableReader& reader, NYT::TRawTableWriter& writer, ui64 blockCount, ui64 blockSize);

} // namespace NYql::NFmr
