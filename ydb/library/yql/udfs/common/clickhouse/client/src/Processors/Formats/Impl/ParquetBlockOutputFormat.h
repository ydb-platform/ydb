#pragma once

//#if !defined(ARCADIA_BUILD)
//#    include "config_formats.h"
//#endif
#if USE_PARQUET
#    include <Processors/Formats/IOutputFormat.h>
#    include <Formats/FormatSettings.h>
#    include <parquet/arrow/writer.h>

namespace NDB
{

class CHColumnToArrowColumn;

class ParquetBlockOutputFormat : public IOutputFormat
{
public:
    ParquetBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "ParquetBlockOutputFormat"; }
    void consume(Chunk) override;
    void finalize() override;

    String getContentType() const override { return "application/octet-stream"; }

private:
    const FormatSettings format_settings;

    std::unique_ptr<parquet20::arrow20::FileWriter> file_writer;
    std::unique_ptr<CHColumnToArrowColumn> ch_column_to_arrow_column;
};

}

#endif
