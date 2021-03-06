#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/TabSeparatedRowOutputFormat.h>


namespace NDB
{

/** A stream for outputting data in tsv format, but without escaping individual values.
  * (That is, the output is irreversible.)
  */
class TabSeparatedRawRowOutputFormat : public TabSeparatedRowOutputFormat
{
public:
    TabSeparatedRawRowOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        bool with_names_,
        bool with_types_,
        const RowOutputFormatParams & params_,
        const FormatSettings & format_settings_)
        : TabSeparatedRowOutputFormat(out_, header_, with_names_, with_types_, params_, format_settings_)
    {
    }

    String getName() const override { return "TabSeparatedRawRowOutputFormat"; }

    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override
    {
        serialization.serializeText(column, row_num, out, format_settings);
    }
};

}
