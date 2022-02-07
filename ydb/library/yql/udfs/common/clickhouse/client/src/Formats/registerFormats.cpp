//#if !defined(ARCADIA_BUILD)
//#    include <Common/config.h>
//#endif

#include <Formats/FormatFactory.h>


namespace DB
{

void registerInputFormatProcessorNative(FormatFactory & factory);
void registerInputFormatProcessorJSONEachRow(FormatFactory & factory);
void registerInputFormatProcessorRawBLOB(FormatFactory & factory);
void registerInputFormatProcessorORC(FormatFactory & factory);
void registerInputFormatProcessorArrow(FormatFactory & factory);
void registerInputFormatProcessorParquet(FormatFactory & factory);
void registerInputFormatProcessorAvro(FormatFactory & factory);
void registerInputFormatProcessorCSV(FormatFactory & factory);
void registerInputFormatProcessorTSKV(FormatFactory & factory);
void registerInputFormatProcessorTabSeparated(FormatFactory & factory);

void registerFormats()
{
    auto & factory = FormatFactory::instance();

    registerInputFormatProcessorNative(factory);
    registerInputFormatProcessorJSONEachRow(factory);
    registerInputFormatProcessorRawBLOB(factory);
    registerInputFormatProcessorORC(factory);
    registerInputFormatProcessorArrow(factory);
    registerInputFormatProcessorParquet(factory);
    registerInputFormatProcessorAvro(factory);
    registerInputFormatProcessorCSV(factory);
    registerInputFormatProcessorTSKV(factory);
    registerInputFormatProcessorTabSeparated(factory);
}

}

