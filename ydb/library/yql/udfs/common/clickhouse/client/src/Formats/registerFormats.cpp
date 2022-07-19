//#if !defined(ARCADIA_BUILD)
//#    include <Common/config.h>
//#endif

#include <Formats/FormatFactory.h>


namespace NDB
{

void registerInputFormatProcessorNative(FormatFactory & factory);
void registerInputFormatProcessorJSONAsString(FormatFactory & factory);
void registerInputFormatProcessorJSONEachRow(FormatFactory & factory);
void registerOutputFormatProcessorJSONEachRow(FormatFactory & factory);
void registerInputFormatProcessorRawBLOB(FormatFactory & factory);
void registerInputFormatProcessorORC(FormatFactory & factory);
void registerInputFormatProcessorArrow(FormatFactory & factory);
void registerInputFormatProcessorParquet(FormatFactory & factory);
void registerOutputFormatProcessorParquet(FormatFactory & factory);
void registerInputFormatProcessorAvro(FormatFactory & factory);
void registerInputFormatProcessorCSV(FormatFactory & factory);
void registerOutputFormatProcessorCSV(FormatFactory & factory);
void registerInputFormatProcessorTSKV(FormatFactory & factory);
void registerOutputFormatProcessorTSKV(FormatFactory & factory);
void registerInputFormatProcessorTabSeparated(FormatFactory & factory);
void registerOutputFormatProcessorTabSeparated(FormatFactory & factory);

void registerFormats()
{
    auto & factory = FormatFactory::instance();

    const std::unique_lock lock(factory.getSync());
    if (factory.getAllFormats().empty()) {
        registerInputFormatProcessorNative(factory);
        registerInputFormatProcessorJSONAsString(factory);
        registerInputFormatProcessorJSONEachRow(factory);
        registerOutputFormatProcessorJSONEachRow(factory);
        registerInputFormatProcessorRawBLOB(factory);
        registerInputFormatProcessorORC(factory);
        registerInputFormatProcessorArrow(factory);
        registerInputFormatProcessorParquet(factory);
        registerOutputFormatProcessorParquet(factory);
        registerInputFormatProcessorAvro(factory);
        registerInputFormatProcessorCSV(factory);
        registerOutputFormatProcessorCSV(factory);
        registerInputFormatProcessorTSKV(factory);
        registerOutputFormatProcessorTSKV(factory);
        registerInputFormatProcessorTabSeparated(factory);
        registerOutputFormatProcessorTabSeparated(factory);
    }
}

}
