#define WITH_INDENT(printer) if (::NKikimr::NProtobuf::TScopeIndentPrinterGuard indentGuard(printer); true)
#define WITH_PLUGIN_MARKUP(printer, pluginName) if (::NKikimr::NProtobuf::TPluginMarkupPrinterGuard indentGuard(printer, pluginName); true)
