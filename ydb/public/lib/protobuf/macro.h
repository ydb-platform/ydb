#define WITH_INDENT(printer) if (TScopeIndentPrinterGuard indentGuard(printer); true)
#define WITH_PLUGIN_MARKUP(printer, pluginName) if (TPluginMarkupPrinterGuard indentGuard(printer, pluginName); true)
