#include "sql_select_window.h"

namespace NSQLTranslationV1 {

TSqlWindow::TSqlWindow(const TSqlTranslation& that)
    : TSqlTranslation(that)
{
}

TSQLStatus TSqlWindow::Build(const TRule_window_definition& node, TWinSpecs& winSpecs) {
    const TString windowName = Id(node.GetRule_new_window_name1().GetRule_window_name1().GetRule_an_id_window1(), *this);
    if (winSpecs.contains(windowName)) {
        Ctx_.Error() << "Unable to declare window with same name: " << windowName;
        return std::unexpected(ESQLError::Basic);
    }

    auto windowSpec = WindowSpecification(node.GetRule_window_specification3().GetRule_window_specification_details2());
    if (!windowSpec) {
        return std::unexpected(windowSpec.error());
    }

    winSpecs.emplace(windowName, std::move(*windowSpec));
    return std::monostate();
}

TSQLStatus TSqlWindow::Build(const TRule_window_clause& node, TWinSpecs& winSpecs) {
    const auto& windowList = node.GetRule_window_definition_list2();

    if (auto status = Build(windowList.GetRule_window_definition1(), winSpecs); !status) {
        return std::unexpected(status.error());
    }

    for (auto& block : windowList.GetBlock2()) {
        if (auto status = Build(block.GetRule_window_definition2(), winSpecs); !status) {
            return std::unexpected(status.error());
        }
    }

    return std::monostate();
}

} // namespace NSQLTranslationV1
