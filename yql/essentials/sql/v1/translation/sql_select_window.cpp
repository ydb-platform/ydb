#include "sql_select_window.h"

namespace NSQLTranslationV1 {

TSqlWindow::TSqlWindow(const TSqlTranslation& that)
    : TSqlTranslation(that)
{
}

bool TSqlWindow::Build(const TRule_window_definition& node, TWinSpecs& winSpecs) {
    const TString windowName = Id(node.GetRule_new_window_name1().GetRule_window_name1().GetRule_an_id_window1(), *this);
    if (winSpecs.contains(windowName)) {
        Ctx_.Error() << "Unable to declare window with same name: " << windowName;
        return false;
    }

    auto windowSpec = WindowSpecification(node.GetRule_window_specification3().GetRule_window_specification_details2());
    if (!windowSpec) {
        return false;
    }

    winSpecs.emplace(windowName, std::move(windowSpec));
    return true;
}

bool TSqlWindow::Build(const TRule_window_clause& node, TWinSpecs& winSpecs) {
    auto windowList = node.GetRule_window_definition_list2();

    if (!Build(windowList.GetRule_window_definition1(), winSpecs)) {
        return false;
    }

    for (auto& block : windowList.GetBlock2()) {
        if (!Build(block.GetRule_window_definition2(), winSpecs)) {
            return false;
        }
    }

    return true;
}

} // namespace NSQLTranslationV1
