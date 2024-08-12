TExecFunc arrow_brin_desummarize_range() { return MakeIndirectExec<true, true>(&brin_desummarize_range); }
TExecFunc arrow_brin_summarize_new_values() { return MakeIndirectExec<true, true>(&brin_summarize_new_values); }
TExecFunc arrow_brin_summarize_range() { return MakeIndirectExec<true, true>(&brin_summarize_range); }
TExecFunc arrow_brinhandler() { return MakeIndirectExec<true, true>(&brinhandler); }
