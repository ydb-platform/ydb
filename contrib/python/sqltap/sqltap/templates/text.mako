========================================================================
${"======{TITLE: ^60}======".format(TITLE=report_title)}
========================================================================
Report Generated Time: ${report_time}

========================================================================
${"======{0: ^60}======".format("Summary")}
========================================================================
Total queries: ${len(all_group.queries)}
Total time: ${'%.2f' % all_group.sum} second(s)
Total profiling time: ${'%.2f' % duration} second(s)

========================================================================
${"======{0: ^60}======".format("Details")}
========================================================================

% for i, group in enumerate(query_groups):
${"============{0: ^48}============".format("QueryGroup %d" % i)}
${"------------{0: ^48}------------".format("QueryGroup %d summary" % i)}
Query count: ${len(group.queries)}
Query max time: ${'%.3f' % group.max} second(s)
Query min time: ${'%.3f' % group.min} second(s)
Query mean time: ${'%.3f' % group.mean} second(s)
Query median time: ${'%.3f' % group.median} second(s)

${"------------{0: ^48}------------".format("QueryGroup %d SQL pattern" % i)}
${group.formatted_text}

${"------------{0: ^48}------------".format("QueryGroup %d breakdown" % i)}
% for j, query in enumerate(reversed(group.queries)):
${"Query %d:" % j}
  Query duration: ${'%.3f' % query.duration} second(s)
  Query params:
    % for key, value in query.params.items():
    ${key}: ${value}
    % endfor
  Query rowcount: ${'%d' % query.rowcount}
% endfor ## end for j, query in enumerate(reversed(group.queries))

${"------------{0: ^48}------------".format("QueryGroup %d stacks" % i)}
Total unique stack(s): ${len(group.stacks)}
% for k, trace in enumerate(group.stacks):
<%
    count = group.stacks[trace]
    fr = group.callers[trace]

    def reindent(text, space=2):
        text = text.split('\n')
        text = [(' ' * space) + line for line in text]
        text = '\n'.join(text)
        return text
%>
${"Stack %d:" % k}
  ${count} call(s) from ${fr[2]} @${fr[0].split()[-1]}:${fr[1]}
  Traceback:
  ${reindent(trace)}
% endfor ## end for k, trace in enumerate(group.stacks)

% endfor ## end for i, group in enumerate(query_groups)


