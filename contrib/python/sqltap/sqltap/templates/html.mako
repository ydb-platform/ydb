<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="description" content="sqltap profile">
    <meta name="author" content="inconshreveable">

    <title>${report_title}</title>

    <!-- Bootstrap core CSS -->
    <link href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css" rel="stylesheet">

    <!-- syntax highlighting -->
    <link href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/8.8.0/styles/default.min.css" rel="stylesheet">
    <style type="text/css">
      body { padding-top: 60px; }
      #query-groups {
        border-right: 1px solid #ccc;
      }
      #total-time { color: #fff; }
      #total-time .sum { color: #0f0; font-size: 16px; }
      #total-time .count { color: #0f0; font-size: 16px; }
      a.toggle { cursor: pointer }
      a.toggle strong { color: red; }
    </style>
  </head>

  <body>

    <div class="container">
      <div class="navbar navbar-inverse navbar-fixed-top" role="navigation">
        <div class="container">
          <div class="navbar-header">
            <a class="navbar-brand" href="https://github.com/inconshreveable/sqltap">sqltap</a>
          </div>
          <ul class="navbar-right nav navbar-nav">
            <li><a target="_blank" href="http://sqltap.inconshreveable.com/">Documentation</a></li>
            <li><a target="_blank" href="https://github.com/inconshreveable/sqltap">Code</a></li>
          </ul>
          <p id="total-time" class="navbar-text">
            <span class="count">${len(all_group.queries)}</span> queries spent
            <span class="sum">${'%.2f' % all_group.sum}</span> seconds
            over <span class="sum">${'%.2f' % duration}</span> seconds of profiling
          </p>
          <%block name="header_extra"></%block>
        </div>
      </div>
      <h1>
      </h1>
      <div class="row">
        <div class="col-xs-3" id="query-groups" style="min-height: 600px">

          <ul class="nav nav-pills nav-stacked" id="myTabs">
            % for i, group in enumerate(query_groups):
            <li class="${'active' if i==0 else ''}">
              <a href="#query-${i}" data-toggle="tab">
                <span class="label label-default pull-right"
                      style="margin-right: 5px; width: 8ex; text-align: right;">
                    ${group.rowcounts}r
                </span>
                <span class="label label-warning pull-right" style="margin-right: 5px;">
                    ${'%.3f' % group.sum}s
                </span>
                <span class="label label-info pull-right" style="margin-right: 5px;">
                  ${len(group.queries)}q
                </span>
${group.first_word}
              </a>
            </li>
          % endfor
          </ul>

          <hr />

            <div>
            <span>Report Generated: ${report_time}</span>
          </div>
        </div>

        <!-- ================================================== -->

        <div class="col-xs-9">

          <div class="tab-content">
            % for i, group in enumerate(query_groups):
            <div id="query-${i}" class="tab-pane ${'active' if i==0 else ''}">
              <h4 class="toggle">
                  <ul class="list-inline">
                    <li>
                      <dt>Query Count</dt>
                      <dd>${len(group.queries)}</dd>
                    </li>
                    <li>
                      <dt>Row Count</dt>
                      <dd>${'%d' % group.rowcounts}</dd>
                    </li>
                    <li>
                      <dt>Total Time</dt>
                      <dd>${'%.3f' % group.sum}</dd>
                    </li>
                    <li>
                      <dt>Mean</dt>
                      <dd>${'%.3f' % group.mean}</dd>
                    </li>
                    <li>
                      <dt>Median</dt>
                      <dd>${'%.3f' % group.median}</dd>
                    </li>
                    <li>
                      <dt>Min</dt>
                      <dd>${'%.3f' % group.min}</dd>
                    </li>
                    <li>
                      <dt>Max</dt>
                      <dd>${'%.3f' % group.max}</dd>
                    </li>
                  </ul>
              </h4>

              <hr />
              <pre><code class="sql">${group.formatted_text}</code></pre>
              <hr />

              <%
                params = group.get_param_names()
              %>
              <h4>
                Query Breakdown
              </h4>
              <table class="table">
                <tr>
                  <th>Query Time</th>
                % for param_name in params:
                  <th><code>${param_name}</code></th>
                % endfor
                  <th>Row Count</th>
                  <th>Params ID</th>
                </tr>
                % for idx, query in enumerate(reversed(group.queries)):
                <tr class="${'hidden' if idx >= 3 else ''}">
                    <td>${'%.3f' % query.duration}</td>
                    % for param_name in params:
                    <td>${query.params.get(param_name, '')}</td>
                    % endfor
                    <td>${'%d' % query.rowcount}</td>
                    <td>${'%d' % query.params_id}</td>
                </tr>
                % endfor
              </table>
              % if len(group.queries) > 3:
                <a href="#" class="morequeries">show ${len(group.queries)-3} older queries</a>
              % endif

              <hr />
              <% params_hash_count = len(group.params_hashes) %>
              <h4>
                  ${params_hash_count} unique parameter
                  % if params_hash_count == 1:
                      set is
                  % else:
                      sets are
                  % endif
                  supplied.
              </h4>
              <ul class="details">
                % for idx, (count, params_id, params) in enumerate(group.params_hashes.values()):
                  <li class="${'hidden' if idx >= 3 else ''}">
                    <h5>
                      ${count}
                      ${'call' if count == 1 else 'calls'}
                      (Params ID: ${params_id}) with
                      <tt>
                        ${", ".join(["%s=%r" % (k,params[k]) for k in sorted(params.keys()) if params[k] is not None])}
                      </tt>
                    </h5>
                  </li>
                % endfor
              </ul>
              % if len(group.params_hashes) > 3:
                  <a href="#" class="moreparams">show ${len(group.params_hashes)-3} more parameter sets</a>
              % endif

              <hr />
              <% stack_count = len(group.stacks) %>
              <h4>
                  ${stack_count} unique
                  % if stack_count == 1:
                      stack issues
                  % else:
                      stacks issue
                  % endif
                  this query
              </h4>
              <ul class="details">
                  % for trace, count in group.stacks.items():
                  <li>
                    <a class="toggle">
                      <h5>
                      <% fr = group.callers[trace] %>
                      ${count}
                      ${'call' if count == 1 else 'calls'} from
                      <strong>${fr[2]}</strong> @${fr[0].split()[-1]}:${fr[1]}
                      </h5>
                    </a>
                    <pre class="trace hidden"><code class="python">${trace}</code></pre>
                  </li>
                  % endfor
              </ul>
            </div>

            <!-- ================================================== -->

            % endfor
          </div>
        </div>
    </div><!-- /.container -->


    <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.0/jquery.min.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/8.8.0/highlight.min.js"></script>
    <script>hljs.initHighlightingOnLoad();</script>
    <script type="text/javascript">
        jQuery(function($) {
            $(".toggle").click(function() {
                $(this).siblings(".trace").toggleClass("hidden");
            });
            $('#myTabs a').click(function (e) {
                $(this).tab('show');
                e.preventDefault();
            });
            $(".morequeries").click(function(e) {
                e.preventDefault();
                $(this).hide();
                $(this).prev("table").find("tr.hidden").removeClass("hidden");
            });
            $(".moreparams").click(function(e) {
                e.preventDefault();
                $(this).hide();
                $(this).prev("ul").find("li.hidden").removeClass("hidden");
            });
        });
    </script>
  </body>
</html>
