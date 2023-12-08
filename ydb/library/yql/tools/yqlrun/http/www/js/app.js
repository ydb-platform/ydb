require("ace/mode/yql");

var entityMap = {
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': '&quot;',
    "'": '&#39;',
    "/": '&#x2F;'
};

String.prototype.capitalize = function() {
    return this.charAt(0).toUpperCase() + this.slice(1);
}

String.prototype.escapeHtml = function() {
    return String(this).replace(/[&<>"'\/]/g, function (s) {
      return entityMap[s];
    });
}

var printAst = false;
var printExpr = false;
var traceOpt = false;
var outputTable = (YQL_TYPE == "file")
        ? "Output"
        : "YqlOutput_" + Math.random().toString(36).substring(2, 7);

var yqlEditor = ace.edit("yql-editor");
yqlEditor.getSession().setMode("ace/mode/yql");
yqlEditor.setTheme("ace/theme/tomorrow");
yqlEditor.setValue("(\n"
    + "# read data from Input table\n"
    + "(let mr_source (DataSource 'yt 'plato))\n"
    + "(let x (Read! world mr_source\n"
    + "    (Key '('table (String 'Input)))\n"
    + "    '('key 'subkey 'value) '()))\n"
    + "(let world (Left! x))\n"
    + "(let table1 (Right! x))\n"
    + "\n"
    + "# filter keys less than 100\n"
    + "(let tresh (Int32 '100))\n"
    + "(let table1low (FlatMap table1 (lambda '(item) (block '(\n"
    + "   (let intValueOpt (FromString (Member item 'key) 'Int32))\n"
    + "   (let ret (FlatMap intValueOpt (lambda '(item2) (block '(\n"
    + "      (return (ListIf (< item2 tresh) item))\n"
    + "   )))))\n"
    + "   (return ret)\n"
    + ")))))\n"
    + "\n"
    + "# write table1low to " + outputTable + " table\n"
    + "(let mr_sink (DataSink 'yt 'plato))\n"
    + "(let world (Write! world mr_sink\n"
    + "    (Key '('table (String '" + outputTable + ")))\n"
    + "    table1low '('('mode 'append))))\n"
    + "\n"
    + "# write table1low to result sink\n"
    + "(let res_sink (DataSink 'result))\n"
    + "(let world (Write! world res_sink\n"
    + "    (Key)\n"
    + "    table1low '()))\n"
    + "\n"
    + "# finish\n"
    + "(let world (Commit! world mr_sink))\n"
    + "(let world (Commit! world res_sink))\n"
    + "(return world)\n"
    + ")"
);

yqlEditor.gotoLine(1);

var sqlEditor = ace.edit("sql-editor");
sqlEditor.getSession().setMode("ace/mode/sql");
sqlEditor.setTheme("ace/theme/tomorrow");
sqlEditor.setValue("USE plato;\n"
    + "\n"
    + "INSERT INTO " + outputTable + "\n"
    + "SELECT\n"
    + "    key as key,\n"
    + "    \"\" as subkey,\n"
    + "    \"value:\" || value as value\n"
    + "FROM Input\n"
    + "WHERE key < \"100\"\n"
    + "ORDER BY key;");
sqlEditor.gotoLine(1);

var tableInputEditor = ace.edit("table-input-editor");
tableInputEditor.getSession().setMode("ace/mode/sql");
tableInputEditor.setTheme("ace/theme/tomorrow");
tableInputEditor.setValue(""
    + "{\"key\"=\"023\";\"subkey\"=\"3\";\"value\"=\"aaa\"};\n"
    + "{\"key\"=\"037\";\"subkey\"=\"5\";\"value\"=\"ddd\"};\n"
    + "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n"
    + "{\"key\"=\"150\";\"subkey\"=\"1\";\"value\"=\"aaa\"};\n"
    + "{\"key\"=\"150\";\"subkey\"=\"3\";\"value\"=\"iii\"};\n"
    + "{\"key\"=\"150\";\"subkey\"=\"8\";\"value\"=\"zzz\"};\n"
    + "{\"key\"=\"200\";\"subkey\"=\"7\";\"value\"=\"qqq\"};\n"
    + "{\"key\"=\"527\";\"subkey\"=\"4\";\"value\"=\"bbb\"};\n"
    + "{\"key\"=\"761\";\"subkey\"=\"6\";\"value\"=\"ccc\"};\n"
    + "{\"key\"=\"911\";\"subkey\"=\"2\";\"value\"=\"kkk\"};\n"
    );
tableInputEditor.gotoLine(1);

var tableAttrEditor = ace.edit("table-attr-editor");
tableAttrEditor.setTheme("ace/theme/tomorrow");
tableAttrEditor.setValue("{\"_yql_row_spec\"={\n"
    + "\t\"Type\"=[\"StructType\";[\n"
    + "\t\t[\"key\";[\"DataType\";\"String\"]];\n"
    + "\t\t[\"subkey\";[\"DataType\";\"String\"]];\n"
    + "\t\t[\"value\";[\"DataType\";\"String\"]]\n"
    + "\t]];\n"
    + "\t\"SortDirections\"=[1;1;];\n"
    + "\t\"SortedBy\"=[\"key\";\"subkey\";];\n"
    + "\t\"SortedByTypes\"=[[\"DataType\";\"String\";];[\"DataType\";\"String\";];];\n"
    + "\t\"SortMembers\"=[\"key\";\"subkey\";];\n"
    + "}}\n"
    );
tableAttrEditor.gotoLine(1);

var paramsEditor = ace.edit("params-editor");
paramsEditor.setTheme("ace/theme/tomorrow");
paramsEditor.setValue("{\"$foo\"={Data=\"bar\"}}\n");
paramsEditor.gotoLine(1);

var exprEditor = ace.edit("expr-editor");
exprEditor.getSession().setMode("ace/mode/yql");
exprEditor.setTheme("ace/theme/tomorrow");
exprEditor.setOptions({
    readOnly: true,
    highlightActiveLine: false,
    highlightGutterLine: false
})

function showOutput(output) {
    var headers = "<tr>" + $.map(output.headers, function(header) {
        return "<th>" + header + "</th>";
    }).join("") + "</tr>";

    var rows = $.map(output.rows, function(row) {
        var cells = $.map(row, function(cell) {
            return "<td>" + cell + "</td>";
        }).join("");
        return "<tr>" + cells + "</tr>";
    }).join("");

    var table =
            "<table class='table table-condensed'>" +
                "<thead>" + headers + "</thead>" +
                "<tbody>" + rows + "<tbody>" +
            "</table>";
    $("#output").html(table);
}

function showResults(results) {
    $("#results").html("<pre>" + results.escapeHtml() + "</pre>");
}

function showOptTrace(optTrace) {
    $("#opt-trace")
        .html("<pre>" + optTrace.escapeHtml() + "</pre>")
        .show();
}

function showExpr(expr) {
    exprEditor.setValue(expr);
    exprEditor.gotoLine(1);
    $("#expr-editor").show();
}

function showLocation(location) {
    var link = "<a href='" + location + "' target='_blank'>" + location + "</a>";
    var $success = $("#status-success");
    $success.find(".message").html(link);
    $success.show();
}

function showStatus(success, text) {
    var $status = success ? $("#status-success") : $("#status-fail");
    $status.find(".message").html(text + (success ? " was successful" : " failed"));
    $status.show();

    if (window.statusTimeout !== undefined) {
        clearTimeout(window.statusTimeout);
    }

    window.statusTimeout = setTimeout(function() {
        $status.hide();
    }, 3000);
}

function showIssues(issueHint, issues, lang) {
    if (issues.length == 0) return;

    var $issues = $(issueHint);
    var issuesHtml = "<ul>" + $.map(issues, function(e) {
        return "<li><pre>" + e.escapeHtml() + "</pre></li>";
    }).join('') + "</ul>";
    $issues.find(".message").html(issuesHtml);
    $issues.show();

    if (lang !== undefined) {
        var shownIssues = [];
        for (var i in issues) {
            if (!issues[i]) continue;
            var s = issues[i];
            while (s[0] == ">") {
               s = s.substring(1);
            }

            var issue = s.split(':');

            shownIssues.push({
                row: (parseInt(issue[0]) - 1),
                column: parseInt(issue[1]),
                type: "issue",
                text: issue.slice(2).join(':').trim()
            });
        }
        var editor = (lang === "yql") ? yqlEditor : sqlEditor;
        editor.getSession().setAnnotations(shownIssues);
    }
}

function showSql(sql) {
    sqlEditor.setValue(sql);
    sqlEditor.gotoLine(1);
    sqlEditor.show();
}
 
function showAst(root) {
    var id = 0;
    function addNode(g, p, n) {
        n.id = id++;
        var options = { label: n.content };
        if (n.type == "list") options["class"] = "list";
        g.setNode(n.id, options);
        if (p != null) {
            g.setEdge(p.id, n.id, { label: "" });
        }
        if (n.type == "list") {
            for (var c in n.children) {
                addNode(g, n, n.children[c]);
            }
        }
    }

    $("#graph").show();

    // Create a new directed graph
    var g = new dagreD3.graphlib.Graph().setGraph({});

    addNode(g, null, root);

    // Set some general styles
    g.nodes().forEach(function(v) {
      var node = g.node(v);
      node.rx = node.ry = 5;
    });

    var svg = d3.select("svg"),
        inner = svg.select("g");

    // Set up zoom support
    var zoom = d3.behavior.zoom().on("zoom", function() {
      inner.attr("transform", "translate(" + d3.event.translate + ")" +
                                  "scale(" + d3.event.scale + ")");
    });
    svg.call(zoom);

    // Create the renderer
    var render = new dagreD3.render();

    // Run the renderer. This is what draws the final graph.
    render(inner, g);

    // Center the graph
    var initialScale = 0.75;
    zoom
      .translate([(svg.attr("width") - g.graph().width * initialScale) / 2, 20])
      .scale(initialScale)
      .event(svg);
    svg.attr('height', 400);
    svg.attr('width', screen.width);
}

function clearView() {
    yqlEditor.getSession().clearAnnotations();
    sqlEditor.getSession().clearAnnotations();
    tableInputEditor.getSession().clearAnnotations();
    tableAttrEditor.getSession().clearAnnotations();
    exprEditor.setValue("");
    $("#expr-editor").hide();
    $("#opt-trace").html("").hide();
    $("#graph").hide();
    $("#errors").hide();
    $("#warnings").hide();
    $("#infos").hide();
    $("#status-success").hide();
    $("#status-fail").hide();
    $("#output").html("");
    $("#results").html("");
}

function sendProgram(e) {
    e.preventDefault();
    clearView();

    var action = $(this).attr('id');
    var url = "/api/yql/" + action;

    var params = [];
    if (printAst) params.push("printAst=true");
    if (printExpr) params.push("printExpr=true");
    if (traceOpt) params.push("traceOpt=true");
    if (params.length > 0) {
        url += "?" + params.join("&");
    }

    var program;
    var lang = $("#editor-tabs li.active a").html().toLowerCase();
    if (lang == "yql") {
        program = yqlEditor.getValue();
    } else if (lang == "sql") {
        program = sqlEditor.getSelectedText();
        if (!program) {
            program = sqlEditor.getValue();
        }
    } else {
        showIssues("#errors", "Unknow program language: " + lang);
    }

    var tableInput = tableInputEditor.getValue();
    var tableAttr = tableAttrEditor.getValue();
    var parameters = paramsEditor.getValue();

    $.ajax({
        url: url,
        timeout: 60 * 60 * 1000, // 1 hour
        dataType: "json",
        type: "POST",
        jsonp: false,
        data: JSON.stringify({
            program: program,
            tableInput: tableInput,
            tableAttr: tableAttr,
            lang: lang,
            parameters: parameters
        })
    })
    .always(function(response) {
        if (response.status >= 400) {
            var r = JSON.parse(response.responseText);
            showIssues("#errors", r.errors, lang);
        } else {
            if ("responseJSON" in response) response = response.responseJSON;

            if ("sql" in response) showSql(response.sql);
            if ("ast" in response) showAst(response.ast);
            if ("expr" in response) showExpr(response.expr);
            if ("output" in response && "headers" in response.output && "rows" in response.output) {
                showOutput(response.output);
            }
            if ("results" in response) showResults(response.results);
            if ("opttrace" in response) showOptTrace(response.opttrace);
            if ("location" in response) {
                showLocation(response.location);
                return;
            }

            if ("errors" in response) {
                showIssues("#errors", response.errors, lang);
            }
            if ("warnings" in response) {
                showIssues("#warnings", response.warnings, lang);
            }
            if ("infos" in response) {
                showIssues("#infos", response.infos, lang);
            }
            showStatus(response.succeeded, action.capitalize());
        }
    });
}

$(function() {
    $('#paste').click(sendProgram);
    $('#parse').click(sendProgram);
    $('#compile').click(sendProgram);
    $('#optimize').click(sendProgram);
    $('#validate').click(sendProgram);
    $('#peephole').click(sendProgram);
    $('#lineage').click(sendProgram);
    $('#run').click(sendProgram);
    $('#format').click(sendProgram);
    $("#alerts .close").click(function() {
        $(this).parent().hide();
    });

    $("#show-ast").click(function(e) {
        e.preventDefault();
        var $li = $(this).parent();
        $li.toggleClass("active");
        printAst = $li.hasClass("active");
    });

    $("#show-expr").click(function(e) {
        e.preventDefault();
        var $li = $(this).parent();
        $li.toggleClass("active");
        printExpr = $li.hasClass("active");
    });

    $("#trace-opt").click(function(e) {
        e.preventDefault();
        var $li = $(this).parent();
        $li.toggleClass("active");
        traceOpt = $li.hasClass("active");
    });
});
