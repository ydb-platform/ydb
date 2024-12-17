'use strict';

var ConsoleLogState = {
    fromId: undefined,
    fromTimestamp: undefined,
    limit: 100,
    totalRecords: 0,
    filterByKind: false,
    usersExclude: false,
};

function RenderScope(content) {
    var result = "{"
    if (content.hasOwnProperty("TenantAndNodeTypeFilter")) {
        var filter = content["TenantAndNodeTypeFilter"];
        var filters = [];
        if (filter.hasOwnProperty("Tenant")) {
            filters.push("Tenant:" + filter["Tenant"]);
        }
        if (filter.hasOwnProperty("NodeType")) {
            filters.push("NodeType:" + filter["NodeType"]);
        }
        result += filters.join(",");
    }
    if (content.hasOwnProperty("HostFilter")) {
        result += "Hosts:[" + content["HostFilter"]["Hosts"].join(",") + "]";
    }
    if (content.hasOwnProperty("NodeFilter")) {
        result += "Hosts:[" + content["NodeFilter"]["Nodes"].join(",") + "]";
    }
    result += "}"
    return result;
}

function RenderId(content) {
    var id = content["Id"];
    var gen = content["Generation"];
    return id + "." + gen;
}

function RenderKind(content) {
    return cmsEnums.get('ItemKinds', content);
}

const MergeStrategies = {
    1: "OVERWRITE",
    2: "MERGE",
    3: "MERGE_OVERWRITE_REPEATED",
}

function RenderMergeStrategy(content) {
    return MergeStrategies[content];
}

function RenderAffectedConfigs(content) {
    return  "\n\t\t" + Object.keys(content).join("\n\t\t")
}

function RenderBasicConfigItem(ci) {
    var result = "";

    if (ci.hasOwnProperty("Id")) {
        var cid = ci["Id"];
        result += "\n\tId:" + RenderId(cid);
    }

    if (ci.hasOwnProperty("Kind")) {
        var kind = ci["Kind"];
        result += "\n\tKind:" + RenderKind(kind);
    }

    if (ci.hasOwnProperty("UsageScope")) {
        var scope = ci["UsageScope"];
        result += "\n\tScope:" + RenderScope(scope);
    }

    if (ci.hasOwnProperty("Order")) {
        result += "\n\tOrder:" + ci["Order"];
    }

    if (ci.hasOwnProperty("MergeStrategy")) {
        result += "\n\tMergeStrategy:" + RenderMergeStrategy(ci["MergeStrategy"]);
    } else {
        result += "\n\tMergeStrategy:MERGE";
    }

    if (ci.hasOwnProperty("Cookie")) {
        result += "\n\tCookie:" + ci["Cookie"];
    }

    if (ci.hasOwnProperty("Config")) {
        result += "\n\tAffectedConfigs:" + RenderAffectedConfigs(ci["Config"]);
    }

    return result;
}

function RenderAddConfigItem(content) {
    var ci = content["ConfigItem"];
    var result = "<b>AddItem:</b>"

    result += RenderBasicConfigItem(ci);

    return result;
}

function RenderRemoveConfigItem(content) {
    var cid = content["ConfigItemId"];
    return "<b>RemoveItem:</b>\n\tId:" + RenderId(cid);
}

function RenderModifyConfigItem(content) {
    var ci = content["ConfigItem"];
    var result = "<b>ModifyItem:</b>"

    result += RenderBasicConfigItem(ci);

    return result;
}

function RenderRemoveConfigItems(content) {
    var cf = content["CookieFilter"]["Cookies"];
    return "<b>RemoveItems:</b>\n\tCookies:\n\t\t" + cf.join("\n\t\t");
}

function RenderYamlConfigChange(content) {
    return "<b>click to view</b>"
}

const ActionRenderers = {
    "AddConfigItem": RenderAddConfigItem,
    "RemoveConfigItem": RenderRemoveConfigItem,
    "ModifyConfigItem": RenderModifyConfigItem,
    "RemoveConfigItems": RenderRemoveConfigItems,
}

function onConsoleLogLoaded(data) {
    if (data['Status']['Code'] != 'SUCCESS') {
        onConsoleLogFailed(data);
        return;
    }

    $('#console-log-error').html('');

    var recs = data['LogRecords'];
    if (recs === undefined)
        recs = [];

    if (recs.length > 0) {
        if (ConsoleLogState.reverse) {
            ConsoleLogState.fromId = Math.min(recs[recs.length - 1]['Id'], recs[0]['Id']);
        } else {
            ConsoleLogState.fromId = Math.max(recs[recs.length - 1]['Id'], recs[0]['Id']);
        }
        ConsoleLogState.totalRecords = recs.length;
    }
    $("#console-from-id").val(ConsoleLogState.fromId);

    for (var i = 0; i < recs.length; ++i) {
        var rec = recs[i];
        var line = document.createElement('tr');
        line.className = "pointer";

        var cell0 = document.createElement('td');
        var cell1 = document.createElement('td');
        var cell2 = document.createElement('td');
        var cell3 = document.createElement('td');
        var cell4 = document.createElement('td');
        var isYamlChange = false;

        if (rec['Data'].hasOwnProperty('AffectedKinds')) {
            var affectedKinds = rec['Data']['AffectedKinds'];
            if (affectedKinds.length === 1 && affectedKinds[0] === 32768) {
                isYamlChange = true;
            } else {
                cell3.innerHTML = "<pre>" + affectedKinds.map(x => cmsEnums.get('ItemKinds', x)).join("\n") + "</pre>";
            }
        }

        cell0.textContent = rec['Id'];
        cell0.dataset.ordervalue = -rec['Id']; // hack for rev order
        var timestamp = new Date(rec['Timestamp'] / 1000);
        cell1.textContent = timestamp.toLocaleString('en-GB', { timeZone: 'UTC' });
        cell2.textContent = rec['User'];
        var contents = [];
        if (!isYamlChange) {
            for (var action of rec['Data']['Action']['Actions']) {
                var key = (Object.keys(action)[0]);
                var content = action[key];
                contents.push(ActionRenderers[key](content));
            }
            cell4.innerHTML = "<pre>" + contents.join("<br/>") + "</pre>";
            cell4.title = JSON.stringify(rec['Data']['Action'], null, 2);
            cell4.setAttribute('data', JSON.stringify(rec['Data'], null, 2));
        } else {
            cell4.innerHTML = RenderYamlConfigChange(rec['Data']['YamlConfigChange']);
            cell4.title = "yaml-config-change";
            cell4.setAttribute('data', JSON.stringify(rec['Data'], null, 2));
        }
        line.appendChild(cell0);
        line.appendChild(cell1);
        line.appendChild(cell2);
        line.appendChild(cell3);
        line.appendChild(cell4);

        document.getElementById('console-log-body').appendChild(line);
    }

    if (recs.length > 0) {
        $("#console-log-table").trigger("update", [true]);
    }
}

function onConsoleLogFailed(data) {
    if (data && data['Status'] && data['Status']['Reason'])
        $('#console-log-error').html(data['Status']['Reason']);
    else
        $('#console-log-error').html("Cannot get CONSOLE log update");
    setTimeout(loadConsoleLog, ConsoleLogState.retryInterval);
}

function loadConsoleLog(reverse) {
    $("#console-log-body").empty();

    $("#console-limit").text(ConsoleLogState.limit);

    var users = $("#console-user-filter").val();
    if (users !== "") {
        ConsoleLogState.users = users.split(',');
    } else {
        ConsoleLogState.users = undefined;
    }

    var affected = $("#console-affected-filter").val();
    if (affected !== "") {
        ConsoleLogState.affected = affected.split(',').map(x => cmsEnums.parse('ItemKinds', x));
    } else {
        ConsoleLogState.affected = undefined;
    }

    var url = 'cms/api/json/console/log?limit=' + ConsoleLogState.limit;

    if (ConsoleLogState.fromTimestamp !== undefined) {
        url += '&from-timestamp=' + ConsoleLogState.fromTimestamp;
        ConsoleLogState.fromTimestamp = undefined;
    } else if (ConsoleLogState.fromId !== undefined) {
        url += '&from-id=' + ConsoleLogState.fromId;
    }

    if (ConsoleLogState.users !== undefined) {
        if (ConsoleLogState.usersExclude) {
            url += '&exclude-users=';
        } else {
            url += '&users=';
        }
        url += ConsoleLogState.users;
    }

    if (ConsoleLogState.affected !== undefined) {
        url += '&affected-kinds=' + ConsoleLogState.affected;
    }

    if (reverse) {
        url += '&reverse=true';
    }

    $.get(url).done(onConsoleLogLoaded).fail(onConsoleLogFailed);
}

function syntaxHighlight(json) {
    json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
        var cls = 'number';
        if (/^"/.test(match)) {
            if (/:$/.test(match)) {
                cls = 'key';
            } else {
                cls = 'string';
            }
        } else if (/true|false/.test(match)) {
            cls = 'boolean';
        } else if (/null/.test(match)) {
            cls = 'null';
        }
        return '<span class="' + cls + '">' + match + '</span>';
    });
}

function renderSpoiler(header, bodyElem) {
    var spoilerHead = $("<div></div>");

    spoilerHead.text(header);
    spoilerHead.addClass("spoiler-head");
    spoilerHead.click(function() {
        $(this)
            .parents('.spoiler-wrap')
            .toggleClass("active")
            .find('.spoiler-body')
            .slideToggle();
    });

    var spoilerBody = $("<div></div>");

    spoilerBody.append(bodyElem);
    spoilerBody.addClass("spoiler-body");

    var spoiler = $("<div></div>");

    spoiler.append(spoilerHead);
    spoiler.append(spoilerBody);
    spoiler.addClass("spoiler-wrap");
    spoiler.addClass("disabled");

    return spoiler;
}

function filterUnaffected(inp, filter) {
    if (!ConsoleLogState.filterByKind) {
        return inp;
    }
    var result = {};
    var affected = filter.map(x => cmsEnums.get('ItemKinds', x).slice(0, -4));
    for (var key of Object.keys(inp)) {
        if (affected.includes(key) || affected.length == 0) {
            result[key] = inp[key];
        }
    }
    return result;
}

function copyDataToClipboard(ev) {
    var element = ev.data.elem;
    var temp = $("<input>");
    $("body").append(temp);
    temp.val($(element).text()).select();
    document.execCommand("copy");
    temp.remove();
}

function GnuNormalFormat(item) {
    var i,
        nf = [],
        op,
        str = [];

    // del add description
    // 0   >0  added count to rhs
    // >0  0   deleted count from lhs
    // >0  >0  changed count lines
    if (item.lhs.del === 0 && item.rhs.add > 0) {
        op = 'a';
    } else if (item.lhs.del > 0 && item.rhs.add === 0) {
        op = 'd';
    } else {
        op = 'c';
    }

    function encodeSide(side, key) {
        // encode side as a start,stop if a range
        str.push(side.at + 1);
        if (side[key] > 1) {
            str.push(',');
            str.push(side[key] + side.at);
        }
    }
    encodeSide(item.lhs, 'del');
    str.push(op);
    encodeSide(item.rhs, 'add');

    nf.push(str.join(''));
    for (i = item.lhs.at; i < item.lhs.at + item.lhs.del; ++i) {
        nf.push('< ' + item.lhs.ctx.getLine(i));
    }
    if (item.rhs.add && item.lhs.del) {
        nf.push('---');
    }
    for (i = item.rhs.at; i < item.rhs.at + item.rhs.add; ++i) {
        nf.push('> ' + item.rhs.ctx.getLine(i));
    }
    return nf.join('\n');
}

function renderCopyablePre(data) {
    var div = $("<div></div>");
    var pre = $("<pre></pre>");
    pre.html(data);
    div.append(pre);
    var copy = $("<div class=\"icon-copy\" style=\"float:right; cursor:pointer;\"></div>");
    copy.click({elem: pre}, copyDataToClipboard);
    div.append(copy);
    return div;
}

function renderConsolePopup(data, user, time) {
    // set header
    $("#popup-header").text("Change by \"" + user + "\" at " + time);

    var div = $("<div></div>");

    if (!data.hasOwnProperty('AffectedKinds')) {
        data['AffectedKinds'] = [];
    }

    var affectedKinds = data['AffectedKinds'];
    if (affectedKinds.length === 1 && affectedKinds[0] === 32768) {
        var cmd = renderCopyablePre(syntaxHighlight(JSON.stringify(data, null, 2)));
        div.append(renderSpoiler("Raw", cmd));

        var lhs = data['YamlConfigChange']['OldYamlConfig'];
        var old = renderCopyablePre(lhs);
        div.append(renderSpoiler("Old", old));

        var rhs = data['YamlConfigChange']['NewYamlConfig'];
        var new_ = renderCopyablePre(rhs);
        div.append(renderSpoiler("New", new_));

        const diff = Myers.diff(lhs, rhs);

        var i = 0;
        var out = [];
        for (i = 0; i < diff.length; ++i) {
            out.push(GnuNormalFormat(diff[i]));
        }
        var result = out.join('\n')

        var diffs = renderCopyablePre(result);
        div.append(renderSpoiler("Diff", diffs));
    } else {
        // create action view
        var cmdDiv = $("<div></div>")
        var cmd = $("<pre></pre>");
        cmd.html(syntaxHighlight(JSON.stringify(data['Action'], null, 2)));
        var copyCmd = $("<div class=\"icon-copy\" style=\"float:right; cursor:pointer;\"></div>");
        copyCmd.click({elem: cmd}, copyDataToClipboard);
        cmdDiv.append(copyCmd);
        cmdDiv.append(cmd);
        div.append(renderSpoiler("Command", cmdDiv));

        if (!isYamlChangedata.hasOwnProperty("AffectedConfigs") && !isYamlChange) {
            for (var i in data['AffectedConfigs']) {
                var oldConfigDiv = $("<div></div>")
                var oldConfig = $("<pre></pre>");
                var oldConfigData = data['AffectedConfigs'][i]['OldConfig'];
                oldConfigData = filterUnaffected(oldConfigData, affectedKinds);
                oldConfig.html(syntaxHighlight(JSON.stringify(oldConfigData, null, 2)));
                var copyOldConfig = $("<div class=\"icon-copy\" style=\"float:right; cursor:pointer;\"></div>");
                copyOldConfig.click({elem: oldConfig}, copyDataToClipboard);
                oldConfigDiv.append(copyOldConfig);
                oldConfigDiv.append(oldConfig);
                div.append(renderSpoiler(
                    "Old Config for Tenant:\"" + data['AffectedConfigs'][i]['Tenant'] +
                    "\" NodeType:\"" + data['AffectedConfigs'][i]['NodeType'] + "\"", oldConfigDiv));

                var newConfigDiv = $("<div></div>")
                var newConfig = $("<pre></pre>");
                var newConfigData = data['AffectedConfigs'][i]['NewConfig'];
                newConfigData = filterUnaffected(newConfigData, affectedKinds);
                newConfig.html(syntaxHighlight(JSON.stringify(newConfigData, null, 2)));
                var copyNewConfig = $("<div class=\"icon-copy\" style=\"float:right; cursor:pointer;\"></div>");
                copyNewConfig.click({elem: newConfig}, copyDataToClipboard);
                newConfigDiv.append(copyNewConfig);
                newConfigDiv.append(newConfig);
                div.append(renderSpoiler(
                    "New Config for Tenant:\"" + data['AffectedConfigs'][i]['Tenant'] +
                    "\" NodeType:\"" + data['AffectedConfigs'][i]['NodeType'] + "\"", newConfigDiv));
            }
        }
    }

    // clear and fill popup
    var content = $("#popup-content");
    content.empty();
    content.append(div);

    // actually show popup
    $("#popup").show();
}

function initConsoleLogTab() {
    $("#console-log-table")
        .on('click', 'tbody tr', function() {
            // load data
            var data = JSON.parse($(this).closest('tr').children().eq(4).attr('data'));
            var user = $(this).closest('tr').children().eq(2).text();
            var time = $(this).closest('tr').children().eq(1).text();

            renderConsolePopup(data, user, time);
        })
        .tablesorter({
            theme: 'blue',
            sortList: [[0,0]],
            headers: {
                0: {
                    sorter: 'numeric-ordervalue',
                },
                1: {
                    sorter: false,
                },
                2: {
                    sorter: false,
                },
                3: {
                    sorter: false,
                },
                4: {
                    sorter: false,
                }
            },
            widgets : ['zebra'],
        });

    $("#console-first-page")
        .click(function() {
            ConsoleLogState.limit = parseInt($("#console-limit").val(), 10);
            ConsoleLogState.fromId = undefined;
            loadConsoleLog(false);
    });

    $("#console-prev-page")
        .click(function() {
            ConsoleLogState.limit = parseInt($("#console-limit").val(), 10);
            ConsoleLogState.fromId += ConsoleLogState.limit;
            loadConsoleLog(ConsoleLogState.reverse);
        });

    $("#console-next-page")
        .click(function() {
            ConsoleLogState.limit = parseInt($("#console-limit").val(), 10);
            if (!ConsoleLogState.reverse) {
                if (ConsoleLogState.fromId - ConsoleLogState.limit >= ConsoleLogState.limit - 1) {
                    ConsoleLogState.fromId -= ConsoleLogState.limit;
                } else {
                    ConsoleLogState.fromId = ConsoleLogState.limit - 1;
                }
            } else {
                if (ConsoleLogState.fromId - ConsoleLogState.limit >= 0) {
                    ConsoleLogState.fromId -= ConsoleLogState.limit;
                } else {
                    ConsoleLogState.fromId = 0;
                }
            }
            loadConsoleLog(ConsoleLogState.reverse);
        });

    $("#console-last-page")
        .click(function() {
            ConsoleLogState.limit = parseInt($("#console-limit").val(), 10);
            ConsoleLogState.fromId = undefined;
            loadConsoleLog(true);
        });

    $("#console-from-id-fetch")
        .click(function() {
            ConsoleLogState.limit = $("#console-limit").val();
            ConsoleLogState.fromId = $("#console-from-id").val();
            loadConsoleLog(ConsoleLogState.reverse);
        });

    $('input[type=radio][name=console-order]').change(function() {
        if (this.value === 'normal') {
            ConsoleLogState.reverse = false;
        }
        else if (this.value === 'reverse') {
            ConsoleLogState.reverse = true;
        }
        loadConsoleLog(ConsoleLogState.reverse);
    });

    $('input[type=checkbox][name=console-user-filter-exclude]').change(function() {
        ConsoleLogState.usersExclude = this.checked;
    })

    $('#console-datetime').val(new Date(Date.now()).toISOString().slice(0, 19));

    $("#console-datetime-search")
        .click(function() {
            ConsoleLogState.limit = parseInt($("#console-limit").val(), 10);
            ConsoleLogState.fromTimestamp = Date.parse($('#console-datetime').val() + "Z");
            loadConsoleLog(ConsoleLogState.reverse);
        });

    loadConsoleLog();
}
