'use strict';

var CmsSentinelState = {
    fetchInterval: 5000,
};

function syntaxHighlight(json) {
    if (typeof json != 'string') {
         json = JSON.stringify(json, undefined, 4);
    }
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

function onCmsSentinelStateLoaded(data) {
    $("#sentinel-state-content").html(syntaxHighlight(data));
    setTimeout(loadCmsSentinelState, CmsSentinelState.fetchInterval);
}

function loadCmsSentinelState() {
    var url = 'cms/api/json/sentinel';
    $.get(url).done(onCmsSentinelStateLoaded);
}

function initCmsSentinelTab() {
    loadCmsSentinelState();
}
