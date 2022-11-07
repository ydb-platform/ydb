'use strict';

var CmsSentinelState = {
    fetchInterval: 5000,
    nodes: {},
    pdisks: {},
    config: {},
    stateUpdater: {},
    configUpdater: {},
};

function id(arg) {
    return { "value": arg === undefined ? "nil" : arg };
}

var TPDiskState = [
    "Initial",
    "InitialFormatRead",
    "InitialFormatReadError",
    "InitialSysLogRead",
    "InitialSysLogReadError",
    "InitialSysLogParseError",
    "InitialCommonLogRead",
    "InitialCommonLogReadError",
    "InitialCommonLogParseError",
    "CommonLoggerInitError",
    "Normal",
    "OpenFileError",
    "ChunkQuotaError",
    "DeviceIoError",
];

TPDiskState[252] = "Missing";
TPDiskState[253] = "Timeout";
TPDiskState[254] = "NodeDisconnected";
TPDiskState[255] = "Unknown";

function state(highlight, arg) {
    var res = {
        "value": arg + ":" + TPDiskState[arg]
    };
    if (highlight == true) {
        res.class = arg == 10 ? "green" : "red"
    }
    return res;
}

const EPDiskStatus = [
    "UNKNOWN",
    "ACTIVE",
    "INACTIVE",
    "BROKEN",
    "FAULTY",
    "TO_BE_REMOVED",
];

function status(arg) {
    return { "value": arg === undefined ? "nil" : arg + ":" + EPDiskStatus[arg], "class": arg === 1 ? "green" : (arg === undefined ? undefined : "red") };
}

function bool(arg) {
    return { "value": arg === true ? "+" : "-" };
}

const PDiskInfoValueMappers = {
    "State": function(arg) { return state(true, arg); },
    "PrevState": function(arg) { return state(false, arg); },
    "StateCounter": id,
    "Status": status,
    "ChangingAllowed": bool,
    "Touched": bool,
    "DesiredStatus": status,
    "StatusChangeAttempts": id,
}

function nameToSelector(name) {
    return (name.charAt(0).toLowerCase() + name.slice(1)).replace(/([A-Z])/g, "-$1").toLowerCase();
}

function nameToMember(name) {
    return (name.charAt(0).toLowerCase() + name.slice(1));
}

function restartAnimation(node) {
    var el = node;
    var newone = el.clone(true);
    el.before(newone);
    el.remove()
}

function mapPDiskState(cell, key, text, silent = false) {
    if (PDiskInfoValueMappers.hasOwnProperty(key)) {
        var data = PDiskInfoValueMappers[key](text);
        cell.setText(
            data.value,
            silent
        );
        if (data.hasOwnProperty("class")) {
            cell.elem.removeClass("red");
            cell.elem.removeClass("yellow");
            cell.elem.removeClass("green");
            cell.elem.addClass(data.class);
        }
    }
}

const PDiskHeaders = [
    "PDiskId",
    "State",
    "PrevState",
    "StateCounter",
    "Status",
    "ChangingAllowed",
    "Touched",
    "DesiredStatus",
    "StatusChangeAttempts",
];

function buildPDisksTableHeader(table, width) {
    var headers = ["Node", "PDisk"];
    for (var i = headers.length; i < width; ++i) {
        headers.push("");
    }
    var row = table.addRow(headers);
    row[0].setHeader(true);
    row[1].setHeader(true);
    table.merge(0, 0, 1, width);
}

function buildNodeHeader(table, NodeId) {
    var headers = [NodeId].concat(PDiskHeaders);
    var row = table.addRow(headers);
    row[0].elem.addClass("side");
    for (var i = 1; i < row.length; ++i) {
        row[i].setHeader(true);
    }
    return row;
}

function buildPDisk(table, header, id, diskData) {
    diskData["PDiskId"] = id;
    var data = [""].concat(PDiskHeaders.map((x) => diskData[x]));
    var row = table.insertRowAfter(header[0], data);
    for (var i = 2; i < row.length; ++i) {
        var key = PDiskHeaders[i - 1];
        mapPDiskState(row[i], key, diskData[key], true)
    }
    return row;
}

function updatePDisk(table, row, id, data, prevData) {
    for (var i = 2; i < row.length; ++i) {
        var key = PDiskHeaders[i - 1];
        if (data[key] !== prevData[key]) {
            mapPDiskState(row[i], key, data[key])
        }
    }
}

function buildPVHeader(table, header) {
    var headers = [header, ""];
    var row = table.addRow(headers);
    row[0].setHeader(true);
    table.merge(0, 0, 0, 1);
    headers = ["Param", "Value"];
    row = table.addRow(headers);
    row[0].setHeader(true);
    row[1].setHeader(true);
    return row;
}

function addPVEntry(table, header, key, value) {
    var data = [key, value];
    var row = table.insertRowAfter(header, data);
    return row;
}

function updatePVEntry(table, row, value, prevValue) {
    if(value !== prevValue) {
        row[1].setText(value);
    }
}

function renderPVEntry(entry, newData) {
    var table = entry.table;
    var headers = entry.header;
    var data = entry.data;
    for (var entry in newData) {
        if (!data.hasOwnProperty(entry)) {
            var row = addPVEntry(table, headers[0], entry, newData[entry]);
            data[entry] = {
                row: row,
                data: newData[entry],
            };
        } else {
            updatePVEntry(
                table,
                data[entry].row,
                newData[entry],
                data[entry].data);
            data[entry].data = newData[entry];
        }
    }
}

function renderPDisksTable(data) {
    var table = CmsSentinelState.pdisksTable;

    for (var ipdisk in data["PDisks"]) {
        var pdisk = data["PDisks"][ipdisk];
        var NodeId = pdisk["Id"]["NodeId"];
        var PDiskId = pdisk["Id"]["DiskId"];
        var PDiskInfo = pdisk["Info"];
        if (!CmsSentinelState.nodes.hasOwnProperty(NodeId)) {
            var row = buildNodeHeader(table, NodeId);
            CmsSentinelState.nodes[NodeId] = {
                header: row,
                pdisks: {}
            };
        }
        if (!CmsSentinelState.nodes[NodeId].pdisks.hasOwnProperty(PDiskId)) {
            var header = CmsSentinelState.nodes[NodeId].header;
            var row = buildPDisk(table, header, PDiskId, PDiskInfo);

            table.mergeCells(header[0], row[0]);

            CmsSentinelState.nodes[NodeId].pdisks[PDiskId] = {
                row: row,
                data: PDiskInfo,
            };
        } else {
            var prevState = CmsSentinelState.nodes[NodeId].pdisks[PDiskId];
            updatePDisk(table, prevState.row, PDiskId, PDiskInfo, prevState.data);
            prevState.data = PDiskInfo;
        }
    }
}

function onCmsSentinelStateLoaded(data) {
    if (data?.Status?.Code === "OK") {
        $("#sentinel-error").empty();

        renderPDisksTable(data);

        renderPVEntry(CmsSentinelState.config, data["SentinelConfig"]);

        var flattenStateUpdaterResp = data["StateUpdater"]["UpdaterInfo"];
        flattenStateUpdaterResp["WaitNodes"] = data["StateUpdater"]["WaitNodes"];
        renderPVEntry(CmsSentinelState.stateUpdater, flattenStateUpdaterResp);

        var flattenConfigUpdaterResp = data["ConfigUpdater"]["UpdaterInfo"];
        flattenConfigUpdaterResp["Attempt"] = data["ConfigUpdater"]["Attempt"];
        renderPVEntry(CmsSentinelState.configUpdater, flattenConfigUpdaterResp);
    } else {
        $("#sentinel-error").text("Error while updating state");
    }
    setTimeout(loadCmsSentinelState, CmsSentinelState.fetchInterval);
    restartAnimation($("#sentinel-anim"));
}

function loadCmsSentinelState() {
    var url = 'cms/api/json/sentinel';
    $.get(url).done(onCmsSentinelStateLoaded);
}

function onCellUpdate(cell) {
    cell.elem.addClass("highlight");
    var el = cell.elem;
    var newone = el.clone(true);
    el.before(newone);
    el.remove();
    cell.elem = newone;
}

function preparePVTable(name) {
    var table = new Table($("#sentinel-" + nameToSelector(name)), onCellUpdate);
    var header = buildPVHeader(table, name);
    CmsSentinelState[nameToMember(name)] = {
        header: header,
        table: table,
        data: {},
    };
}

function initCmsSentinelTab() {
    $("#sentinel-anim").addClass("anim");

    for (var name of ["Config", "StateUpdater", "ConfigUpdater"]) {
        preparePVTable(name);
    }

    CmsSentinelState.pdisksTable = new Table($("#sentinel-nodes"), onCellUpdate);
    buildPDisksTableHeader(CmsSentinelState.pdisksTable, PDiskHeaders.length + 1);

    loadCmsSentinelState();
}
