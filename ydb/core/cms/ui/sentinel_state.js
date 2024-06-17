'use strict';

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

const EPDiskStatus = {
    0: "UNKNOWN",
    1: "ACTIVE",
    2: "INACTIVE",
    3: "BROKEN",
    5: "FAULTY",
    6: "TO_BE_REMOVED",
};

const PDiskHeaders = [
    "PDiskId",
    "State",
    "PrevState",
    "StateCounter",
    "Status",
    "DesiredStatus",
    "ChangingAllowed",
    "LastStatusChange",
    "StatusChangeFailed",
    "Touched",
    "StatusChangeAttempts",
    "PrevDesiredStatus",
    "PrevStatusChangeAttempts",
    "IgnoreReason",
];

class CmsSentinelState {

    constructor() {
        this.fetchInterval = 5000;
        this.nodes = {};
        this.pdisks = {};
        this.config = {};
        this.stateUpdater = {};
        this.configUpdater = {};
        this.show = "UNHEALTHY";
        this.range = "1-20";
        this.filtered = {};
        this.filteredSize = 0;
        this.gen = 0;

        this.initTab();
    }

    buildPVHeader(table, header) {
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

    addPVEntry(table, header, key, value) {
        var data = [key, value];
        var row = table.insertRowAfter(header, data);
        return row;
    }

    updatePVEntry(table, row, value, prevValue) {
        if(value !== prevValue) {
            row[1].setText(value);
        }
    }

    renderPVEntry(entry, newData) {
        var table = entry.table;
        var headers = entry.header;
        var data = entry.data;
        for (var entry in newData) {
            if (!data.hasOwnProperty(entry)) {
                var row = this.addPVEntry(table, headers[0], entry, newData[entry]);
                data[entry] = {
                    row: row,
                    data: newData[entry],
                };
            } else {
                this.updatePVEntry(
                    table,
                    data[entry].row,
                    newData[entry],
                    data[entry].data);
                data[entry].data = newData[entry];
            }
        }
    }

    id(arg) {
        return { "value": arg === undefined ? "nil" : arg };
    }

    state(highlight, arg) {
        var res = {
            "value": arg + ":" + TPDiskState[arg]
        };
        if (highlight == true) {
            res.class = arg == 10 ? "green" : "red"
        }
        return res;
    }

    status(arg) {
        return { "value": arg === undefined ? "nil" : arg + ":" + EPDiskStatus[arg], "class": arg === 1 ? "green" : (arg === undefined ? undefined : "red") };
    }

    bool(arg) {
        return { "value": arg === true ? "+" : "-" };
    }

    getPDiskInfoValueMappers() {
        return {
            "State": function(arg) { return this.state(true, arg); }.bind(this),
            "PrevState": function(arg) { return this.state(false, arg); }.bind(this),
            "StateCounter": this.id.bind(this),
            "Status": this.status.bind(this),
            "ChangingAllowed": this.bool.bind(this),
            "Touched": this.bool.bind(this),
            "DesiredStatus": this.status.bind(this),
            "StatusChangeAttempts": this.id.bind(this),
            "PrevDesiredStatus": this.id.bind(this),
            "PrevStatusChangeAttempts": this.id.bind(this),
            "LastStatusChange": this.id.bind(this),
            "IgnoreReason": this.id.bind(this),
        };
    }

    getHiddenPDiskInfo() {
        return [
            "PrevDesiredStatus",
            "PrevStatusChangeAttempts",
        ];
    }

    nameToSelector(name) {
        return (name.charAt(0).toLowerCase() + name.slice(1)).replace(/([A-Z])/g, "-$1").toLowerCase();
    }

    nameToMember(name) {
        return (name.charAt(0).toLowerCase() + name.slice(1));
    }

    restartAnimation(node) {
        var el = node;
        var newone = el.clone(true);
        el.before(newone);
        el.remove()
    }

    mapPDiskState(cell, key, text, silent = false) {
        if (this.getPDiskInfoValueMappers().hasOwnProperty(key)) {
            var data = this.getPDiskInfoValueMappers()[key](text);
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

    buildPDisksTableHeader(table, width) {
        var headers = ["Node", "PDisk"];
        for (var i = headers.length; i < width; ++i) {
            headers.push("");
        }
        var row = table.addRow(headers);
        row[0].setHeader(true);
        row[1].setHeader(true);
        table.merge(0, 0, 1, width);
    }

    columnFilter(el) {
        if (this.filtered.hasOwnProperty(el) && this.filtered[el]) {
            return false;
        }
        return true;
    }

    buildNodeHeader(table, NodeId) {
        var headers = [NodeId].concat(PDiskHeaders).filter(this.columnFilter.bind(this));
        var row = table.addRow(headers);
        row[0].elem.addClass("side");
        for (var i = 1; i < row.length; ++i) {
            row[i].setHeader(true);
        }
        return row;
    }

    buildPDisk(table, header, id, diskData) {
        diskData["PDiskId"] = id;
        var deletionMarker = "DELETED";
        var data = [""].concat(PDiskHeaders.map((x) => this.columnFilter(x) ? diskData[x] : deletionMarker)).filter((x) => x !== deletionMarker);
        var row = table.insertRowAfter(header[0], data);
        var filteredHeaders = PDiskHeaders.filter(this.columnFilter.bind(this));
        for (var i = 2; i < row.length; ++i) {
            var key = filteredHeaders[i - 1];
            this.mapPDiskState(row[i], key, diskData[key], true);
        }
        return row;
    }

    updatePDisk(table, row, id, data, prevData) {
        for (var i = 2; i < row.length; ++i) {
            var key = PDiskHeaders[i - 1];
            if (data[key] !== prevData[key]) {
                this.mapPDiskState(row[i], key, data[key]);
            }
        }
    }

    removeOutdated() {
        for (var node in this.nodes) {
            for (var pdisk in this.nodes[node].pdisks) {
                if (this.nodes[node].pdisks[pdisk].gen != this.gen) {
                    this.pdisksTable.removeRowByElem(this.nodes[node].pdisks[pdisk].row[1]); // first because zero is a proxy
                    delete this.nodes[node].pdisks[pdisk];
                }
            }
            if (Object.keys(this.nodes[node].pdisks).length === 0) {
                this.pdisksTable.removeRowByElem(this.nodes[node].header[0]);
                delete this.nodes[node];
            }
        }
    }

    renderPDisksTable(data) {
        var table = this.pdisksTable;

        this.gen++;
        var currentGen = this.gen;

        for (var ipdisk in data["PDisks"]) {
            var pdisk = data["PDisks"][ipdisk];
            var NodeId = pdisk["Id"]["NodeId"];
            var PDiskId = pdisk["Id"]["DiskId"];
            var PDiskInfo = pdisk["Info"];
            if (!this.nodes.hasOwnProperty(NodeId)) {
                var row = this.buildNodeHeader(table, NodeId);
                this.nodes[NodeId] = {
                    header: row,
                    pdisks: {}
                };
            }
            if (!this.nodes[NodeId].pdisks.hasOwnProperty(PDiskId)) {
                var header = this.nodes[NodeId].header;
                var row = this.buildPDisk(table, header, PDiskId, PDiskInfo);

                table.mergeCells(header[0], row[0]);

                this.nodes[NodeId].pdisks[PDiskId] = {
                    row: row,
                    data: PDiskInfo,
                    gen: currentGen,
                };
            } else {
                var prevState = this.nodes[NodeId].pdisks[PDiskId];
                this.updatePDisk(table, prevState.row, PDiskId, PDiskInfo, prevState.data);
                prevState.data = PDiskInfo;
                prevState.gen = currentGen;
            }
        }

        this.removeOutdated();
    }

    onThisLoaded(data) {
        if (data?.Status?.Code === "OK") {
            $("#sentinel-error").empty();

            this.renderPDisksTable(data);

            this.renderPVEntry(this.config, data["SentinelConfig"]);

            var flattenStateUpdaterResp = data["StateUpdater"]["UpdaterInfo"];
            flattenStateUpdaterResp["WaitNodes"] = data["StateUpdater"]["WaitNodes"];
            for (var key in data["StateUpdater"]["PrevUpdaterInfo"]) {
                flattenStateUpdaterResp["Prev" + key] = data["StateUpdater"]["PrevUpdaterInfo"][key];
            }
            this.renderPVEntry(this.stateUpdater, flattenStateUpdaterResp);

            var flattenConfigUpdaterResp = data["ConfigUpdater"]["UpdaterInfo"];
            flattenConfigUpdaterResp["BSCAttempt"] = data["ConfigUpdater"]["BSCAttempt"];
            flattenConfigUpdaterResp["CMSAttempt"] = data["ConfigUpdater"]["CMSAttempt"];
            flattenConfigUpdaterResp["PrevBSCAttempt"] = data["ConfigUpdater"]["PrevBSCAttempt"];
            flattenConfigUpdaterResp["PrevCMSAttempt"] = data["ConfigUpdater"]["PrevCMSAttempt"];
            for (var key in data["StateUpdater"]["PrevUpdaterInfo"]) {
                flattenConfigUpdaterResp["Prev" + key] = data["ConfigUpdater"]["PrevUpdaterInfo"][key];
            }
            this.renderPVEntry(this.configUpdater, flattenConfigUpdaterResp);
        } else {
            $("#sentinel-error").text("Error while updating state");
        }
        setTimeout(this.loadThis.bind(this), this.fetchInterval);
        this.restartAnimation($("#sentinel-anim"));
    }

    loadThis() {
        var show = $('input[name="sentinel-switch"]:checked').val();

        if (show != this.show) {
            this.cleanup();
        }

        this.show = show;
        var url = 'cms/api/json/sentinel?show=' + this.show;
        if (this.range != "") {
            url = url + "&range=" + this.range;
        }
        $.get(url).done(this.onThisLoaded.bind(this));
    }

    onCellUpdate(cell) {
        cell.elem.addClass("highlight");
        var el = cell.elem;
        var newone = el.clone(true);
        el.before(newone);
        el.remove();
        cell.elem = newone;
    }

    onInsertColumn(cell, columnId) {
        cell.onUpdate = cell.onUpdate;
    }

    preparePVTable(name) {
        var table = new Table($("#sentinel-" + this.nameToSelector(name)), this.onCellUpdate);
        var header = this.buildPVHeader(table, name);
        this[this.nameToMember(name)] = {
            header: header,
            table: table,
            data: {},
        };
    }

    refreshRange() {
        var value = $("#sentinel-range").val();
        const re = /^(?:(?:\d+|(?:\d+-\d+)),)*(?:\d+|(?:\d+-\d+))$/;
        if (re.test(value)) {
            $("#sentinel-range-error").empty();
            this.range = value;
            this.cleanup();
        } else {
            $("#sentinel-range-error").text("Invalid range");
        }
    }

    addCheckbox(elem, name) {
        var params = { type: 'checkbox', id: 'cb-' + name, value: name };
        if (!this.getHiddenPDiskInfo().includes(name)) {
            params.checked = 'checked';
        } else {
            this.filtered[name] = true;
            this.filteredSize++;
        }
        var cb = $('<input />', params);

        cb.change(function() {
            if(cb[0].checked) {
                    this.filtered[name] = false;
                    this.filteredSize--;
                } else {
                    this.filtered[name] = true;
                    this.filteredSize++;
                }
                this.cleanup();
            }.bind(this)).appendTo(elem);
        $('<label />', { 'for': 'cb-' + name, text: name,  }).appendTo(elem);
    }

    addFilterCheckboxes() {
        var elem = $("#sentinel-filter-controls");
        for (var column of PDiskHeaders) {
            this.filtered[column] = false;
            if (column !== "PDiskId") {
                var div = $('<div />', { class: 'sentinel-checkbox' })
                this.addCheckbox(div, column)
                div.appendTo(elem);
            }
        }
    }

    cleanup() {
        this.nodes = {};
        this.pdisks = {};
        this.pdisksTable?.elem.empty();
        this.pdisksTable = new Table($("#sentinel-nodes"), this.onCellUpdate, this.onInsertColumn);
        this.buildPDisksTableHeader(this.pdisksTable, PDiskHeaders.length + 1 - this.filteredSize);
    }

    initTab() {
        $("#sentinel-anim").addClass("anim");
        $("#sentinel-refresh-range").click(this.refreshRange.bind(this));

        for (var name of ["Config", "StateUpdater", "ConfigUpdater"]) {
            this.preparePVTable(name);
        }

        this.addFilterCheckboxes();

        this.cleanup();

        this.loadThis();
    }
}

var cmsSentinelState;

function initCmsSentinelTab() {
    cmsSentinelState = new CmsSentinelState();
}
