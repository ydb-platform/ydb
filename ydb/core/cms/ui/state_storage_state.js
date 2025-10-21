'use strict';

class StateStorageState {

    constructor() {
        this.fetchInterval = 2000;
        this.nodes = {};
        this.stateStorageConfig = {};
        this.boardConfig = {};
        this.schemeBoardConfig = {};
        this.states = {};
        this.sentinelStates = {};
        this.ssData = undefined;
        this.ssbData = undefined;
        this.sbData = undefined;
        this.headers = ["Group", "Ring", "NodeId", "Location", "Distconf Status", "Sentinel Status"];
        this.nodeIdColumnIdx = 2;
        this.dcStatusColumnIdx = 4;
        this.sStatusColumnIdx = 5;
        this.initTab();
    }

    buildPVHeader(table, header) {
        let h = [header];
        for (let i = 0; i < this.headers.length - 1; i++) {
            h.push("");
        }
        var row = table.addRow(h);
        row[0].setHeader(true);
        table.merge(0, 0, 0, this.headers.length - 1);
        row = table.addRow(this.headers);
        for (let i = 0; i < this.headers.length; i++)
            row[i].setHeader(true);
        return row;
    }

    updateColor(cell, value, isSentinel) {
        cell.elem.removeClass("red");
        cell.elem.removeClass("yellow");
        cell.elem.removeClass("green");
        cell.elem.addClass(this.codeToColor(value[this.nodeIdColumnIdx], isSentinel));
    }

    addPVEntry(table, value) {
        var row = table.addRow(value);
        this.updateColor(row[this.dcStatusColumnIdx], value, false);
        this.updateColor(row[this.sStatusColumnIdx], value, true);
        return row;
    }

    updatePVEntry(row, value, prevValue) {
        if(value.join() !== prevValue.join()) {
            for(let i = 0; i < this.headers.length; i++) {
                if (value[i] !== prevValue[i]) {
                    row[i].setText(value[i]);
                }
            }
            this.updateColor(row[this.dcStatusColumnIdx], value, false);
            this.updateColor(row[this.sStatusColumnIdx], value, true);
        }
    }

    renderPVEntry(tableEntry, newData) {
        var table = tableEntry.table;
        var data = tableEntry.data;
        for (var entry in newData) {
            if (!data.hasOwnProperty(entry)) {
                var row = this.addPVEntry(table, newData[entry]);
                data[entry] = {
                    row: row,
                    data: newData[entry],
                };
            } else {
                this.updatePVEntry(
                    data[entry].row,
                    newData[entry],
                    data[entry].data);
                data[entry].data = newData[entry];
            }
        }
        while (tableEntry.table.rows.length - 2 > newData.length) {
            table.removeRow(newData.length + 2);
            delete data[tableEntry.table.rows.length - 2];
        }
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

    codeToColor(nodeId, isSentinel) {
        let st = isSentinel ? this.sentinelStates : this.states;
        if (!st.hasOwnProperty(nodeId)) {
            return "yellow";
        }
        switch (st[nodeId].State) {
            case 0: return "green";
            case 1: return "yellow";
            case 2: return "yellow";
            case 3: return "yellow";
            case 4: return "red";
            case 5: return "yellow";
        }
    }

    codeToNodeState(nodeId, isSentinel) {
        let st = isSentinel ? this.sentinelStates : this.states;
        if (!st.hasOwnProperty(nodeId)) {
            return "UNKNOWN";
        }
        switch (st[nodeId].State) {
            case 0: return "GOOD";
            case 1: return "PRETTY GOOD";
            case 2: return "MAYBE GOOD";
            case 3: return "MAYBE BAD";
            case 4: return "BAD";
            case 5: return "UNKNOWN";
        }
    }

    getNodeName(nodeId) {
        if (!this.states.hasOwnProperty(nodeId)) {
            return nodeId;
        }
        return this.states[nodeId].Location;
    }

    prepareData(data) {
        let res = [];
        let rgs = [];
        if (data.hasOwnProperty("RingGroups")) {
            for (let rg of data["RingGroups"]) {
                rgs.push(rg);
            }
        }
        if (data.hasOwnProperty("Ring")) {
            rgs.push(data["Ring"]);
        }
        let rgId = 1;
        for (let rg of rgs) {
            let ringId = 1;
            let rgName = rgId + " NToSelect: " + rg.NToSelect;
            if (rg.hasOwnProperty("BridgePileId")) {
                rgName += " Pile: " + rg.BridgePileId
                if (rg.hasOwnProperty("PileState")) {
                    rgName += " " + rg.PileState;
                }
            }
            if (rg.WriteOnly) {
                rgName += " WriteOnly";
            }
            if (rg.hasOwnProperty("Ring")) {
                for (let r of rg["Ring"]) {
                    let ringName = ringId;
                    if (r.IsDisabled) {
                        ringName += " Disabled";
                    }
                    if (r.UseRingSpecificNodeSelection) {
                        ringname += " UseRingSpecificNodeSelection";
                    }
                    for (let n of r["Node"]) {
                        res.push([rgName, ringName, n, this.getNodeName(n), this.codeToNodeState(n, false), this.codeToNodeState(n, true)]);
                    }
                    ringId++;
                }
            }
            if (rg.hasOwnProperty("Node")) {
                for (let n of rg["Node"]) {
                    res.push([rgName, ringId, n, this.getNodeName(n), this.codeToNodeState(n, false), this.codeToNodeState(n, true)]);
                    ringId++;
                }
            }
            rgId++;
        }
        return res;
    }

    tryRender() {
        if (this.ssData && this.ssbData && this.sbData) {
            this.renderPVEntry(this.stateStorageConfig, this.ssData);
            this.renderPVEntry(this.boardConfig, this.ssbData);
            this.renderPVEntry(this.schemeBoardConfig, this.sbData);
        }
    }

    onDistconfLoaded(data) {
        if (data && data.hasOwnProperty("StateStorageConfig")) {
            $("#state-storage-error").empty();
            if (data["StateStorageConfig"].hasOwnProperty("NodesState")) {
                for (let state of data["StateStorageConfig"]["NodesState"]) {
                    this.states[state.NodeId] = state;
                }
            }
            this.ssData = this.prepareData(data["StateStorageConfig"]["StateStorageConfig"]);
            this.ssbData = this.prepareData(data["StateStorageConfig"]["StateStorageBoardConfig"]);
            this.sbData = this.prepareData(data["StateStorageConfig"]["SchemeBoardConfig"]);
            this.tryRender();
        } else {
            $("#state-storage-error").text("Error while updating distconf state");
        }
        setTimeout(this.loadDistconfStatus.bind(this), this.fetchInterval);
        this.restartAnimation($("#state-storage-anim"));
    }

    onSentinelLoaded(data) {
        if (data?.Status?.Code === "OK") {
            $("#state-storage-error").empty();
            if (data.hasOwnProperty("NodesState")) {
                for (let state of data["NodesState"]) {
                    this.sentinelStates[state.NodeId] = state;
                }
            }
            this.tryRender();
        } else {
            $("#state-storage-error").text("Error while updating sentinel state");
        }
        setTimeout(this.loadSentinelStatus.bind(this), this.fetchInterval);
    }

    onError(jqXHR, exception) {
        let msg = "Error while updating state. ";
        if (jqXHR.status === 0) {
            msg += 'Not connect. Verify Network.';
        } else if (jqXHR.status == 404) {
            msg += 'Requested page not found (404).';
        } else if (jqXHR.status == 500) {
            msg += 'Internal Server Error (500).';
        } else if (exception === 'parsererror') {
            msg += 'Requested JSON parse failed.';
        } else if (exception === 'timeout') {
            msg += 'Time out error.';
        } else if (exception === 'abort') {
            msg += 'Ajax request aborted.';
        } else {
            msg += 'Uncaught Error. ' + jqXHR.responseText;
        }
        $("#state-storage-error").text(msg);
    }
    loadDistconfStatus() {
        this.cleanup();
        $.ajax({
            url: '/actors/nodewarden?page=distconf',
            type: "POST",
            data: '{"GetStateStorageConfig": {"NodesState": true}}',
            dataType: "json",
            contentType: "application/json; charset=utf-8",
            success: this.onDistconfLoaded.bind(this),
            error: this.onError.bind(this)
        });
    }

    loadSentinelStatus() {
        this.cleanup();
        $.ajax({
            url: 'cms/api/json/sentinel',
            type: "GET",
            dataType: "json",
            contentType: "application/json; charset=utf-8",
            success: this.onSentinelLoaded.bind(this),
            error: this.onError.bind(this)
        });
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
        var table = new Table($("#state-storage-" + this.nameToSelector(name)), this.onCellUpdate);
        var header = this.buildPVHeader(table, name);
        this[this.nameToMember(name)] = {
            header: header,
            table: table,
            data: {},
        };
    }

    refreshRange() {
        var value = $("#state-storage-range").val();
        const re = /^(?:(?:\d+|(?:\d+-\d+)),)*(?:\d+|(?:\d+-\d+))$/;
        if (re.test(value)) {
            $("#state-storage-range-error").empty();
            this.range = value;
            this.cleanup();
        } else {
            $("#state-storage-range-error").text("Invalid range");
        }
    }


    cleanup() {
        this.nodes = {};
    }

    initTab() {
        $("#state-storage-anim").addClass("anim");
        $("#state-storage-refresh-range").click(this.refreshRange.bind(this));

        for (var name of ["StateStorageConfig", "BoardConfig", "SchemeBoardConfig"]) {
            this.preparePVTable(name);
        }

        this.cleanup();

        this.loadDistconfStatus();
        this.loadSentinelStatus();
    }
}

var stateStorageState;

function initStateStorageTab() {
    stateStorageState = new StateStorageState();
}
