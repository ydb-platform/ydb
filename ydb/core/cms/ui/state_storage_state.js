'use strict';

class StateStorageState {

    constructor() {
        this.fetchInterval = 5000;
        this.nodes = {};
        this.stateStorageConfig = {};
        this.boardConfig = {};
        this.schemeBoardConfig = {};
        this.states = {};

        this.initTab();
    }

    buildPVHeader(table, header) {
        var headers = [header, "", "", "", "", ""];
        var row = table.addRow(headers);
        row[0].setHeader(true);
        table.merge(0, 0, 0, 5);
        headers = ["Pile", "Group", "Ring", "NodeId", "Location", "Status"];
        row = table.addRow(headers);
        for(let i = 0; i < 6; i++)
            row[i].setHeader(true);
        return row;
    }

    updateColor(cell, value) {
        cell.elem.removeClass("red");
        cell.elem.removeClass("yellow");
        cell.elem.removeClass("green");
        cell.elem.addClass(this.codeToColor(value[3]));
    }

    addPVEntry(table, value) {
        var row = table.addRow(value);
        this.updateColor(row[5], value);
        return row;
    }

    updatePVEntry(row, value, prevValue) {
        if(value.join() !== prevValue.join()) {
            for(let i = 0; i < 5; i++)
                row[i].setText(value[i]);
            let cell = row[5];
            cell.setText(value[5]);
            this.updateColor(cell, value);
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

    codeToColor(nodeId) {
        if (!this.states.hasOwnProperty(nodeId)) {
            return "yellow";
        }
        switch (this.states[nodeId].State) {
            case 0: return "green";
            case 1: return "yellow";
            case 2: return "yellow";
            case 3: return "yellow";
            case 4: return "red";
            case 5: return "yellow";
        }
    }

    codeToNodeState(nodeId) {
        if (!this.states.hasOwnProperty(nodeId)) {
            return "UNKNOWN";
        }
        switch (this.states[nodeId].State) {
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
        let rgId = 0;
        for (let rg of rgs) {
            let ringId = 1;
            let rgName = rgId + " NToSelect: " + rg.NToSelect;
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
                        res.push(["", rgName, ringName, n, this.getNodeName(n), this.codeToNodeState(n)]);
                    }
                    ringId++;
                }
            }
            if (rg.hasOwnProperty("Node")) {
                for (let n of rg["Node"]) {
                    res.push(["", rgName, ringId, n, this.getNodeName(n), this.codeToNodeState(n)]);
                    ringId++;
                }
            }
            rgId++;
        }
        return res;
    }

    onThisLoaded(data) {
        if (data && data.hasOwnProperty("StateStorageConfig")) {
            $("#state-storage-error").empty();
            if (data["StateStorageConfig"].hasOwnProperty("NodesState")) {
                for (let state of data["StateStorageConfig"]["NodesState"]) {
                    this.states[state.NodeId] = state;
                }
            }
            this.renderPVEntry(this.stateStorageConfig, this.prepareData(data["StateStorageConfig"]["StateStorageConfig"]));
            this.renderPVEntry(this.boardConfig, this.prepareData(data["StateStorageConfig"]["StateStorageBoardConfig"]));
            this.renderPVEntry(this.schemeBoardConfig, this.prepareData(data["StateStorageConfig"]["SchemeBoardConfig"]));
        } else {
            $("#state-storage-error").text("Error while updating state");
        }
        setTimeout(this.loadThis.bind(this), this.fetchInterval);
        this.restartAnimation($("#state-storage-anim"));
    }

    loadThis() {
        this.cleanup();
        $.ajax({
            url: '/actors/nodewarden?page=distconf',
            type: "POST",
            data: '{"GetStateStorageConfig": {"NodesStatus": true}}',
            dataType: "json",
            contentType: "application/json; charset=utf-8",
            success: this.onThisLoaded.bind(this)
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

        this.loadThis();
    }
}

var stateStorageState;

function initStateStorageTab() {
    stateStorageState = new StateStorageState();
}
