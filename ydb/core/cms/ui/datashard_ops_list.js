'use strict';

var OperationsListState = {
    fetchInterval: 5000,
    retryInterval: 5000,
    loading: false,
    scheduledLoad: false,
    ops: new Map(),
};

class OperationInfo {
    constructor(info) {
        var trHtml = this._makeTrHtml(info);
        $(trHtml).appendTo($('#ds-ops-list-body'));

        this.txId = info.TxId;
    }

    update(info) {
        $('#ds-ops-list-row-' + this.txId).replaceWith(this._makeTrHtml(info));
    }

    remove() {
        $('#ds-ops-list-row-' + this.txId).remove();
    }

    _makeTrHtml(info) {
        return `
            <tr id="ds-ops-list-row-${info.TxId}">
                <td><a href="#page=ds-op&op=${info.TxId}" onclick="showOp(${info.TxId})">${info.TxId}</a></td>
                <td>${info.Step}</td>
                <td>${info.Kind}</td>
                <td>${info.IsImmediate}</td>
                <td>${info.IsReadOnly}</td>
                <td>${info.IsWaiting}</td>
                <td>${info.IsExecuting}</td>
                <td>${info.IsCompleted}</td>
                <td>${info.ExecutionUnit}</td>
                <td>${timeToString(info.ReceivedAt)}</td>
            </tr>
        `;
    }
}

function onOperationsListLoaded(data) {
    OperationsListState.loading = false;

    if (data['Status']['Code'] != 'SUCCESS') {
        onOperationsListFailed(data);
        return;
    }

    $('#ds-ops-list-error').html('');

    var ops = new Set();
    if (data.Operations) {
        for (var op of data.Operations) {
            ops.add(op.TxId);
            if (OperationsListState.ops.has(op.TxId)) {
                OperationsListState.ops.get(op.TxId).update(op);
            } else {
                OperationsListState.ops.set(op.TxId, new OperationInfo(op));
            }
        }
    }

    var toRemove = [];
    for (var txId of OperationsListState.ops.keys()) {
        if (!ops.has(txId))
            toRemove.push(txId);
    }

    for (var txId of toRemove) {
        OperationsListState.ops.get(txId).remove();
        OperationsListState.ops.delete(txId);
    }

    $('#ds-ops-list-table').trigger('update', [true]);

    scheduleLoadOperationsList(OperationsListState.fetchInterval);
}

function onOperationsListFailed(data) {
    OperationsListState.loading = false;

    if (data && data['Status'] && data['Status']['Issues'])
        $('#ds-ops-list-error').html(JSON.stringify(data['Status']['Issues']));
    else
        $('#ds-ops-list-error').html("Cannot get operations list");
    scheduleLoadOperationsList(OperationsListState.retryInterval);
}

function loadOperationsList() {
    if (OperationsListState.loading)
        return;

    if (!$('#ds-ops-list-link').hasClass('active'))
        return;

    OperationsListState.loading = true;
    var url = '../cms/api/datashard/json/listoperations?tabletid=' + TabletId;
    $.get(url).done(onOperationsListLoaded).fail(onOperationsListFailed);
}

function scheduledLoadOperationsList() {
    OperationsListState.scheduledLoad = false;
    loadOperationsList();
}

function scheduleLoadOperationsList(timeout) {
    if (OperationsListState.scheduledLoad)
        return;
    OperationsListState.scheduledLoad = true;
    setTimeout(scheduledLoadOperationsList, timeout);
}

function initOperationsListTab() {
    $(document).on('shown.bs.tab', '', function(e) {
        if (e.target.id == 'ds-ops-list-link') {
            $('#ds-ops-list-table').tablesorter({
                theme: 'blue',
                sortList: [[0,0]],
                widgets : ['zebra', 'filter'],
            });
            scheduleLoadOperationsList(0);
        }
    });

    loadOperationsList();
}
