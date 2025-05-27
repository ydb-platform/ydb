'use strict';

var ReadSetsState = {
    fetchInterval: 5000,
    retryInterval: 5000,
    loading: false,
    scheduledLoad: false,
    rs: new Map(),
    acks: new Map(),
    delayedAcks: new Map(),
    expectations: new Map(),
    pipes: new Map(),
};

function makeRSKey(info) {
    return `${info.TxId}-${info.Origin}-${info.Source}-${info.Destination}-${info.SeqNo}`;
}

function makeRSExpectationKey(info) {
    return `${info.TxId}-${info.Source}`;
}

function makeRSPipeKey(info) {
    return `${info.Destination}`;
}

class RSInfo {
    constructor(info) {
        this.key = makeRSKey(info);

        var trHtml = this._makeTrHtml(info);
        $(trHtml).appendTo($('#ds-out-rs-body'));
    }

    update(info) {
        $('#ds-out-rs-row-' + this.key).replaceWith(this._makeTrHtml(info));
    }

    remove() {
        $('#ds-out-rs-row-' + this.key).remove();
    }

    _makeTrHtml(info) {
        return `
            <tr id="ds-out-rs-row-${this.key}">
                <td><a href="#page=ds-op&op=${info.TxId}" onclick="showOp(${info.TxId})">${info.TxId}</a></td>
                <td><a href="app?TabletID=${info.Origin}">${info.Origin}</a></td>
                <td><a href="app?TabletID=${info.Source}">${info.Source}</a></td>
                <td><a href="app?TabletID=${info.Destination}">${info.Destination}</a></td>
                <td>${info.SeqNo}</td>
            </tr>
        `;
    }
}

class RSAckInfo {
    constructor(info,body) {
        this.key = makeRSKey(info);

        var trHtml = this._makeTrHtml(info);
        $(trHtml).appendTo($('#' + body));
    }

    update(info) {
        $('#ds-out-rs-ack-row-' + this.key).replaceWith(this._makeTrHtml(info));
    }

    remove() {
        $('#ds-out-rs-ack-row-' + this.key).remove();
    }

    _makeTrHtml(info) {
        return `
            <tr id="ds-out-rs-ack-row-${this.key}">
                <td><a href="#page=ds-op&op=${info.TxId}" onclick="showOp(${info.TxId})">${info.TxId}</a></td>
                <td><a href="app?TabletID=${info.Origin}">${info.Origin}</a></td>
                <td><a href="app?TabletID=${info.Source}">${info.Source}</a></td>
                <td><a href="app?TabletID=${info.Destination}">${info.Destination}</a></td>
                <td>${info.SeqNo}</td>
            </tr>
        `;
    }
}

class RSExpectationInfo {
    constructor(info, body) {
        this.key = makeRSExpectationKey(info);

        var trHtml = this._makeTrHtml(info);
        $(trHtml).appendTo($('#' + body));
    }

    update(info) {
        $('#ds-rs-expectation-row-' + this.key).replaceWith(this._makeTrHtml(info));
    }

    remove() {
        $('#ds-rs-expectation-row-' + this.key).remove();
    }

    _makeTrHtml(info) {
        return `
            <tr id="ds-rs-expectation-row-${this.key}">
                <td>${info.TxId}</td>
                <td>${info.Step}</td>
                <td><a href="app?TabletID=${info.Source}">${info.Source}</a></td>
            </tr>
        `;
    }
}

class RSPipeInfo {
    constructor(info, body) {
        this.key = makeRSPipeKey(info);

        var trHtml = this._makeTrHtml(info);
        $(trHtml).appendTo($('#' + body));
    }

    update(info) {
        $('#ds-rs-pipe-row-' + this.key).replaceWith(this._makeTrHtml(info));
    }

    remove() {
        $('#ds-rs-pipe-row-' + this.key).remove();
    }

    _makeTrHtml(info) {
        return `
            <tr id="ds-rs-pipe-row-${this.key}">
                <td><a href="app?TabletID=${info.Destination}">${info.Destination}</a></td>
                <td>${info.OutReadSets}</td>
                <td>${info.Subscribed}</td>
            </tr>
        `;
    }
}

function updateReadSetExpectations(data) {
    var expectations = new Set();
    if (data.Expectations) {
        for (var info of data.Expectations) {
            var key = makeRSExpectationKey(info);
            expectations.add(key);
            if (ReadSetsState.expectations.has(key)) {
                ReadSetsState.expectations.get(key).update(info);
            } else {
                ReadSetsState.expectations.set(key, new RSExpectationInfo(info, 'ds-rs-expectations-body'));
            }
        }
    }

    var toRemove = [];
    for (var key of ReadSetsState.expectations.keys()) {
        if (!expectations.has(key)) {
            toRemove.push(key);
        }
    }

    for (var key of toRemove) {
        ReadSetsState.expectations.get(key).remove();
        ReadSetsState.expectations.delete(key);
    }
}

function updateReadSetPipes(data) {
    var pipes = new Set();
    if (data.Pipes) {
        for (var info of data.Pipes) {
            var key = makeRSPipeKey(info);
            pipes.add(key);
            if (ReadSetsState.pipes.has(key)) {
                ReadSetsState.pipes.get(key).update(info);
            } else {
                ReadSetsState.pipes.set(key, new RSPipeInfo(info, 'ds-rs-pipes-body'));
            }
        }
    }

    var toRemove = [];
    for (var key of ReadSetsState.pipes.keys()) {
        if (!pipes.has(key)) {
            toRemove.push(key);
        }
    }

    for (var key of toRemove) {
        ReadSetsState.pipes.get(key).remove();
        ReadSetsState.pipes.delete(key);
    }
}

function onReadSetsLoaded(data) {
    ReadSetsState.loading = false;

    if (data['Status']['Code'] != 'SUCCESS') {
        onReadSetsFailed(data);
        return;
    }

    $('#ds-rs-error').html('');

    var outRS = new Set();
    if (data.OutReadSets) {
        for (var rs of data.OutReadSets) {
            var key = makeRSKey(rs);
            outRS.add(key);
            if (ReadSetsState.rs.has(key)) {
                ReadSetsState.rs.get(key).update(rs);
            } else {
                ReadSetsState.rs.set(key, new RSInfo(rs));
            }
        }
    }

    var toRemove = [];
    for (var key of ReadSetsState.rs.keys()) {
        if (!outRS.has(key))
            toRemove.push(key);
    }

    for (var key of toRemove) {
        ReadSetsState.rs.get(key).remove();
        ReadSetsState.rs.delete(key);
    }

    var acks = new Set();
    if (data.OutRSAcks) {
        for (var ack of data.OutRSAcks) {
            var key = makeRSKey(ack);
            acks.add(key);
            if (ReadSetsState.acks.has(key)) {
                ReadSetsState.acks.get(key).update(ack);
            } else {
                ReadSetsState.acks.set(key, new RSAckInfo(ack, 'ds-out-rs-ack-body'));
            }
        }
    }

    toRemove = [];
    for (var key of ReadSetsState.acks.keys()) {
        if (!acks.has(key))
            toRemove.push(key);
    }

    for (var key of toRemove) {
        ReadSetsState.acks.get(key).remove();
        ReadSetsState.acks.delete(key);
    }

    var delayedAcks = new Set();
    if (data.DelayedRSAcks) {
        for (var ack of data.DelayedRSAcks) {
            var key = makeRSKey(ack);
            delayedAcks.add(key);
            if (ReadSetsState.delayedAcks.has(key)) {
                ReadSetsState.delayedAcks.get(key).update(ack);
            } else {
                ReadSetsState.delayedAcks.set(key, new RSAckInfo(ack, 'ds-delayed-ack-body'));
            }
        }
    }

    toRemove = [];
    for (var key of ReadSetsState.delayedAcks.keys()) {
        if (!delayedAcks.has(key))
            toRemove.push(key);
    }

    for (var key of toRemove) {
        ReadSetsState.delayedAcks.get(key).remove();
        ReadSetsState.delayedAcks.delete(key);
    }

    updateReadSetExpectations(data);
    updateReadSetPipes(data);

    $('#ds-out-rs-table').trigger('update', [true]);
    $('#ds-out-rs-ack-table').trigger('update', [true]);
    $('#ds-delayed-ack-table').trigger('update', [true]);
    $('#ds-rs-expectations-table').trigger('update', [true]);
    $('#ds-rs-pipes-table').trigger('update', [true]);

    scheduleLoadReadSets(ReadSetsState.fetchInterval);
}

function onReadSetsFailed(data) {
    ReadSetsState.loading = false;

    if (data && data['Status'] && data['Status']['Issues'])
        $('#ds-rs-error').html(JSON.stringify(data['Status']['Issues']));
    else
        $('#ds-rs-error').html("Cannot get read sets info");
    scheduleLoadReadSets(ReadSetsState.retryInterval);
}

function loadReadSets() {
    if (ReadSetsState.loading)
        return;

    if (!$('#ds-rs-link').hasClass('active'))
        return;

    ReadSetsState.loading = true;
    var url = '../cms/api/datashard/json/getrsinfo?tabletid=' + TabletId;
    $.get(url).done(onReadSetsLoaded).fail(onReadSetsFailed);
}

function scheduledLoadReadSets() {
    ReadSetsState.scheduledLoad = false;
    loadReadSets();
}

function scheduleLoadReadSets(timeout) {
    if (ReadSetsState.scheduledLoad)
        return;
    ReadSetsState.scheduledLoad = true;
    setTimeout(scheduledLoadReadSets, timeout);
}

function initReadSetsTab() {
    $(document).on('shown.bs.tab', '', function(e) {
        if (e.target.id == 'ds-rs-link') {
            $('#ds-out-rs-table').tablesorter({
                theme: 'blue',
                sortList: [[0,0]],
                widgets : ['zebra', 'filter'],
            });
            $('#ds-out-rs-ack-table').tablesorter({
                theme: 'blue',
                sortList: [[0,0]],
                widgets : ['zebra', 'filter'],
            });
            $('#ds-delayed-ack-table').tablesorter({
                theme: 'blue',
                sortList: [[0,0]],
                widgets : ['zebra', 'filter'],
            });
            $('#ds-rs-expectations-table').tablesorter({
                theme: 'blue',
                sortList: [[0,0]],
                widgets : ['zebra', 'filter'],
            });
            $('#ds-rs-pipes-table').tablesorter({
                theme: 'blue',
                sortList: [[0,0]],
                widgets : ['zebra', 'filter'],
            });
            scheduleLoadReadSets(0);
        }
    });

    loadReadSets();
}
