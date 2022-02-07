'use strict';

var OperationState = {
    fetchInterval: 5000,
    retryInterval: 5000,
    sinkStateFetchInterval: 5000,
    sinkStateRetryInterval: 5000,
    scanStateFetchInterval: 5000,
    scanStateRetryInterval: 5000,
    streamStateFetchInterval: 5000,
    streamStateRetryInterval: 5000,
    loading: false,
    scheduledLoad: false,
    id: undefined,
    op: undefined,
};

function showOp(id) {
    setHashParam('op', id);

    if (!$('#ds-op-link').hasClass('active')) {
        $('#ds-op-link').tab('show')
    } else {
        $('#ds-op-error').html('');
        loadOperation(id);
    }
}

class Operation {
    constructor(info) {
        this.id = info.BasicInfo.TxId;
        this._createView();
        this.updateBasicInfo(info);
    }

    updateBasicInfo(info) {
        this.info = info;

        $('#ds-op-info-id').text(info.BasicInfo.TxId);
        $('#ds-op-info-step').text(info.BasicInfo.Step);
        $('#ds-op-info-kind').text(info.BasicInfo.Kind);
        $('#ds-op-info-received-at').text(timeToString(info.BasicInfo.ReceivedAt));
        $('#ds-op-info-min-step').text(info.BasicInfo.MinStep);
        $('#ds-op-info-max-step').text(info.BasicInfo.MaxStep);
        $('#ds-op-info-flags').text(info.BasicInfo.Flags.join(", "));

        // Fill execution plan.
        var profile = new Map();
        if (info.ExecutionProfile.UnitProfiles) {
            for (var unit of info.ExecutionProfile.UnitProfiles) {
                var total = unit.WaitTime + unit.ExecuteTime + unit.CommitTime
                    + unit.CompleteTime;

                var p = {};
                p.total = durationToStringMs(total);
                p.wait = durationToStringMs(unit.WaitTime);
                p.execute = durationToStringMs(unit.ExecuteTime);
                p.commit = durationToStringMs(unit.CommitTime);
                p.complete = durationToStringMs(unit.CompleteTime);
                p.show = $('#ds-op-unit-ptofile-' + unit.UnitKind).hasClass('show');
                p.count = unit.ExecuteCount;

                profile.set(unit.UnitKind, p);
            }
        }

        $('#ds-op-plan-body').html('');
        var cur = info.ExecutionPlan.CurrentUnit;
        for (var i = 0; i < info.ExecutionPlan.Units.length; ++i) {
            var unit = info.ExecutionPlan.Units[i];
            var unitCl = i < cur ? "ds-plan-executed" : (i == cur ? "ds-plan-executing" : "ds-plan-to-execute");
            var p = profile.has(unit) ? profile.get(unit) : {};
            var trHtml = `
                <tr class="ds-info">
                  <td class="ds-info va-top ${unitCl}">${unit}</td>
                  <td>
                    <a data-toggle="collapse" href="#ds-op-unit-ptofile-${unit}">${p.total}</a><br>
                    <div class="collapse ${p.show ? "show" : ""}" id="ds-op-unit-ptofile-${unit}">
                      Wait: ${p.wait}<br>
                      Execute (${p.count}): ${p.execute}</br>
                      Commit: ${p.commit}</br>
                      Complete: ${p.complete}</br>
                    </div>
                  </td>
                </tr>
            `;
            $(trHtml).appendTo($('#ds-op-plan-body'));
        }

        $('#ds-op-dependencies-body').html('');
        $('#ds-op-dependents-body').html('');
        if (info.Dependencies) {
            this._addDeps(info.Dependencies.Dependencies, $('#ds-op-dependencies-body'));
            this._addDeps(info.Dependencies.Dependents, $('#ds-op-dependents-body'));
        }

        if (info.InputData && info.InputData.InputRS) {
            $('#ds-op-input-rs').show();
            $('#ds-op-input-rs-body').html('');

            for (var rs of info.InputData.InputRS) {
                var trHtml = `
                    <tr class="ds-info">
                      <td class="ds-info"><a href="app?TabletID=${rs.From}">${rs.From}</td>
                      <td class="ds-info">${rs.Received} received</td>
                    </tr>
                `;
                $(trHtml).appendTo($('#ds-op-input-rs-body'));
            }
            var trHtml = `
                <tr class="ds-info">
                  <td class="ds-info">Total remains</td>
                  <td class="ds-info">${info.InputData.RemainedInputRS}</td>
                </tr>
            `;
            $(trHtml).appendTo($('#ds-op-input-rs-body'));
        } else {
            $('#ds-op-input-rs').hide();
        }

        if (info.ReadTableState) {
            $('#ds-op-readtable').show();

            $('#ds-op-readtable-table').text(info.ReadTableState.TableId);
            $('#ds-op-readtable-snapshot').text(info.ReadTableState.SnapshotId);
            $('#ds-op-readtable-scan-task').text(info.ReadTableState.ScanTaskId);
            $('#ds-op-readtable-sink').text(info.ReadTableState.SinkActor);
            $('#ds-op-readtable-scan-actor').text(info.ReadTableState.ScanActor);
        } else {
            $('#ds-op-readtable').hide();
        }
    }


    remove() {
        $('#ds-op-info').html('');
    }

    onTabShown() {
        if (this.requestedSinkState)
            this._scheduleLoadSinkState();
        if (this.requestedScanState)
            this._scheduleLoadScanState();
        if (this.requestedStreamState)
            this._scheduleLoadStreamState();
    }

    requestScanState() {
        this.requestedScanState = true;
        $('#ds-op-scan-state-div').show();
        this._scheduleLoadScanState(0);
    }

    requestSinkState() {
        this.requestedSinkState = true;
        $('#ds-op-sink-state-div').show();
        this._scheduleLoadSinkState(0);
    }

    requestStreamState() {
        this.requestedStreamState = true;
        $('#ds-op-stream-state-div').show();
        this._scheduleLoadStreamState(0);
    }

    _addDeps(deps, body) {
        if (!deps) {
            var trHtml = `
                <tr class="ds-info">
                  <td class="ds-info">None</td>
                </tr>
            `;
            $(trHtml).appendTo(body);
            return;
        }

        for (var dep of deps) {
            var trHtml = `
                <tr class="ds-info">
                  <td class="ds-info"><a href="#page=ds-op&op=${dep.Target}" onclick="showOp(${dep.Target})">${dep.Target}</a></td>
                  <td class="ds-info">${dep.Types.join(", ")}</td>
                </tr>
            `;
            $(trHtml).appendTo(body);
        }
    }

    _createView() {
        var html = `
            <div class="row">
              <div class="col">
                <table class="ds-info">
                  <caption class="ds-info">Basic info</caption>
                  <tbody class="ds-info">
                    <tr class="ds-info">
                      <td class="ds-info">TxId</td>
                      <td class="ds-info" id="ds-op-info-id"></td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Step</td>
                      <td class="ds-info" id="ds-op-info-step"></td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Kind</td>
                      <td class="ds-info" id="ds-op-info-kind"></td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">ReceivedAt</td>
                      <td class="ds-info" id="ds-op-info-received-at"></td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">MinStep</td>
                      <td class="ds-info" id="ds-op-info-min-step"></td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">MaxStep</td>
                      <td class="ds-info" id="ds-op-info-max-step"></td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Flags</td>
                      <td class="ds-info" id="ds-op-info-flags"></td>
                    </tr>
                  </tbody>
                </table>
                <table class="ds-info">
                  <caption class="ds-info">Execution plan</caption>
                  <tbody class="ds-info" id="ds-op-plan-body">
                  </tbody>
                </table>
              </div>
              <div class="col">
                <table class="ds-info" id="ds-op-input-rs">
                  <caption class="ds-info">Input read sets</caption>
                  <tbody class="ds-info" id="ds-op-input-rs-body">
                  </tbody>
                </table>
                <table class="ds-info">
                  <caption class="ds-info">Dependencies</caption>
                  <tbody class="ds-info" id="ds-op-dependencies-body">
                  </tbody>
                </table>
                <table class="ds-info">
                  <caption class="ds-info">Dependent operations</caption>
                  <tbody class="ds-info" id="ds-op-dependents-body">
                  </tbody>
                </table>
                <table class="ds-info" id="ds-op-readtable">
                  <caption class="ds-info">ReadTable state</caption>
                  <tbody class="ds-info">
                    <tr class="ds-info">
                      <td class="ds-info">Table ID</td>
                      <td class="ds-info" id="ds-op-readtable-table"></td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Snapshot ID</td>
                      <td class="ds-info" id="ds-op-readtable-snapshot"></td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Scan task ID</td>
                      <td class="ds-info" id="ds-op-readtable-scan-task"></td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Sink actor</td>
                      <td class="ds-info" id="ds-op-readtable-sink"></td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Scan actor</td>
                      <td class="ds-info" id="ds-op-readtable-scan-actor"></td>
                    </tr>
                  </tbody>
                  <tfoot class="ds-info">
                    <tr class="ds-info">
                      <td  class="ds-info" colspan="2">
                        <button class="btn btn-primary btn-sm" onclick="requestScanState(${this.id})">Get scan state</button>
                        <button class="btn btn-primary btn-sm" onclick="requestSinkState(${this.id})">Get proxy state</button>
                        <button class="btn btn-primary btn-sm" onclick="requestStreamState(${this.id})">Get stream state</button>
                      </td>
                    </tr>
                  </tfoot>
                </table>
              </div>
            </div>
            <div class="row">
              <div class="col" style="display: none;" id="ds-op-scan-state-div">
                <div class="error" id="ds-op-scan-state-error"></div>
                <table class="ds-info" id="ds-op-readtable">
                  <caption class="ds-info">Scan actor state</caption>
                  <tbody class="ds-info">
                    <tr class="ds-info">
                      <td class="ds-info">Message quota</td>
                      <td class="ds-info" id="ds-op-scan-quota">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Message size limit</td>
                      <td class="ds-info" id="ds-op-scan-size-limit">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Message rows limit</td>
                      <td class="ds-info" id="ds-op-scan-rows-limit">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Pending acks</td>
                      <td class="ds-info" id="ds-op-scan-pending-acks">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Result size</td>
                      <td class="ds-info" id="ds-op-scan-res-size">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Result rows</td>
                      <td class="ds-info" id="ds-op-scan-res-rows">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Has upper border</td>
                      <td class="ds-info" id="ds-op-scan-upper">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Finished</td>
                      <td class="ds-info" id="ds-op-scan-finished">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Error</td>
                      <td class="ds-info" id="ds-op-scan-error">Loading...</td>
                    </tr>
                 </tbody>
                </table>
              </div>
              <div class="col" style="display: none;" id="ds-op-sink-state-div">
                <div class="error" id="ds-op-sink-state-error"></div>
                <table class="ds-info" id="ds-op-readtable">
                  <caption class="ds-info">Proxy actor state</caption>
                  <tbody class="ds-info">
                    <tr class="ds-info">
                      <td class="ds-info">TxId</td>
                      <td class="ds-info" id="ds-op-sink-txid">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Accepted</td>
                      <td class="ds-info" id="ds-op-sink-accepted">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">ResolveStarted</td>
                      <td class="ds-info" id="ds-op-sink-resolve-started">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Resolved</td>
                      <td class="ds-info" id="ds-op-sink-resolved">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Prepared</td>
                      <td class="ds-info" id="ds-op-sink-prepared">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Planned</td>
                      <td class="ds-info" id="ds-op-sink-planned">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Timeout</td>
                      <td class="ds-info" id="ds-op-sink-timeout">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Coordinator</td>
                      <td class="ds-info" id="ds-op-sink-coordinator">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Request source</td>
                      <td class="ds-info" id="ds-op-sink-source">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Request version</td>
                      <td class="ds-info" id="ds-op-sink-request-version">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Response version</td>
                      <td class="ds-info" id="ds-op-sink-response-version">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info va-top">Clearance requests</td>
                      <td class="ds-info" id="ds-op-sink-clearance">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info va-top">Quota requests</td>
                      <td class="ds-info" id="ds-op-sink-quota">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info va-top">Streaming shards</td>
                      <td class="ds-info" id="ds-op-sink-streaming">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info va-top">Shards queue</td>
                      <td class="ds-info" id="ds-op-sink-queue">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Ordered</td>
                      <td class="ds-info" id="ds-op-sink-ordered">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Limited</td>
                      <td class="ds-info" id="ds-op-sink-limited">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Rows remain</td>
                      <td class="ds-info" id="ds-op-sink-remain">Loading...</td>
                    </tr>
                 </tbody>
                </table>
              </div>
              <div class="col" style="display: none;" id="ds-op-stream-state-div">
                <div class="error" id="ds-op-stream-state-error"></div>
                <table class="ds-info" id="ds-op-readtable">
                  <caption class="ds-info">Stream actor state</caption>
                  <tbody class="ds-info">
                    <tr class="ds-info">
                      <td class="ds-info">Ready</td>
                      <td class="ds-info" id="ds-op-stream-ready">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Finished</td>
                      <td class="ds-info" id="ds-op-stream-finished">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Responses in queue</td>
                      <td class="ds-info" id="ds-op-stream-queue">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info va-top">Quota requests</td>
                      <td class="ds-info" id="ds-op-stream-requests">Loading...</td>
                    </tr>
                    <tr class="ds-info va-top">
                      <td class="ds-info">Shard quotas</td>
                      <td class="ds-info" id="ds-op-stream-quotas">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Quota limit</td>
                      <td class="ds-info" id="ds-op-stream-limit">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Quota reserved</td>
                      <td class="ds-info" id="ds-op-stream-reserved">Loading...</td>
                    </tr>
                    <tr class="ds-info va-top">
                      <td class="ds-info">Released shards</td>
                      <td class="ds-info" id="ds-op-stream-released">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Client stream timeout</td>
                      <td class="ds-info" id="ds-op-stream-client-timeout">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Server stream timeout</td>
                      <td class="ds-info" id="ds-op-stream-server-timeout">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Last data stream</td>
                      <td class="ds-info" id="ds-op-stream-last-stream">Loading...</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">Last status</td>
                      <td class="ds-info" id="ds-op-stream-last-status">Loading...</td>
                    </tr>
                 </tbody>
                </table>
                <div id="ds-op-stream-state"></div>
              </div>
            </div>
        `;
        $(html).appendTo($('#ds-op-info'));
    }

    _maybeGetScanStateLink(state) {
        if (state.ScanActor)
            return `(<a onclick="requestScanState(${this.id})">get state</a>)`;
        return "";
    }

    _updateSinkInfo(info) {
        $('#ds-op-sink-txid').html(info.TxId);
        $('#ds-op-sink-accepted').html(timeToString(info.WallClockAccepted));
        $('#ds-op-sink-resolve-started').html(timeToString(info.WallClockResolveStarted));
        $('#ds-op-sink-resolved').html(timeToString(info.WallClockResolved));
        $('#ds-op-sink-prepared').html(timeToString(info.WallClockPrepared));
        $('#ds-op-sink-planned').html(timeToString(info.WallClockPlanned));
        $('#ds-op-sink-timeout').html(this._durationToString(info.ExecTimeoutPeriod));
        $('#ds-op-sink-coordinator').html(`<a href="../tablets?TabletID=${info.SelectedCoordinator}">${info.SelectedCoordinator}</a>`);
        $('#ds-op-sink-source').html(info.RequestSource);
        $('#ds-op-sink-request-version').html(info.RequestVersion);
        $('#ds-op-sink-response-version').html(info.ResponseVersion);
        $('#ds-op-sink-clearance').html(this._makeShardsList(info.ClearanceRequests));
        $('#ds-op-sink-quota').html(this._makeShardsList(info.QuotaRequests));
        $('#ds-op-sink-streaming').html(this._makeShardsList(info.StreamingRequests));
        $('#ds-op-sink-queue').html(this._makeShardsList(info.ShardsQueue));
        $('#ds-op-sink-ordered').html(info.Ordered.toString());
        $('#ds-op-sink-limited').html(info.RowsLimited.toString());
        $('#ds-op-sink-remain').html(info.RowsRemain);
    }

    _updateScanInfo(info) {
        $('#ds-op-scan-quota').html(info.MessageQuota);
        $('#ds-op-scan-size-limit').html(info.MessageSizeLimit);
        $('#ds-op-scan-rows-limit').html(info.RowsLimit);
        $('#ds-op-scan-pending-acks').html(info.PendingAcks);
        $('#ds-op-scan-res-size').html(info.ResultSize);
        $('#ds-op-scan-res-rows').html(info.ResultRows);
        $('#ds-op-scan-upper').html(info.HasUpperBorder.toString());
        $('#ds-op-scan-finished').html(info.Finished.toString());
        $('#ds-op-scan-error').html(info.Error);
    }

    _updateStreamInfo(info) {
        $('#ds-op-stream-ready').html(info.Ready === undefined ? 'unknown' : info.Ready.toString());
        $('#ds-op-stream-finished').html(info.Finished === undefined ? 'unknown' : info.Finished.toString());
        $('#ds-op-stream-queue').html(info.ResponseQueueSize);
        $('#ds-op-stream-requests').html(this._makeShardsList(info.QuotaRequests));
        $('#ds-op-stream-quotas').html(this._makeShardQuotasList(info.ShardQuotas));
        $('#ds-op-stream-limit').html(info.QuotaLimit);
        $('#ds-op-stream-reserved').html(info.QuotaReserved);
        $('#ds-op-stream-released').html(this._makeShardsList(info.ReleasedShards));
        $('#ds-op-stream-client-timeout').html(this._durationToString(info.InactiveClientTimeout));
        $('#ds-op-stream-server-timeout').html(this._durationToString(info.InactiveServerTimeout));
        $('#ds-op-stream-last-stream').html(timeToString(info.LastDataStreamTimestamp));
        $('#ds-op-stream-last-status').html(timeToString(info.LastStatusTimestamp));
    }

    _makeShardsList(shards) {
        if (!shards)
            return '';

        var res = '';
        for (var shard of shards) {
            res += `<a href="app?TabletID=${shard.Id}#page=ds-op&op=${this.id}">${shard.Id}</a><br>`;
        }

        return res;
    }

    _makeShardQuotasList(quotas) {
        if (!quotas)
            return '';

        var res = '';
        for (var quota of quotas) {
            res += `<a href="app?TabletID=${quota.ShardId}#page=ds-op&op=${this.id}">${quota.ShardId} (${quota.Quota})</a><br>`;
        }

        return res;
    }

    // Load sink state
    _onSinkStateLoaded(data) {
        if (OperationState.id != this.id)
            return;

        this.loadingSinkState = false;

        if (data['Status']['Code'] != 'SUCCESS') {
            this._onSinkStateFailed(data);
            return;
        }

        $('#ds-op-sink-state-error').html('');

        this._updateSinkInfo(data);

        this._scheduleLoadSinkState(OperationState.sinkStateFetchInterval);
    }

    _onSinkStateFailed(data) {
        if (OperationState.id != this.id)
            return;

        this.loadingSinkState = false;

        if (data && data['Status'] && data['Status']['Issues'])
            $('#ds-op-sink-state-error').html(JSON.stringify(data['Status']['Issues']));
        else
            $('#ds-op-sink-state-error').html("Cannot load proxy actor state " + this.id);
        this._scheduleLoadSinkState(OperationState.sinkStateRetryInterval);
    }

    _loadSinkState() {
        if (OperationState.id != this.id)
            return;

        if (this.loadingSinkState)
            return;

        if (!$('#ds-op-link').hasClass('active'))
            return;

        this.loadingSinkState = true;
        var _this = this;
        var url = '../cms/api/datashard/json/getreadtablesinkstate?tabletid=' + TabletId
            + '&opid=' + this.id;
        $.get(url)
            .done(function(data) { _this._onSinkStateLoaded(data); })
            .fail(function(data) { _this._onSinkStateFailed(data); });
    }

    _scheduleLoadSinkState(timeout) {
        if (this.scheduledLoadSinkState)
            return;

        this.scheduledLoadSinkState = true;

        var _this = this;
        setTimeout(function() {
            _this.scheduledLoadSinkState = false;
            _this._loadSinkState();
        }, timeout);
    }

    // Load scan state
    _onScanStateLoaded(data) {
        if (OperationState.id != this.id)
            return;

        this.loadingScanState = false;

        if (data['Status']['Code'] != 'SUCCESS') {
            this._onScanStateFailed(data);
            return;
        }

        $('#ds-op-scan-state-error').html('');

        this._updateScanInfo(data);

        this._scheduleLoadScanState(OperationState.scanStateFetchInterval);
    }

    _onScanStateFailed(data) {
        if (OperationState.id != this.id)
            return;

        this.loadingScanState = false;

        if (data && data['Status'] && data['Status']['Issues'])
            $('#ds-op-scan-state-error').html(JSON.stringify(data['Status']['Issues']));
        else
            $('#ds-op-scan-state-error').html("Cannot load scan actor state " + this.id);
        this._scheduleLoadScanState(OperationState.scanStateRetryInterval);
    }

    _loadScanState() {
        if (OperationState.id != this.id)
            return;

        if (this.loadingScanState)
            return;

        if (!$('#ds-op-link').hasClass('active'))
            return;

        this.loadingScanState = true;
        var _this = this;
        var url = '../cms/api/datashard/json/getreadtablescanstate?tabletid=' + TabletId
            + '&opid=' + this.id;
        $.get(url)
            .done(function(data) { _this._onScanStateLoaded(data); })
            .fail(function(data) { _this._onScanStateFailed(data); });
    }

    _scheduleLoadScanState(timeout) {
        if (this.scheduledLoadScanState)
            return;

        this.scheduledLoadScanState = true;

        var _this = this;
        setTimeout(function() {
            _this.scheduledLoadScanState = false;
            _this._loadScanState();
        }, timeout);
    }

    // Load stream state
    _onStreamStateLoaded(data) {
        if (OperationState.id != this.id)
            return;

        this.loadingStreamState = false;

        if (data['Status']['Code'] != 'SUCCESS') {
            this._onStreamStateFailed(data);
            return;
        }

        $('#ds-op-stream-state-error').html('');

        this._updateStreamInfo(data);

        this._scheduleLoadStreamState(OperationState.streamStateFetchInterval);
    }

    _onStreamStateFailed(data) {
        if (OperationState.id != this.id)
            return;

        this.loadingStreamState = false;

        if (data && data['Status'] && data['Status']['Issues'])
            $('#ds-op-stream-state-error').html(JSON.stringify(data['Status']['Issues']));
        else
            $('#ds-op-stream-state-error').html("Cannot load stream actor state " + this.id);
        this._scheduleLoadStreamState(OperationState.streamStateRetryInterval);
    }

    _loadStreamState() {
        if (OperationState.id != this.id)
            return;

        if (this.loadingStreamState)
            return;

        if (!$('#ds-op-link').hasClass('active'))
            return;

        this.loadingStreamState = true;
        var _this = this;
        var url = '../cms/api/datashard/json/getreadtablestreamstate?tabletid=' + TabletId
            + '&opid=' + this.id;
        $.get(url)
            .done(function(data) { _this._onStreamStateLoaded(data); })
            .fail(function(data) { _this._onStreamStateFailed(data); });
    }

    _scheduleLoadStreamState(timeout) {
        if (this.scheduledLoadStreamState)
            return;

        this.scheduledLoadStreamState = true;

        var _this = this;
        setTimeout(function() {
            _this.scheduledLoadStreamState = false;
            _this._loadStreamState();
        }, timeout);
    }

    _durationToString(val) {
        var print = function(v) {
            if (v >= 10)
                return v.toString();
            return '0' + v;
        };

        val = val / 1000000;
        var hour = parseInt(val / 3600);
        var min = parseInt(val % 3600 / 60);
        var sec = parseInt(val % 60);
        return `${hour}:${print(min)}:${print(sec)}`;
    }
}

function requestSinkState(id) {
    if (OperationState.op && OperationState.op.id == id)
        OperationState.op.requestSinkState();
}

function requestScanState(id) {
    if (OperationState.op && OperationState.op.id == id)
        OperationState.op.requestScanState();
}

function requestStreamState(id) {
    if (OperationState.op && OperationState.op.id == id)
        OperationState.op.requestStreamState();
}

function onOperationLoaded(id, data) {
    if (OperationState.id != id)
        return;

    OperationState.loading = false;

    if (data['Status']['Code'] != 'SUCCESS') {
        onOperationFailed(id, data);
        return;
    }

    $('#ds-op-error').html('');

    if (OperationState.op) {
        if (OperationState.op.id != id) {
            OperationState.op.remove();
            OperationState.op = new Operation(data);
        } else {
            OperationState.op.updateBasicInfo(data);
        }
    } else {
        OperationState.op = new Operation(data);
    }

    scheduleLoadOperation(OperationState.fetchInterval);
}

function onOperationFailed(id, data) {
    if (OperationState.id != id)
        return;

    OperationState.loading = false;

    if (data && data['Status'] && data['Status']['Issues'])
        $('#ds-op-error').html(JSON.stringify(data['Status']['Issues']));
    else {
        var error = "Cannot load operation " + id;
        if (data && data['Status'])
            error += ": " + data['Status']['Code'];
        $('#ds-op-error').html(error);
    }

    if (data && data['Status'] && data['Status']['Code'] == 'NOT_FOUND')
        return;

    scheduleLoadOperation(OperationState.retryInterval);
}

function loadOperation(id) {
    if (OperationState.id != id) {
        if (Operation.op) {
            Operaiton.op.remove();
            Operaiton.op = undefined;
        }
        OperationState.id = id;
        OperationState.loading = false;
    } else {
        if (OperationState.loading)
            return;
    }

    if (!$('#ds-op-link').hasClass('active'))
        return;

    if (!OperationState.id)
        return;

    OperationState.loading = true;
    var url = '../cms/api/datashard/json/getoperation?tabletid=' + TabletId
        + '&opid=' + OperationState.id;
    $.get(url)
        .done(function(data) { onOperationLoaded(id, data);})
        .fail(function(data) { onOperationFailed(id, data);});
}

function scheduledLoadOperation() {
    OperationState.scheduledLoad = false;
    if (OperationState.id)
        loadOperation(OperationState.id);
}

function scheduleLoadOperation(timeout) {
    if (OperationState.scheduledLoad)
        return;
    OperationState.scheduledLoad = true;
    setTimeout(scheduledLoadOperation, timeout);
}

function initOperationTab() {
    $(document).on('shown.bs.tab', '', function(e) {
        if (Parameters.op)
            OperationState.id = Parameters.op;
        if (OperationState.op)
            OperationState.op.onTabShown();
        scheduleLoadOperation(0);
    });

    $('#ds-op').find('[data-action="show"]').click(function() {
        var id = $('#ds-op-id-input').val();
        if (!id) {
            $('#ds-op-error').html('Operation id is empty');
            return;
        }

        setHashParam('op', id);

        $('#ds-op-error').html('');
        loadOperation(id);
    });
}
