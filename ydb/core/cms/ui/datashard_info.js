'use strict';

var DataShardInfoState = {
    fetchInterval: 5000,
    retryInterval: 5000,
    loadingInfo: false,
    scheduledLoad: false,
};

function onDataShardInfoLoaded(data) {
    DataShardInfoState.loadingInfo = false;

    if (data['Status']['Code'] != 'SUCCESS') {
        onDataShardInfoFailed(data);
        return;
    }

    $('#ds-info-error').html('');

    var info = data.TabletInfo;
    var tables = data.UserTables;
    if (tables) {
        for (var table of tables) {
            var tableInfoHTML = `
                <table class="ds-info">
                  <caption class="ds-info">User table ${table.Name}</caption>
                  <tbody class="ds-info">
                    <tr class="ds-info">
                      <td class="ds-info">Path</td>
                      <td class="ds-info"><a href="../viewer/#page=schema&path=${table.Path}">${table.Path}</a></td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">PathId</td>
                      <td class="ds-info"><a href="app?TabletID=${info.SchemeShard}&Page=PathInfo&PathId=${table.PathId}">${table.PathId}</a></td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">LocalId</td>
                      <td class="ds-info">${table.LocalId}</td>
                    </tr>
                    <tr class="ds-info">
                      <td class="ds-info">SchemaVersion</td>
                      <td class="ds-info">${table.SchemaVersion}</td>
                    </tr>
                  </tbody>
                </table>
            `;
            if (table.Stats) {
                tableInfoHTML += `
                    <table class="ds-info">
                      <caption class="ds-info-table-stats">Metrics</caption>
                      <tbody class="ds-info">
                        <tr class="ds-info">
                          <td class="ds-info">CPU</td>
                          <td class="ds-info">` + table.Metrics.CPU/10000 + `%</td>
                        </tr>
                      </tbody>
                    </table>

                    <table class="ds-info">
                      <caption class="ds-info-table-stats">Statistics</caption>
                      <tbody class="ds-info">
                        <tr class="ds-info">
                          <td class="ds-info">Rows count</td>
                          <td class="ds-info">${table.Stats.RowCount}</td>
                        </tr>
                        <tr class="ds-info">
                          <td class="ds-info">Data size</td>
                          <td class="ds-info">${table.Stats.DataSize}</td>
                        </tr>
                        <tr class="ds-info">
                          <td class="ds-info">Last access time</td>
                          <td class="ds-info">${table.Stats.LastAccessTime}</td>
                        </tr>
                        <tr class="ds-info">
                          <td class="ds-info">Last update time</td>
                          <td class="ds-info">${table.Stats.LastUpdateTime}</td>
                        </tr>
                        <tr class="ds-info">
                          <td class="ds-info">Last stats update time</td>
                          <td class="ds-info">${table.Stats.LastStatsUpdateTime}</td>
                        </tr>
                        <tr class="ds-info">
                          <td class="ds-info">Last stats report time</td>
                          <td class="ds-info">${table.Stats.LastStatsReportTime}</td>
                        </tr>
                      </tbody>
                    </table>
                `;
            }
            $('#user-tables').html(tableInfoHTML);
        }
    }

    if (tables && tables.length == 1) {
        var path = tables[0].Path;
        $('#main-title').text('DataShard ' + TabletId + ' (' + path + ')');
    }

    $('#tablet-info-schemeshard').html('<a href="../tablets?TabletID=' + info.SchemeShard
                                       + '">' + info.SchemeShard + '</a>');
    $('#tablet-info-mediator').html('<a href="../tablets?TabletID=' + info.Mediator
                                    + '">' + info.Mediator + '</a>');
    $('#tablet-info-generation').text(info.Generation);
    $('#tablet-info-role').text(info.IsFollower ? 'Follower' : 'Leader');
    $('#tablet-info-state').text(info.State + (info.IsActive ? ' (active)' : ' (inactive)'));
    $('#tablet-info-shared-blobs').text(info.HasSharedBlobs);
    $('#tablet-info-change-sender').html('<a href="app?TabletID=' + TabletId + '&page=change-sender">Viewer</a>');
    $('#tablet-info-volatile-txs').html(`<a href="app?TabletID=${TabletId}&page=volatile-txs">Viewer</a>`);

    var activities = data.Activities;
    if (activities) {
        $('#activities-last-planned-tx').text(activities.LastPlannedStep
                                              + '.' + activities.LastPlannedTx);
        $('#activities-last-completed-tx').text(activities.LastCompletedStep
                                                + '.' + activities.LastCompletedTx);
        $('#activities-utmost-completed-tx').text(activities.UtmostCompletedStep
                                                  + '.' + activities.UtmostCompletedTx);
        $('#activities-data-tx-complete-lag').text(activities.DataTxCompleteLag);
        $('#activities-scan-tx-complete-lag').text(activities.ScanTxCompleteLag);
        $('#activities-in-fly-planned').text(activities.InFlyPlanned);
        $('#activities-in-fly-immediate').text(activities.InFlyImmediate);
        $('#activities-executing-ops').text(activities.ExecutingOps);
        $('#activities-waiting-ops').text(activities.WaitingOps);
        $('#activities-execute-blockers').text(activities.ExecuteBlockers);
        $('#activities-data-tx-cached').text(activities.DataTxCached);
        $('#activities-out-rs').text(activities.OutReadSets);
        $('#activities-our-rs-acks').text(activities.OutReadSetsAcks);
        $('#activities-delayed-acks').text(activities.DelayedAcks);
        $('#activities-locks').text(activities.Locks);
        $('#activities-broken-locks').text(activities.BrokenLocks);
    }

    var controls = data.Controls;
    $('#tablet-info-controls').html('');
    if (controls && controls.length) {
        for (var control of controls) {
            var tr = `
                <tr class="ds-info">
                  <td class="ds-info">${control.Name}</td>
                  <td class="ds-info">${control.Value}</td>
                </tr>
            `;
            $('#tablet-info-controls').append(tr);
        }
    }

    var pcfg = data.PipelineConfig;
    if (pcfg) {
        $('#pipeline-out-of-order-enabled').text(pcfg.OutOfOrderEnabled);
        $('#pipeline-active-txs-limit').text(pcfg.ActiveTxsLimit);
        $('#pipeline-allow-immediate').text(pcfg.AllowImmediate);
        $('#pipeline-force-online-rw').text(pcfg.ForceOnlineRW);
        $('#pipeline-dirty-online').text(pcfg.DirtyOnline);
        $('#pipeline-dirty-immediate').text(pcfg.DirtyImmediate);
        $('#pipeline-data-tx-cache-limit').text(pcfg.DataTxCacheSize);
    }

    scheduleLoadDataShardInfo(DataShardInfoState.fetchInterval);
}

function onDataShardInfoFailed(data) {
    DataShardInfoState.loadingInfo = false;

    if (data && data['Status'] && data['Status']['Issues'])
        $('#ds-info-error').html(JSON.stringify(data['Status']['Issues']));
    else
        $('#ds-info-error').html("Cannot get DataShard info");
    scheduleLoadDataShardInfo(DataShardInfoState.retryInterval);
}

function loadDataShardInfo() {
    if (DataShardInfoState.loadingInfo)
        return;

    if (!$('#ds-info-link').hasClass('active'))
        return;

    DataShardInfoState.loadingInfo = true;
    var url = '../cms/api/datashard/json/getinfo?tabletid=' + TabletId;
    $.get(url).done(onDataShardInfoLoaded).fail(onDataShardInfoFailed);
}

function scheduledLoadDataShardInfo() {
    DataShardInfoState.scheduledLoad = false;
    loadDataShardInfo();
}

function scheduleLoadDataShardInfo(timeout) {
    if (DataShardInfoState.scheduledLoad)
        return;
    DataShardInfoState.scheduledLoad = true;
    setTimeout(scheduledLoadDataShardInfo, timeout);
}

function initDataShardInfoTab() {
    $(document).on('shown.bs.tab', '', function(e) {
        if (e.target.id == 'ds-info-link')
            scheduleLoadDataShardInfo(0);
    });

    loadDataShardInfo();
}
