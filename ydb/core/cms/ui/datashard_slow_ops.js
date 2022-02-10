'use strict';
var SlowOperationsState = {
    fetchInterval: 30000,
    retryInterval: 5000,
    loading: false,
    scheduledLoad: false,
    ops: new Set(),
};

function onSlowOperationsLoaded(data) {
    SlowOperationsState.loading = false;

    if (data['Status']['Code'] != 'SUCCESS') {
        onSlowOperationsFailed(data);
        return;
    }

    $('#ds-slow-ops-error').html('');

    if (data.Profiles) {
        for (var profile of data.Profiles) {
            var id = profile.BasicInfo.TxId;

            if (SlowOperationsState.ops.has(id))
                continue;

            var step = profile.BasicInfo.Step;
            var kind = profile.BasicInfo.Kind;
            var received = timeToString(profile.BasicInfo.ReceivedAt);
            var immediate = false;
            var readOnly = false;

            for (var flag of profile.BasicInfo.Flags) {
                if (flag == "ReadOnly")
                    readOnly = true;
                else if (flag == "Immediate")
                    immediate = true;
            }

            var fullProfile = "";
            var total = 0;
            for (var unit of profile.ExecutionProfile.UnitProfiles) {
                var unitTotal = unit.WaitTime + unit.ExecuteTime + unit.CommitTime
                    + unit.CompleteTime;
                fullProfile += `
                    <b>${unit.UnitKind}: ${durationToStringMs(unitTotal)}</b><br>
                    Wait: ${durationToStringMs(unit.WaitTime)}<br>
                    Execute: ${durationToStringMs(unit.ExecuteTime)}<br>
                    Commit: ${durationToStringMs(unit.CommitTime)}<br>
                    Complete: ${durationToStringMs(unit.CompleteTime)}<br>
                `;
                total += unitTotal;
            }

            var trHtml = `
                <tr>
                  <td>${id}</td>
                  <td>${step}</td>
                  <td>${kind}</td>
                  <td>${received}</td>
                  <td>${immediate.toString()}</td>
                  <td>${readOnly.toString()}</td>
                  <td>
                    <a data-toggle="collapse" href="#ds-slow-ops-profile-${id}">${durationToStringMs(total)}</a><br>
                    <div class="collapse" id="ds-slow-ops-profile-${id}">
                      ${fullProfile}
                    </div>
                  </td>
                </tr>
            `;
            $(trHtml).appendTo($('#ds-slow-ops-body'));

            SlowOperationsState.ops.add(id);
        }
    }

    $('#ds-slow-ops-table').trigger('update', [true]);

    scheduleLoadSlowOperations(SlowOperationsState.fetchInterval);
}

function onSlowOperationsFailed(data) {
    SlowOperationsState.loading = false;

    if (data && data['Status'] && data['Status']['Issues'])
        $('#ds-slow-ops-error').html(JSON.stringify(data['Status']['Issues']));
    else
        $('#ds-slow-ops-error').html("Cannot get operations list");
    scheduleLoadSlowOperations(SlowOperationsState.retryInterval);
}

function loadSlowOperations() {
    if (SlowOperationsState.loading)
        return;

    if (!$('#ds-slow-ops-link').hasClass('active'))
        return;

    SlowOperationsState.loading = true;
    var url = '../cms/api/datashard/json/getslowopprofiles?tabletid=' + TabletId;
    $.get(url).done(onSlowOperationsLoaded).fail(onSlowOperationsFailed);
}

function scheduledLoadSlowOperations() {
    SlowOperationsState.scheduledLoad = false;
    loadSlowOperations();
}

function scheduleLoadSlowOperations(timeout) {
    if (SlowOperationsState.scheduledLoad)
        return;
    SlowOperationsState.scheduledLoad = true;
    setTimeout(scheduledLoadSlowOperations, timeout);
}

function initSlowOperationsTab() {
    $(document).on('shown.bs.tab', '', function(e) {
        if (e.target.id == 'ds-slow-ops-link') {
            $('#ds-slow-ops-table').tablesorter({
                theme: 'blue',
                sortList: [[0,0]],
                widgets : ['zebra', 'filter'],
            });
            scheduleLoadSlowOperations(0);
        }
    });

    loadSlowOperations();
}
