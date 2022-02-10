'use strict';

var HistogramState = {
    fetchInterval: 300000,
    retryInterval: 5000,
    loading: false,
    scheduledLoad: false,
    ops: new Map(),
};

function onHistogramLoaded(data) {
    HistogramState.loading = false;

    if (data['Status']['Code'] != 'SUCCESS') {
        onHistogramFailed(data);
        return;
    }

    $('#ds-hist-error').html('');

    $('#ds-hist-tables').empty();

    if (data.TableHistograms) {
        for (var table of data.TableHistograms) {
            addTableHistograms(table);
        }
    }

    $('#ds-ops-list-table').trigger('update', [true]);

    scheduleLoadHistogram(HistogramState.fetchInterval);
}

function addTableHistograms(data) {
    var tableName = data.TableName;
    var keys = data.KeyNames;

    if (data.SizeHistogram)
        addHistogram(tableName, keys, 'Data size',  data.SizeHistogram)

    if (data.CountHistogram)
        addHistogram(tableName, keys, 'Rows count', data.CountHistogram)

    if (data.KeyAccessSample)
        addHistogram(tableName, keys, 'Key access sample', data.KeyAccessSample)
}

function addHistogram(tableName, keys, histName, hist) {
    var headCells = `<th data-sorter="false">${histName}</th>`;
    for (var key of keys)
        headCells += `<th data-sorter="false">${key}</th>`;

    var bodyRows = '';
    if (hist.Items) {
        for (var item of hist.Items) {
            var rowCells = `<td>${item.Value}</td>`;
            for (var val of item.KeyValues)
                rowCells += `<td>${val}</td>`;
            bodyRows += `<tr>${rowCells}</tr>`;
        }
    }

    var table = $(`
        <table class="tablesorter">
            <caption class="ds-info">${histName} histogram for ${tableName}</caption>
            <thead>
                <tr>${headCells}</tr>
            </thead>
            <tbody>${bodyRows}</tbody>
        </table>
    `);

    table.appendTo($('#ds-hist-tables'));
    table.tablesorter({
            theme: 'blue',
            sortList: [],
            widgets : ['zebra'],
    });

    console.log(table);
}

function onHistogramFailed(data) {
    HistogramState.loading = false;

    if (data && data['Status'] && data['Status']['Issues'])
        $('#ds-hist-error').html(JSON.stringify(data['Status']['Issues']));
    else
        $('#ds-hist-error').html("Cannot get data histograms");
    scheduleLoadHistogram(HistogramState.retryInterval);
}

function loadHistogram() {
    if (HistogramState.loading)
        return;

    if (!$('#ds-hist-link').hasClass('active'))
        return;

    HistogramState.loading = true;
    var url = '../cms/api/datashard/json/getdatahist?tabletid=' + TabletId;
    $.get(url).done(onHistogramLoaded).fail(onHistogramFailed);
}

function scheduledLoadHistogram() {
    HistogramState.scheduledLoad = false;
    loadHistogram();
}

function scheduleLoadHistogram(timeout) {
    if (HistogramState.scheduledLoad)
        return;
    HistogramState.scheduledLoad = true;
    setTimeout(scheduledLoadHistogram, timeout);
}

function initHistogramTab() {
    $(document).on('shown.bs.tab', '', function(e) {
        if (e.target.id == 'ds-hist-link') {
            $('#ds-size-hist-table').tablesorter({
                theme: 'blue',
                sortList: [],
                widgets : ['zebra'],
            });
            $('#ds-count-hist-table').tablesorter({
                theme: 'blue',
                sortList: [],
                widgets : ['zebra'],
            });
            scheduleLoadHistogram(0);
        }
    });

    loadHistogram();
}
