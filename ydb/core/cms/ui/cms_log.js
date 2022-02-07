'use strict';

var CmsLogState = {
    lastDate: 0,
    firstDate: 0,
    prevLastDate: 0,
    isFull: true,
    limit: 10000,
    fetchInterval: 5000,
    retryInterval: 5000,
    totalRecords: 0,
    initialFetchLimit: 1000000,
    recordTypes: new Set(),
};

function onCmsLogLoaded(data) {
    if (data['Status']['Code'] != 'OK') {
        onCmsLogFailed(data);
        return;
    }

    $('#cms-log-error').html('');

    var recs = data['LogRecords'];
    if (recs === undefined)
        recs = [];

    if (recs.length > 0) {
        if (CmsLogState.isFull) {
            CmsLogState.prevLastDate = CmsLogState.lastDate;
            CmsLogState.lastDate = recs[recs.length - 1]['Timestamp'];
        }
        CmsLogState.firstDate = recs[0]['Timestamp'];
        CmsLogState.totalRecords += recs.length;
    }

    for (var i = 0; i < recs.length; ++i) {
        var rec = recs[i];
        var line = document.createElement('tr');
        var cell1 = document.createElement('td');
        var cell2 = document.createElement('td');

        var timestamp = new Date(rec['Timestamp'] / 1000);
        cell1.textContent = timestamp.toLocaleString();
        cell1.dataset.ordervalue = rec['Timestamp'];
        cell2.textContent = rec['Message'];
        cell2.dataset.filtervalue = rec['RecordType'];
        cell2.title = JSON.stringify(rec['Data'], null, 2);

        line.appendChild(cell1);
        line.appendChild(cell2);

        document.getElementById('cms-log-body').appendChild(line);

        CmsLogState.recordTypes.add(rec['RecordType']);
    }

    if (recs.length > 0) {
        $("#cms-log-table").trigger("update", [true]);
    }

    if (recs.length == CmsLogState.limit) {
        CmsLogState.isFull = false;
        loadCmsLog();
    } else {
        CmsLogState.isFull = true;
        setTimeout(loadCmsLog, CmsLogState.fetchInterval);
    }
}

function onCmsLogFailed(data) {
    if (data && data['Status'] && data['Status']['Reason'])
        $('#cms-log-error').html(data['Status']['Reason']);
    else
        $('#cms-log-error').html("Cannot get CMS log update");
    setTimeout(loadCmsLog, CmsLogState.retryInterval);
}

function loadCmsLog() {
    var url = 'cms/api/json/log?data=1&limit=' + CmsLogState.limit;
    if (CmsLogState.isFull) {
        url += '&from=' + (CmsLogState.lastDate + 1);
    } else {
        if (CmsLogState.prevLastDate != 0) {
            url += '&from=' + (CmsLogState.prevLastDate + 1);
            url += '&to=' + (CmsLogState.firstDate - 1);
        } else {
            if (CmsLogState.totalRecords >= CmsLogState.initialFetchLimit) {
                $('#cms-log-error').html("Stop fetching too long log");
                CmsLogState.isFull = true;
                setTimeout(loadCmsLog, CmsLogState.fetchInterval);
                return;
            } else {
                url += '&from=0&to=' + (CmsLogState.firstDate - 1);
            }
        }
    }
    $.get(url).done(onCmsLogLoaded).fail(onCmsLogFailed);
}

function initCmsLogTab() {
    $("#cms-log-table").tablesorter({
        theme: 'blue',
        sortList: [[0,0]],
        headers: {
            0: {
                sorter: 'numeric-ordervalue',
            },
            1: {
                sorter: false,
            }
        },
        widgets : ['zebra'],
    });

    loadCmsLog();
}
