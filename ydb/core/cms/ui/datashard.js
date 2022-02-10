'use strict';

var TabletId;

function main() {
    // making main container wider
    //$('.container').toggleClass('container container-fluid');

    if (window.location.hash == '') {
        window.location.hash = 'page=info';
    } else {
        parseHashParams();
        if (Parameters.page !== undefined) {
            $('.nav-tabs a[href="#' + Parameters.page + '"]').tab('show');
        }
        if (Parameters.show !== undefined) {
            for (var id of Parameters.show.split(',')) {
                ShownElements.add(id);
            }
        }
    }

    var args = {};
    window.location.search.substr(1).split('&').forEach((o) => { var a = o.split('='); args[a[0]] = decodeURIComponent(a[1]); } );
    if (args.TabletID !== undefined) {
        TabletId = args.TabletID;
    }

    document.getElementById('host-ref').textContent += " - " + window.location.hostname;
    $('#shard-ref').text('DataShard ' + TabletId);
    $('#main-title').text('DataShard ' + TabletId);

    $('.nav-tabs a').on('shown.bs.tab', function (e) {
        setHashParam('page', e.target.hash.substr(1));
    })

    initCommon();
    initDataShardInfoTab();
    initOperationsListTab();
    initOperationTab();
    initSlowOperationsTab();
    initReadSetsTab();
    initHistogramTab();
}

$(document).ready(main);
