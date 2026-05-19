'use strict';

var TabletId = 0;
var FollowerId = 0;
var TabletDevUiSecurePathEnabled = false;

function getTabletDevUiPath() {
    return TabletDevUiSecurePathEnabled ? 'app/secure' : 'app';
}

function makeTabletDevUiUrl(queryAndMaybeHash) {
    return getTabletDevUiPath() + '?' + queryAndMaybeHash;
}

function detectTabletDevUiModeAndRun(onReady) {
    $.get('../viewer/capabilities')
        .done(function(data) {
            TabletDevUiSecurePathEnabled = Boolean(
                data &&
                data.Settings &&
                data.Settings.Features &&
                data.Settings.Features.EnableTabletDevUiSecurePath
            );
            onReady();
        })
        .fail(function() {
            TabletDevUiSecurePathEnabled = false;
            onReady();
        });
}

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
    if (args.FollowerID !== undefined) {
        FollowerId = args.FollowerID;
    }

    document.getElementById('host-ref').textContent += " - " + window.location.hostname;
    $('#shard-ref').text('DataShard ' + TabletId);
    $('#main-title').text('DataShard ' + TabletId);

    $('.nav-tabs a').on('shown.bs.tab', function (e) {
        setHashParam('page', e.target.hash.substr(1));
    })

    detectTabletDevUiModeAndRun(function() {
        initCommon();
        initDataShardInfoTab();
        initOperationsListTab();
        initOperationTab();
        initSlowOperationsTab();
        initReadSetsTab();
        initHistogramTab();
    });
}

$(document).ready(main);
