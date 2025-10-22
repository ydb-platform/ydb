'use strict';

function setLocation(parameters) {
    window.location.hash = $.param(parameters);
}

function showTab(parameters) {
    $('.nav-tabs a[href="#' + parameters.page + '"]').tab('show');
}

function setTab(parameters) {
    setLocation(parameters);
    showTab(parameters);
}

function main() {
    // making main container wider
    //$('.container').toggleClass('container container-fluid');

    if (window.location.hash == '') {
        var activeTabs = $('.nav-tabs a.active');
        for (var i = 0; i < activeTabs.length; ++i) {
            setTab({page: activeTabs[i].hash.substr(1)});
            break;
        }
    } else {
        window.location.hash.substr(1).split('&').forEach((o) => { var a = o.split('='); Parameters[a[0]] = decodeURIComponent(a[1]); } );
        if (Parameters.page !== undefined) {
            showTab(Parameters);
        }
        if (Parameters.show !== undefined) {
            for (var id of Parameters.show.split(',')) {
                ShownElements.add(id);
            }
        }
    }

    document.getElementById('host-ref').textContent += " - " + window.location.hostname;

    $('.nav-tabs a').on('shown.bs.tab', function (e) {
        setLocation({page: e.target.hash.substr(1)});
    })

    initCommon();
    initConfigsTab();
    initYamlConfigTab();
    initValidatorsTab();
    initCmsLogTab();
    initConsoleLogTab();
    initCmsSentinelTab();
    initStateStorageTab();

    $('#popup').on('click', function(e) {
        if (e.target !== this)
            return;

        togglePopup();
    });
}

function togglePopup() {
    $("#popup").toggle();
}
