'use strict';

function main() {
    // making main container wider
    //$('.container').toggleClass('container container-fluid');

    if (window.location.hash == '') {
        window.location.hash = 'page=configs';
    } else {
        window.location.hash.substr(1).split('&').forEach((o) => { var a = o.split('='); Parameters[a[0]] = decodeURIComponent(a[1]); } );
        if (Parameters.page !== undefined) {
            $('.nav-tabs a[href="#' + Parameters.page + '"]').tab('show');
        }
        if (Parameters.show !== undefined) {
            for (var id of Parameters.show.split(',')) {
                ShownElements.add(id);
            }
        }
    }

    document.getElementById('host-ref').textContent += " - " + window.location.hostname;

    $('.nav-tabs a').on('shown.bs.tab', function (e) {
        Parameters.page = e.target.hash.substr(1);
        window.location.hash = $.param(Parameters);
    })

    initCommon();
    initConfigsTab();
    initYamlConfigTab();
    initValidatorsTab();
    initCmsLogTab();
    initConsoleLogTab();
    initCmsSentinelTab();

    $('#popup').on('click', function(e) {
        if (e.target !== this)
            return;

        togglePopup();
    });
}

function togglePopup() {
    $("#popup").toggle();
}
