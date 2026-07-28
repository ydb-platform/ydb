'use strict';

// Hive DevUI pages are rendered server-side, so the DevUI path is resolved there and published
// as HiveDevUiAppPath before this script is loaded. That is why there is no /viewer/capabilities
// lookup here: unlike the DataShard page, which is a static template that has to query the
// feature flag at runtime, Hive already knows the answer at render time.

function getMonRootPath() {
    var marker = '/tablets/app';
    var markerPos = window.location.pathname.indexOf(marker);
    return markerPos >= 0 ? window.location.pathname.slice(0, markerPos) : '';
}

function makeMonUrl(path) {
    return getMonRootPath() + path;
}

function getTabletDevUiPath() {
    return window.HiveDevUiAppPath || 'app';
}

function makeTabletDevUiUrl(queryAndMaybeHash) {
    return makeMonUrl('/tablets/' + getTabletDevUiPath() + '?' + queryAndMaybeHash);
}

function hiveAppUrl(query) {
    return makeTabletDevUiUrl(query);
}
