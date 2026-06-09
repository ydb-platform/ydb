'use strict';

// Shared with datashard.js: proxy-safe mon URLs under /tablets/app[...].
var EnableTabletDevUiSecurePath = false;

function getMonRootPath() {
    var marker = '/tablets/app';
    var markerPos = window.location.pathname.indexOf(marker);
    return markerPos >= 0 ? window.location.pathname.slice(0, markerPos) : '';
}

function makeMonUrl(path) {
    return getMonRootPath() + path;
}

function getTabletDevUiPath() {
    return EnableTabletDevUiSecurePath ? 'app/secure' : 'app';
}

function makeTabletDevUiUrl(queryAndMaybeHash) {
    return makeMonUrl('/tablets/' + getTabletDevUiPath() + '?' + queryAndMaybeHash);
}

function hiveAppUrl(query) {
    return makeTabletDevUiUrl(query);
}
