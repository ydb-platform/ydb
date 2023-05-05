'use strict';

var Parameters = {};
var ShownElements = new Set();

function onShown(id) {
    ShownElements.add(id);
    Parameters.show = Array.from(ShownElements.keys()).join(',');
    window.location.hash = $.param(Parameters);
}

function getOnShown(id) {
    return function() { onShown(id) };
}

function onHidden(id) {
    ShownElements.delete(id);
    Parameters.show = Array.from(ShownElements.keys()).join(',');
    window.location.hash = $.param(Parameters);
}

function getOnHidden(id) {
    return function() { onHidden(id) };
}

function removeElement(elem) {
    elem.parentElement.removeChild(elem);
}

function* makeUniqueId() {
    var id = 0;
    while (true) {
        ++id;
        console.log('elem-id-' + id);
        yield 'elem-id-' + id;
    }
}

function initCommon() {
    $.tablesorter.addParser({
        id: 'numeric-ordervalue',
        is: function() {
            return false;
        },
        format: function(s, table, cell, cellIndex) {
            return cell.dataset.ordervalue;
        },
        type: 'numeric'
    });

    $.tablesorter.addParser({
        id: 'filtervalue',
        is: function() {
            return false;
        },
        format: function(s, table, cell, cellIndex) {
            return cell.dataset.filtervalue;
        },
        type: 'text',
        parsed: true
    });
}

function parseHashParams() {
    window.location.hash.substr(1).split('&').forEach(
        function(o) {
            var a = o.split('=');
            Parameters[a[0]] = decodeURIComponent(a[1]);
        }
    );
}

function setHashParam(name, val) {
    Parameters[name] = val;
    window.location.hash = $.param(Parameters);
}

function timeToString(val) {
    var date = new Date(val / 1000);
    return date.toLocaleString();
}

function durationToStringMs(val) {
    return `${val / 1000} ms`;
}

function copyToClipboard(textToCopy) {
    // navigator clipboard api needs a secure context (https)
    if (navigator.clipboard && window.isSecureContext) {
        // navigator clipboard api method'
        return navigator.clipboard.writeText(textToCopy);
    } else {
        // text area method
        let textArea = document.createElement("textarea");
        textArea.value = textToCopy;
        // make the textarea out of viewport
        textArea.style.position = "fixed";
        textArea.style.left = "-999999px";
        textArea.style.top = "-999999px";
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        return new Promise((res, rej) => {
            // here the magic happens
            document.execCommand('copy') ? res() : rej();
            textArea.remove();
        });
    }
}
