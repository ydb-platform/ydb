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
