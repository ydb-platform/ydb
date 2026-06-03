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

// Locate the editor range of the leaf key described by a collector path like
// "/blob_storage_config/foo". Descends ancestor keys to disambiguate repeated names.
// Returns a monaco range or null if not found.
function findUnknownFieldRange(model, path, name) {
    if (!model) {
        return null;
    }
    var segments = (path || '').split('/').filter(function(s) {
        return s.length > 0 && !/^\d+$/.test(s); // drop empty parts and array indices
    });
    if (segments.length === 0) {
        segments = [name];
    }
    var leaf = segments[segments.length - 1];

    function keyMatch(key, fromLine) {
        // YAML key at the start of a line (after indentation), e.g. "  foo:"
        var escaped = key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        var matches = model.findMatches('^\\s*' + escaped + '\\s*:', false, true, false, null, true);
        for (var i = 0; i < matches.length; ++i) {
            if (matches[i].range.startLineNumber >= fromLine) {
                return matches[i].range;
            }
        }
        return null;
    }

    var fromLine = 1;
    // Descend ancestors to scope the search to the correct subtree.
    for (var i = 0; i < segments.length - 1; ++i) {
        var anc = keyMatch(segments[i], fromLine);
        if (anc) {
            fromLine = anc.startLineNumber + 1;
        }
    }
    return keyMatch(leaf, fromLine);
}

// Highlights unknown (red) / deprecated (amber) fields in a monaco editor and
// renders a clickable list into listContainer. Clicking a list item scrolls the
// editor to the field occurrence.
//   editor        - monaco editor instance
//   listContainer - DOM element (or null) to render the list into
//   fields        - [{path, name, proto, deprecated}]
function highlightUnknownFields(editor, listContainer, fields) {
    if (!editor || typeof monaco === 'undefined') {
        return;
    }
    var model = editor.getModel();
    fields = fields || [];

    var decorations = [];
    var located = [];
    fields.forEach(function(f) {
        var cls = f.deprecated ? 'deprecated-field-amber' : 'unknown-field-red';
        var range = findUnknownFieldRange(model, f.path, f.name);
        if (range) {
            decorations.push({
                range: range,
                options: {
                    inlineClassName: cls,
                    className: cls,
                    isWholeLine: false,
                    overviewRuler: {
                        color: f.deprecated ? '#d8a200' : '#d33',
                        position: monaco.editor.OverviewRulerLane.Right
                    },
                    hoverMessage: {
                        value: (f.deprecated ? 'Deprecated' : 'Unknown') +
                            ' field `' + (f.name || '') + '` in `' + (f.proto || '') + '`'
                    }
                }
            });
        }
        located.push({field: f, range: range});
    });

    editor.__unknownFieldDecorations = editor.deltaDecorations(
        editor.__unknownFieldDecorations || [], decorations);

    if (listContainer) {
        listContainer.innerHTML = '';
        if (located.length === 0) {
            listContainer.textContent = 'No unknown fields.';
            return;
        }
        var ul = document.createElement('ul');
        ul.className = 'unknown-fields-ul';
        located.forEach(function(item) {
            var f = item.field;
            var li = document.createElement('li');
            li.className = f.deprecated ? 'deprecated-field-item' : 'unknown-field-item';
            var badge = document.createElement('span');
            badge.className = 'unknown-fields-badge';
            badge.textContent = f.deprecated ? 'deprecated' : 'unknown';
            li.appendChild(badge);
            li.appendChild(document.createTextNode(' ' + (f.path || f.name)));
            if (item.range) {
                li.classList.add('unknown-fields-clickable');
                li.onclick = function() {
                    editor.revealLineInCenter(item.range.startLineNumber);
                    editor.setPosition({
                        lineNumber: item.range.startLineNumber,
                        column: item.range.startColumn
                    });
                    editor.focus();
                };
            }
            ul.appendChild(li);
        });
        listContainer.appendChild(ul);
    }
}
