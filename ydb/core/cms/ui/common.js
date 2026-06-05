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

// Locate the editor ranges for a collector path like
// "/selector_config/2/config/blob_storage_config/foo": the leaf key range plus the range of
// each ancestor (keys *and* sequence items, e.g. the selector entry) found along the path.
// Numeric segments are array indices and are descended into the matching sequence item, so a
// field nested inside a selector_config entry is located in the right entry and its parents
// (selector_config, that selector, config, ...) can be tinted. Returns
// {leaf: monaco range or null, ancestors: [monaco range, ...]}.
function findUnknownFieldRanges(model, path, name) {
    if (!model) {
        return {leaf: null, ancestors: []};
    }
    var segments = (path || '').split('/').filter(function(s) {
        return s.length > 0; // keep array indices so selector entries can be located
    });
    if (segments.length === 0) {
        segments = [name];
    }
    var leaf = segments[segments.length - 1];

    // Match a YAML key at the start of a line, optionally right after a "- " list marker
    // (e.g. "  foo:" or "- foo:"), at or after fromLine.
    function keyMatch(key, fromLine) {
        var escaped = key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        var matches = model.findMatches('^\\s*(-\\s+)?' + escaped + '\\s*:', false, true, false, null, true);
        for (var i = 0; i < matches.length; ++i) {
            if (matches[i].range.startLineNumber >= fromLine) {
                return matches[i].range;
            }
        }
        return null;
    }

    function indentOf(lineNumber) {
        return model.getLineContent(lineNumber).match(/^\s*/)[0].length;
    }

    // Locate the (index)-th item of the YAML sequence whose items start at/after fromLine.
    // Sequence items begin with "- "; lock onto the indentation of the first such item and
    // count only siblings at that indentation, stopping when the block dedents.
    function arrayItemMatch(index, fromLine) {
        var matches = model.findMatches('^\\s*-\\s', false, true, false, null, true);
        var siblings = [];
        var indent = null;
        for (var i = 0; i < matches.length; ++i) {
            var r = matches[i].range;
            if (r.startLineNumber < fromLine) {
                continue;
            }
            var ind = indentOf(r.startLineNumber);
            if (indent === null) {
                indent = ind;
            }
            if (ind < indent) {
                break;            // sequence ended (dedent)
            }
            if (ind === indent) {
                siblings.push(r); // deeper markers belong to a nested sequence -> skip
            }
        }
        return siblings[index] || null;
    }

    var fromLine = 1;
    var ancestors = [];
    // Descend ancestors to scope the search to the correct subtree, collecting each.
    for (var i = 0; i < segments.length - 1; ++i) {
        var seg = segments[i];
        var anc = /^\d+$/.test(seg)
            ? arrayItemMatch(parseInt(seg, 10), fromLine)
            : keyMatch(seg, fromLine);
        if (anc) {
            ancestors.push(anc);
            // Continue from this ancestor's line (inclusive) so a key sharing the line with a
            // "- " marker (e.g. "- config:") is still matched on the next descent step.
            fromLine = anc.startLineNumber;
        }
    }
    return {leaf: keyMatch(leaf, fromLine), ancestors: ancestors};
}

// Back-compat thin wrapper: just the leaf range (or null).
function findUnknownFieldRange(model, path, name) {
    return findUnknownFieldRanges(model, path, name).leaf;
}

function lineIndent(model, lineNumber) {
    return model.getLineContent(lineNumber).match(/^\s*/)[0].length;
}

// Whole-line ranges for every descendant of the field whose key is at leafRange: the indented
// block beneath it (blanks skipped) until the YAML dedents back to the field's own level. Lets
// us mark the field's whole subtree as a consequence of the same issue.
function childLineRanges(model, leafRange) {
    var startLn = leafRange.startLineNumber;
    var baseIndent = lineIndent(model, startLn);
    var total = model.getLineCount();
    var ranges = [];
    for (var ln = startLn + 1; ln <= total; ++ln) {
        if (/^\s*$/.test(model.getLineContent(ln))) {
            continue;                       // blank line inside the block
        }
        if (lineIndent(model, ln) <= baseIndent) {
            break;                          // dedent -> end of this field's subtree
        }
        ranges.push({startLineNumber: ln, startColumn: 1, endLineNumber: ln, endColumn: 1});
    }
    return ranges;
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
    var leafLines = {};            // line numbers that carry a strong (leaf) highlight
    var consequenceByLine = {};    // line -> {range, unknown}: parents + children of a field

    // A consequence line (ancestor or descendant of a flagged field) gets a light tint and a
    // dimmed gutter -- it is only fallout from the real issue. Unknown (red) outranks
    // deprecated (amber) when a line is fallout from both kinds.
    function markConsequence(range, unknown) {
        var ln = range.startLineNumber;
        var prev = consequenceByLine[ln];
        consequenceByLine[ln] = {
            range: range,
            unknown: !!unknown || !!(prev && prev.unknown)
        };
    }

    fields.forEach(function(f) {
        var cls = f.deprecated ? 'deprecated-field-amber' : 'unknown-field-red';
        var found = findUnknownFieldRanges(model, f.path, f.name);
        var range = found.leaf;
        if (range) {
            leafLines[range.startLineNumber] = true;
            decorations.push({
                range: range,
                options: {
                    inlineClassName: cls,
                    className: cls,
                    linesDecorationsClassName: cls + '-gutter',
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
            // The field's own subtree is a consequence of the same issue.
            childLineRanges(model, range).forEach(function(childRange) {
                markConsequence(childRange, !f.deprecated);
            });
        }
        // Ancestors are consequences too: a collapsed parent still hints at trouble inside.
        found.ancestors.forEach(function(anc) {
            markConsequence(anc, !f.deprecated);
        });
        located.push({field: f, range: range});
    });

    // Light tint + dimmed gutter for consequence lines (parents and children), skipping lines
    // that already carry a strong leaf highlight.
    Object.keys(consequenceByLine).forEach(function(ln) {
        if (leafLines[ln]) {
            return;
        }
        var c = consequenceByLine[ln];
        var ccls = c.unknown ? 'unknown-consequence' : 'deprecated-consequence';
        decorations.push({
            range: c.range,
            options: {
                isWholeLine: true,
                className: ccls,
                linesDecorationsClassName: ccls + '-gutter',
                overviewRuler: {
                    color: c.unknown ? 'rgba(221, 51, 51, 0.5)' : 'rgba(216, 162, 0, 0.5)',
                    position: monaco.editor.OverviewRulerLane.Left
                },
                hoverMessage: {
                    value: 'Part of ' + (c.unknown ? 'an unknown' : 'a deprecated') + ' field'
                }
            }
        });
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
