/* ══════════════════════════════════════════════════════════════
   Diff Renderer Module
   ══════════════════════════════════════════════════════════════

   Renders a diff result (array of DiffNode) for one side (A or B)
   using compact connector runs, matching the normal tree rendering
   style.

   Side 'a' shows 'same', 'modified', 'removed', and 'movedFrom'.
   Side 'b' shows 'same', 'modified', 'added', and 'movedTo'.
   ══════════════════════════════════════════════════════════════ */

var DiffRenderer = (function() {
    function diffTreeSession(options) {
        return options && options.treeSession ? options.treeSession : null;
    }

    /**
     * Filter diff nodes to those visible on the given side.
     */
    function filterVisible(nodes, side) {
        var result = [];
        for (var i = 0; i < nodes.length; i++) {
            var node = nodes[i];
            if (side === 'a' &&
                (node.status === 'added' || node.status === 'movedTo')) {
                continue;
            }
            if (side === 'b' &&
                (node.status === 'removed' || node.status === 'movedFrom')) {
                continue;
            }
            result.push(node);
        }
        return result;
    }

    /**
     * Get the CSS class for a diff node's status.
     */
    function diffClass(node) {
        var status = node.status;
        var classes = '';
        if (status === 'added') return ' diff-mark-add';
        if (status === 'removed') return ' diff-mark-rem';
        if (status === 'movedFrom') classes = ' diff-mark-move diff-mark-move-from';
        if (status === 'movedTo') classes = ' diff-mark-move diff-mark-move-to';
        if (status === 'modified' || node.modified) classes += ' diff-mark-modified';
        return classes;
    }

    function diffLinkAttrs(node, side, options) {
        var kind = '';
        var id = '';
        var title = '';

        if ((node.status === 'movedFrom' || node.status === 'movedTo') && node.moveId) {
            kind = 'move';
            id = String(node.moveId);
            title = node.status === 'movedFrom'
                ? 'Moved from this position'
                : 'Moved to this position';
            if (node.modified) title += '; label, selected fields, or subtree also changed';
        } else if (node.modified && node.changeId) {
            kind = 'change';
            id = String(node.changeId);
            title = 'Matched node with changed label or selected fields';
        } else {
            return '';
        }

        return ' data-diff-link-id="' + htmlEscape(kind + '-' + id) + '"' +
               ' data-diff-link-kind="' + kind + '"' +
               ' data-diff-link-side="' + (side === 'a' ? 'from' : 'to') + '"' +
               ' data-diff-link-scope="' + htmlEscape(options.moveScope || 'diff') + '"' +
               ' data-diff-link-title="' + htmlEscape(title) + '"';
    }

    function commonPrefixLength(a, b) {
        var max = Math.min(a.length, b.length);
        var i = 0;
        while (i < max && a.charAt(i) === b.charAt(i)) i++;
        return i;
    }

    function commonSuffixLength(a, b, prefixLength) {
        var max = Math.min(a.length, b.length) - prefixLength;
        var i = 0;
        while (i < max &&
               a.charAt(a.length - 1 - i) === b.charAt(b.length - 1 - i)) {
            i++;
        }
        return i;
    }

    function diffLabelTextHtml(label, otherLabel, side) {
        label = String(label == null ? '' : label);
        otherLabel = String(otherLabel == null ? '' : otherLabel);
        if (!otherLabel || label === otherLabel) return htmlEscape(label);

        var prefixLength = commonPrefixLength(label, otherLabel);
        var suffixLength = commonSuffixLength(label, otherLabel, prefixLength);
        var prefix = label.substring(0, prefixLength);
        var middle = label.substring(prefixLength, label.length - suffixLength);
        var suffix = suffixLength ? label.substring(label.length - suffixLength) : '';
        var cls = side === 'a' ? 'diff-label-del' : 'diff-label-add';

        return htmlEscape(prefix) +
            (middle ? '<span class="' + cls + '">' + htmlEscape(middle) + '</span>' : '') +
            htmlEscape(suffix);
    }

    function diffTargetAttrs(node, pid) {
        var original = node && node.original || null;
        var attrs = ' data-diff-pid="' + htmlEscape(String(pid || '')) + '"';
        if (original && original.id !== undefined && original.id !== null && original.id !== '') {
            attrs += ' data-info-target-node-id="' + htmlEscape(String(original.id)) + '"';
        }
        return attrs;
    }

    function diffRunStatus(node) {
        var status = node.status;
        if (status === 'added' || status === 'removed' ||
            status === 'movedFrom' || status === 'movedTo') {
            return status;
        }
        if (status === 'modified' || node.modified) return 'modified';
        return '';
    }

    function childPid(basePid, index) {
        return basePid ? basePid + '-' + index : String(index);
    }

    function collectRenderedRows(nodes, side, basePid, rows, depth) {
        depth = depth || 0;
        if (nodes && nodes.sideSpecific) {
            nodes = nodes[side] || [];
        }

        var visible = filterVisible(nodes, side);
        for (var i = 0; i < visible.length; i++) {
            var node = visible[i];
            var pid = childPid(basePid, i);
            rows.push({ pid: pid, status: diffRunStatus(node), depth: depth });

            if (filterVisible(node.children || [], side).length > 0) {
                collectRenderedRows(node.children, side, pid, rows, depth + 1);
            }
        }
    }

    function buildDiffRunClassMap(nodes, side, basePid) {
        var rows = [];
        var result = {};
        collectRenderedRows(nodes, side, basePid, rows);

        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            var status = row.status;
            if (status !== 'added' && status !== 'removed' &&
                status !== 'movedFrom' && status !== 'movedTo' &&
                status !== 'modified') {
                continue;
            }

            var prev = rows[i - 1];
            var next = rows[i + 1];
            var samePrev = prev && prev.status === status;
            var sameNext = next && next.status === status;
            var classes = '';
            if (!samePrev) {
                classes += ' diff-run-start';
            }
            if (!sameNext) {
                classes += ' diff-run-end';
            }
            if (!samePrev || prev.depth !== row.depth) {
                classes += ' diff-corner-top-left';
            }
            if (!sameNext || next.depth !== row.depth) {
                classes += ' diff-corner-bottom-left';
            }
            result[row.pid] = classes;
        }
        return result;
    }

    function sideNodes(nodes, side) {
        if (nodes && nodes.sideSpecific) return nodes[side] || [];
        return nodes || [];
    }

    function copyDiffOriginalNode(node, children) {
        var original = node && node.original || null;
        var copy = {};
        if (original && typeof original === 'object') {
            for (var key in original) {
                if (!Object.prototype.hasOwnProperty.call(original, key) || key === 'c') continue;
                copy[key] = original[key];
            }
        }
        copy.l = node && node.label !== undefined ? node.label : (copy.l || '');
        copy.c = children || [];
        if (!copy.a && original && original.a) copy.a = original.a;
        return copy;
    }

    function stableDiffFieldChangesSignature(fieldChanges) {
        if (!fieldChanges) return '';
        var keys = [];
        for (var key in fieldChanges) {
            if (Object.prototype.hasOwnProperty.call(fieldChanges, key)) keys.push(key);
        }
        keys.sort();
        var parts = [];
        for (var i = 0; i < keys.length; i++) {
            var change = fieldChanges[keys[i]] || {};
            parts.push([
                keys[i],
                change.presentA === false ? '0' : '1',
                change.presentB === false ? '0' : '1',
                change.a || '',
                change.b || ''
            ].join(':'));
        }
        return parts.join(';');
    }

    function diffDecorationSignature(node, side, className, attrs) {
        return [
            side || '',
            node && node.status || '',
            node && node.modified ? 'modified' : '',
            node && node.moveId || '',
            node && node.changeId || '',
            node && node.label || '',
            node && node.otherLabel || '',
            stableDiffFieldChangesSignature(node && node.fieldChanges),
            className || '',
            attrs || ''
        ].join('|');
    }

    function diffRowDecoration(node, pid, side, options, runClass) {
        var className = (diffClass(node) + (runClass || '')).trim();
        var attrs = diffTargetAttrs(node, pid) + diffLinkAttrs(node, side, options || {});
        return {
            className: className,
            attrs: attrs,
            labelHtml: node && node.modified
                ? diffLabelTextHtml(node.label, node.otherLabel, side)
                : htmlEscape(node && node.label || ''),
            fieldChanges: node && node.fieldChanges || null,
            diffSide: side,
            signature: diffDecorationSignature(node, side, className, attrs)
        };
    }

    function buildDiffRenderTreeNode(node, side, pid, options, runClassMap, decorations) {
        var visibleChildren = filterVisible(node && node.children || [], side);
        var children = [];
        for (var i = 0; i < visibleChildren.length; i++) {
            children.push(buildDiffRenderTreeNode(
                visibleChildren[i],
                side,
                childPid(pid, i),
                options,
                runClassMap,
                decorations
            ));
        }
        decorations[pid] = diffRowDecoration(node, pid, side, options, runClassMap[pid] || '');
        return copyDiffOriginalNode(node, children);
    }

    function emptyDiffTreeModel() {
        return {
            rows: [],
            totalHeight: 1,
            minWidth: 1
        };
    }

    function buildSideRenderModel(nodes, side, options) {
        options = options || {};
        var basePid = options.basePid || '';
        var sourceNodes = sideNodes(nodes, side);
        var visible = filterVisible(sourceNodes, side);
        var runClassMap = buildDiffRunClassMap(nodes, side, basePid);
        var decorations = {};
        var tree = null;
        var model = emptyDiffTreeModel();

        if (visible.length > 0) {
            tree = buildDiffRenderTreeNode(
                visible[0],
                side,
                childPid(basePid, 0),
                options,
                runClassMap,
                decorations
            );
            model = buildTreeRows(tree, diffTreeSession(options), {
                showFields: !!options.showFields,
                showPinned: !!options.showPinned
            });
        }

        return {
            tree: tree,
            model: model,
            renderOptions: {
                treeRenderKind: 'diff',
                rowDecorations: decorations,
                renderSignature: [
                    'diff',
                    side || '',
                    options.moveScope || '',
                    Object.keys(decorations).sort().map(function(pid) {
                        return pid + ':' + decorations[pid].signature;
                    }).join(',')
                ].join('|'),
                diffSide: side || '',
                moveScope: options.moveScope || ''
            }
        };
    }

    /** Render the visible nodes for one side of the diff through the shared
        tree row renderer. */
    function renderSide(nodes, ruleKey, basePid, side, parentPrefix, isRoot, options, runClassMap) {
        options = options || {};
        options.basePid = basePid || '';
        var renderModel = buildSideRenderModel(nodes, side, options);
        return renderFullTreeRowsFromModel(
            renderModel.model,
            ruleKey,
            !!options.showFields,
            !!options.showPinned,
            '',
            '',
            renderModel.renderOptions
        );
    }

    /* Public API */
    return {
        buildSideRenderModel: buildSideRenderModel,
        renderSide: renderSide
    };

})();
