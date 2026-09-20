var InfoGraphModel = (function() {
    function graphGlobal() {
        if (typeof globalThis !== 'undefined') return globalThis;
        if (typeof window !== 'undefined') return window;
        if (typeof self !== 'undefined') return self;
        return null;
    }

    function graphLib() {
        var globalObj = graphGlobal();
        if (globalObj && globalObj.graphlib && globalObj.graphlib.Graph) return globalObj.graphlib;
        if (globalObj && globalObj.window && globalObj.window.graphlib && globalObj.window.graphlib.Graph) {
            return globalObj.window.graphlib;
        }
        if (globalObj && globalObj.dagre && globalObj.dagre.graphlib) return globalObj.dagre.graphlib;
        if (globalObj && globalObj.window && globalObj.window.dagre && globalObj.window.dagre.graphlib) {
            return globalObj.window.dagre.graphlib;
        }
        if (typeof graphlib !== 'undefined' && graphlib && graphlib.Graph) return graphlib;
        if (typeof dagre !== 'undefined' && dagre && dagre.graphlib) return dagre.graphlib;
        return null;
    }

    function dagreLib() {
        var globalObj = graphGlobal();
        if (globalObj && globalObj.dagre && globalObj.dagre.layout) return globalObj.dagre;
        if (globalObj && globalObj.window && globalObj.window.dagre && globalObj.window.dagre.layout) {
            return globalObj.window.dagre;
        }
        if (typeof dagre !== 'undefined' && dagre && dagre.layout) return dagre;
        return null;
    }

    function esc(text) {
        return typeof htmlEscape === 'function'
            ? htmlEscape(text)
            : String(text == null ? '' : text);
    }

    function hi(text, query, scope, searchContext) {
        return typeof infoHighlight === 'function'
            ? infoHighlight(text, query, scope, searchContext)
            : esc(text);
    }

    function graphSearchContextWith(context, extra) {
        if (typeof searchContextWith === 'function') return searchContextWith(context, extra);
        var result = {};
        context = context || {};
        extra = extra || {};
        for (var key in context) {
            if (Object.prototype.hasOwnProperty.call(context, key)) result[key] = context[key];
        }
        for (var extraKey in extra) {
            if (Object.prototype.hasOwnProperty.call(extra, extraKey)) result[extraKey] = extra[extraKey];
        }
        return result;
    }

    function svgHi(text, query, scope, searchContext) {
        return esc(text == null ? '' : text);
    }

    function numberValue(value, fallback) {
        var number = Number(value);
        return Number.isFinite(number) && number >= 0 ? number : fallback;
    }

    function isImplicitNodePrimary(target) {
        return !!target && target.type === 'tree';
    }

    function targetsOf(item) {
        return item && Array.isArray(item.targets) ? item.targets : [];
    }

    function primaryFor(item, role) {
        if (item && item.primaryTarget) return item.primaryTarget;
        var targets = targetsOf(item);
        if (role === 'node' && targets.length === 1 && isImplicitNodePrimary(targets[0])) {
            return targets[0];
        }
        return null;
    }

    function dataAttrsForTargets(item, role, context, helpers) {
        helpers = helpers || {};
        var targets = targetsOf(item);
        var primary = primaryFor(item, role);
        var hasTargets = targets.length || primary;
        var attrs = '';
        if (hasTargets && helpers.infoTargetDataAttrs) {
            attrs += helpers.infoTargetDataAttrs(targets, primary, {
                idScope: context && context.idScope ? context.idScope : ''
            });
            attrs += ' data-info-target-hover-mode="' + (role === 'node' ? 'preview' : 'highlight') + '"';
        }

        var clickable = false;
        if (role === 'node') clickable = !!primary;
        else if (role === 'edge') clickable = !!(item && item.primaryTarget);
        if (clickable && helpers.traceActionAttr) {
            attrs = helpers.traceActionAttr('activate-info-target', context.si, context.gi, context.ri) +
                attrs + ' role="button" tabindex="0"';
        }
        return attrs;
    }

    function targetTraceRefs(item, context) {
        var refs = [];
        var seen = {};
        var allTargets = targetsOf(item).slice();
        if (item && item.primaryTarget) allTargets.unshift(item.primaryTarget);
        for (var i = 0; i < allTargets.length; i++) {
            var target = allTargets[i];
            var id = '';
            if (InfoTargetController && InfoTargetController.traceRefNodeIdForTarget) {
                id = InfoTargetController.traceRefNodeIdForTarget(target, context);
            }
            if (id && !seen[id]) {
                seen[id] = true;
                refs.push(id);
            }
        }
        return refs;
    }

    function traceRefAttr(item, context) {
        var refs = targetTraceRefs(item, context);
        return refs.length
            ? ' data-trace-ref="' + esc(refs.join(',')) + '" data-trace-ref-mode="passive"'
            : '';
    }

    function nodeSize(node) {
        var labelWidth = String(node.label || node.id || '').length * 7 + 28;
        var width = Math.max(88, Math.min(220, labelWidth));
        var height = 42;
        return { width: width, height: height };
    }

    function edgeLabelSize(edge) {
        if (!edge || !edge.label) return { width: 0, height: 0 };
        return {
            width: Math.max(34, Math.min(160, String(edge.label).length * 7 + 16)),
            height: 18
        };
    }

    function graphSettings(section) {
        var layout = section && section.layout || {};
        return {
            rankdir: layout.rankdir || 'LR',
            ranksep: numberValue(layout.ranksep, 44),
            nodesep: numberValue(layout.nodesep, 28),
            marginx: 16,
            marginy: 16
        };
    }

    function isDirected(section) {
        return !section || section.directed !== false;
    }

    function graphItemMonospace(section, item, role) {
        if (item && item.monospace === true) return true;
        var mono = section && section.monospace || {};
        return role === 'edge' ? !!mono.edge : !!mono.node;
    }

    function layoutGraph(section) {
        var graphlibRef = graphLib();
        var dagreRef = dagreLib();
        if (!graphlibRef || !dagreRef) {
            return { ok: false, reason: 'layout-library-missing' };
        }

        try {
            var graph = new graphlibRef.Graph({
                multigraph: true,
                directed: isDirected(section)
            });
            graph.setGraph(graphSettings(section));
            graph.setDefaultEdgeLabel(function() { return {}; });

            var nodes = section.nodes || [];
            var known = {};
            for (var i = 0; i < nodes.length; i++) {
                var node = nodes[i] || {};
                var size = nodeSize(node);
                known[node.id] = true;
                graph.setNode(node.id, {
                    width: size.width,
                    height: size.height,
                    model: node,
                    searchIndex: i
                });
            }

            var edges = section.edges || [];
            for (var j = 0; j < edges.length; j++) {
                var edge = edges[j] || {};
                if (!known[edge.from] || !known[edge.to]) continue;
                var labelSize = edgeLabelSize(edge);
                graph.setEdge(edge.from, edge.to, {
                    width: labelSize.width,
                    height: labelSize.height,
                    model: edge,
                    searchIndex: j
                }, edge.id || String(j));
            }

            dagreRef.layout(graph);
            return { ok: true, graph: graph };
        } catch (err) {
            return { ok: false, reason: 'layout-error', error: err };
        }
    }

    function edgePath(points) {
        if (!points || !points.length) return '';
        var path = 'M ' + points[0].x + ' ' + points[0].y;
        for (var i = 1; i < points.length; i++) {
            path += ' L ' + points[i].x + ' ' + points[i].y;
        }
        return path;
    }

    function renderNode(nodeId, layoutNode, context, helpers) {
        var node = layoutNode.model || {};
        var x = layoutNode.x - layoutNode.width / 2;
        var y = layoutNode.y - layoutNode.height / 2;
        var attrs = dataAttrsForTargets(node, 'node', context, helpers) +
            traceRefAttr(node, context) +
            ' data-graph-node-id="' + esc(nodeId) + '"';
        var title = node.text ? '<title>' + esc(node.text) + '</title>' : '';
        var classes = 'info-graph-model-node' +
            (graphItemMonospace(context.section, node, 'node') ? ' info-graph-model-node-mono' : '');
        var html = '<g class="' + classes + '"' +
            ' transform="translate(' + x + ' ' + y + ')"' + attrs + '>';
        html += title;
        html += '<rect width="' + layoutNode.width + '" height="' + layoutNode.height + '" rx="5"></rect>';
        html += '<text class="info-graph-model-node-label" x="' + (layoutNode.width / 2) +
            '" y="19" text-anchor="middle">' +
            svgHi(node.label || nodeId, context.query, context.scope, graphSearchContextWith(context.searchContext, {
                section: context.sectionIndex,
                part: 'graph-node',
                node: layoutNode.searchIndex
            })) + '</text>';
        html += '</g>';
        return html;
    }

    function renderEdge(edgeObj, layoutEdge, context, helpers, markerId) {
        var edge = layoutEdge.model || {};
        var path = edgePath(layoutEdge.points);
        if (!path) return '';
        var attrs = dataAttrsForTargets(edge, 'edge', context, helpers) +
            traceRefAttr(edge, context) +
            ' data-graph-edge-id="' + esc(edge.id || edgeObj.name || '') + '"';
        var title = edge.text ? '<title>' + esc(edge.text) + '</title>' : '';
        var classes = 'info-graph-model-edge' +
            (graphItemMonospace(context.section, edge, 'edge') ? ' info-graph-model-edge-mono' : '');
        var html = '<g class="' + classes + '"' + attrs + '>';
        html += title;
        html += '<path class="info-graph-model-edge-hit" d="' + esc(path) + '"></path>';
        html += '<path class="info-graph-model-edge-line"' +
            (markerId ? ' marker-end="url(#' + esc(markerId) + ')"' : '') +
            ' d="' + esc(path) + '"></path>';
        if (edge.label && Number.isFinite(layoutEdge.x) && Number.isFinite(layoutEdge.y)) {
            html += '<text class="info-graph-model-edge-label" x="' + layoutEdge.x +
                '" y="' + (layoutEdge.y - 4) + '" text-anchor="middle">' +
                svgHi(edge.label, context.query, context.scope, graphSearchContextWith(context.searchContext, {
                    section: context.sectionIndex,
                    part: 'graph-edge',
                    edge: layoutEdge.searchIndex
                })) + '</text>';
        }
        html += '</g>';
        return html;
    }

    function renderSvg(section, layout, context, helpers) {
        var graph = layout.graph;
        var graphInfo = graph.graph() || {};
        var width = Math.max(1, Math.ceil(graphInfo.width || 1));
        var height = Math.max(1, Math.ceil(graphInfo.height || 1));
        var directed = isDirected(section);
        var markerId = directed
            ? 'info-graph-model-arrow-' +
                (context.idScope ? context.idScope + '-' : '') +
                [context.si, context.gi, context.ri, context.sectionIndex || 0].join('-') +
                // Sections nested in a tab share the tab's section index; the DOM
                // position keeps sibling graph marker ids unique.
                (context.domIndex !== undefined && context.domIndex !== context.sectionIndex
                    ? '-' + context.domIndex
                    : '')
            : '';
        var html = '<div class="info-graph-model" data-graph-layout="dagre" data-graph-directed="' +
            (directed ? 'true' : 'false') + '">' +
            '<svg class="info-graph-model-svg" role="img" width="' + width +
            '" height="' + height + '" viewBox="0 0 ' + width + ' ' + height + '">';
        if (directed) {
            html += '<defs><marker id="' + esc(markerId) + '" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z"></path></marker></defs>';
        }

        var edges = graph.edges();
        for (var i = 0; i < edges.length; i++) {
            html += renderEdge(edges[i], graph.edge(edges[i]), context, helpers, markerId);
        }
        var nodes = graph.nodes();
        for (var j = 0; j < nodes.length; j++) {
            html += renderNode(nodes[j], graph.node(nodes[j]), context, helpers);
        }
        html += '</svg></div>';
        return html;
    }

    function renderFallbackItem(item, role, context, helpers, label, query, scope) {
        var attrs = dataAttrsForTargets(item, role, context, helpers) + traceRefAttr(item, context);
        var classes = 'info-graph-model-fallback-item' +
            (attrs ? ' interactive' : '') +
            (graphItemMonospace(context.section, item, role) ? ' info-graph-model-fallback-item-mono' : '');
        var extra = {
            section: context.sectionIndex,
            part: role === 'edge' ? 'graph-edge' : 'graph-node'
        };
        if (role === 'edge') extra.edge = item && item.__searchIndex;
        else extra.node = item && item.__searchIndex;
        return '<li class="' + classes + '"' + attrs + '>' +
            hi(label, query, scope, graphSearchContextWith(context.searchContext, extra)) + '</li>';
    }

    function renderFallback(section, query, scope, context, helpers, reason) {
        var html = '<div class="info-graph-model info-graph-model-fallback" data-graph-layout="fallback" data-graph-layout-reason="' +
            esc(reason || 'unknown') + '">';
        html += '<div class="info-graph-model-fallback-title">Graph layout unavailable</div>';
        html += '<ul>';
        var nodes = section.nodes || [];
        for (var i = 0; i < nodes.length; i++) {
            nodes[i].__searchIndex = i;
            html += renderFallbackItem(nodes[i], 'node', context, helpers, nodes[i].label || nodes[i].id, query, scope);
        }
        var edges = section.edges || [];
        for (var j = 0; j < edges.length; j++) {
            edges[j].__searchIndex = j;
            var separator = isDirected(section) ? ' -> ' : ' -- ';
            var label = (edges[j].label || (edges[j].from + separator + edges[j].to));
            html += renderFallbackItem(edges[j], 'edge', context, helpers, label, query, scope);
        }
        html += '</ul></div>';
        return html;
    }

    function render(section, query, scope, context, helpers) {
        section = section || {};
        context = context || {};
        helpers = helpers || {};
        var layout = layoutGraph(section);
        context.query = query || '';
        context.scope = scope || '';
        context.section = section;
        if (!layout.ok) return renderFallback(section, query, scope, context, helpers, layout.reason);
        return renderSvg(section, layout, context, helpers);
    }

    return {
        layoutGraph: layoutGraph,
        render: render
    };
})();
