/* ══════════════════════════════════════════════════════════════
   Tree Diff Module
   ══════════════════════════════════════════════════════════════

   Computes readable structural diffs between two ordered plan trees.

   Input:  two plan tree nodes (format: {op: string, l: string, c: [{...}], ...})
   Output: side-specific arrays of DiffNode objects:
       {
           status:   'same' | 'modified' | 'added' | 'removed' |
                     'movedFrom' | 'movedTo',
           label:    string,
           children: [DiffNode, ...]
       }

   The algorithm:
   1. Preserve each side's real tree shape.
   2. Compute a Zhang-Shasha ordered tree edit script using postorder
      indexes, leftmost leaves, forest-distance tables, and
      insert/delete/relabel costs.
   3. After the initial diff, pair high-confidence removed/added
      subtrees as moves using ordered tree similarity.
   ══════════════════════════════════════════════════════════════ */

function createTreeDiffModule() {

    var SAME_OP_RELABEL_COST = 0.7;
    var DIFFERENT_OP_RELABEL_COST = 3.0;
    var INSERT_NODE_COST = 1.0;
    var DELETE_NODE_COST = 1.0;
    var MOVE_SIMILARITY_THRESHOLD = 0.62;
    var MOVE_CLEAR_WIN_MARGIN = 0.08;

    var nextNodeId = 1;
    var nextChangeId = 1;
    var nodeIds = (typeof WeakMap !== 'undefined') ? new WeakMap() : null;
    var subtreeSizeCache = (typeof WeakMap !== 'undefined') ? new WeakMap() : null;
    var tokenSetCache = (typeof WeakMap !== 'undefined') ? new WeakMap() : null;
    var structuralSignatureCache = (typeof WeakMap !== 'undefined') ? new WeakMap() : null;
    var postorderIndexCache = (typeof WeakMap !== 'undefined') ? new WeakMap() : null;
    var treeDistanceCache = new MapLikeLRU(4096);

    var OP_DELETE = 1;
    var OP_INSERT = 2;
    var OP_RELABEL = 3;
    var OP_SUBTREE = 4;
    var EDIT_EPSILON = 0.000001;

    /**
     * Main entry point. Returns side-specific diff trees:
     *   { sideSpecific: true, a: [DiffNode], b: [DiffNode] }
     */
    function diff(nodeA, nodeB, options) {
        resetDiffCounters();
        var ctx = createDiffContext(options);
        var result = baseSideDiff(nodeA, nodeB, ctx);
        markLikelyMoves(result, ctx);
        return result;
    }

    /**
     * Diff two nodes while preserving each side's real tree shape.
     */
    function baseSideDiff(nodeA, nodeB, ctx) {
        if (!nodeA && !nodeB) return makeSideResult([], []);
        if (!nodeA) return makeSideResult([], [markEntireSubtree(nodeB, 'added')]);
        if (!nodeB) return makeSideResult([markEntireSubtree(nodeA, 'removed')], []);
        if (structuralSignature(nodeA, ctx) === structuralSignature(nodeB, ctx)) {
            return makeSideResult(
                [markMatchedSubtree(nodeA, 'same')],
                [markMatchedSubtree(nodeB, 'same')]
            );
        }

        var edit = computeZhangShashaEdit(nodeA, nodeB, ctx);
        return makeSideResult(
            [buildDiffTree(edit.indexA, edit.rootA, 'a', edit, ctx)],
            [buildDiffTree(edit.indexB, edit.rootB, 'b', edit, ctx)]
        );
    }

    function makeSideResult(nodesA, nodesB) {
        return { sideSpecific: true, a: nodesA, b: nodesB };
    }

    function makeDiffNode(planNode, status, children, extra) {
        extra = extra || {};
        return {
            status: status,
            label: planNode ? planNode.l : '',
            original: planNode || null,
            children: children || [],
            moveId: extra.moveId || null,
            changeId: extra.changeId || null,
            otherLabel: extra.otherLabel || '',
            modified: !!extra.modified,
            fieldChanges: extra.fieldChanges || null
        };
    }

    function markMatchedSubtree(planNode, status) {
        var diffNode = makeDiffNode(planNode, status, []);
        if (planNode.c) {
            for (var i = 0; i < planNode.c.length; i++) {
                diffNode.children.push(markMatchedSubtree(planNode.c[i], status));
            }
        }
        return diffNode;
    }

    /**
     * Mark an entire subtree as added or removed.
     */
    function markEntireSubtree(planNode, status) {
        var diffNode = makeDiffNode(planNode, status, []);
        if (planNode.c) {
            for (var i = 0; i < planNode.c.length; i++) {
                diffNode.children.push(markEntireSubtree(planNode.c[i], status));
            }
        }
        return diffNode;
    }

    function diffMatchedNodes(nodeA, nodeB, forcedStatusA, forcedStatusB, moveId, ctx) {
        var childDiff = baseSideDiff(nodeA, nodeB, ctx);
        var modified = nodeChanged(nodeA, nodeB, ctx);
        if (childDiff.a[0]) {
            childDiff.a[0].status = forcedStatusA || childDiff.a[0].status;
            childDiff.a[0].moveId = moveId || null;
            childDiff.a[0].modified = modified || childDiff.a[0].modified;
            childDiff.a[0].otherLabel = nodeB.l || '';
        }
        if (childDiff.b[0]) {
            childDiff.b[0].status = forcedStatusB || childDiff.b[0].status;
            childDiff.b[0].moveId = moveId || null;
            childDiff.b[0].modified = modified || childDiff.b[0].modified;
            childDiff.b[0].otherLabel = nodeA.l || '';
        }
        if (modified && childDiff.a[0] && childDiff.b[0] &&
            (!childDiff.a[0].changeId || !childDiff.b[0].changeId)) {
            var changeId = String(nextChangeId++);
            childDiff.a[0].changeId = changeId;
            childDiff.b[0].changeId = changeId;
        }
        return childDiff;
    }

    function computeZhangShashaEdit(nodeA, nodeB, ctx) {
        var indexA = buildPostorderIndex(nodeA, ctx);
        var indexB = buildPostorderIndex(nodeB, ctx);
        var pairAtoB = {};
        var pairBtoA = {};
        var deletedA = {};
        var insertedB = {};
        var treeMemoWidth = indexB.nodes.length;
        var treeMemo = new Float64Array(indexA.nodes.length * treeMemoWidth);
        treeMemo.fill(-1);
        var forestMemo = new Array(indexA.nodes.length * treeMemoWidth);

        function treeCost(i, j) {
            var key = i * treeMemoWidth + j;
            if (treeMemo[key] >= 0) return treeMemo[key];
            if (indexA.nodes[i].signature === indexB.nodes[j].signature) {
                treeMemo[key] = 0;
                return 0;
            }
            var table = computeForest(indexA.nodes[i].leftmost, i,
                                      indexB.nodes[j].leftmost, j);
            treeMemo[key] = table.cost;
            return table.cost;
        }

        function computeForest(startA, endA, startB, endB) {
            var key = endA * treeMemoWidth + endB;
            if (forestMemo[key]) return forestMemo[key];

            var rows = endA - startA + 2;
            var cols = endB - startB + 2;
            var dp = new Float64Array(rows * cols);
            var choice = new Uint8Array(rows * cols);

            for (var rDel = 1; rDel < rows; rDel++) {
                var delCell = rDel * cols;
                dp[delCell] = dp[delCell - cols] + DELETE_NODE_COST;
                choice[delCell] = OP_DELETE;
            }
            for (var cIns = 1; cIns < cols; cIns++) {
                dp[cIns] = dp[cIns - 1] + INSERT_NODE_COST;
                choice[cIns] = OP_INSERT;
            }

            for (var r = 1; r < rows; r++) {
                var i = startA + r - 1;
                var rowBase = r * cols;
                var prevRowBase = rowBase - cols;
                for (var c = 1; c < cols; c++) {
                    var j = startB + c - 1;
                    var pos = rowBase + c;
                    var best = dp[prevRowBase + c] + DELETE_NODE_COST;
                    var bestOp = OP_DELETE;

                    var insertCost = dp[pos - 1] + INSERT_NODE_COST;
                    if (betterEditChoice(insertCost, OP_INSERT, best, bestOp)) {
                        best = insertCost;
                        bestOp = OP_INSERT;
                    }

                    var leftA = indexA.nodes[i].leftmost;
                    var leftB = indexB.nodes[j].leftmost;
                    if (leftA === startA && leftB === startB) {
                        var relabelCost = dp[prevRowBase + c - 1] +
                                          labelChangeCost(indexA.nodes[i], indexB.nodes[j], ctx);
                        if (betterEditChoice(relabelCost, OP_RELABEL, best, bestOp)) {
                            best = relabelCost;
                            bestOp = OP_RELABEL;
                        }
                    } else {
                        var preRow = leftA - startA;
                        var preCol = leftB - startB;
                        var subtreeCost = dp[preRow * cols + preCol] + treeCost(i, j);
                        if (betterEditChoice(subtreeCost, OP_SUBTREE, best, bestOp)) {
                            best = subtreeCost;
                            bestOp = OP_SUBTREE;
                        }
                    }

                    dp[pos] = best;
                    choice[pos] = bestOp;
                }
            }

            var table = {
                choice: choice,
                cols: cols,
                cost: dp[(rows - 1) * cols + cols - 1]
            };
            forestMemo[key] = table;
            return table;
        }

        function backtraceForest(startA, endA, startB, endB) {
            var table = computeForest(startA, endA, startB, endB);
            var r = endA - startA + 1;
            var c = endB - startB + 1;

            while (r > 0 || c > 0) {
                var op;
                if (r === 0) {
                    op = OP_INSERT;
                } else if (c === 0) {
                    op = OP_DELETE;
                } else {
                    op = table.choice[r * table.cols + c];
                }

                var i = startA + r - 1;
                var j = startB + c - 1;
                if (op === OP_DELETE) {
                    deletedA[i] = true;
                    r--;
                } else if (op === OP_INSERT) {
                    insertedB[j] = true;
                    c--;
                } else if (op === OP_RELABEL) {
                    pairAtoB[i] = j;
                    pairBtoA[j] = i;
                    r--;
                    c--;
                } else {
                    var leftA = indexA.nodes[i].leftmost;
                    var leftB = indexB.nodes[j].leftmost;
                    backtraceForest(leftA, i, leftB, j);
                    r = leftA - startA;
                    c = leftB - startB;
                }
            }
        }

        var rootTable = computeForest(1, indexA.root, 1, indexB.root);
        treeMemo[indexA.root * treeMemoWidth + indexB.root] = rootTable.cost;
        backtraceForest(1, indexA.root, 1, indexB.root);
        markUnpaired(indexA, pairAtoB, deletedA);
        markUnpaired(indexB, pairBtoA, insertedB);

        return {
            indexA: indexA,
            indexB: indexB,
            rootA: indexA.root,
            rootB: indexB.root,
            pairAtoB: pairAtoB,
            pairBtoA: pairBtoA,
            changeIdsA: {},
            changeIdsB: {},
            deletedA: deletedA,
            insertedB: insertedB,
            distance: rootTable.cost
        };
    }

    function buildPostorderIndex(root, ctx) {
        var cache = diffContextUsesFieldProjection(ctx) ? ctx.postorderIndexCache : postorderIndexCache;
        if (cache) {
            var cached = cache.get(root);
            if (cached) return cached;
        }

        var nodes = [null];
        function visit(node, parentId) {
            var childIds = [];
            var children = node.c || [];
            for (var i = 0; i < children.length; i++) {
                childIds.push(visit(children[i], null));
            }

            var id = nodes.length;
            var leftmost = childIds.length ? nodes[childIds[0]].leftmost : id;
            nodes[id] = {
                id: id,
                node: node,
                label: node.l || '',
                compareLabel: nodeComparableText(node, ctx),
                children: childIds,
                parent: parentId || null,
                leftmost: leftmost,
                op: explicitOperatorName(node),
                tokens: tokenSet(node, ctx),
                signature: structuralSignature(node, ctx)
            };
            for (var c = 0; c < childIds.length; c++) {
                nodes[childIds[c]].parent = id;
            }
            return id;
        }

        var rootId = visit(root, null);
        var index = { nodes: nodes, root: rootId };
        if (cache) cache.set(root, index);
        return index;
    }

    function buildDiffTree(index, id, side, edit, ctx) {
        var entry = index.nodes[id];
        var pairId = side === 'a' ? edit.pairAtoB[id] : edit.pairBtoA[id];
        var status;
        var modified = false;
        var changeId = null;
        var otherLabel = '';
        var fieldChanges = null;
        if (pairId) {
            var otherIndex = side === 'a' ? edit.indexB : edit.indexA;
            var otherNode = otherIndex.nodes[pairId].node;
            otherLabel = otherIndex.nodes[pairId].label;
            modified = nodeChanged(entry.node, otherNode, ctx);
            fieldChanges = side === 'a'
                ? diffFieldChanges(entry.node, otherNode, ctx)
                : diffFieldChanges(otherNode, entry.node, ctx);
            if (modified) changeId = changeIdForPair(side, id, pairId, edit);
            status = modified ? 'modified' : 'same';
        } else {
            status = side === 'a' ? 'removed' : 'added';
        }

        var children = [];
        for (var i = 0; i < entry.children.length; i++) {
            children.push(buildDiffTree(index, entry.children[i], side, edit, ctx));
        }
        return makeDiffNode(entry.node, status, children, {
            modified: modified,
            changeId: changeId,
            otherLabel: otherLabel,
            fieldChanges: fieldChanges
        });
    }

    function changeIdForPair(side, id, pairId, edit) {
        if (side === 'a') {
            if (!edit.changeIdsA[id]) {
                var changeIdA = String(nextChangeId++);
                edit.changeIdsA[id] = changeIdA;
                edit.changeIdsB[pairId] = changeIdA;
            }
            return edit.changeIdsA[id];
        }

        if (!edit.changeIdsB[id]) {
            var changeIdB = String(nextChangeId++);
            edit.changeIdsB[id] = changeIdB;
            edit.changeIdsA[pairId] = changeIdB;
        }
        return edit.changeIdsB[id];
    }

    function markUnpaired(index, pairMap, statusMap) {
        for (var i = 1; i < index.nodes.length; i++) {
            if (!pairMap[i]) statusMap[i] = true;
        }
    }

    function betterEditChoice(candidateCost, candidateOp, bestCost, bestOp) {
        if (candidateCost < bestCost - EDIT_EPSILON) return true;
        if (candidateCost > bestCost + EDIT_EPSILON) return false;
        return editChoicePriority(candidateOp) < editChoicePriority(bestOp);
    }

    function editChoicePriority(op) {
        if (op === OP_RELABEL) return 0;
        if (op === OP_SUBTREE) return 1;
        if (op === OP_DELETE) return 2;
        if (op === OP_INSERT) return 3;
        return 4;
    }

    function insertNodeCost() { return INSERT_NODE_COST; }
    function deleteNodeCost() { return DELETE_NODE_COST; }

    function subtreeSize(node) {
        if (!node) return 0;
        if (subtreeSizeCache) {
            var cached = subtreeSizeCache.get(node);
            if (cached != null) return cached;
        } else {
            var savedKey = nodeId(node);
            if (subtreeSize.cache[savedKey] != null) return subtreeSize.cache[savedKey];
        }

        var size = 1;
        var children = node.c || [];
        for (var i = 0; i < children.length; i++) {
            size += subtreeSize(children[i]);
        }
        if (subtreeSizeCache) {
            subtreeSizeCache.set(node, size);
        } else {
            subtreeSize.cache[savedKey] = size;
        }
        return size;
    }
    subtreeSize.cache = {};

    function subtreeInsertCost(node) {
        return INSERT_NODE_COST * subtreeSize(node);
    }

    function subtreeDeleteCost(node) {
        return DELETE_NODE_COST * subtreeSize(node);
    }

    function treeDistance(nodeA, nodeB, ctx) {
        if (!nodeA && !nodeB) return 0;
        if (!nodeA) return subtreeInsertCost(nodeB);
        if (!nodeB) return subtreeDeleteCost(nodeA);

        var key = nodeCacheKey(nodeA, nodeB, ctx);
        var cached = treeDistanceCache.get(key);
        if (cached != null) return cached;

        if (structuralSignature(nodeA, ctx) === structuralSignature(nodeB, ctx)) {
            treeDistanceCache.set(key, 0);
            return 0;
        }

        var edit = computeZhangShashaEdit(nodeA, nodeB, ctx);
        treeDistanceCache.set(key, edit.distance);
        return edit.distance;
    }

    function labelChangeCost(entryA, entryB, ctx) {
        var labelA = String(entryA.compareLabel || '');
        var labelB = String(entryB.compareLabel || '');
        var opA = entryA.op;
        var opB = entryB.op;
        if (labelA === labelB && opA === opB) return 0;

        if (opA && opA === opB) {
            return SAME_OP_RELABEL_COST + (1 - tokenSimilarity(entryA, entryB, ctx)) * 0.6;
        }
        return DIFFERENT_OP_RELABEL_COST;
    }

    function tokenSimilarity(aSource, bSource, ctx) {
        var a = tokenSet(aSource, ctx);
        var b = tokenSet(bSource, ctx);
        var total = 0;
        var common = 0;
        var seen = {};

        for (var key in a) {
            if (!Object.prototype.hasOwnProperty.call(a, key)) continue;
            seen[key] = true;
            total++;
            if (b[key]) common++;
        }
        for (var keyB in b) {
            if (!Object.prototype.hasOwnProperty.call(b, keyB) || seen[keyB]) continue;
            total++;
        }
        return total ? common / total : 0;
    }

    function tokenSet(source, ctx) {
        if (source && source.tokens) return source.tokens;
        var cache = diffContextUsesFieldProjection(ctx) ? ctx.tokenSetCache : tokenSetCache;
        if (source && source.l !== undefined && cache) {
            var cached = cache.get(source);
            if (cached) return cached;
        }

        var result = {};
        var text = source && source.compareLabel !== undefined ? source.compareLabel :
            (source && source.label !== undefined ? source.label :
                (source && source.l !== undefined ? nodeTokenText(source, ctx) : source));
        var parts = String(text || '').toLowerCase().match(/[a-z0-9_.$]+/g) || [];
        for (var i = 0; i < parts.length; i++) {
            if (parts[i].length > 0) result[parts[i]] = true;
        }
        if (source && source.l !== undefined && cache) {
            cache.set(source, result);
        }
        return result;
    }

    function markLikelyMoves(diffResult, ctx) {
        var removed = [];
        var added = [];
        collectStatusNodes(diffResult.a || [], 'removed', removed);
        collectStatusNodes(diffResult.b || [], 'added', added);

        if (!removed.length || !added.length) return;

        var candidates = [];
        var bestForRemoved = {};
        var bestForAdded = {};

        for (var ri = 0; ri < removed.length; ri++) {
            for (var ai = 0; ai < added.length; ai++) {
                var score = moveSimilarity(removed[ri].node.original, added[ai].node.original, ctx);
                if (score < MOVE_SIMILARITY_THRESHOLD) continue;

                var candidate = {
                    removed: removed[ri].node,
                    added: added[ai].node,
                    score: score,
                    size: Math.max(
                        subtreeSize(removed[ri].node.original),
                        subtreeSize(added[ai].node.original)
                    )
                };
                candidates.push(candidate);

                var rKey = removed[ri].id;
                var aKey = added[ai].id;
                if (!bestForRemoved[rKey] || score > bestForRemoved[rKey].score) {
                    bestForRemoved[rKey] = candidate;
                }
                if (!bestForAdded[aKey] || score > bestForAdded[aKey].score) {
                    bestForAdded[aKey] = candidate;
                }
            }
        }

        candidates.sort(function(a, b) {
            var scoreDelta = b.score - a.score;
            if (Math.abs(scoreDelta) > 0.15) return scoreDelta;
            return b.size - a.size;
        });

        var usedRemoved = [];
        var usedAdded = [];
        var moveId = 1;
        for (var ci = 0; ci < candidates.length; ci++) {
            var c = candidates[ci];
            if (nodeOrAncestorUsed(c.removed, usedRemoved) ||
                nodeOrAncestorUsed(c.added, usedAdded)) {
                continue;
            }
            if (!candidateIsClearWinner(c, bestForRemoved, bestForAdded)) continue;

            applyMove(c.removed, c.added, moveId++, ctx);
            usedRemoved.push(c.removed);
            usedAdded.push(c.added);
        }
    }

    function collectStatusNodes(nodes, status, result, parent) {
        for (var i = 0; i < nodes.length; i++) {
            var node = nodes[i];
            node._diffParent = parent || null;
            if (node.status === status && node.original) {
                result.push({ id: nodeId(node.original), node: node });
            }
            collectStatusNodes(node.children || [], status, result, node);
        }
    }

    function candidateIsClearWinner(candidate, bestForRemoved, bestForAdded) {
        var rBest = bestForRemoved[nodeId(candidate.removed.original)];
        var aBest = bestForAdded[nodeId(candidate.added.original)];
        if (rBest !== candidate && rBest &&
            rBest.score > candidate.score + MOVE_CLEAR_WIN_MARGIN) {
            return false;
        }
        if (aBest !== candidate && aBest &&
            aBest.score > candidate.score + MOVE_CLEAR_WIN_MARGIN) {
            return false;
        }
        return true;
    }

    function nodeOrAncestorUsed(node, used) {
        for (var i = 0; i < used.length; i++) {
            var cur = node;
            while (cur) {
                if (cur === used[i]) return true;
                cur = cur._diffParent || null;
            }
        }
        return false;
    }

    function applyMove(removedNode, addedNode, moveId, ctx) {
        var moved = diffMatchedNodes(
            removedNode.original,
            addedNode.original,
            'movedFrom',
            'movedTo',
            moveId,
            ctx
        );

        copyMoveNode(removedNode, moved.a[0]);
        copyMoveNode(addedNode, moved.b[0]);
    }

    function copyMoveNode(target, source) {
        var keepExistingChildren = hasPairedDescendant(target);
        target.status = source.status;
        target.label = source.label;
        target.original = source.original;
        target.moveId = source.moveId;
        target.changeId = source.changeId;
        target.otherLabel = source.otherLabel;
        target.modified = source.modified;
        target.fieldChanges = source.fieldChanges;
        if (!keepExistingChildren) {
            target.children = source.children;
        }
    }

    function hasPairedDescendant(node) {
        var children = node.children || [];
        for (var i = 0; i < children.length; i++) {
            var status = children[i].status;
            if (status === 'same' || status === 'modified' ||
                status === 'movedFrom' || status === 'movedTo') {
                return true;
            }
            if (hasPairedDescendant(children[i])) return true;
        }
        return false;
    }

    function moveSimilarity(nodeA, nodeB, ctx) {
        if (!nodeA || !nodeB) return 0;

        var opA = explicitOperatorName(nodeA);
        var opB = explicitOperatorName(nodeB);
        if (!opA || opA !== opB) return 0;

        var sizeA = subtreeSize(nodeA);
        var sizeB = subtreeSize(nodeB);
        var maxSize = Math.max(sizeA, sizeB);
        if (maxSize < 2) return 0;

        var maxCost = INSERT_NODE_COST * (sizeA + sizeB);
        if (maxCost <= 0) return 0;

        var lowerBound = Math.abs(sizeA - sizeB) * Math.min(INSERT_NODE_COST, DELETE_NODE_COST);
        if (1 - Math.min(lowerBound, maxCost) / maxCost + 0.12 < MOVE_SIMILARITY_THRESHOLD) {
            return 0;
        }

        if (structuralSignature(nodeA, ctx) === structuralSignature(nodeB, ctx)) return 1;

        var distance = treeDistance(nodeA, nodeB, ctx);
        if (typeof distance !== 'number' || !isFinite(distance)) return 0;
        var similarity = 1 - Math.min(distance, maxCost) / maxCost;

        var labelSimilarity = tokenSimilarity(nodeA, nodeB, ctx);
        return Math.min(1, similarity + labelSimilarity * 0.12);
    }

    function explicitOperatorName(source) {
        return source && source.op !== undefined ? String(source.op || '') : '';
    }

    function nodeChanged(nodeA, nodeB, ctx) {
        return nodeComparableText(nodeA, ctx) !== nodeComparableText(nodeB, ctx) ||
            explicitOperatorName(nodeA) !== explicitOperatorName(nodeB);
    }

    function structuralSignature(node, ctx) {
        if (!node) return '0:';
        var cache = diffContextUsesFieldProjection(ctx) ? ctx.structuralSignatureCache : structuralSignatureCache;
        if (cache) {
            var cached = cache.get(node);
            if (cached) return cached;
        }

        var label = nodeComparableText(node, ctx);
        var op = explicitOperatorName(node);
        var children = node.c || [];
        var parts = [op.length + ':' + op, '|', label.length + ':' + label, '[' + children.length + ':'];
        for (var i = 0; i < children.length; i++) {
            parts.push(structuralSignature(children[i], ctx));
        }
        parts.push(']');
        var signature = parts.join('');
        if (cache) cache.set(node, signature);
        return signature;
    }

    function createDiffContext(options) {
        var keys = normalizeCompareFieldKeys(options && (options.compareFields || options.compareFieldKeys));
        var set = {};
        for (var i = 0; i < keys.length; i++) set[keys[i]] = true;
        return {
            compareFieldKeys: keys,
            compareFieldKeySet: set,
            compareFieldSignature: keys.join('\n'),
            tokenSetCache: (typeof WeakMap !== 'undefined') ? new WeakMap() : null,
            structuralSignatureCache: (typeof WeakMap !== 'undefined') ? new WeakMap() : null,
            postorderIndexCache: (typeof WeakMap !== 'undefined') ? new WeakMap() : null
        };
    }

    function normalizeCompareFieldKeys(rawKeys) {
        var keys = [];
        var seen = {};
        rawKeys = Array.isArray(rawKeys) ? rawKeys : [];
        for (var i = 0; i < rawKeys.length; i++) {
            var key = String(rawKeys[i] || '');
            if (!key || seen[key]) continue;
            seen[key] = true;
            keys.push(key);
        }
        return keys;
    }

    function diffContextUsesFieldProjection(ctx) {
        return !!(ctx && ctx.compareFieldKeys && ctx.compareFieldKeys.length);
    }

    function fieldRowKey(row) {
        if (Array.isArray(row)) return String(row[0] == null ? '' : row[0]);
        return row && row.key !== undefined ? String(row.key || '') : '';
    }

    function fieldRowValue(row) {
        if (Array.isArray(row)) return String(row[1] == null ? '' : row[1]);
        return row && row.value !== undefined ? String(row.value || '') : '';
    }

    function nodeFieldValue(node, key) {
        var attrs = node && Array.isArray(node.a) ? node.a : [];
        for (var i = 0; i < attrs.length; i++) {
            if (attrs[i] && fieldRowKey(attrs[i]) === key) {
                return {
                    present: true,
                    value: fieldRowValue(attrs[i])
                };
            }
        }
        return { present: false, value: '' };
    }

    function nodeComparableText(node, ctx) {
        if (!node) return '';
        var label = String(node.l || '');
        if (!diffContextUsesFieldProjection(ctx)) return label;

        var parts = [label.length + ':' + label];
        var keys = ctx.compareFieldKeys;
        for (var i = 0; i < keys.length; i++) {
            var key = keys[i];
            var field = nodeFieldValue(node, key);
            var value = field.value;
            parts.push(
                '|f', key.length + ':' + key,
                '=', field.present ? '1' : '0',
                ':', value.length + ':' + value
            );
        }
        return parts.join('');
    }

    function nodeTokenText(node, ctx) {
        if (!node) return '';
        var text = String(node.l || '');
        if (!diffContextUsesFieldProjection(ctx)) return text;

        var parts = [text];
        var keys = ctx.compareFieldKeys;
        for (var i = 0; i < keys.length; i++) {
            var key = keys[i];
            var field = nodeFieldValue(node, key);
            if (!field.present) continue;
            parts.push(key);
            parts.push(field.value);
        }
        return parts.join(' ');
    }

    function diffFieldChanges(nodeA, nodeB, ctx) {
        if (!diffContextUsesFieldProjection(ctx)) return null;

        var changes = null;
        var keys = ctx.compareFieldKeys;
        for (var i = 0; i < keys.length; i++) {
            var key = keys[i];
            var fieldA = nodeFieldValue(nodeA, key);
            var fieldB = nodeFieldValue(nodeB, key);
            if (fieldA.present === fieldB.present && fieldA.value === fieldB.value) continue;
            if (!changes) changes = {};
            changes[key] = {
                a: fieldA.value,
                b: fieldB.value,
                presentA: fieldA.present,
                presentB: fieldB.present
            };
        }
        return changes;
    }

    function MapLikeLRU(limit) {
        this.limit = limit || 4096;
        this.values = {};
        this.order = [];
    }

    MapLikeLRU.prototype.get = function(key) {
        return Object.prototype.hasOwnProperty.call(this.values, key) ? this.values[key] : undefined;
    };

    MapLikeLRU.prototype.set = function(key, value) {
        if (!Object.prototype.hasOwnProperty.call(this.values, key)) {
            this.order.push(key);
            if (this.order.length > this.limit) {
                delete this.values[this.order.shift()];
            }
        }
        this.values[key] = value;
    };

    function nodeCacheKey(nodeA, nodeB, ctx) {
        var key = nodeId(nodeA) + '|' + nodeId(nodeB);
        var signature = ctx && ctx.compareFieldSignature || '';
        return signature ? key + '|f' + signature.length + ':' + signature : key;
    }

    function nodeId(node) {
        if (!node) return '0';
        if (nodeIds) {
            var saved = nodeIds.get(node);
            if (saved) return saved;
            var id = String(nextNodeId++);
            nodeIds.set(node, id);
            return id;
        }
        if (!node.__traceDiffId) {
            node.__traceDiffId = String(nextNodeId++);
        }
        return node.__traceDiffId;
    }

    function resetDiffCounters() {
        nextChangeId = 1;
    }

    /* Public API */
    return {
        diff: diff
    };

}

var TreeDiff = createTreeDiffModule();
