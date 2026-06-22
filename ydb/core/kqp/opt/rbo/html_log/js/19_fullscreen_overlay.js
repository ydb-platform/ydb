/* ══════════════════════════════════════════════════════════════
   Fullscreen overlay portal
   ══════════════════════════════════════════════════════════════ */

function fullscreenOverlayRoot() {
    return hasDOM() ? document.getElementById('fullscreen-root') : null;
}

var fullscreenSurface = createVisualSurface({
    name: 'fullscreen-shell',
    epochScopes: ['trace', 'fullscreen'],
    allowMissingTarget: true,
    resolveTarget: function(ref, options) {
        options = options || {};
        if (options.target) return options.target;
        if (options.body && hasDOM()) return document.body || null;
        if (options.root) return fullscreenOverlayRoot();
        if (options.targetId && hasDOM() && document.getElementById) {
            return document.getElementById(options.targetId);
        }
        if (options.selector && hasDOM() && document.querySelector) {
            return document.querySelector(options.selector);
        }
        return null;
    }
});

function canCommitFullscreenShell(label, target, details) {
    details = cloneVisualDetails(details);
    if (fullscreenRuntime().fullscreenTransitionOwnerId && details.ownerId === undefined) {
        details.ownerId = fullscreenRuntime().fullscreenTransitionOwnerId;
    }
    if (details.fullscreenDomGeneration !== undefined &&
            !fullscreenDomGenerationCurrent(details.fullscreenDomGeneration)) {
        recordVisualCommitDenied('fullscreen-shell', label, 'stale_dom_generation', details);
        return false;
    }
    if (!canCommitVisualWork('fullscreen-shell', label, details)) return false;
    if (target && target.isConnected === false) {
        if (target.id) details.targetId = target.id;
        recordVisualCommitDenied('fullscreen-shell', label, 'detached_target', details);
        return false;
    }
    return true;
}

function commitFullscreenSurface(visualCtx, options, writer) {
    options = options || {};
    if (options.fullscreenDomGeneration !== undefined) {
        options.domGenerations = cloneVisualDomGenerations(options.domGenerations);
        options.domGenerations.fullscreen = options.fullscreenDomGeneration;
    }
    if (!canCommitFullscreenShell(
            options.label || 'fullscreen-surface',
            options.target || null,
            options
    )) {
        return false;
    }
    return fullscreenSurface.commit(visualCtx, options, writer);
}

function withFullscreenTransition(label, details, fn) {
    if (typeof fn !== 'function') return false;

    var fullscreen = fullscreenRuntime();
    if (fullscreen.fullscreenTransitionOwnerId) return fn();

    var owner = createVisualSurfaceOwner(
        'fullscreen',
        label || 'fullscreen-transition',
        ['fullscreen-shell'],
        details
    );
    if (!acquireVisualSurfaceOwners(owner.surfaces, owner)) return false;

    fullscreen.fullscreenTransitionOwnerId = owner.id;
    try {
        return fn(owner);
    } finally {
        fullscreen.fullscreenTransitionOwnerId = null;
        releaseVisualSurfaceOwners(owner.id);
    }
}

function toggleFullscreenBodyClass(className, active, label, details) {
    if (!hasDOM() || !document.body || !document.body.classList) return false;
    details = cloneVisualDetails(details);
    details.className = className;
    if (!canCommitFullscreenShell(label || 'fullscreen-body-class', document.body, details)) return false;
    if (active) {
        document.body.classList.add(className);
    } else {
        document.body.classList.remove(className);
    }
    bumpFullscreenDomGeneration();
    return true;
}

function ensureFullscreenOverlayRoot() {
    if (!hasDOM() || !document.body || !document.createElement) return null;

    var root = fullscreenOverlayRoot();
    if (!canCommitFullscreenShell('fullscreen-root', root)) return null;
    if (!root) {
        root = document.createElement('div');
        root.id = 'fullscreen-root';
        root.className = 'fullscreen-root';
        root.setAttribute('role', 'dialog');
        root.setAttribute('aria-modal', 'true');
        document.body.appendChild(root);
        bumpFullscreenDomGeneration();
    }
    return root;
}

function removeFullscreenOverlay() {
    var root = fullscreenOverlayRoot();
    if (root && root.parentNode) {
        if (!canCommitFullscreenShell('remove-fullscreen-root', root)) return false;
        root.parentNode.removeChild(root);
        bumpFullscreenDomGeneration();
        return true;
    }
    return false;
}

function fullscreenRuleRenderKey(ref) {
    return scopedRuleKey(ref, { idScope: 'fs' });
}

function fullscreenInfoContentElement(ref) {
    if (!ref || !hasDOM() || !document.getElementById) return null;
    return document.getElementById('info-content-' + fullscreenRuleRenderKey(ref));
}

function captureFullscreenInfoScrollSnapshot(ref) {
    if (typeof captureInfoScrollSnapshot !== 'function') return null;
    var content = fullscreenInfoContentElement(ref);
    return content ? captureInfoScrollSnapshot(content) : null;
}

function restoreFullscreenInfoScrollSnapshot(ref, snapshot) {
    if (!snapshot || typeof restoreInfoScrollSnapshot !== 'function') return false;
    var content = fullscreenInfoContentElement(ref);
    return content ? restoreInfoScrollSnapshot(content, snapshot) : false;
}

function renderFullscreenInfoPanel(ref) {
    if (!effectiveRuleFeature(ref.si, ref.gi, ref.ri, 'info') || !ruleHasInfo(ref)) return;

    var content = fullscreenInfoContentElement(ref);
    if (!content) return;

    renderInfoPanel(
        content,
        traceRuleInfo(ref.si, rawRuleIndex(ref.si, ref.gi, ref.ri)),
        currentSearchQuery(),
        currentSearchScope(),
        ref.si,
        ref.gi,
        ref.ri,
        { idScope: 'fs' }
    );
}

/** Re-render the fullscreen info panel after the rule's tab/switcher session
    changed, keeping the overlay in sync with the shared session state. */
function rerenderFullscreenInfoPanel(si, gi, ri) {
    if (!hasDOM() || !isFullscreenOpen()) return false;
    if (isFullscreenDiff()) {
        if (!isActiveDiffRule(si, gi, ri)) return false;
        return syncFullscreenOverlay();
    }
    if (!isFullscreenRule(si, gi, ri)) return false;
    var ref = fullscreenCurrentRule();
    if (!effectiveRuleFeature(ref.si, ref.gi, ref.ri, 'info') || !ruleHasInfo(ref)) {
        return syncFullscreenOverlay();
    }
    return runFullscreenSurfaceCommitNow('fullscreen-info-panel', function(visualCtx) {
        var committed = false;
        commitFullscreenSurface(visualCtx, {
            label: 'fullscreen-info-panel'
        }, function() {
            renderFullscreenInfoPanel(ref);
            committed = true;
        });
        return committed;
    });
}

function renderFullscreenOverlayContents(query, searchScope) {
    var ref = fullscreenCurrentRule();
    if (!ref || !hasDOM()) {
        removeFullscreenOverlay();
        clearFullscreenBackgroundInert();
        return false;
    }

    var root = ensureFullscreenOverlayRoot();
    if (!root) return false;

    var renderKey = fullscreenRuleRenderKey(ref);
    var diffMode = isFullscreenDiff();
    if (!canCommitFullscreenShell('fullscreen-overlay-contents', root, {
        ruleKey: ruleKey(ref.si, ref.gi, ref.ri),
        mode: diffMode ? 'diff' : 'rule'
    })) return false;
    var infoScrollSnapshot = diffMode ? null : captureFullscreenInfoScrollSnapshot(ref);
    root.innerHTML =
        '<div class="rule-cell expanded fullscreen-rule' +
        (ruleIsTextTile(ref) ? ' text-rule' : '') +
        (diffMode ? ' fullscreen-diff-mode' : '') +
        '" id="fullscreen-rule" data-rule-key="' + ruleKey(ref.si, ref.gi, ref.ri) + '">' +
        ruleCellBodyHtml(ref, { idScope: 'fs' }) +
        '</div>';
    bumpFullscreenDomGeneration();

    var container = document.getElementById('tree-' + renderKey);
    if (!container) return false;

    if (diffMode && renderFullscreenDiffPair(container, ruleKey(ref.si, ref.gi, ref.ri), activeDiffResultForRender())) {
        ruleState(ref.si, ref.gi, ref.ri).renderQueued = false;
    } else {
        renderPlainRuleTreeInto(container, ref, renderKey, query, searchScope);
        renderFullscreenInfoPanel(ref);
        restoreFullscreenInfoScrollSnapshot(ref, infoScrollSnapshot);
    }

    updateRuleFeatureButtons(ref.si, ref.gi, ref.ri);
    syncFullscreenBackgroundInert();
    reactivateActiveSearchMatchForRule(ref.si, ref.gi, ref.ri);
    return true;
}

function syncFullscreenOverlay(query, searchScope) {
    return withFullscreenTransition('sync-fullscreen-overlay', {
        open: isFullscreenOpen()
    }, function() {
        if (!isFullscreenOpen()) {
            removeFullscreenOverlay();
            clearFullscreenBackgroundInert();
            return false;
        }

        return renderFullscreenOverlayContents(
            query === undefined ? currentSearchQuery() : query,
            searchScope === undefined ? currentSearchScope() : searchScope
        );
    });
}

function runFullscreenSurfaceCommitNow(label, callback) {
    return runVisualSurfaceCommitNow(
        label,
        ['fullscreen-shell'],
        ['trace', 'fullscreen'],
        callback
    );
}

function setFullscreenBackgroundInert(el, inert, visualCtx) {
    if (!el) return false;
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runFullscreenSurfaceCommitNow('fullscreen-background-inert', function(ctx) {
            return setFullscreenBackgroundInert(el, inert, ctx);
        });
    }

    var committed = false;
    commitFullscreenSurface(visualCtx, {
        label: 'fullscreen-background-inert',
        target: el,
        inert: !!inert
    }, function(target) {
        if (inert) {
            target.setAttribute('data-fullscreen-background-inert', '');
            target.inert = true;
        } else {
            target.inert = false;
            target.removeAttribute('inert');
            target.removeAttribute('data-fullscreen-background-inert');
        }
        bumpFullscreenDomGeneration();
        committed = true;
    });
    return committed;
}

function clearFullscreenBackgroundInert(visualCtx) {
    if (!hasDOM() || !document.querySelectorAll) return false;
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runFullscreenSurfaceCommitNow('clear-fullscreen-background-inert', function(ctx) {
            return clearFullscreenBackgroundInert(ctx);
        });
    }

    var committed = false;
    commitFullscreenSurface(visualCtx, {
        label: 'clear-fullscreen-background-inert'
    }, function() {
        var active = document.querySelectorAll('[data-fullscreen-background-inert]');
        for (var i = 0; i < active.length; i++) {
            setFullscreenBackgroundInert(active[i], false, visualCtx);
        }
        committed = true;
    });
    return committed;
}

function syncFullscreenBackgroundInert(fullscreenDomGeneration, visualCtx) {
    if (visualCommitContextIsBranded(fullscreenDomGeneration)) {
        visualCtx = fullscreenDomGeneration;
        fullscreenDomGeneration = undefined;
    }
    if (!hasDOM() || !document.querySelector || !document.querySelectorAll) return false;
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runFullscreenSurfaceCommitNow('sync-fullscreen-background-inert', function(ctx) {
            return syncFullscreenBackgroundInert(fullscreenDomGeneration, ctx);
        });
    }

    var options = { label: 'sync-fullscreen-background-inert' };
    if (fullscreenDomGeneration !== undefined) {
        options.fullscreenDomGeneration = fullscreenDomGeneration;
    }
    var committed = false;
    commitFullscreenSurface(visualCtx, options, function() {
        clearFullscreenBackgroundInert(visualCtx);
        if (!isFullscreenOpen()) {
            committed = true;
            return;
        }

        var fullscreenCell = document.querySelector('.rule-cell.fullscreen-rule');
        if (!fullscreenCell) {
            committed = true;
            return;
        }

        var candidates = document.querySelectorAll(
            '.top-bar, .stage-col, .stage-hdr, .empty-stage-card, ' +
            '.group-cell, .group-title-bar, .group-col-wrap, ' +
            '.rule-cell:not(.fullscreen-rule)'
        );
        for (var i = 0; i < candidates.length; i++) {
            var el = candidates[i];
            if (el === fullscreenCell || el.contains(fullscreenCell) || fullscreenCell.contains(el)) continue;
            setFullscreenBackgroundInert(el, true, visualCtx);
        }
        committed = true;
    });
    return committed;
}

function renderFullscreenRuleImmediately(si, gi, ri, query, searchScope) {
    if (!isFullscreenRule(si, gi, ri)) return;
    syncFullscreenOverlay(query, searchScope);
}

function rerenderActiveFullscreenDiff(diffResult) {
    if (!isFullscreenDiff()) return;
    var ref = fullscreenCurrentRule();
    if (!shouldRenderFullscreenDiff(ref.si, ref.gi, ref.ri)) return;
    syncFullscreenOverlay();
}

function rerenderFullscreenAfterFeatureChange(si, gi, ri) {
    if (!isFullscreenOpen()) return false;
    var ref = fullscreenCurrentRule();
    if (!ref) return false;
    if (!sameRuleRefValues(ref, si, gi, ri) && !(isFullscreenDiff() && isActiveDiffRule(si, gi, ri))) {
        return false;
    }
    return syncFullscreenOverlay();
}
