(function () {
    var cb = document.getElementById('latencyAutoRefresh');
    var inp = document.getElementById('latencyRefreshRate');
    var key = 'pdLatencyAutoRefresh';
    var rateKey = 'pdLatencyRefreshRate';
    var uiKey = 'pdLatencyUiState';
    var timer = null;
    var refreshing = false;
    var live = document.getElementById('latencyLiveContent');
    var opNames = [];
    try {
        opNames = JSON.parse((live && live.getAttribute('data-op-names')) || '[]');
    } catch (e) {
        opNames = [];
    }

    function val(id) {
        var el = document.getElementById(id);
        return el ? el.value : '';
    }
    function checked(id) {
        var el = document.getElementById(id);
        return !!(el && el.checked);
    }
    function setChecked(id, v) {
        var el = document.getElementById(id);
        if (el) el.checked = !!v;
    }
    function setVal(id, v) {
        var el = document.getElementById(id);
        if (el) {
            for (var i = 0; i < el.options.length; i++) {
                if (el.options[i].value === v) {
                    el.value = v;
                    return;
                }
            }
        }
    }
    function saveUi() {
        var s = {
            showSlots: checked('latShowSlots'),
            slotNode: val('latSlotNodeFilter'),
            showDetail: checked('latShowDetail'),
            fNode: val('latFilterNode'),
            fPdisk: val('latFilterPdisk'),
            fType: val('latFilterType'),
            fOp: val('latFilterOp')
        };
        sessionStorage.setItem(uiKey, JSON.stringify(s));
        return s;
    }
    function loadUi() {
        try {
            return JSON.parse(sessionStorage.getItem(uiKey) || '{}');
        } catch (e) {
            return {};
        }
    }
    function restoreUi(s) {
        if (!s) s = loadUi();
        setChecked('latShowSlots', s.showSlots);
        setVal('latSlotNodeFilter', s.slotNode || '');
        setChecked('latShowDetail', s.showDetail);
        setVal('latFilterNode', s.fNode || '');
        setVal('latFilterPdisk', s.fPdisk || '');
        setVal('latFilterType', s.fType || '');
        setVal('latFilterOp', s.fOp || '');
    }
    function applySlots() {
        var show = document.getElementById('latShowSlots');
        var body = document.getElementById('latSlotsBody');
        var nodeSel = document.getElementById('latSlotNodeFilter');
        if (!show || !body || !nodeSel) return;
        body.classList.toggle('lat-hidden', !show.checked);
        var filter = nodeSel.value;
        var nodes = body.querySelectorAll('.lat-slot-node');
        for (var i = 0; i < nodes.length; i++) {
            var n = nodes[i];
            n.classList.toggle('lat-hidden', !!(filter && n.dataset.node !== filter));
        }
    }
    function applyDetail() {
        var show = document.getElementById('latShowDetail');
        var body = document.getElementById('latDetailBody');
        var table = document.getElementById('latencyDetailTable');
        if (!show || !body) return;
        body.classList.toggle('lat-hidden', !show.checked);
        if (!show.checked || !table) return;
        var tbody = table.tBodies[0];
        var fNode = document.getElementById('latFilterNode');
        var fPdisk = document.getElementById('latFilterPdisk');
        var fType = document.getElementById('latFilterType');
        var fOp = document.getElementById('latFilterOp');
        var rows = tbody.rows;
        for (var i = 0; i < rows.length; i++) {
            var r = rows[i];
            var ok = (!fNode.value || r.dataset.node === fNode.value)
                && (!fPdisk.value || r.dataset.pdisk === fPdisk.value)
                && (!fType.value || r.dataset.type === fType.value)
                && (!fOp.value || r.dataset.op === fOp.value);
            r.classList.toggle('lat-hidden', !ok);
        }
    }
    function bindUi() {
        var showSlots = document.getElementById('latShowSlots');
        var slotNode = document.getElementById('latSlotNodeFilter');
        var showDetail = document.getElementById('latShowDetail');
        var table = document.getElementById('latencyDetailTable');
        if (showSlots) {
            showSlots.onchange = function () { saveUi(); applySlots(); };
        }
        if (slotNode) {
            slotNode.onchange = function () { saveUi(); applySlots(); };
        }
        if (showDetail) {
            showDetail.onchange = function () { saveUi(); applyDetail(); };
        }
        ['latFilterNode', 'latFilterPdisk', 'latFilterType', 'latFilterOp'].forEach(function (id) {
            var el = document.getElementById(id);
            if (el) {
                el.onchange = function () { saveUi(); applyDetail(); };
            }
        });
        if (table) {
            table.tHead.rows[0].onclick = function (ev) {
                var th = ev.target.closest('th[data-sort]');
                if (!th) return;
                var key = th.getAttribute('data-sort');
                var asc = th.getAttribute('data-asc') !== '1';
                th.setAttribute('data-asc', asc ? '1' : '0');
                var tbody = table.tBodies[0];
                var rows = Array.prototype.slice.call(tbody.rows);
                rows.sort(function (a, b) {
                    var av = Number(a.dataset[key] || 0), bv = Number(b.dataset[key] || 0);
                    return asc ? av - bv : bv - av;
                });
                rows.forEach(function (r) { tbody.appendChild(r); });
            };
        }
        applySlots();
        applyDetail();
    }
    function selectedP() {
        var a = document.querySelector('a.lat-nav[data-p].btn-primary');
        return a ? a.getAttribute('data-p') : '99';
    }
    function selectedOp() {
        var a = document.querySelector('a.lat-nav[data-op].btn-primary');
        return a ? a.getAttribute('data-op') : '';
    }
    function pTitle(p) {
        return p === 'max' ? 'max' : ('p' + p);
    }
    function fmtUs(us) {
        us = Number(us) || 0;
        if (us === 0) return '0';
        if (us < 1000) return us + 'us';
        if (us < 1000000) return (us / 1000).toFixed(3) + 'ms';
        return (us / 1000000).toFixed(3) + 's';
    }
    function latColor(us) {
        us = Number(us) || 0;
        if (us === 0) return '#f0f0f0';
        if (us < 500) return '#90ee90';
        if (us < 1000) return '#228b22';
        if (us < 5000) return '#ffd54f';
        if (us < 20000) return '#e74c3c';
        return '#8b0000';
    }
    function latWidth(us) {
        us = Number(us) || 0;
        if (us === 0) return 0;
        return Math.min(100, Math.max(4, Math.floor(us * 100 / 20000)));
    }
    function pickUs(stats, p) {
        if (p === '50') return Number(stats.p50);
        if (p === '90') return Number(stats.p90);
        if (p === 'max') return Number(stats.max);
        return Number(stats.p99);
    }
    function paintBar(el, p) {
        var stats = {
            p50: el.dataset.p50,
            p90: el.dataset.p90,
            p99: el.dataset.p99,
            max: el.dataset.max
        };
        var us = pickUs(stats, p);
        var text = el.querySelector('.lat-bar-text');
        var fill = el.querySelector('.lat-bar-fill');
        if (text) text.textContent = fmtUs(us);
        if (fill) {
            fill.style.width = latWidth(us) + '%';
            fill.style.background = latColor(us);
        }
    }
    function barHtml(stats, p) {
        var us = pickUs(stats, p);
        var title = 'n=' + stats.c + ' min=' + fmtUs(stats.min)
            + ' p50=' + fmtUs(stats.p50) + ' p90=' + fmtUs(stats.p90)
            + ' p99=' + fmtUs(stats.p99) + ' max=' + fmtUs(stats.max);
        return "<div class='lat-bar'"
            + " data-count='" + stats.c + "' data-min='" + stats.min + "'"
            + " data-p50='" + stats.p50 + "' data-p90='" + stats.p90 + "'"
            + " data-p99='" + stats.p99 + "' data-max='" + stats.max + "'"
            + " title='" + title + "'>"
            + "<div class='lat-bar-text'>" + fmtUs(us) + "</div>"
            + "<div class='lat-bar-track'>"
            + "<div class='lat-bar-fill' style='width:"
            + latWidth(us) + "%; background:" + latColor(us) + ";'></div>"
            + "</div></div>";
    }
    function slotStats(ops, op) {
        if (op !== '' && op != null) {
            var i = Number(op);
            return (ops && ops[i]) ? ops[i] : null;
        }
        var worst = null;
        for (var i = 0; i < (ops || []).length; i++) {
            var s = ops[i];
            if (!s) continue;
            if (!worst || Number(s.p99) > Number(worst.p99)) worst = s;
        }
        return worst;
    }
    function redrawViews() {
        var p = selectedP();
        var op = selectedOp();
        var title = document.getElementById('latHeatmapTitle');
        if (title) title.textContent = 'Latency by node (' + pTitle(p) + ')';
        var desc = document.getElementById('latSlotDesc');
        if (desc) {
            var opPart = (op === '') ? ' (worst op by p99)' :
                (' for ' + (opNames[Number(op)] || op));
            desc.textContent = 'Each cell is one ddisk / pbuffer actor slot '
                + '(node:pdisk:slot), one pdisk per row. Uses the selected percentile'
                + opPart + '.';
        }
        var heat = document.getElementById('latHeatmapTable');
        if (heat) {
            var bars = heat.querySelectorAll('.lat-bar');
            for (var i = 0; i < bars.length; i++) paintBar(bars[i], p);
        }
        var slots = document.querySelectorAll('a.lat-slot');
        for (var i = 0; i < slots.length; i++) {
            var a = slots[i];
            var ops = [];
            try {
                ops = JSON.parse(a.getAttribute('data-ops') || '[]');
            } catch (e) {
                ops = [];
            }
            var stats = slotStats(ops, op);
            var box = a.querySelector('.lat-slot-val');
            if (!box) continue;
            if (!stats) {
                box.innerHTML = "<span class='lat-none'>-</span>";
            } else {
                box.innerHTML = barHtml(stats, p);
            }
        }
    }
    function setNavActive(a) {
        if (a.hasAttribute('data-p')) {
            document.querySelectorAll('a.lat-nav[data-p]').forEach(
                function (el) {
                    el.className = el === a
                        ? 'btn btn-primary btn-sm lat-nav lat-nav-btn'
                        : 'btn btn-default btn-sm lat-nav lat-nav-btn';
                });
        } else if (a.hasAttribute('data-op')) {
            document.querySelectorAll('a.lat-nav[data-op]').forEach(
                function (el) {
                    el.className = el === a
                        ? 'btn btn-primary btn-sm lat-nav lat-nav-btn'
                        : 'btn btn-default btn-sm lat-nav lat-nav-btn';
                });
        }
    }
    function syncNavFromUrl() {
        var q = new URLSearchParams(location.search);
        var p = q.get('p') || '99';
        var op = q.has('op') ? q.get('op') : '';
        document.querySelectorAll('a.lat-nav[data-p]').forEach(
            function (el) {
                el.className = (el.getAttribute('data-p') === p)
                    ? 'btn btn-primary btn-sm lat-nav lat-nav-btn'
                    : 'btn btn-default btn-sm lat-nav lat-nav-btn';
            });
        document.querySelectorAll('a.lat-nav[data-op]').forEach(
            function (el) {
                var v = el.getAttribute('data-op');
                el.className = (v === op)
                    ? 'btn btn-primary btn-sm lat-nav lat-nav-btn'
                    : 'btn btn-default btn-sm lat-nav lat-nav-btn';
            });
        redrawViews();
    }
    function refreshLive() {
        if (refreshing) return;
        refreshing = true;
        var ui = saveUi();
        fetch(location.href, { credentials: 'same-origin', cache: 'no-store' })
            .then(function (r) {
                if (!r.ok) throw new Error('http ' + r.status);
                return r.text();
            })
            .then(function (html) {
                var doc = new DOMParser().parseFromString(html, 'text/html');
                var next = doc.getElementById('latencyLiveContent');
                var cur = document.getElementById('latencyLiveContent');
                if (next && cur) { cur.innerHTML = next.innerHTML; }
                restoreUi(ui);
                bindUi();
            })
            .catch(function () { })
            .then(function () { refreshing = false; });
    }
    function schedule() {
        if (timer) { clearInterval(timer); timer = null; }
        if (!cb.checked) { return; }
        var sec = parseInt(inp.value, 10);
        if (!(sec > 0)) sec = 1;
        timer = setInterval(refreshLive, sec * 1000);
    }
    // Percentile / Slot grid operation: pushState + client redraw only.
    document.addEventListener('click', function (ev) {
        var a = ev.target.closest('a.lat-nav');
        if (!a) return;
        var liveEl = document.getElementById('latencyLiveContent');
        if (!liveEl || !liveEl.contains(a)) return;
        ev.preventDefault();
        var href = a.getAttribute('href');
        if (!href) return;
        setNavActive(a);
        if (href !== location.pathname + location.search) {
            history.pushState(null, '', href);
        }
        redrawViews();
    });
    window.addEventListener('popstate', function () { syncNavFromUrl(); });
    cb.checked = sessionStorage.getItem(key) === '1';
    var saved = sessionStorage.getItem(rateKey);
    if (saved) { inp.value = saved; }
    cb.addEventListener('change', function () {
        sessionStorage.setItem(key, cb.checked ? '1' : '0');
        if (cb.checked) { refreshLive(); }
        schedule();
    });
    inp.addEventListener('change', function () {
        sessionStorage.setItem(rateKey, inp.value);
        schedule();
    });
    restoreUi(loadUi());
    bindUi();
    schedule();
})();
