(function () {
    var cache = {};
    var loading = false;

    function bindSort(table) {
        if (!table || !table.tHead || !table.tHead.rows.length) {
            return;
        }
        table.tHead.rows[0].onclick = function (ev) {
            var th = ev.target.closest('th[data-sort]');
            if (!th) {
                return;
            }
            var key = th.getAttribute('data-sort');
            var asc = th.getAttribute('data-asc') !== '1';
            var headers = table.tHead.rows[0].querySelectorAll('th[data-sort]');
            for (var i = 0; i < headers.length; i++) {
                if (headers[i] !== th) {
                    headers[i].removeAttribute('data-asc');
                }
            }
            th.setAttribute('data-asc', asc ? '1' : '0');
            var tbody = table.tBodies[0];
            var rows = Array.prototype.slice.call(tbody.rows);
            rows.sort(function (a, b) {
                var av = Number(a.dataset[key] || 0);
                var bv = Number(b.dataset[key] || 0);
                return asc ? av - bv : bv - av;
            });
            rows.forEach(function (r) {
                tbody.appendChild(r);
            });
        };
    }

    function currentDbg() {
        var dbg = document.getElementById('vcDbgFilter');
        return dbg ? dbg.value : '';
    }

    function applyVisibility() {
        var show = document.getElementById('vcShowVChunks');
        var body = document.getElementById('vcVChunksBody');
        if (!show || !body) {
            return;
        }
        body.classList.toggle('lat-hidden', !show.checked);
    }

    function bindVChunkTable() {
        bindSort(document.getElementById('vcVChunksTable'));
    }

    function selectPrompt() {
        return "<div class='alert alert-info'>Select a DBG to list its vchunks.</div>";
    }

    function setBody(html) {
        var body = document.getElementById('vcVChunksBody');
        if (!body) {
            return;
        }
        body.innerHTML = html;
        bindVChunkTable();
    }

    function fetchUrl(dbg) {
        var q = new URLSearchParams(location.search);
        q.set('page', 'vchunkcounters');
        q.set('showvchunks', '1');
        q.set('dbg', dbg);
        return location.pathname + '?' + q.toString();
    }

    function loadDbg(dbg) {
        if (!dbg) {
            setBody(selectPrompt());
            return;
        }
        if (cache[dbg]) {
            setBody(cache[dbg]);
            return;
        }
        if (loading) {
            return;
        }
        loading = true;
        setBody("<div class='alert alert-info'>Loading…</div>");
        fetch(fetchUrl(dbg), { credentials: 'same-origin', cache: 'no-store' })
            .then(function (r) {
                if (!r.ok) {
                    throw new Error('http ' + r.status);
                }
                return r.text();
            })
            .then(function (html) {
                var doc = new DOMParser().parseFromString(html, 'text/html');
                var next = doc.getElementById('vcVChunksBody');
                if (next) {
                    cache[dbg] = next.innerHTML;
                    setBody(next.innerHTML);
                }
            })
            .catch(function () {
                setBody("<div class='alert alert-warning'>Failed to load vchunk counters.</div>");
            })
            .then(function () {
                loading = false;
            });
    }

    function syncUrl() {
        var show = document.getElementById('vcShowVChunks');
        var q = new URLSearchParams(location.search);
        q.set('page', 'vchunkcounters');
        if (show && show.checked) {
            q.set('showvchunks', '1');
        } else {
            q.delete('showvchunks');
        }
        var dbg = currentDbg();
        if (dbg) {
            q.set('dbg', dbg);
        } else {
            q.delete('dbg');
        }
        var href = location.pathname + '?' + q.toString();
        if (href !== location.pathname + location.search) {
            history.replaceState(null, '', href);
        }
    }

    function onChange() {
        applyVisibility();
        syncUrl();
        var show = document.getElementById('vcShowVChunks');
        if (show && show.checked) {
            loadDbg(currentDbg());
        }
    }

    var show = document.getElementById('vcShowVChunks');
    var dbg = document.getElementById('vcDbgFilter');
    var body = document.getElementById('vcVChunksBody');
    if (show) {
        show.onchange = onChange;
    }
    if (dbg) {
        dbg.onchange = onChange;
    }
    applyVisibility();
    bindSort(document.getElementById('vcDbgTable'));
    bindVChunkTable();
    if (show && show.checked && currentDbg() && body &&
        document.getElementById('vcVChunksTable'))
    {
        cache[currentDbg()] = body.innerHTML;
    }
})();
