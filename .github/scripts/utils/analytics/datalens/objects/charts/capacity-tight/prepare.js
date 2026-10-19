const Dataset = require('libs/dataset/v2');

function loadRows() {
    try {
        const rows = Dataset.getDatasetRows({datasetName: 'data'});
        if (rows && rows.length) {
            return rows;
        }
    } catch (e) {
        console.log('[prepare] getDatasetRows failed', String(e));
    }
    const loaded = Editor.getLoadedData() || {};
    const block = loaded.data;
    if (!block || !block.result || !block.result.data) {
        return [];
    }
    const titles = (block.result.fields || []).map(function(field) { return field.title; });
    return (block.result.data.Data || []).map(function(row) {
        const obj = {};
        titles.forEach(function(title, i) { obj[title] = row[i]; });
        return obj;
    });
}

function num(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function ms(value) {
    if (value === null || value === undefined || value === '') {
        return null;
    }
    if (typeof value === 'number' && Number.isFinite(value)) {
        return value > 1e12 ? value : value * 1000;
    }
    const parsed = Date.parse(String(value));
    return Number.isFinite(parsed) ? parsed : null;
}

function esc(value) {
    return String(value == null ? '' : value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function firstParam(name, fallback) {
    const values = Editor.getParam(name);
    if (values && values.length && values[0] !== '') {
        return String(values[0]);
    }
    return fallback;
}

function isPostcommit(row) {
    return /postcommit/i.test(String(row.job_name || '')) || /postcommit/i.test(String(row.workflow || ''));
}

const hidePostcommit = firstParam('cap_pc', 'hide') !== 'show';
const zone = firstParam('cap_zone', 'ru-central1-d');

function scopedName(name) {
    return zone === 'all' ? name : name + '@' + zone;
}

function keepRunner(row) {
    return zone === 'all' || String(row.zone || '') === zone;
}

function stamp(value) {
    const t = ms(value);
    if (t == null) {
        return '';
    }
    return new Date(t).toISOString().slice(0, 16).replace('T', ' ');
}

const QUOTA_LABEL = {
    ram_gib: 'Free RAM',
    cores: 'Free vCPU',
    instances: 'Free VM slots',
    nrd_gib: 'Free SSD',
};
const QUOTA_UNIT = {
    ram_gib: 'GiB',
    cores: 'vCPU',
    instances: 'VMs',
    nrd_gib: 'GiB',
};

const rows = loadRows();
let latest = null;
rows.forEach(function(row) {
    const t = ms(row.sampled_at);
    if (t != null && (latest == null || t > latest)) {
        latest = t;
    }
});
const current = rows.filter(function(row) { return ms(row.sampled_at) === latest; });

function byName(kind) {
    const out = {};
    current.forEach(function(row) {
        if (row.kind === kind) {
            out[row.name] = row;
        }
    });
    return out;
}

const quota = byName('quota');
const fit = byName('fit');

function pct(value, limit) {
    const v = Number(value);
    const l = Number(limit);
    if (!Number.isFinite(v) || !Number.isFinite(l) || l <= 0) {
        return null;
    }
    const p = Math.max(0, v / l * 100);
    if (p === 0) {
        return '0%';
    }
    if (p < 0.05) {
        return '<0.1%';
    }
    return p.toFixed(1) + '%';
}

function profileKey(cores, ram) {
    const c = Math.round((num(cores) || 0) / 4) * 4;
    const r = Math.round((num(ram) || 0) / 16) * 16;
    if (c <= 0 || r <= 0) {
        return '';
    }
    return c + 'x' + r;
}

function leftoverAfterIdle(jobs, idles) {
    const pool = {};
    (idles || []).forEach(function(row) {
        const key = profileKey(row.cores, row.ram_gib);
        if (!key) {
            return;
        }
        pool[key] = (pool[key] || 0) + 1;
    });
    const leftover = {ram_gib: 0, cores: 0, nrd_gib: 0, instances: 0};
    let covered = 0;
    (jobs || []).forEach(function(row) {
        const n = Math.max(1, Math.round(num(row.usage_value) || 1));
        const key = profileKey(row.cores, row.ram_gib);
        let need = n;
        if (key && pool[key]) {
            const take = Math.min(need, pool[key]);
            pool[key] -= take;
            covered += take;
            need -= take;
        }
        if (need > 0) {
            leftover.instances += need;
            leftover.ram_gib += (num(row.ram_gib) || 0) * (need / n);
            leftover.cores += (num(row.cores) || 0) * (need / n);
            leftover.nrd_gib += (num(row.nrd_gib) || 0) * (need / n);
        }
    });
    return {leftover: leftover, covered: covered};
}

const queueJobsList = [];
let hiddenPostcommit = 0;
current.forEach(function(row) {
    if (row.kind !== 'queue') {
        return;
    }
    if (hidePostcommit && isPostcommit(row)) {
        hiddenPostcommit += 1;
        return;
    }
    queueJobsList.push(row);
});
const idleRunners = current.filter(function(row) {
    return row.kind === 'runner' && row.state === 'idle' && keepRunner(row);
});
const queueCover = leftoverAfterIdle(queueJobsList, idleRunners);
const queueAsked = queueCover.leftover;
const queueJobs = queueJobsList.reduce(function(acc, row) {
    return acc + Math.max(1, Math.round(num(row.usage_value) || 1));
}, 0);
const coveredJobs = queueCover.covered;
const leftoverJobs = Math.round(queueAsked.instances);

function card(title, free, used, limit, unit, asked) {
    const freeN = num(free);
    const usedN = num(used);
    const limitN = num(limit);
    const askedN = num(asked) || 0;
    const room = Math.max(0, (limitN || 0) - (usedN || 0));
    const shortN = Math.max(0, askedN - room);
    const usedPct = pct(usedN, limitN);
    const leftPct = pct(freeN, limitN);
    const shortPct = pct(shortN, limitN);
    const left = (freeN != null && limitN) ? Math.max(0, freeN / limitN * 100) : null;
    const tone = (shortN > 0 || (left != null && left < 10))
        ? '#E15759'
        : (left == null ? '#888' : (left < 25 ? '#F28E2B' : '#59A14F'));
    const usedLine = (usedPct || '—') + ' in use · ' + (leftPct || '—') + ' left' +
        (limitN == null ? '' : ' · limit ' + Math.round(limitN).toLocaleString('en-US'));
    let overflow = '';
    const coverNote = coveredJobs
        ? ' · ' + coveredJobs + ' covered by idle'
        : '';
    const needNote = leftoverJobs === 1 ? '1 job needs a new VM' : leftoverJobs + ' jobs need new VMs';
    if (queueJobs > 0 && leftoverJobs === 0) {
        overflow = '<div style="color:#59A14F;font-size:12px;margin-top:4px">all ' +
            queueJobs + ' waiting jobs covered by idle of the same size</div>';
    } else if (askedN > 0 && shortN > 0) {
        overflow = '<div style="color:#E15759;font-size:12px;margin-top:4px">leftover after idle overflows free quota by ' +
            Math.round(shortN).toLocaleString('en-US') + ' ' + esc(unit) +
            (shortPct ? ' (' + shortPct + ')' : '') + ' · ' + needNote + coverNote + '</div>';
    } else if (askedN > 0) {
        overflow = '<div style="color:#59A14F;font-size:12px;margin-top:4px">leftover after idle still fits in free quota: ' +
            Math.round(askedN).toLocaleString('en-US') + ' ' + esc(unit) +
            ' · ' + needNote + coverNote + '</div>';
    } else if (hiddenPostcommit) {
        overflow = '<div style="color:#888;font-size:12px;margin-top:4px">no PR queue · ' +
            hiddenPostcommit + ' postcommit hidden</div>';
    }
    return '<div style="flex:1;min-width:140px;padding:12px 14px;border:1px solid #e6e6e6;border-radius:8px">' +
        '<div style="color:#666;font-size:12px">' + esc(title) + '</div>' +
        '<div style="font-size:28px;font-weight:650;letter-spacing:-0.03em;color:' + tone + '">' +
        (freeN == null ? '—' : Math.round(freeN).toLocaleString('en-US')) +
        '<span style="font-size:14px;font-weight:500;color:#444"> ' + esc(unit) + '</span></div>' +
        '<div style="color:#777;font-size:12px">' + usedLine + '</div>' + overflow + '</div>';
}

const cards = ['ram_gib', 'cores', 'instances', 'nrd_gib'].map(function(name) {
    const row = quota[scopedName(name)] || {};
    const unit = QUOTA_UNIT[name] || '';
    return card(
        QUOTA_LABEL[name] || name,
        row.free_value,
        row.usage_value,
        row.limit_value,
        unit,
        queueAsked[name],
    );
}).join('');

function fitCard(name, title) {
    const row = fit[scopedName(name)] || {};
    const n = num(row.free_value);
    const tone = n == null ? '#888' : (n < 1 ? '#E15759' : (n < 4 ? '#F28E2B' : '#59A14F'));
    const shape = (row.cores ? Math.round(row.cores) + 'c' : '') + (row.ram_gib ? '/' + Math.round(row.ram_gib) + 'G' : '');
    return '<div style="flex:1;min-width:180px;padding:12px 14px;border:1px solid #e6e6e6;border-radius:8px">' +
        '<div style="color:#666;font-size:12px">' + esc(title) + '</div>' +
        '<div style="font-size:28px;font-weight:650;color:' + tone + '">' + (n == null ? '—' : Math.round(n)) +
        '<span style="font-size:14px;font-weight:500;color:#444"> more hosts fit</span></div>' +
        '<div style="color:#777;font-size:12px">one host is ' + esc(shape.replace('c', ' vCPU').replace('G', ' GiB RAM')) +
        '. The number is whichever quota runs out first: vCPU, RAM, VM slots, or SSD.</div></div>';
}

const html = '<div style="padding:12px 16px;font:13px/1.35 ui-sans-serif,system-ui,sans-serif;color:#222">' +
    '<div style="color:#666;margin-bottom:8px">Latest snapshot ' + esc(stamp(latest)) + ' UTC' +
    (zone === 'all' ? '' : '. Zone ' + esc(zone) + ' only') + '. ' +
    'Idle runners of the same size take waiting jobs first and do not need extra quota. ' +
    'Green/red is leftover jobs vs free quota, same list as Runners below' +
    (hidePostcommit ? ', without postcommit (selector above the tables).' : '.') +
    '</div>' +
    '<div style="display:flex;gap:8px;flex-wrap:wrap">' + cards + '</div>' +
    '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px">' +
    fitCard('fit.relwithdebinfo', 'relwithdebinfo') +
    fitCard('fit.release-asan', 'release-asan') +
    '</div></div>';

module.exports = {
    render: Editor.wrapFn({
        args: [{html: html}],
        fn: function(_options, cfg) {
            return Editor.generateHtml(cfg.html || '<div style="padding:16px">no capacity snapshots</div>');
        },
    }),
};
