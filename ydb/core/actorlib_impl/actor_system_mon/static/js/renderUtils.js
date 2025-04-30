function renderThreads(threads) {
    if (!threads || threads.length === 0) {
        return '<p>No thread data available.</p>';
    }

    let tableHtml = '<table class="table table-sm table-bordered table-hover thread-table"><thead><tr><th>#</th><th>Used CPU</th><th>Elapsed CPU</th><th>Parked CPU</th></tr></thead><tbody>';
    threads.forEach((thread, index) => {
        tableHtml += `
            <tr>
                <td>${index + 1}</td>
                <td>${formatCpu(thread.usedCpu)}</td>
                <td>${formatCpu(thread.elapsedCpu)}</td>
                <td>${formatCpu(thread.parkedCpu)}</td>
            </tr>`;
    });
    tableHtml += '</tbody></table>';
    return tableHtml;
}

function renderSharedThreads(sharedThreads) {
     if (!sharedThreads || sharedThreads.length === 0) {
        return '<p>No shared thread data available.</p>';
     }
     return `<p>Shared threads count: ${sharedThreads.length}. Rendering details TBD.</p>`;
} 