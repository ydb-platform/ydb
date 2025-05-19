function renderThreads(threads, poolIdx, sharedThreads) {
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

function renderPoolSharedThreads(shared, poolIdx) {
    let sharedThreads = shared.threads;
    let threadOwners = shared.threadOwners;
    if (!sharedThreads || sharedThreads.length === 0) {
        return '<p>No shared thread data available.</p>';
    }
    let tableHtml = '<table class="table table-sm table-bordered table-hover thread-table"><thead><tr><th>#</th><th>Used CPU</th><th>Elapsed CPU</th><th>Parked CPU</th></tr></thead><tbody>';
    sharedThreads.forEach((thread, index) => {
        tableHtml += "<tr><td>";
        if (threadOwners[index] === poolIdx) {
            tableHtml += `<strong>${index + 1}</strong>`;
        } else {
            tableHtml += `${index + 1}`;
        }
        tableHtml += "</td>";
        tableHtml += `
                <td>${formatCpu(thread.byPool[poolIdx].usedCpu)}</td>
                <td>${formatCpu(thread.byPool[poolIdx].elapsedCpu)}</td>
                <td>${formatCpu(thread.byPool[poolIdx].parkedCpu)}</td>
            </tr>`;
    });
    tableHtml += '</tbody></table>';
    return tableHtml;
}

function renderSharedThreads(shared) {
    let sharedThreads = shared.threads;
    let threadOwners = shared.threadOwners;
    if (!sharedThreads || sharedThreads.length === 0) {
        return '<p>No shared thread data available.</p>';
     }

    let tableHtml = '<table class="table table-sm table-bordered table-hover thread-table"><thead><tr><th>#</th><th>Pool Owner</th><th>Total Used CPU</th><th>Total Parked CPU</th></tr></thead><tbody>';
    let overallTotalUsedCpu = 0;
    let overallTotalParkedCpu = 0;
    let overallTotalLastSecondUsedCpu = 0;
    let overallTotalLastSecondParkedCpu = 0;

    sharedThreads.forEach((thread, index) => {
        let threadTotalUsedCpu = 0;
        let threadTotalParkedCpu = 0;
        let threadTotalLastSecondUsedCpu = 0;
        let threadTotalLastSecondParkedCpu = 0;

        if (thread.byPool && thread.byPool.length > 0) {
            thread.byPool.forEach(poolData => {
                threadTotalUsedCpu += poolData.usedCpu.cpu || 0;
                threadTotalParkedCpu += poolData.parkedCpu.cpu || 0;
                threadTotalLastSecondUsedCpu += poolData.usedCpu.lastSecondCpu || 0;
                threadTotalLastSecondParkedCpu += poolData.parkedCpu.lastSecondCpu || 0;
            });
        }

        overallTotalUsedCpu += threadTotalUsedCpu;
        overallTotalParkedCpu += threadTotalParkedCpu;
        overallTotalLastSecondUsedCpu += threadTotalLastSecondUsedCpu;
        overallTotalLastSecondParkedCpu += threadTotalLastSecondParkedCpu;

        tableHtml += `
            <tr>
                <td>${index + 1}</td>
                <td>${threadOwners[index]}</td>
                <td>${rawFormatCpu(threadTotalUsedCpu, threadTotalLastSecondUsedCpu)}</td>
                <td>${rawFormatCpu(threadTotalParkedCpu, threadTotalLastSecondParkedCpu)}</td>
            </tr>`;
    });

    // Add a summary row for totals
    tableHtml += `
        <tr class="table-info">
            <td><strong>Total</strong></td>
            <td></td>
            <td><strong>${rawFormatCpu(overallTotalUsedCpu, overallTotalLastSecondUsedCpu)}</strong></td>
            <td><strong>${rawFormatCpu(overallTotalParkedCpu, overallTotalLastSecondParkedCpu)}</strong></td>
        </tr>
    `;

    tableHtml += '</tbody></table>';
    return tableHtml;
} 