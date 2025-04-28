// Функция для обновления всех графиков
function updateAllCharts() {
    if (!currentData) return;
    
    if ($("#charts-tab").hasClass('active')) {
        renderMetricsChart(); // Вызов из charts.js
        renderPoolChart();    // Вызов из charts.js
    }
    
    if ($("#cpu-tab").hasClass('active')) {
        renderCpuCharts();    // Вызов из charts.js
    }
}

function renderData(data) {
    dataContainer.empty(); // Очищаем предыдущие данные
    if (!data || !data.history || data.history.length === 0) {
        dataContainer.html('<div class="alert alert-warning">No history data found.</div>');
        return;
    }

    // Сохраняем данные для графиков
    currentData = data;
    
    // Назначаем цвета для пулов
    assignPoolColors(data); // Вызов из data.js

    // Выбираем порядок отображения итераций (по умолчанию от старых к новым)
    let historyItems = [...data.history]; // Копируем массив
    
    // Если выбрана опция "новые сверху", то переворачиваем массив
    if (sortNewestFirstCheckbox.prop('checked')) {
        historyItems.reverse();
    }

    // Обновляем список пулов для графиков
    updatePoolSelectAndCheckboxes(data);

    historyItems.forEach((item, idx) => {
        const iterationCard = $(`
            <div class="card iteration-card">
                <div class="card-header iteration-card-header">
                    Iteration ${item.iteration} (Timestamp: ${item.timestamp})
                </div>
                <div class="card-body iteration-card-body">
                    <p><strong>Budget:</strong> ${item.budget?.toFixed(5) ?? 'N/A'}</p>
                    <p><strong>Lost CPU:</strong> ${item.lostCpu ?? 'N/A'}</p>
                    <p><strong>Free Shared CPU:</strong> ${item.freeSharedCpu ?? 'N/A'}</p>
                    <div class="pools-container mt-3"></div>
                    <div class="shared-pool-container mt-3"></div>
                </div>
            </div>
        `);

        const poolsContainer = iterationCard.find('.pools-container');
        const sharedPoolContainer = iterationCard.find('.shared-pool-container');

        if (item.pools && item.pools.length > 0) {
            poolsContainer.append('<h5>Pools</h5>');
            item.pools.forEach(pool => {
                const poolCard = $(`
                    <div class="card pool-card">
                        <div class="card-header pool-card-header">
                            Pool: <strong>${pool.name || 'Unknown'}</strong>
                            <small class="text-muted float-end">Op: ${pool.operation || 'N/A'}</small>
                        </div>
                        <div class="card-body pool-card-body">
                            <div class="row">
                                <div class="col-md-6">
                                    <p><strong>Threads:</strong> ${pool.currentThreadCount ?? 'N/A'} / ${pool.potentialMaxThreadCount ?? 'N/A'} (Pot. Max)</p>
                                    ${pool.defaultThreadCount !== undefined ? `
                                    <p><strong>Config Threads:</strong> ${pool.minThreadCount}-${pool.defaultThreadCount}-${pool.maxThreadCount} (Min-Def-Max)</p>
                                    <p><strong>Priority:</strong> ${pool.priority}</p>
                                    ` : ''}
                                    <p><strong>Queue Size:</strong> ${pool.localQueueSize ?? 'N/A'}</p>
                                    ${pool.minLocalQueueSize !== undefined ? `
                                    <p><strong>Queue Limits:</strong> ${pool.minLocalQueueSize}-${pool.maxLocalQueueSize} (Min-Max)</p>
                                    ` : ''}
                                </div>
                                <div class="col-md-6">
                                    <p><strong>Avg Ping (us):</strong> ${pool.avgPingUs ?? 'N/A'} (Small Window: ${pool.avgPingUsWithSmallWindow ?? 'N/A'}, Max: ${pool.maxAvgPingUs ?? 'N/A'})</p>
                                    <p>
                                        <strong>Status:</strong>
                                        ${pool.isNeedy ? '<span class="badge bg-warning text-dark">Needy</span> ' : ''}
                                        ${pool.isStarved ? '<span class="badge bg-danger">Starved</span> ' : ''}
                                        ${pool.isHoggish ? '<span class="badge bg-success">Hoggish</span> ' : ''}
                                        ${!(pool.isNeedy || pool.isStarved || pool.isHoggish) ? 'Normal' : ''}
                                    </p>
                                </div>
                            </div>
                            <div class="threads-container mt-3"></div>
                        </div>
                    </div>
                `);

                if (pool.threads && pool.threads.length > 0) {
                    poolCard.find('.threads-container').append('<h6>Threads</h6>').append(renderThreads(pool.threads)); // Вызов из renderUtils.js
                }

                poolsContainer.append(poolCard);
            });
        }

        if (item.shared && item.shared.threads && levelSelect.val() === 'thread') { // Отображаем shared только на уровне thread
            sharedPoolContainer.append('<h5>Shared Pool Threads</h5>');
            sharedPoolContainer.append(renderSharedThreads(item.shared.threads)); // Вызов из renderUtils.js
        }

        dataContainer.append(iterationCard);
    });
}

function updatePoolSelectAndCheckboxes(data) {
    if (!data || !data.history || !data.history[0] || !data.history[0].pools) {
        return;
    }
    
    // Обновляем дропдаун для одиночного пула
    poolSelect.empty();
    
    // Получаем уникальный список пулов из первой итерации
    const pools = data.history[0].pools;
    pools.forEach(pool => {
        if (pool.name) {
            poolSelect.append($('<option>').val(pool.name).text(pool.name));
        }
    });
    
    // Обновляем чекбоксы для множественного выбора пулов
    poolCheckboxesContainer.empty();
    
    // Если это первая загрузка, добавляем все пулы в selectedPools
    if (selectedPools.size === 0) {
        pools.forEach(pool => {
            if (pool.name) {
                selectedPools.add(pool.name);
                visiblePools.add(pool.name);
            }
        });
    }
    
    // Создаем чекбоксы
    pools.forEach(pool => {
        if (pool.name) {
            const color = poolColorMap[pool.name] || "#000000";
            const isChecked = selectedPools.has(pool.name);
            
            const checkbox = $(`
                <div class="pool-checkbox-wrapper" style="border: 1px solid ${color};">
                    <input type="checkbox" class="pool-checkbox" data-pool="${pool.name}" ${isChecked ? 'checked' : ''}>
                    <span class="pool-color-indicator" style="background-color: ${color};"></span>
                    <span>${pool.name}</span>
                </div>
            `);
            
            poolCheckboxesContainer.append(checkbox);
        }
    });
    
    // Настраиваем обработчики для чекбоксов (перенесены в app.js)
    // $('.pool-checkbox').on('change', function() { ... });
    
    // Если выбран пул в одиночном дропдауне, перерисовываем его график
    if (poolSelect.val()) {
        renderPoolChart(); // Вызов из charts.js
    }
} 