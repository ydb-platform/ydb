if (typeof jQuery === 'undefined') {
    console.error('jQuery не загружен! Проверьте подключение библиотеки.');
    document.body.innerHTML = '<div style="padding: 20px; color: red; font-weight: bold;">Ошибка: jQuery не загружен. Проверьте консоль браузера для получения дополнительной информации.</div>';
} else {
    console.log('jQuery версия:', jQuery.fn.jquery);
}

let currentData = null;

const poolColors = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", 
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
    "#c49c94", "#f7b6d2", "#c7c7c7", "#dbdb8d", "#9edae5"
];

const poolColorMap = {};

const colorScheme = {
    budget: "#007bff",
    lostCpu: "#dc3545",
    freeSharedCpu: "#28a745",
    avgPingUs: "#fd7e14", 
    currentThreadCount: "#6f42c1",
    localQueueSize: "#20c997",
    totalCpu: "#fd7e14",
    totalThreads: "#20c997"
};

let selectedPools = new Set();
let visiblePools = new Set();

let dataContainer, loadingIndicator, errorContainer, refreshButton, levelSelect, iterationFrom, iterationTo, timeMode, timeFrom, timeTo, sortNewestFirstCheckbox;
let chartMetricSelect, poolSelect, poolMetricSelect, refreshChartButton;
let cpuMetricSelect, cpuValueSelect, refreshCpuButton, showAllPoolsCheckbox, poolCheckboxesContainer, poolSelectorContainer;

function recalculateChartSizes() {
    console.log("Форсированный пересчет размеров графиков...");
    
    const chartElements = document.querySelectorAll('svg[id$="Chart"]');
    
    chartElements.forEach(element => {
        element.style.display = 'block';
        element.style.visibility = 'visible';
        
        const rect = element.getBoundingClientRect();
        console.log(`Размеры элемента ${element.id}:`, {
            width: rect.width,
            height: rect.height
        });
    });
    
    if (currentData) {
        const activeTabId = document.querySelector('#mainTabs .nav-link.active').getAttribute('data-bs-target');
        
        if (activeTabId === '#chartsTab') {
            console.log('Перерисовка графиков на вкладке Метрики после изменения размера');
            renderMetricsChart();
            renderPoolChart();
        } else if (activeTabId === '#cpuTab') {
            console.log('Перерисовка графиков на вкладке CPU после изменения размера');
            renderCpuCharts();
        }
    }
}

$(document).ready(function() {
    console.log("Инициализация приложения...");
    
    if (typeof bootstrap === 'undefined') {
        console.error('Bootstrap не загружен! Проверьте подключение библиотеки bootstrap.js.');
        $('#mainTabsContent').before('<div class="alert alert-danger">Ошибка: Bootstrap JS не загружен. Проверьте консоль браузера для получения дополнительной информации.</div>');
    } else {
        console.log('Bootstrap доступен');
        
        const tabElements = document.querySelectorAll('#mainTabs button[data-bs-toggle="tab"]');
        if (tabElements.length > 0) {
            tabElements.forEach(tabEl => {
                tabEl.addEventListener('click', function(event) {
                    event.preventDefault();
                    const tabId = this.getAttribute('data-bs-target').substring(1);
                    console.log('Переключение на вкладку:', tabId);
                    
                    document.querySelectorAll('#mainTabs .nav-link').forEach(t => {
                        t.classList.remove('active');
                        t.setAttribute('aria-selected', 'false');
                    });
                    
                    document.querySelectorAll('#mainTabsContent .tab-pane').forEach(p => {
                        p.classList.remove('show', 'active');
                        p.classList.remove('fade');
                    });
                    
                    this.classList.add('active');
                    this.setAttribute('aria-selected', 'true');
                    
                    const targetPane = document.querySelector("#" + tabId);
                    if (targetPane) {
                        targetPane.classList.add('show', 'active');
                        
                        setTimeout(recalculateChartSizes, 50);
                        
                        if (currentData) {
                            if (tabId === 'chartsTab') {
                                console.log('Обновляем графики на вкладке Метрики');
                                setTimeout(() => {
                                    renderMetricsChart();
                                    renderPoolChart();
                                }, 100);
                            } else if (tabId === 'cpuTab') {
                                console.log('Обновляем графики на вкладке CPU');
                                setTimeout(() => {
                                    renderCpuCharts();
                                }, 100);
                            }
                        }
                    }
                    
                    const currentUrlParams = new URLSearchParams(window.location.search);
                    currentUrlParams.set('tab', tabId);
                    const newUrl = `${window.location.pathname}?${currentUrlParams.toString()}`;
                    history.pushState({tab: tabId}, '', newUrl);
                    console.log("URL обновлен:", newUrl);
                });
            });
            
            console.log('Вкладки инициализированы');
        }
    }
    
    dataContainer = $('#dataContainer');
    loadingIndicator = $('#loading');
    errorContainer = $('#error');
    refreshButton = $('#refreshButton');
    levelSelect = $('#levelSelect');
    iterationFrom = $('#iterationFrom');
    iterationTo = $('#iterationTo');
    timeMode = $('#timeMode');
    timeFrom = $('#timeFrom');
    timeTo = $('#timeTo');
    sortNewestFirstCheckbox = $('#sortNewestFirst');
    chartMetricSelect = $('#chartMetricSelect');
    poolSelect = $('#poolSelect');
    poolMetricSelect = $('#poolMetricSelect');
    refreshChartButton = $('#refreshChartButton');
    cpuMetricSelect = $('#cpuMetricSelect');
    cpuValueSelect = $('#cpuValueSelect');
    refreshCpuButton = $('#refreshCpuButton');
    showAllPoolsCheckbox = $('#showAllPools');
    poolCheckboxesContainer = $('#poolCheckboxes');
    poolSelectorContainer = $('#poolSelectorContainer');

    const urlParams = new URLSearchParams(window.location.search);
    const fromIterationParam = urlParams.get('fromIteration');
    const toIterationParam = urlParams.get('toIteration');
    const tabParam = urlParams.get('tab');

    if (fromIterationParam !== null) {
        iterationFrom.val(fromIterationParam);
        console.log("Установлено значение 'От итерации' из URL:", fromIterationParam);
    }
    if (toIterationParam !== null) {
        iterationTo.val(toIterationParam);
        console.log("Установлено значение 'До итерации' из URL:", toIterationParam);
    }

    const validTabIds = ['dataTab', 'chartsTab', 'cpuTab'];
    const defaultTabId = 'dataTab';
    let targetTabId = defaultTabId;

    if (tabParam && validTabIds.includes(tabParam)) {
        targetTabId = tabParam;
        console.log(`Активируем вкладку из URL: #${targetTabId}`);
    } else {
        console.log(`Активируем вкладку по умолчанию: #${defaultTabId}`);
    }

    document.querySelectorAll('#mainTabs .nav-link').forEach(t => {
        t.classList.remove('active');
        t.setAttribute('aria-selected', 'false');
    });
    document.querySelectorAll('#mainTabsContent .tab-pane').forEach(p => {
        p.classList.remove('show', 'active');
    });

    const targetTabButton = document.querySelector(`#mainTabs button[data-bs-target="#${targetTabId}"]`);
    const targetTabPane = document.getElementById(targetTabId);

    if (targetTabButton && targetTabPane) {
        targetTabButton.classList.add('active');
        targetTabButton.setAttribute('aria-selected', 'true');
        targetTabPane.classList.add('show', 'active');
    } else {
        console.warn(`Не удалось найти элементы для вкладки #${targetTabId}, активируем ${defaultTabId}`);
        document.querySelector(`#mainTabs button[data-bs-target="#${defaultTabId}"]`)?.classList.add('active');
        document.querySelector(`#mainTabs button[data-bs-target="#${defaultTabId}"]`)?.setAttribute('aria-selected', 'true');
        document.getElementById(defaultTabId)?.classList.add('show', 'active');
    }

    console.log("Проверка элементов формы:", {
        refreshButton: refreshButton.length > 0,
        levelSelect: levelSelect.length > 0,
        iterationFrom: iterationFrom.length > 0,
        iterationTo: iterationTo.length > 0,
        chartMetricSelect: chartMetricSelect.length > 0,
        cpuMetricSelect: cpuMetricSelect.length > 0
    });

    refreshButton.on('click', function() {
        console.log("Нажата кнопка 'Обновить'");
        fetchData();
    });
    
    levelSelect.on('change', function() {
        console.log("Изменен уровень детализации:", levelSelect.val());
        fetchData();
    });
    
    iterationFrom.on('change', function() {
        console.log("Изменено значение 'От итерации':", iterationFrom.val());
        fetchData();
        
        const currentUrlParams = new URLSearchParams(window.location.search);
        currentUrlParams.set('fromIteration', iterationFrom.val());
        const newUrl = `${window.location.pathname}?${currentUrlParams.toString()}`;
        history.pushState(null, '', newUrl);
        console.log("URL обновлен (fromIteration):");
    });
    
    iterationTo.on('change', function() {
        const toValue = iterationTo.val();
        console.log("Изменено значение 'До итерации':", toValue);
        fetchData();
        
        const currentUrlParams = new URLSearchParams(window.location.search);
        if (toValue) {
            currentUrlParams.set('toIteration', toValue);
        } else {
            currentUrlParams.delete('toIteration');
        }
        const newUrl = `${window.location.pathname}?${currentUrlParams.toString()}`;
        history.pushState(null, '', newUrl);
        console.log("URL обновлен (toIteration):");
    });
    
    timeMode.on('change', function() {
        const isTimeMode = $(this).is(':checked');
        $('#iterationMode').toggle(!isTimeMode);
        $('#timeMode').toggle(isTimeMode);
        fetchData();
    });
    
    timeFrom.on('change', function() {
        console.log("Изменено значение 'От времени':", timeFrom.val());
        fetchData();
    });
    
    timeTo.on('change', function() {
        console.log("Изменено значение 'До времени':", timeTo.val());
        fetchData();
    });
    
    sortNewestFirstCheckbox.on('change', function() {
        console.log("Изменен порядок сортировки, новые сверху:", sortNewestFirstCheckbox.prop('checked'));
        if (currentData) {
            renderData(currentData);
        }
    });
    
    chartMetricSelect.on('change', function() {
        console.log("Выбрана метрика:", chartMetricSelect.val());
        renderMetricsChart();
    });
    
    poolSelect.on('change', function() {
        console.log("Выбран пул:", poolSelect.val());
        renderPoolChart();
    });
    
    poolMetricSelect.on('change', function() {
        console.log("Выбрана метрика пула:", poolMetricSelect.val());
        renderPoolChart();
    });
    
    refreshChartButton.on('click', function() {
        console.log("Нажата кнопка 'Обновить график'");
        fetchData();
    });
    
    cpuMetricSelect.on('change', function() {
        console.log("Выбран тип CPU метрики:", cpuMetricSelect.val());
        renderCpuCharts();
    });
    
    cpuValueSelect.on('change', function() {
        console.log("Выбрано значение CPU:", cpuValueSelect.val());
        renderCpuCharts();
    });
    
    refreshCpuButton.on('click', function() {
        console.log("Нажата кнопка 'Обновить' для CPU графиков");
        fetchData();
    });
    
    showAllPoolsCheckbox.on('change', function() {
        console.log("Изменен режим отображения всех пулов:", showAllPoolsCheckbox.prop('checked'));
        if ($(this).is(':checked')) {
            poolSelectorContainer.hide();
        } else {
            poolSelectorContainer.show();
        }
        
        renderCpuCharts();
    });

    poolCheckboxesContainer.on('change', '.pool-checkbox', function() {
        const poolName = $(this).data('pool');
        
        if ($(this).is(':checked')) {
            selectedPools.add(poolName);
        } else {
            selectedPools.delete(poolName);
        }
        
        renderCpuCharts();
    });
    
    // Обработчик для кнопок сворачивания пулов (делегирование событий)
    dataContainer.on('click', '.pool-toggle-button', function() {
        const targetId = $(this).data('bs-target');
        const targetBody = $(targetId);
        const isExpanded = $(this).attr('aria-expanded') === 'true';

        if (isExpanded) {
            targetBody.collapse('hide');
            $(this).attr('aria-expanded', 'false');
            $(this).text('+');
        } else {
            targetBody.collapse('show');
            $(this).attr('aria-expanded', 'true');
            $(this).text('-');
        }
    });
    
    let resizeTimeout;
    $(window).on('resize', function() {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(function() {
            console.log("Изменен размер окна, пересчитываем графики");
            recalculateChartSizes();
        }, 250);
    });
    
    console.log("Загружаем начальные данные...");
    fetchData();
}); 