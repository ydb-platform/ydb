function getApiUrl() {
    const path = window.location.pathname;
    // Ищем префикс /node/<node_id>/
    const nodeMatch = path.match(/^(\/node\/\d+\/)/);
    const prefix = nodeMatch ? nodeMatch[1] : '/';
    const level = levelSelect.val();
    // Исправляем: берем значение напрямую из #lastIterations
    const iterCount = $('#lastIterations').val() || 30; // Значение по умолчанию

    // Формируем URL с параметрами
    const url = `${prefix}actors/actor_system?level=${level}&last_iteration=${iterCount}`;
    console.log("Сформирован URL запроса:", url);
    return url;
}

function formatCpu(cpuData) {
    if (!cpuData) return 'N/A';
    return `CPU: ${cpuData.cpu?.toFixed(4) ?? 'N/A'} (Last Sec: ${cpuData.lastSecondCpu?.toFixed(4) ?? 'N/A'})`;
} 