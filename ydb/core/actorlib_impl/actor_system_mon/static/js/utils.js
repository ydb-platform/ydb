function getApiUrl() {
    const path = window.location.pathname;
    const nodeMatch = path.match(/^(\/node\/\d+\/)/);
    const prefix = nodeMatch ? nodeMatch[1] : '/';
    const level = levelSelect.val();
    const iterCount = $('#lastIterations').val() || 30;

    const url = `${prefix}actors/actor_system?level=${level}&last_iteration=${iterCount}`;
    console.log("Сформирован URL запроса:", url);
    return url;
}

function formatCpu(cpuData) {
    if (!cpuData) return 'N/A';
    return rawFormatCpu(cpuData.cpu, cpuData.lastSecondCpu);
} 

function rawFormatCpu(cpuData, lastSecondCpu) {
    return `CPU: ${cpuData?.toFixed(4) ?? 'N/A'} (Last Sec: ${lastSecondCpu?.toFixed(4) ?? 'N/A'})`;
}
