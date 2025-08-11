import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import os
import sys
from datetime import datetime
from collections import defaultdict
from prettytable import PrettyTable

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
if not GITHUB_TOKEN:
    print("Ошибка: Не установлена переменная окружения GITHUB_TOKEN")
    print("Установите её перед запуском скрипта:")
    print("export GITHUB_TOKEN='ваш_токен'")
    sys.exit(1)
OWNER = "ydb-platform"
REPO = "ydb"

# Настройка сессии с retry-стратегией
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

WORKFLOW_STATUSES = ["queued", "in_progress", "waiting"]
JOB_STATUSES = ["queued", "in_progress", "waiting", "pending", "started"]

def make_request(url, params=None):
    """Выполнить HTTP-запрос с обработкой ошибок"""
    max_attempts = 3
    attempt = 0
    while attempt < max_attempts:
        try:
            response = session.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            attempt += 1
            if attempt == max_attempts:
                print(f"Ошибка при запросе {url}: {str(e)}")
                return None
            print(f"Попытка {attempt} из {max_attempts} не удалась. Повторная попытка через {attempt * 2} секунд...")
            time.sleep(attempt * 2)

def get_workflows():
    """Получить список всех workflows в репозитории"""
    print("\nПолучаем список workflows...")
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/actions/workflows"
    params = {"per_page": 100}
    
    response = make_request(url, params)
    if response:
        workflows = response.get("workflows", [])
        print(f"Найдено {len(workflows)} workflows")
        return workflows
    return []

def get_workflow_runs(workflow_id, workflow_name):
    """Получить runs для конкретного workflow"""
    print(f"\nПолучаем runs для workflow '{workflow_name}'...")
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/actions/workflows/{workflow_id}/runs"
    params = {"per_page": 100}
    
    response = make_request(url, params)
    if response:
        runs = response.get("workflow_runs", [])
        active_runs = [run for run in runs if run["status"] in WORKFLOW_STATUSES]
        print(f"Найдено {len(active_runs)} активных runs")
        return active_runs
    return []

def get_run_jobs(run_id, workflow_name):
    """Получить jobs для конкретного run"""
    print(f"  Получаем jobs для run {run_id} (workflow: '{workflow_name}')...")
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/actions/runs/{run_id}/jobs"
    params = {"per_page": 100}
    
    response = make_request(url, params)
    if response:
        jobs = response.get("jobs", [])
        print(f"  Найдено {len(jobs)} jobs")
        return jobs
    return []

def get_pr_check_details():
    """Получить детальную информацию по PR-check"""
    print("\nПолучаем детальную информацию по PR-check...")
    
    workflows = get_workflows()
    pr_check_workflow = next((wf for wf in workflows if wf["name"] == "PR-check"), None)
    
    if not pr_check_workflow:
        print("PR-check workflow не найден")
        return
    
    runs = get_workflow_runs(pr_check_workflow["id"], "PR-check")
    grouped_jobs = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    
    for run in runs:
        jobs = get_run_jobs(run["id"], "PR-check")
        run_date = datetime.strptime(run["created_at"], "%Y-%m-%dT%H:%M:%SZ")
        
        for job in jobs:
            if job["status"] in JOB_STATUSES:
                job_info = {
                    "job_url": job["html_url"],
                    "job_status": job["status"],
                    "run_status": run["status"],
                    "run_url": run["html_url"],
                    "created_at": run_date,
                    "run_id": run["id"]
                }
                grouped_jobs[job["name"]][job["status"]][run_date.date()].append(job_info)
        
        time.sleep(0.5)
    
    # Детальная таблица
    detail_table = PrettyTable()
    detail_table.field_names = ["Job Name", "Status", "Date", "Job URL", "Run Status", "Run URL"]
    detail_table.max_width = 50
    detail_table.align = "l"
    
    for job_name in sorted(grouped_jobs.keys()):
        for status in JOB_STATUSES:
            if status in grouped_jobs[job_name]:
                for date in sorted(grouped_jobs[job_name][status].keys()):
                    for job_info in grouped_jobs[job_name][status][date]:
                        detail_table.add_row([
                            job_name[:47] + "..." if len(job_name) > 50 else job_name,
                            status,
                            date.strftime("%Y-%m-%d"),
                            job_info["job_url"],
                            job_info["run_status"],
                            job_info["run_url"]
                        ])
    
    print("\nДетальная информация по PR-check:")
    print(detail_table)
    
    # Сводная статистика
    summary = defaultdict(lambda: defaultdict(int))
    for job_name in grouped_jobs:
        for status in grouped_jobs[job_name]:
            for date in grouped_jobs[job_name][status]:
                summary[job_name][status] += len(grouped_jobs[job_name][status][date])
    
    summary_table = PrettyTable()
    summary_table.field_names = ["Job Name"] + JOB_STATUSES
    summary_table.align = "l"
    
    for job_name in sorted(summary.keys()):
        row = [job_name[:47] + "..." if len(job_name) > 50 else job_name]
        for status in JOB_STATUSES:
            row.append(summary[job_name][status])
        summary_table.add_row(row)
    
    print("\nСводная статистика по PR-check jobs:")
    print(summary_table)

def generate_summary_tables():
    """Генерация сводных таблиц по всем workflows и jobs"""
    workflows = get_workflows()
    if not workflows:
        print("Нет доступных workflows")
        return

    workflow_stats = defaultdict(lambda: {status: 0 for status in WORKFLOW_STATUSES})
    job_stats = defaultdict(lambda: {status: 0 for status in JOB_STATUSES})

    for i, wf in enumerate(workflows, 1):
        print(f"\nОбработка workflow {i}/{len(workflows)}: '{wf['name']}'")
        runs = get_workflow_runs(wf["id"], wf["name"])
        
        for run in runs:
            workflow_stats[wf["name"]][run["status"]] += 1
            
            jobs = get_run_jobs(run["id"], wf["name"])
            for job in jobs:
                if job["status"] in JOB_STATUSES:
                    job_stats[wf["name"]][job["status"]] += 1
            
            time.sleep(0.5)

    # Таблица workflows
    workflow_table = PrettyTable()
    workflow_table.field_names = ["Workflow Name"] + WORKFLOW_STATUSES
    
    # Таблица jobs
    job_table = PrettyTable()
    job_table.field_names = ["Workflow Name"] + JOB_STATUSES

    # Заполнение таблиц
    for wf_name in workflow_stats:
        stats = workflow_stats[wf_name]
        if sum(stats.values()) > 0:
            workflow_table.add_row([wf_name] + [stats[status] for status in WORKFLOW_STATUSES])
        
        job_stat = job_stats[wf_name]
        if sum(job_stat.values()) > 0:
            job_table.add_row([wf_name] + [job_stat[status] for status in JOB_STATUSES])

    print("\nСводка по активным workflow runs:")
    print(workflow_table)
    print("\nСводка по активным jobs:")
    print(job_table)
    
    # Детальная информация по PR-check
    get_pr_check_details()

def main():
    try:
        print("Сбор статистики по активным workflow runs и jobs...")
        generate_summary_tables()
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {str(e)}")

if __name__ == "__main__":
    main()
