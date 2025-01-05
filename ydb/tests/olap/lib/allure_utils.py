from __future__ import annotations
import allure
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from urllib.parse import urlencode
from datetime import datetime
from copy import deepcopy
from pytz import timezone


def _set_monitoring(test_info: dict[str, str], start_time: float, end_time: float) -> None:
    monitoring_start = int((start_time) * 1000)
    monitoring_end = int((end_time) * 1000)
    database = '/' + test_info.get('database', '*')
    # monitoring does not show intervals less 1 minute.
    monitoring_addition = 60000 - (monitoring_end - monitoring_start)
    if monitoring_addition > 0:
        monitoring_start -= monitoring_addition
        monitoring_end += monitoring_addition

    if len(YdbCluster.get_monitoring_urls()) > 0:
        test_info['monitoring'] = ', '.join([
            f"<a target='_blank' href='{monitoring.url.format(
                database=database,
                start_time=monitoring_start,
                end_time=monitoring_end
            )}'>{monitoring.caption}</a>"
            for monitoring in YdbCluster.get_monitoring_urls()
        ])


def _set_coredumps(test_info: dict[str, str]) -> None:
    if test_info['name'].startswith('ydb-k8s'):
        core_link = f"https://coredumps.n.yandex-team.ru/index?itype=kikimr&host_list={test_info['nodes_wilcard']}&show_fixed=True"
    else:
        core_link = f"https://kikimr-cores.n.yandex-team.ru/show?server={test_info['nodes_wilcard']}%2A"
    test_info['coredumps'] = f"<a target='_blank' href='{core_link}'>link</a>"


def _set_results_plot(test_info: dict[str, str], suite: str, test: str, refference_set: str) -> None:
    if not ResultsProcessor.send_results:
        return
    params = urlencode({
        'tab': 'o8',
        'suite_b2rp': suite,
        'test_ehyw': test,
        'db_fmdl': ResultsProcessor.get_cluster_id(),
        'cluster_dufr': refference_set
    })
    test_info['results_plot'] = f"<a target='_blank' href='https://datalens.yandex-team.ru/iqnd4b1miaz27-testy-ydb?{params}'>link</a>"


def _set_logs_command(test_info: dict[str, str], start_time: float, end_time: float):
    hosts = []
    for node in YdbCluster.get_cluster_nodes():
        if node.role == YdbCluster.Node.Role.STORAGE:
            hosts.append(node.host)
    hosts_cmd = ' '.join([f'-H {h}' for h in hosts])
    tz = timezone('Europe/Moscow')
    start = datetime.fromtimestamp(start_time, tz).isoformat()
    end = datetime.fromtimestamp(end_time, tz).isoformat()
    time_cmd = f'-S "{start}" -U "{end}"'
    cmd = f"parallel-ssh {hosts_cmd} -o . 'ulimit -n 100500;unified_agent select {time_cmd} -s kikimr'"
    test_info['logs_command'] = f'<code>{cmd}</code>'


def allure_test_description(
    suite: str,
    test: str,
    start_time: float,
    end_time: float,
    addition_table_strings: dict[str, any] = {},
    attachments: tuple[str, str, allure.attachment_type] = [],
    refference_set: str = '',
):
    def _pretty_str(s):
        return ' '.join(s.split('_')).capitalize()

    allure.dynamic.title(f'{suite}.{test}')
    for body, name, type in attachments:
        allure.attach(body, name, type)

    test_info = deepcopy(YdbCluster.get_cluster_info())
    test_info.update(addition_table_strings)

    _set_monitoring(test_info, start_time, end_time)
    _set_coredumps(test_info)
    _set_results_plot(test_info, suite, test, refference_set)
    _set_logs_command(test_info, start_time, end_time)

    service_url = YdbCluster._get_service_url()
    db = test_info['database']
    test_info.update(
        {
            'table_path': YdbCluster.tables_path,
            'db_admin': (
                f"<a target='_blank' href='{service_url}/monitoring/tenant?"
                f"schema=/{db}/{YdbCluster.tables_path}&tenantPage=query"
                f"&diagnosticsTab=nodes&name=/{db}'>{service_url}</a>"
            ),
            'time': f"{datetime.fromtimestamp(start_time).strftime('%a %d %b %y %H:%M:%S')} - {datetime.fromtimestamp(end_time).strftime('%H:%M:%S')}",
        }
    )
    del test_info['nodes_wilcard']
    table_strings = '\n'.join([f'<tr><td>{_pretty_str(k)}</td><td>{v}</td></tr>' for k, v in test_info.items()])
    allure.dynamic.description_html(
        f'''<table border='1' cellpadding='4px'><tbody>
        {table_strings}
        </tbody></table>
    '''
    )
