from __future__ import annotations
import allure
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from urllib.parse import urlencode


def allure_test_description(
    suite: str,
    test: str,
    addition_table_strings: dict[str, any] = {},
    attachments: tuple[str, str, allure.attachment_type] = [],
    refference_set: str = ''
):
    def _pretty_str(s):
        return ' '.join(s.split('_')).capitalize()

    allure.dynamic.title(f'{suite}.{test}')
    for body, name, type in attachments:
        allure.attach(body, name, type)
    test_info = YdbCluster.get_cluster_info()
    if test_info['name'].startswith('ydb-k8s'):
        core_link = f"https://coredumps.n.yandex-team.ru/index?itype=kikimr&host_list={test_info['nodes_wilcard']}&show_fixed=True"
    else:
        core_link = f"https://kikimr-cores.n.yandex-team.ru/show?server={test_info['nodes_wilcard']}%2A"
    monitoring_cluster = (
        YdbCluster.monitoring_cluster if YdbCluster.monitoring_cluster is not None else test_info['name']
    )
    test_info.update(addition_table_strings)
    test_info.update(
        {
            'table_path': YdbCluster.tables_path,
            'monitoring': (
                f"<a target='_blank' href='https://monitoring.yandex-team.ru/projects/kikimr/dashboards/mone0310v4dbc6kui89v?"
                f"p.cluster={monitoring_cluster}&p.database=/{test_info['database']}'>link</a>"
            ),
            'coredumps': f"<a target='_blank' href='{core_link}'>link</a>",
            'db_admin': (
                f"<a target='_blank' href='{test_info['service_url']}/monitoring/tenant?"
                f"schema=/{test_info['database']}/{YdbCluster.tables_path}&tenantPage=query"
                f"&diagnosticsTab=nodes&name=/{test_info['database']}'>link</a>"
            ),
        }
    )
    if ResultsProcessor.send_results:
        params = urlencode({
            'tab': 'o8',
            'suite_b2rp': suite,
            'test_ehyw': test,
            'db_fmdl': ResultsProcessor.get_cluster_id(),
            'cluster_dufr': refference_set
        })
        test_info['results_plot'] = f"<a target='_blank' href='https://datalens.yandex-team.ru/iqnd4b1miaz27-testy-ydb?{params}'>link</a>"
    table_strings = '\n'.join([f'<tr><td>{_pretty_str(k)}</td><td>{v}</td></tr>' for k, v in test_info.items()])
    allure.dynamic.description_html(
        f'''<table border='1' cellpadding='4px'><tbody>
        {table_strings}
        </tbody></table>
    '''
    )
