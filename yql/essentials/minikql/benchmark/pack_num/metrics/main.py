import yatest.common as yc


def test_export_metrics(metrics):
    metrics.set_benchmark(yc.execute_benchmark(
        'yql/essentials/minikql/benchmark/pack_num/pack_num',
        threads=8))
