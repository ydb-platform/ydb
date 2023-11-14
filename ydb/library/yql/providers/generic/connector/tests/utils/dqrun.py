from pathlib import Path
from dataclasses import dataclass
import subprocess
from typing import Final, List, Optional

import jinja2

import yatest.common

from yt import yson

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat

from ydb.library.yql.providers.generic.connector.tests.utils.log import make_logger
from ydb.library.yql.providers.generic.connector.tests.utils.schema import Schema, YsonList
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings, GenericSettings

LOGGER = make_logger(__name__)


class GatewaysConfRenderer:
    template_: Final = '''
Generic {
    Connector {
        Endpoint {
            host: "{{settings.connector.grpc_host}}"
            port: {{settings.connector.grpc_port}}
        }
        UseSsl: false
    }

{% set CLICKHOUSE = 'CLICKHOUSE' %}
{% set POSTGRESQL = 'POSTGRESQL' %}

{% macro data_source(kind, cluster, host, port, username, password, protocol, database, schema) -%}
    ClusterMapping {
        Kind: {{kind}}
        Name: "{{cluster}}"
        DatabaseName: "{{database}}"
        Endpoint {
            host: "{{host}}"
            port: {{port}}
        }
        Credentials {
            basic {
                username: "{{username}}"
                password: "{{password}}"
            }
        }
        UseSsl: false
        Protocol: {{protocol}}

        {% if kind == POSTGRESQL and schema %}
        DataSourceOptions: {
            key: "schema"
            value: "{{schema}}"
        }
        {% endif %}
    }
{%- endmacro -%}

{% set NATIVE = 'NATIVE' %}
{% set HTTP = 'HTTP' %}

{% for cluster in generic_settings.clickhouse_clusters %}

{% if cluster.protocol == EProtocol.NATIVE %}
{% set CLICKHOUSE_PORT = settings.clickhouse.native_port %}
{% set CLICKHOUSE_PROTOCOL = NATIVE %}
{% elif cluster.protocol == EProtocol.HTTP %}
{% set CLICKHOUSE_PORT = settings.clickhouse.http_port %}
{% set CLICKHOUSE_PROTOCOL = HTTP %}
{% endif %}

{{ data_source(
    CLICKHOUSE,
    settings.clickhouse.cluster_name,
    settings.clickhouse.host,
    CLICKHOUSE_PORT,
    settings.clickhouse.username,
    settings.clickhouse.password,
    CLICKHOUSE_PROTOCOL,
    cluster.database,
    NONE)
}}
{% endfor %}

{% for cluster in generic_settings.postgresql_clusters %}
{{ data_source(
    POSTGRESQL,
    settings.postgresql.cluster_name,
    settings.postgresql.host,
    settings.postgresql.port,
    settings.postgresql.username,
    settings.postgresql.password,
    NATIVE,
    cluster.database,
    cluster.schema)
}}
{% endfor %}

    DefaultSettings {
        Name: "DateTimeFormat"
        {% if generic_settings.date_time_format == EDateTimeFormat.STRING_FORMAT %}
        Value: "string"
        {% elif generic_settings.date_time_format == EDateTimeFormat.YQL_FORMAT %}
        Value: "YQL"
        {% endif %}
    }

}

Dq {
    DefaultSettings {
        Name: "EnableComputeActor"
        Value: "1"
    }

    DefaultSettings {
        Name: "ComputeActorType"
        Value: "async"
    }

    DefaultSettings {
        Name: "AnalyzeQuery"
        Value: "true"
    }

    DefaultSettings {
        Name: "MaxTasksPerStage"
        Value: "200"
    }

    DefaultSettings {
        Name: "MaxTasksPerOperation"
        Value: "200"
    }

    DefaultSettings {
        Name: "EnableInsert"
        Value: "true"
    }

    DefaultSettings {
        Name: "_EnablePrecompute"
        Value: "true"
    }

    DefaultSettings {
        Name: "UseAggPhases"
        Value: "true"
    }

    DefaultSettings {
        Name: "HashJoinMode"
        Value: "grace"
    }

    DefaultSettings {
        Name: "UseFastPickleTransport"
        Value: "true"
    }
}
'''

    def __init__(self):
        self.template = jinja2.Environment(loader=jinja2.BaseLoader, undefined=jinja2.DebugUndefined).from_string(
            self.template_
        )
        self.template.globals['EProtocol'] = EProtocol
        self.template.globals['EDateTimeFormat'] = EDateTimeFormat

    def render(self, file_path: Path, settings: Settings, generic_settings: GenericSettings) -> None:
        content = self.template.render(dict(settings=settings, generic_settings=generic_settings))
        with open(file_path, 'w') as f:
            f.write(content)


@dataclass
class Result:
    data_out: Optional[YsonList]
    data_out_with_types: Optional[List]
    stdout: str
    stderr: str


class Runner:
    def __init__(
        self,
        dqrun_path: Path,
        settings: Settings,
    ):
        self.gateways_conf_renderer = GatewaysConfRenderer()
        self.dqrun_path = dqrun_path
        self.settings = settings

    def run(self, test_dir: Path, script: str, generic_settings: GenericSettings) -> Result:
        LOGGER.debug(script)

        # Create file with YQL script
        script_path = test_dir.joinpath('script.yql')
        with open(script_path, "w") as f:
            f.write(script)

        # Create config
        gateways_conf_path = test_dir.joinpath('gateways.conf')
        self.gateways_conf_renderer.render(gateways_conf_path, self.settings, generic_settings)
        fs_conf_path = test_dir.joinpath('fs.conf')
        with open(fs_conf_path, "w") as f:
            pass

        # Run dqrun
        result_path = test_dir.joinpath('result.yson')

        # For debug add option --trace-opt to args
        cmd = f'{self.dqrun_path} -s -p {script_path} --fs-cfg={fs_conf_path} --gateways-cfg={gateways_conf_path} --result-file={result_path} --format="binary" -v 7'
        out = subprocess.run(cmd, shell=True, capture_output=True)

        data_out = None
        data_out_with_types = None

        if out.returncode == 0:
            # Parse output
            with open(result_path, 'r') as f:
                result = yson.loads(f.read().encode('ascii'))

            # Dqrun's data output is missing type information (everything is a string),
            # so we have to recover schema and transform the results to make them comparable with the inputs
            data_out = result[0]['Write'][0]['Data']
            schema = Schema.from_yson(result[0]['Write'][0]['Type'][1][1])
            data_out_with_types = [schema.cast_row(row) for row in data_out]

            LOGGER.debug('Data out: %s', data_out)
            LOGGER.debug('Data out with types: %s', data_out_with_types)
        else:
            LOGGER.error('STDOUT: ')
            for line in out.stdout.decode('utf-8').splitlines():
                LOGGER.error(line)
            LOGGER.error('STDERR: ')
            for line in out.stderr.decode('utf-8').splitlines():
                LOGGER.error(line)

            unique_suffix = test_dir.name
            err_file = yatest.common.output_path(f'dqrun-{unique_suffix}.err')
            with open(err_file, "w") as f:
                f.write(out.stderr.decode('utf-8'))

            out_file = yatest.common.output_path(f'dqrun-{unique_suffix}.out')
            with open(out_file, "w") as f:
                f.write(out.stdout.decode('utf-8'))

        return Result(
            data_out=data_out,
            data_out_with_types=data_out_with_types,
            stdout=out.stdout.decode('utf-8'),
            stderr=out.stderr.decode('utf-8'),
        )
