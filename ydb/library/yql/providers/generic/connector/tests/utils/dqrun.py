from pathlib import Path
import subprocess
from typing import Final

import jinja2

from yt import yson

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat

import ydb.library.yql.providers.generic.connector.tests.utils.artifacts as artifacts
from ydb.library.yql.providers.generic.connector.tests.utils.runner import Result, Runner
from ydb.library.yql.providers.generic.connector.tests.utils.log import make_logger
from ydb.library.yql.providers.generic.connector.tests.utils.schema import Schema
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
{% set CLICKHOUSE_PORT = settings.clickhouse.native_port_internal %}
{% set CLICKHOUSE_PROTOCOL = NATIVE %}
{% elif cluster.protocol == EProtocol.HTTP %}
{% set CLICKHOUSE_PORT = settings.clickhouse.http_port_internal %}
{% set CLICKHOUSE_PROTOCOL = HTTP %}
{% endif %}

{{ data_source(
    CLICKHOUSE,
    settings.clickhouse.cluster_name,
    settings.clickhouse.host_internal,
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
    settings.postgresql.host_internal,
    settings.postgresql.port_internal,
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
        LOGGER.debug(content)
        with open(file_path, 'w') as f:
            f.write(content)


class DqRunner(Runner):
    def __init__(
        self,
        dqrun_path: Path,
        settings: Settings,
    ):
        self.gateways_conf_renderer = GatewaysConfRenderer()
        self.dqrun_path = dqrun_path
        self.settings = settings

    def run(self, test_name: str, script: str, generic_settings: GenericSettings) -> Result:
        LOGGER.debug(script)

        # Create file with YQL script
        script_path = artifacts.make_path(test_name, 'script.yql')
        with open(script_path, "w") as f:
            f.write(script)

        # Create config
        gateways_conf_path = artifacts.make_path(test_name, 'gateways.conf')
        self.gateways_conf_renderer.render(gateways_conf_path, self.settings, generic_settings)
        fs_conf_path = artifacts.make_path(test_name, 'fs.conf')
        with open(fs_conf_path, "w") as f:
            pass

        # Run dqrun
        result_path = artifacts.make_path(test_name, 'result.yson')

        # For debug add option --trace-opt to args
        cmd = f'{self.dqrun_path} -s -p {script_path} --fs-cfg={fs_conf_path} --gateways-cfg={gateways_conf_path} --result-file={result_path} --format="binary" -v 7'
        out = subprocess.run(cmd, shell=True, capture_output=True)

        data_out = None
        data_out_with_types = None
        schema = None

        if out.returncode == 0:
            LOGGER.info('Execution succeeded: ')
            # Parse output
            with open(result_path, 'r') as f:
                result = yson.loads(f.read().encode('ascii'))

            # Dqrun's data output is missing type information (everything is a string),
            # so we have to recover schema and transform the results to make them comparable with the inputs
            data_out = result[0]['Write'][0]['Data']
            schema = Schema.from_yson(result[0]['Write'][0]['Type'][1][1])
            data_out_with_types = [schema.cast_row(row) for row in data_out]

            LOGGER.debug('Resulting schema: %s', schema)

            artifacts.dump_yson(data_out, test_name, "data_out.yson")
            artifacts.dump_str(data_out_with_types, test_name, "data_out_with_types.yson")
        else:
            LOGGER.error(
                'Execution failed:\n\nSTDOUT: %s\n\nSTDERR: %s\n\n',
                out.stdout.decode('utf-8'),
                out.stderr.decode('utf-8'),
            )

        with open(artifacts.make_path(test_name, "dqrun.err"), "w") as f:
            f.write(out.stderr.decode('utf-8'))

        with open(artifacts.make_path(test_name, "dqrun.out"), "w") as f:
            f.write(out.stdout.decode('utf-8'))

        return Result(
            data_out=data_out,
            data_out_with_types=data_out_with_types,
            schema=schema,
            stdout=out.stdout.decode('utf-8'),
            stderr=out.stderr.decode('utf-8'),
            returncode=out.returncode,
        )
