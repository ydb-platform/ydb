from pathlib import Path
from typing import Final
import json
import subprocess

import jinja2

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat

import ydb.library.yql.providers.generic.connector.tests.utils.artifacts as artifacts
from ydb.library.yql.providers.generic.connector.tests.utils.log import make_logger
from ydb.library.yql.providers.generic.connector.tests.utils.schema import Schema
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings, GenericSettings

from ydb.library.yql.providers.generic.connector.tests.utils.run.parent import Runner
from ydb.library.yql.providers.generic.connector.tests.utils.run.result import Result

LOGGER = make_logger(__name__)


class SchemeRenderer:
    template_: Final = '''

{% macro create_data_source(kind, data_source, host, port, login, password, protocol, database, schema) -%}

CREATE OBJECT {{data_source}}_local_password (TYPE SECRET) WITH (value = "{{password}}");

CREATE EXTERNAL DATA SOURCE {{data_source}} WITH (
    SOURCE_TYPE="{{kind}}",
    LOCATION="{{host}}:{{port}}",
    DATABASE_NAME="{{database}}",
    AUTH_METHOD="BASIC",
    LOGIN="{{login}}",
    PASSWORD_SECRET_NAME="{{data_source}}_local_password",
    {% if protocol %}
    PROTOCOL="{{protocol}}",
    {% endif %}
    USE_TLS="FALSE"

    {% if kind == POSTGRESQL and schema %}
        ,SCHEMA="{{schema}}"
    {% endif %}
);

{%- endmacro -%}

{% set CLICKHOUSE = 'ClickHouse' %}
{% set MYSQL = 'MySQL' %}
{% set POSTGRESQL = 'PostgreSQL' %}
{% set YDB = 'Ydb' %}

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

{{ create_data_source(
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

{% for cluster in generic_settings.mysql_clusters %}
{{ create_data_source(
    MYSQL,
    settings.mysql.cluster_name,
    settings.mysql.host_internal,
    settings.mysql.port_internal,
    settings.mysql.username,
    settings.mysql.password,
    NONE,
    cluster.database,
    NONE)
}}
{% endfor %}

{% for cluster in generic_settings.postgresql_clusters %}
{{ create_data_source(
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

{% for cluster in generic_settings.ydb_clusters %}
{{ create_data_source(
    YDB,
    settings.ydb.cluster_name,
    settings.ydb.host_internal,
    settings.ydb.port_internal,
    settings.ydb.username,
    settings.ydb.password,
    NONE,
    cluster.database,
    NONE)
}}
{% endfor %}

'''

    def __init__(self):
        self.template = jinja2.Environment(loader=jinja2.BaseLoader, undefined=jinja2.DebugUndefined).from_string(
            self.template_
        )
        self.template.globals['EProtocol'] = EProtocol

    def render(self, file_path: Path, settings: Settings, generic_settings: GenericSettings) -> None:
        content = self.template.render(dict(settings=settings, generic_settings=generic_settings))
        with open(file_path, 'w') as f:
            f.write(content)


class AppConfigRenderer:
    template_: Final = '''

FeatureFlags {
  EnableExternalDataSources: true
  EnableScriptExecutionOperations: true
}

QueryServiceConfig {
  Generic {
    Connector {
        Endpoint {
            host: "{{settings.connector.grpc_host}}"
            port: {{settings.connector.grpc_port}}
        }
        UseSsl: false
    }

    DefaultSettings {
        Name: "DateTimeFormat"
        {% if generic_settings.date_time_format == EDateTimeFormat.STRING_FORMAT %}
        Value: "string"
        {% elif generic_settings.date_time_format == EDateTimeFormat.YQL_FORMAT %}
        Value: "YQL"
        {% endif %}
    }
  }
}

'''

    def __init__(self):
        self.template = jinja2.Environment(loader=jinja2.BaseLoader, undefined=jinja2.DebugUndefined).from_string(
            self.template_
        )
        self.template.globals['EDateTimeFormat'] = EDateTimeFormat

    def render(self, file_path: Path, settings: Settings, generic_settings: GenericSettings) -> None:
        content = self.template.render(dict(settings=settings, generic_settings=generic_settings))
        with open(file_path, 'w') as f:
            f.write(content)


class KqpRunner(Runner):
    def __init__(
        self,
        kqprun_path: Path,
        settings: Settings,
        udf_dir: Path,
    ):
        self.scheme_renderer = SchemeRenderer()
        self.app_conf_renderer = AppConfigRenderer()
        self.kqprun_path = kqprun_path
        self.settings = settings

        self.udf_dir = udf_dir

    def run(self, test_name: str, script: str, generic_settings: GenericSettings) -> Result:
        LOGGER.debug(script)

        # Create file with YQL script
        script_path = artifacts.make_path(test_name=test_name, artifact_name='script.yql')
        with open(script_path, "w") as f:
            f.write(script)

        # Create config
        app_conf_path = artifacts.make_path(test_name=test_name, artifact_name='app_conf.txt')
        self.app_conf_renderer.render(app_conf_path, settings=self.settings, generic_settings=generic_settings)

        # Create scheme
        scheme_path = artifacts.make_path(test_name=test_name, artifact_name='scheme.txt')
        self.scheme_renderer.render(scheme_path, settings=self.settings, generic_settings=generic_settings)

        # Run kqprun
        result_path = artifacts.make_path(test_name=test_name, artifact_name='result.json')

        # For debug add option --trace-opt to args
        cmd = f'{self.kqprun_path} -s {scheme_path} -p {script_path} --app-config={app_conf_path} --result-file={result_path} --result-format=full-json --udfs-dir={self.udf_dir} '

        output = None
        data_out = None
        data_out_with_types = None
        schema = None
        returncode = 0

        try:
            output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, timeout=60)
        except subprocess.CalledProcessError as e:
            LOGGER.error(
                'Execution failed:\n\nSTDOUT: %s\n\nSTDERR: %s\n\n',
                e.stdout.decode('utf-8') if e.stdout else None,
                e.stderr.decode('utf-8') if e.stderr else None,
            )

            output = e.stdout
            returncode = e.returncode
        else:
            # Parse output
            with open(result_path, 'r') as f:
                result = json.loads(f.read().encode('utf-8'), strict=False)

            # Kqprun's data output is missing type information (everything is a string),
            # so we have to recover schema and transform the results to make them comparable with the inputs
            data_out = result["rows"]
            schema = Schema.from_json(result["columns"])

            data_out_with_types = []
            for row in data_out:
                row_values = []
                for item in row['items']:
                    name, value = list(item.items())[0]
                    if name == 'null_flag_value':
                        row_values.append(None)
                    else:
                        row_values.append(value)

                data_out_with_types.append(schema.cast_row(row_values))

            LOGGER.debug('Schema: %s', schema)

            artifacts.dump_json(data_out, test_name, "data_out.yson")
            artifacts.dump_str(data_out_with_types, test_name, "data_out_with_types.yson")

        finally:
            with open(artifacts.make_path(test_name, "kqprun.out"), "w") as f:
                f.write(output.decode('utf-8'))

        return Result(
            data_out=data_out,
            data_out_with_types=data_out_with_types,
            schema=schema,
            output=output.decode('utf-8') if output else None,
            returncode=returncode,
        )
