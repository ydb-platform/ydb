# datamodel-code-generator

This code generator creates [pydantic v1 and v2](https://docs.pydantic.dev/) model, [dataclasses.dataclass](https://docs.python.org/3/library/dataclasses.html), [typing.TypedDict](https://docs.python.org/3/library/typing.html#typing.TypedDict) 
and [msgspec.Struct](https://github.com/jcrist/msgspec) from an openapi file and others.

[![PyPI version](https://badge.fury.io/py/datamodel-code-generator.svg)](https://pypi.python.org/pypi/datamodel-code-generator)
[![Conda-forge](https://img.shields.io/conda/v/conda-forge/datamodel-code-generator)](https://anaconda.org/conda-forge/datamodel-code-generator)
[![Downloads](https://pepy.tech/badge/datamodel-code-generator/month)](https://pepy.tech/project/datamodel-code-generator)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/datamodel-code-generator)](https://pypi.python.org/pypi/datamodel-code-generator)
[![codecov](https://codecov.io/gh/koxudaxi/datamodel-code-generator/graph/badge.svg?token=plzSSFb9Li)](https://codecov.io/gh/koxudaxi/datamodel-code-generator)
![license](https://img.shields.io/github/license/koxudaxi/datamodel-code-generator.svg)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Pydantic v1](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/pydantic/pydantic/main/docs/badge/v1.json)](https://pydantic.dev)
[![Pydantic v2](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/pydantic/pydantic/main/docs/badge/v2.json)](https://pydantic.dev)

## Help
See [documentation](https://koxudaxi.github.io/datamodel-code-generator) for more details.

## Quick Installation

To install `datamodel-code-generator`:
```bash
$ pip install datamodel-code-generator
```

## Simple Usage
You can generate models from a local file.
```bash
$ datamodel-codegen --input api.yaml --output model.py
```

<details>
<summary>api.yaml</summary>

```yaml
openapi: "3.0.0"
info:
  version: 1.0.0
  title: Swagger Petstore
  license:
    name: MIT
servers:
  - url: http://petstore.swagger.io/v1
paths:
  /pets:
    get:
      summary: List all pets
      operationId: listPets
      tags:
        - pets
      parameters:
        - name: limit
          in: query
          description: How many items to return at one time (max 100)
          required: false
          schema:
            type: integer
            format: int32
      responses:
        '200':
          description: A paged array of pets
          headers:
            x-next:
              description: A link to the next page of responses
              schema:
                type: string
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Pets"
        default:
          description: unexpected error
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Error"
                x-amazon-apigateway-integration:
                  uri:
                    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${PythonVersionFunction.Arn}/invocations
                  passthroughBehavior: when_no_templates
                  httpMethod: POST
                  type: aws_proxy
    post:
      summary: Create a pet
      operationId: createPets
      tags:
        - pets
      responses:
        '201':
          description: Null response
        default:
          description: unexpected error
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Error"
                x-amazon-apigateway-integration:
                  uri:
                    Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${PythonVersionFunction.Arn}/invocations
                  passthroughBehavior: when_no_templates
                  httpMethod: POST
                  type: aws_proxy
  /pets/{petId}:
    get:
      summary: Info for a specific pet
      operationId: showPetById
      tags:
        - pets
      parameters:
        - name: petId
          in: path
          required: true
          description: The id of the pet to retrieve
          schema:
            type: string
      responses:
        '200':
          description: Expected response to a valid request
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Pets"
        default:
          description: unexpected error
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Error"
    x-amazon-apigateway-integration:
      uri:
        Fn::Sub: arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${PythonVersionFunction.Arn}/invocations
      passthroughBehavior: when_no_templates
      httpMethod: POST
      type: aws_proxy
components:
  schemas:
    Pet:
      required:
        - id
        - name
      properties:
        id:
          type: integer
          format: int64
        name:
          type: string
        tag:
          type: string
    Pets:
      type: array
      items:
        $ref: "#/components/schemas/Pet"
    Error:
      required:
        - code
        - message
      properties:
        code:
          type: integer
          format: int32
        message:
          type: string
    apis:
      type: array
      items:
        type: object
        properties:
          apiKey:
            type: string
            description: To be used as a dataset parameter value
          apiVersionNumber:
            type: string
            description: To be used as a version parameter value
          apiUrl:
            type: string
            format: uri
            description: "The URL describing the dataset's fields"
          apiDocumentationUrl:
            type: string
            format: uri
            description: A URL to the API console for each API
```

</details>

<details>
<summary>model.py</summary>

```python
# generated by datamodel-codegen:
#   filename:  api.yaml
#   timestamp: 2020-06-02T05:28:24+00:00

from __future__ import annotations

from typing import List, Optional

from pydantic import AnyUrl, BaseModel, Field


class Pet(BaseModel):
    id: int
    name: str
    tag: Optional[str] = None


class Pets(BaseModel):
    __root__: List[Pet]


class Error(BaseModel):
    code: int
    message: str


class Api(BaseModel):
    apiKey: Optional[str] = Field(
        None, description='To be used as a dataset parameter value'
    )
    apiVersionNumber: Optional[str] = Field(
        None, description='To be used as a version parameter value'
    )
    apiUrl: Optional[AnyUrl] = Field(
        None, description="The URL describing the dataset's fields"
    )
    apiDocumentationUrl: Optional[AnyUrl] = Field(
        None, description='A URL to the API console for each API'
    )


class Apis(BaseModel):
    __root__: List[Api]
```
</details>

## Supported input types
-  OpenAPI 3 (YAML/JSON, [OpenAPI Data Type](https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.2.md#data-types));
-  JSON Schema ([JSON Schema Core](http://json-schema.org/draft/2019-09/json-schema-validation.html)/[JSON Schema Validation](http://json-schema.org/draft/2019-09/json-schema-validation.html));
-  JSON/YAML/CSV Data (it will be converted to JSON Schema);
-  Python dictionary (it will be converted to JSON Schema);
-  GraphQL schema ([GraphQL Schemas and Types](https://graphql.org/learn/schema/));

## Supported output types
- [pydantic](https://docs.pydantic.dev/1.10/).BaseModel;
- [pydantic_v2](https://docs.pydantic.dev/2.0/).BaseModel;
- [dataclasses.dataclass](https://docs.python.org/3/library/dataclasses.html);
- [typing.TypedDict](https://docs.python.org/3/library/typing.html#typing.TypedDict);
- [msgspec.Struct](https://github.com/jcrist/msgspec);
- Custom type from your [jinja2](https://jinja.palletsprojects.com/en/3.1.x/) template;

## Sponsors
<table>
  <tr>
    <td valign="top" align="center">
    <a href="https://github.com/JetBrainsOfficial">
      <img src="https://avatars.githubusercontent.com/u/60931315?s=100&v=4" alt="JetBrains Logo" style="width: 100px;">
      <p>JetBrains</p>
    </a>
    </td>
  <td valign="top" align="center">
    <a href="https://github.com/astral-sh">
      <img src="https://avatars.githubusercontent.com/u/115962839?s=200&v=4" alt="Astral Logo" style="width: 100px;">
      <p>Astral</p>
    </a>
  </td>
  </tr>
</table>

## Projects that use datamodel-code-generator
 
These OSS projects use datamodel-code-generator to generate many models. 
See the following linked projects for real world examples and inspiration.

- [airbytehq/airbyte](https://github.com/airbytehq/airbyte)
  - *[Generate Python, Java/Kotlin, and Typescript protocol models](https://github.com/airbytehq/airbyte-protocol/tree/main/protocol-models/bin)*
- [apache/iceberg](https://github.com/apache/iceberg)
  - *[Generate Python code](https://github.com/apache/iceberg/blob/d2e1094ee0cc6239d43f63ba5114272f59d605d2/open-api/README.md?plain=1#L39)* 
    *[`make generate`](https://github.com/apache/iceberg/blob/d2e1094ee0cc6239d43f63ba5114272f59d605d2/open-api/Makefile#L24-L34)*
- [argoproj-labs/hera](https://github.com/argoproj-labs/hera)
  - *[`Makefile`](https://github.com/argoproj-labs/hera/blob/c8cbf0c7a676de57469ca3d6aeacde7a5e84f8b7/Makefile#L53-L62)*
- [awslabs/aws-lambda-powertools-python](https://github.com/awslabs/aws-lambda-powertools-python)
  - *Recommended for [advanced-use-cases](https://awslabs.github.io/aws-lambda-powertools-python/2.6.0/utilities/parser/#advanced-use-cases) in the official documentation*
- [cloudcoil/cloudcoil](https://github.com/cloudcoil/cloudcoil)
  - *[Cloudcoil - Model generation](https://github.com/cloudcoil/cloudcoil#%EF%B8%8F-model-generation)
- [DataDog/integrations-core](https://github.com/DataDog/integrations-core)
  - *[Config models](https://github.com/DataDog/integrations-core/blob/master/docs/developer/meta/config-models.md)*
- [hashintel/hash](https://github.com/hashintel/hash)
  - *[`codegen.sh`](https://github.com/hashintel/hash/blob/9762b1a1937e14f6b387677e4c7fe4a5f3d4a1e1/libs/%40local/hash-graph-client/python/scripts/codegen.sh#L21-L39)*
- [IBM/compliance-trestle](https://github.com/IBM/compliance-trestle)
  - *[Building the models from the OSCAL schemas.](https://github.com/IBM/compliance-trestle/blob/develop/docs/contributing/website.md#building-the-models-from-the-oscal-schemas)*
- [Netflix/consoleme](https://github.com/Netflix/consoleme)
  - *[How do I generate models from the Swagger specification?](https://github.com/Netflix/consoleme/blob/master/docs/gitbook/faq.md#how-do-i-generate-models-from-the-swagger-specification)*
- [Nike-Inc/brickflow](https://github.com/Nike-Inc/brickflow)
  - *[Code generate tools](https://github.com/Nike-Inc/brickflow/blob/e3245bf638588867b831820a6675ada76b2010bf/tools/README.md?plain=1#L8)[`./tools/gen-bundle.sh`](https://github.com/Nike-Inc/brickflow/blob/e3245bf638588867b831820a6675ada76b2010bf/tools/gen-bundle.sh#L15-L22)*
- [open-metadata/OpenMetadata](https://github.com/open-metadata/OpenMetadata)
  - *[Makefile](https://github.com/open-metadata/OpenMetadata/blob/main/Makefile)*
- [PostHog/posthog](https://github.com/PostHog/posthog)
  - *[Generate models via `npm run`](https://github.com/PostHog/posthog/blob/e1a55b9cb38d01225224bebf8f0c1e28faa22399/package.json#L41)* 
- [SeldonIO/MLServer](https://github.com/SeldonIO/MLServer)
  - *[generate-types.sh](https://github.com/SeldonIO/MLServer/blob/master/hack/generate-types.sh)*

## Installation

To install `datamodel-code-generator`:
```bash
$ pip install datamodel-code-generator
```

### `http` extra option
If you want to resolve `$ref` for remote files then you should specify `http` extra option.
```bash
$ pip install 'datamodel-code-generator[http]'
```

### `graphql` extra option

If you want to generate data model from a GraphQL schema then you should specify `graphql` extra option.
```bash
$ pip install 'datamodel-code-generator[graphql]'
```

### Docker Image
The docker image is in [Docker Hub](https://hub.docker.com/r/koxudaxi/datamodel-code-generator)
```bash
$ docker pull koxudaxi/datamodel-code-generator
```

## Advanced Uses
You can generate models from a URL.
```bash
$ datamodel-codegen --url https://<INPUT FILE URL> --output model.py
```
This method needs the [http extra option](#http-extra-option)


## All Command Options

The `datamodel-codegen` command:

<!-- start command help -->
```bash
usage: 
  datamodel-codegen [options]

Generate Python data models from schema definitions or structured data

Options:
  --additional-imports ADDITIONAL_IMPORTS
                        Custom imports for output (delimited list input). For example
                        "datetime.date,datetime.datetime"
  --custom-formatters CUSTOM_FORMATTERS
                        List of modules with custom formatter (delimited list input).
  --formatters {black,isort,ruff-check,ruff-format} [{black,isort,ruff-check,ruff-format} ...]
                        Formatters for output (default: [black, isort])
  --http-headers HTTP_HEADER [HTTP_HEADER ...]
                        Set headers in HTTP requests to the remote host. (example:
                        "Authorization: Basic dXNlcjpwYXNz")
  --http-ignore-tls     Disable verification of the remote host''s TLS certificate
  --http-query-parameters HTTP_QUERY_PARAMETERS [HTTP_QUERY_PARAMETERS ...]
                        Set query parameters in HTTP requests to the remote host. (example:
                        "ref=branch")
  --input INPUT         Input file/directory (default: stdin)
  --input-file-type {auto,openapi,jsonschema,json,yaml,dict,csv,graphql}
                        Input file type (default: auto)
  --output OUTPUT       Output file (default: stdout)
  --output-model-type {pydantic.BaseModel,pydantic_v2.BaseModel,dataclasses.dataclass,typing.TypedDict,msgspec.Struct}
                        Output model type (default: pydantic.BaseModel)
  --url URL             Input file URL. `--input` is ignored when `--url` is used

Typing customization:
  --base-class BASE_CLASS
                        Base Class (default: pydantic.BaseModel)
  --disable-future-imports
                        Disable __future__ imports
  --enum-field-as-literal {all,one}
                        Parse enum field as literal. all: all enum field type are Literal.
                        one: field type is Literal when an enum has only one possible value
  --field-constraints   Use field constraints and not con* annotations
  --set-default-enum-member
                        Set enum members as default values for enum field
  --strict-types {str,bytes,int,float,bool} [{str,bytes,int,float,bool} ...]
                        Use strict types
  --use-annotated       Use typing.Annotated for Field(). Also, `--field-constraints` option
                        will be enabled.
  --use-generic-container-types
                        Use generic container types for type hinting (typing.Sequence,
                        typing.Mapping). If `--use-standard-collections` option is set, then
                        import from collections.abc instead of typing
  --use-non-positive-negative-number-constrained-types
                        Use the Non{Positive,Negative}{FloatInt} types instead of the
                        corresponding con* constrained types.
  --use-one-literal-as-default
                        Use one literal as default value for one literal field
  --use-standard-collections
                        Use standard collections for type hinting (list, dict)
  --use-subclass-enum   Define Enum class as subclass with field type when enum has type
                        (int, float, bytes, str)
  --use-union-operator  Use | operator for Union type (PEP 604).
  --use-unique-items-as-set
                        define field type as `set` when the field attribute has
                        `uniqueItems`

Field customization:
  --capitalise-enum-members, --capitalize-enum-members
                        Capitalize field names on enum
  --empty-enum-field-name EMPTY_ENUM_FIELD_NAME
                        Set field name when enum value is empty (default: `_`)
  --field-extra-keys FIELD_EXTRA_KEYS [FIELD_EXTRA_KEYS ...]
                        Add extra keys to field parameters
  --field-extra-keys-without-x-prefix FIELD_EXTRA_KEYS_WITHOUT_X_PREFIX [FIELD_EXTRA_KEYS_WITHOUT_X_PREFIX ...]
                        Add extra keys with `x-` prefix to field parameters. The extra keys
                        are stripped of the `x-` prefix.
  --field-include-all-keys
                        Add all keys to field parameters
  --force-optional      Force optional for required fields
  --no-alias            Do not add a field alias. E.g., if --snake-case-field is used along
                        with a base class, which has an alias_generator
  --original-field-name-delimiter ORIGINAL_FIELD_NAME_DELIMITER
                        Set delimiter to convert to snake case. This option only can be used
                        with --snake-case-field (default: `_` )
  --remove-special-field-name-prefix
                        Remove field name prefix if it has a special meaning e.g.
                        underscores
  --snake-case-field    Change camel-case field name to snake-case
  --special-field-name-prefix SPECIAL_FIELD_NAME_PREFIX
                        Set field name prefix when first character can''t be used as Python
                        field name (default: `field`)
  --strip-default-none  Strip default None on fields
  --union-mode {smart,left_to_right}
                        Union mode for only pydantic v2 field
  --use-default         Use default value even if a field is required
  --use-default-kwarg   Use `default=` instead of a positional argument for Fields that have
                        default values.
  --use-field-description
                        Use schema description to populate field docstring

Model customization:
  --allow-extra-fields  Deprecated: Allow passing extra fields. This flag is deprecated. Use
                        `--extra-fields=allow` instead.
  --allow-population-by-field-name
                        Allow population by field name
  --class-name CLASS_NAME
                        Set class name of root model
  --collapse-root-models
                        Models generated with a root-type field will be merged into the
                        models using that root-type model
  --disable-appending-item-suffix
                        Disable appending `Item` suffix to model name in an array
  --disable-timestamp   Disable timestamp on file headers
  --enable-faux-immutability
                        Enable faux immutability
  --enable-version-header
                        Enable package version on file headers
  --extra-fields {allow,ignore,forbid}
                        Set the generated models to allow, forbid, or ignore extra fields.
  --frozen-dataclasses  Generate frozen dataclasses (dataclass(frozen=True)). Only applies
                        to dataclass output.
  --keep-model-order    Keep generated models'' order
  --keyword-only        Defined models as keyword only (for example
                        dataclass(kw_only=True)).
  --output-datetime-class {datetime,AwareDatetime,NaiveDatetime}
                        Choose Datetime class between AwareDatetime, NaiveDatetime or
                        datetime. Each output model has its default mapping (for example
                        pydantic: datetime, dataclass: str, ...)
  --parent-scoped-naming
                        Set name of models defined inline from the parent model
  --reuse-model         Reuse models on the field when a module has the model with the same
                        content
  --target-python-version {3.9,3.10,3.11,3.12,3.13,3.14}
                        target python version
  --treat-dot-as-module
                        treat dotted module names as modules
  --use-exact-imports   import exact types instead of modules, for example: "from .foo
                        import Bar" instead of "from . import foo" with "foo.Bar"
  --use-pendulum        use pendulum instead of datetime
  --use-schema-description
                        Use schema description to populate class docstring
  --use-title-as-name   use titles as class names of models

Template customization:
  --aliases ALIASES     Alias mapping file
  --custom-file-header CUSTOM_FILE_HEADER
                        Custom file header
  --custom-file-header-path CUSTOM_FILE_HEADER_PATH
                        Custom file header file path
  --custom-formatters-kwargs CUSTOM_FORMATTERS_KWARGS
                        A file with kwargs for custom formatters.
  --custom-template-dir CUSTOM_TEMPLATE_DIR
                        Custom template directory
  --encoding ENCODING   The encoding of input and output (default: utf-8)
  --extra-template-data EXTRA_TEMPLATE_DATA
                        Extra template data for output models. Input is supposed to be a
                        json/yaml file. For OpenAPI and Jsonschema the keys are the spec
                        path of the object, or the name of the object if you want to apply
                        the template data to multiple objects with the same name. If you are
                        using another input file type (e.g. GraphQL), the key is the name of
                        the object. The value is a dictionary of the template data to add.
  --use-double-quotes   Model generated with double quotes. Single quotes or your black
                        config skip_string_normalization value will be used without this
                        option.
  --wrap-string-literal
                        Wrap string literal by using black `experimental-string-processing`
                        option (require black 20.8b0 or later)

OpenAPI-only options:
  --include-path-parameters
                        Include path parameters in generated parameter models in addition to
                        query parameters (Only OpenAPI)
  --openapi-scopes {schemas,paths,tags,parameters} [{schemas,paths,tags,parameters} ...]
                        Scopes of OpenAPI model generation (default: schemas)
  --strict-nullable     Treat default field as a non-nullable field (Only OpenAPI)
  --use-operation-id-as-name
                        use operation id of OpenAPI as class names of models
  --validation          Deprecated: Enable validation (Only OpenAPI). this option is
                        deprecated. it will be removed in future releases

General options:
  --debug               show debug message (require "debug". `$ pip install ''datamodel-code-
                        generator[debug]''`)
  --disable-warnings    disable warnings
  --no-color            disable colorized output
  --version             show version
  -h, --help            show this help message and exit
```
<!-- end command help -->

## Related projects
### fastapi-code-generator
This code generator creates [FastAPI](https://github.com/tiangolo/fastapi) app from an openapi file.

[https://github.com/koxudaxi/fastapi-code-generator](https://github.com/koxudaxi/fastapi-code-generator)

### pydantic-pycharm-plugin
[A JetBrains PyCharm plugin](https://plugins.jetbrains.com/plugin/12861-pydantic) for [`pydantic`](https://github.com/samuelcolvin/pydantic).

[https://github.com/koxudaxi/pydantic-pycharm-plugin](https://github.com/koxudaxi/pydantic-pycharm-plugin)

## PyPi

[https://pypi.org/project/datamodel-code-generator](https://pypi.org/project/datamodel-code-generator)

## Contributing

See `docs/development-contributing.md` for how to get started!

## License

datamodel-code-generator is released under the MIT License. http://www.opensource.org/licenses/mit-license
