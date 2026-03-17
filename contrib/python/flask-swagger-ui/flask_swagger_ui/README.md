# flask-swagger-ui

Simple Flask blueprint for adding [Swagger UI](https://github.com/swagger-api/swagger-ui) to your flask application.

Included Swagger UI version: 5.21.0.

**This project is not actively maintained, but might receive occasional updates. Please fork it if you need a newer version.**

## Installation

`pip install flask-swagger-ui`

## Usage

Example application:

```python
from flask import Flask
from flask_swagger_ui import get_swaggerui_blueprint

app = Flask(__name__)


SWAGGER_URL = '/api/docs'  # URL for exposing Swagger UI (without trailing '/')
API_URL = 'http://petstore.swagger.io/v2/swagger.json'  # Our API url (can of course be a local resource)

# Call factory function to create our blueprint
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,  # Swagger UI static files will be mapped to '{SWAGGER_URL}/dist/'
    API_URL,
    config={  # Swagger UI config overrides
        'app_name': "Test application"
    },
    # oauth_config={  # OAuth config. See https://github.com/swagger-api/swagger-ui#oauth2-configuration .
    #    'clientId': "your-client-id",
    #    'clientSecret': "your-client-secret-if-required",
    #    'realm': "your-realms",
    #    'appName': "your-app-name",
    #    'scopeSeparator': " ",
    #    'additionalQueryStringParams': {'test': "hello"}
    # }
)

app.register_blueprint(swaggerui_blueprint)

app.run()

# Now point your browser to localhost:5000/api/docs/

```

## Configuration

The blueprint supports overloading all Swagger UI configuration options that can be JSON serialized.
See https://github.com/swagger-api/swagger-ui/blob/master/docs/usage/configuration.md#parameters for options.

Plugins and function parameters are not supported at this time.

OAuth2 parameters can be found at https://github.com/swagger-api/swagger-ui#oauth2-configuration .
