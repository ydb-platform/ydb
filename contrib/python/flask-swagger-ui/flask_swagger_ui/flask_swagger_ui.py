import os
import json
from flask import Blueprint, send_from_directory, render_template, request
from flask import render_template_string, abort, Response
import mimetypes
import importlib.resources


def get_swaggerui_blueprint(
    base_url, api_url, config=None, oauth_config=None, blueprint_name="swagger_ui"
):

    swagger_ui = Blueprint(
        blueprint_name,
        __name__,
        static_folder="dist",
        template_folder="templates",
        url_prefix=base_url,
    )

    default_config = {
        "app_name": "Swagger UI",
        "dom_id": "#swagger-ui",
        "url": api_url,
        "layout": "StandaloneLayout",
        "deepLinking": True,
    }

    if config:
        default_config.update(config)

    fields = {
        # Some fields are used directly in template
        "base_url": base_url,
        "app_name": default_config.pop("app_name"),
        # Rest are just serialized into json string for inclusion in the .js file
        "config_json": json.dumps(default_config),
    }
    if oauth_config:
        fields["oauth_config_json"] = json.dumps(oauth_config)

    @swagger_ui.route("/")
    @swagger_ui.route("/<path:path>")
    def show(path=None):
        if not path or path == "index.html":
            if not default_config.get("oauth2RedirectUrl", None):
                default_config.update(
                    {
                        "oauth2RedirectUrl": os.path.join(
                            request.base_url, "oauth2-redirect.html"
                        )
                    }
                )
                fields["config_json"] = json.dumps(default_config)
            return render_template_string((importlib.resources.files(__package__) / "templates" / "index.template.html").read_text(), **fields)
        else:
            filename = path.split("/")[-1]
            try:
                file_content = (importlib.resources.files(__package__) / "dist" / filename).read_bytes()
            except:
                abort(404)
            else:
                return Response(file_content, mimetype=mimetypes.guess_type(filename)[0])

    return swagger_ui
