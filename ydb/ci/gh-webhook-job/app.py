import hashlib
import hmac
import json
import os
import time
import logging
import logging.config

import requests
from functools import wraps
from flask import Flask, current_app, jsonify, request
from gunicorn.app.base import BaseApplication

flask_app = Flask("github-webhook")


logger = logging.getLogger(__name__)


def check_github_signature(f):
    @wraps(f)
    def decorated_func(*args, **kwargs):
        try:
            sign_header = request.headers["x-hub-signature-256"]
        except KeyError:
            return {"status": False, "description": "x-hub-signature-256 header is missing!"}, 403

        secret_token = current_app.config["GH_WEBHOOK_SECRET"]
        hash_object = hmac.new(secret_token, msg=request.data, digestmod=hashlib.sha256)
        expected_signature = "sha256=" + hash_object.hexdigest()

        if not hmac.compare_digest(expected_signature, sign_header):
            return {"status": False, "description": "Request signatures didn't match!"}, 403

        return f(*args, **kwargs)

    return decorated_func


@flask_app.route("/webhooks", methods=["GET", "POST"])
@check_github_signature
def webhooks():
    cfg = current_app.config

    job = request.get_json()

    if "workflow_job" not in job:
        print("No workflow_job, skip")
        return jsonify({"status": False, "description": "No workflow_job in the request"})

    # noinspection HttpUrlsUsage
    ch_url = f"http://{current_app.config['CH_FQDNS'][0]}:8123"

    query = "INSERT INTO workflow_jobs FORMAT JSONEachRow"

    params = {
        "database": cfg["CH_DATABASE"],
        "query": query,
        "date_time_input_format": "best_effort",
        "send_logs_level": "warning",
    }

    headers = {}

    if cfg["CH_USERNAME"]:
        headers.update(
            {
                "X-ClickHouse-User": cfg["CH_USERNAME"],
                "X-ClickHouse-Key": cfg["CH_PASSWORD"],
            }
        )

    job = job["workflow_job"]
    job["steps"] = len(job["steps"])

    for i in range(5):
        response = None
        try:
            response = requests.post(ch_url, params=params, data=json.dumps(job), headers=headers)
            response.raise_for_status()
            break
        except Exception as e:
            logger.exception("while inserting into clickhouse", exc_info=e)
            if response:
                logger.warning("Response text %s", response.text)
            time.sleep(0.1 * i)

    return jsonify({"status": True})


# https://docs.gunicorn.org/en/stable/custom.html
class GunicornApp(BaseApplication):
    def __init__(self, app: Flask, gunicorn_config: dict):
        self.flask_app = app
        self.gunicorn_config = gunicorn_config
        super().__init__()

    def load_config(self):
        for key, value in self.gunicorn_config.items():
            self.cfg.set(key, value)

    def load(self):
        return self.flask_app


def prepare_logger():
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
            },
            "handlers": {
                "default": {
                    #
                    "level": "INFO",
                    "formatter": "standard",
                    "class": "logging.StreamHandler",
                },
            },
            "loggers": {
                "": {
                    #
                    "handlers": ["default"],
                    "level": "INFO",
                    "propagate": False,
                },
            },
        }
    )


def main():
    prepare_logger()
    port = os.environ.get("PORT", 8888)
    flask_app.config.update(
        {
            "CH_FQDNS": os.environ["CH_FQDNS"].split(","),
            "CH_DATABASE": os.environ["CH_DATABASE"],
            "CH_USERNAME": os.environ.get("CH_USERNAME"),
            "CH_PASSWORD": os.environ.get("CH_PASSWORD"),
            "GH_WEBHOOK_SECRET": os.environ["GH_WEBHOOK_SECRET"].encode("utf8"),
        }
    )

    # https://docs.gunicorn.org/en/stable/settings.html
    config = {
        "bind": f"[::]:{port}",
        "workers": os.environ.get("WORKERS", 1),
        "accesslog": "-",
    }
    GunicornApp(flask_app, config).run()


if __name__ == "__main__":
    main()
