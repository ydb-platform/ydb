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


logger = logging.getLogger("wh")


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


def ch_execute(cfg, query, data):
    ch_url = f"http://{cfg['CH_FQDNS'][0]}:8123"

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

    for i in range(5):
        response = None
        try:
            response = requests.post(ch_url, params=params, data=data, headers=headers)
            response.raise_for_status()
            break
        except Exception as e:
            logger.exception("while inserting into clickhouse", exc_info=e)
            if response:
                logger.warning("Response text %s", response.text)
            time.sleep(0.1 * i)
    else:
        raise Exception("Unable to execute")

    return response


def save_workflow_job(cfg, event):
    logger.info("save_workflow_job")
    job = event["workflow_job"]
    orig_steps = job["steps"]
    job["steps"] = len(job["steps"])

    logger.info("save job")
    ch_execute(cfg, "INSERT INTO workflow_jobs FORMAT JSONEachRow", json.dumps(job))

    for step in orig_steps:
        step = step.copy()
        for f in ["id", "run_id", "started_at"]:
            step[f"wf_{f}"] = job[f]
        logger.info("save step")
        ch_execute(cfg, "INSERT INTO workflow_job_steps FORMAT JSONEachRow", json.dumps(step))


def save_pullrequest(cfg, event):
    action = event["action"]

    logger.info("save_pullrequest")

    pr = event["pull_request"]
    data = {
        "id": pr["id"],
        "action": action,
        "state": pr["state"],
        "url": pr["url"],
        "html_url": pr["html_url"],
        "number": pr["number"],
        "user_login": pr["user"]["login"],
        "labels": [l["name"] for l in pr["labels"]],
        "head_sha": pr["head"]["sha"],
        "head_ref": pr["head"]["ref"],
        "base_sha": pr["base"]["sha"],
        "base_ref": pr["base"]["ref"],
        "merge_commit_sha": pr["merge_commit_sha"],
        "merged": pr["merged"],
        "draft": pr["draft"],
        "created_at": pr["created_at"],
        "updated_at": pr["updated_at"],
        "closed_at": pr["closed_at"],
        "merged_at": pr["merged_at"],
    }
    ch_execute(cfg, "INSERT INTO pull_request FORMAT JSONEachRow", json.dumps(data))


@flask_app.route("/webhooks", methods=["GET", "POST"])
@check_github_signature
def webhooks():
    cfg = current_app.config

    event = request.get_json()

    if "workflow_job" in event:
        save_workflow_job(cfg, event)
    elif "pull_request" in event:
        save_pullrequest(cfg, event)
    else:
        logger.error("Unknown event action=%s, keys=%r, skip", event["action"], list(event.keys()))
        return jsonify({"status": False, "description": "No workflow_job in the request"})

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
                "wh": {
                    "handlers": ["default"],
                    "level": "INFO",
                    "propagate": False,
                }
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
