import logging
import json

log = logging.getLogger(__name__)

CONF_PATH = '/var/elasticbeanstalk/xray/environment.conf'
SERVICE_NAME = 'elastic_beanstalk'
ORIGIN = 'AWS::ElasticBeanstalk::Environment'


def initialize():
    global runtime_context
    try:
        with open(CONF_PATH) as f:
            runtime_context = json.load(f)
    except Exception:
        runtime_context = None
        log.warning("failed to load Elastic Beanstalk environment config file")
