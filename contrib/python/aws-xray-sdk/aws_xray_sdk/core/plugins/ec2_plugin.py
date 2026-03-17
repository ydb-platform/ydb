import json
import logging
from urllib.request import Request, urlopen

log = logging.getLogger(__name__)

SERVICE_NAME = 'ec2'
ORIGIN = 'AWS::EC2::Instance'
IMDS_URL = 'http://169.254.169.254/latest/'


def initialize():
    """
    Try to get EC2 instance-id and AZ if running on EC2
    by querying http://169.254.169.254/latest/meta-data/.
    If not continue.
    """
    global runtime_context

    # get session token with 60 seconds TTL to not have the token lying around for a long time
    token = get_token()

    # get instance metadata
    runtime_context = get_metadata(token)


def get_token():
    """
    Get the session token for IMDSv2 endpoint valid for 60 seconds
    by specifying the X-aws-ec2-metadata-token-ttl-seconds header.
    """
    token = None
    try:
        headers = {"X-aws-ec2-metadata-token-ttl-seconds": "60"}
        token = do_request(url=IMDS_URL + "api/token",
                           headers=headers,
                           method="PUT")
    except Exception:
        log.warning("Failed to get token for IMDSv2")
    return token


def get_metadata(token=None):
    try:
        header = None
        if token:
            header = {"X-aws-ec2-metadata-token": token}

        metadata_json = do_request(url=IMDS_URL + "dynamic/instance-identity/document",
                                   headers=header,
                                   method="GET")

        return parse_metadata_json(metadata_json)
    except Exception:
        log.warning("Failed to get EC2 metadata")
        return {}


def parse_metadata_json(json_str):
    data = json.loads(json_str)
    dict = {
        'instance_id': data['instanceId'],
        'availability_zone': data['availabilityZone'],
        'instance_type': data['instanceType'],
        'ami_id': data['imageId']
    }

    return dict


def do_request(url, headers=None, method="GET"):
    if headers is None:
        headers = {}

    if url is None:
        return None

    req = Request(url=url)
    req.headers = headers
    req.method = method
    res = urlopen(req, timeout=1)
    return res.read().decode('utf-8')
