import socket
import requests


def is_connected_http(url="http://ya.ru", timeout=5):
    """
    Check internet connectivity using HTTP request.
    """
    try:
        response = requests.get(url, timeout=timeout)
        return response.status_code == 200
    except requests.ConnectionError:
        return False
    except requests.Timeout:
        return False


IS_INTERNET_AVAILABLE = True if is_connected_http() else False
