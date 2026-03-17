#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import base64
import ipaddress
import json
import os
import re
import socket
import ssl
import tempfile
from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import Any, AnyStr
from urllib.request import getproxies

import OpenSSL

from .compat import IS_WINDOWS, urlparse
from .cursor import SnowflakeCursor
from .vendored import urllib3

logger = getLogger(__name__)

if IS_WINDOWS:
    import winreg


class ConnectionDiagnostic:
    """Implementation of a connection test utility for Snowflake connector

    Use new ConnectionTest() to get the object.
    """

    def __init__(
        self,
        account: str,
        host: str,
        connection_diag_log_path: str | None = None,
        connection_diag_whitelist_path: str | None = None,
        proxy_host: str | None = None,
        proxy_port: str | None = None,
        proxy_user: str | None = None,
        proxy_password: str | None = None,
    ) -> None:
        self.account = account
        self.host = host
        self.test_results: dict[str, list[str]] = {
            "INITIAL": [],
            "PROXY": [],
            "SNOWFLAKE_URL": [],
            "STAGE": [],
            "OCSP_RESPONDER": [],
            "OUT_OF_BAND_TELEMETRY": [],
            "IGNORE": [],
        }
        host_type = "INITIAL"
        self.__append_message(host_type, f"Specified snowflake account: {self.account}")
        self.__append_message(
            host_type, f"Host based on specified account: {self.host}"
        )
        if ".com.snowflakecomputing.com" in self.host:
            self.host = host.split(".com.snow", 1)[0] + ".com"
            logger.warning(
                f"Account should not have snowflakecomputing.com in it. You provided {host}.  "
                f"Continuing with fixed host."
            )
            self.__append_message(
                host_type,
                f"We removed extra .snowflakecomputing.com and will continue with host: "
                f"{self.host}",
            )
        else:
            self.host = host

        self.ocsp_urls: list[str] = []
        self.crl_urls: list[str] = []
        self.cert_info: dict[str, dict[str, Any]] = {}
        self.proxy_type: str = "params"
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_password = proxy_password
        if self.proxy_host is None:
            proxy_url = os.getenv("HTTPS_PROXY")
            self.proxy_type = "environment"
        else:
            proxy_url = getproxies()["https"]
            self.proxy_type = "system"

        (
            self.proxy_host,
            self.proxy_port,
            self.proxy_user,
            self.proxy_password,
        ) = self.__parse_proxy(proxy_url)
        self.__https_host_report(self.host)
        self.full_connection_diag_log_path: Path | None = (
            Path(connection_diag_log_path)
            if connection_diag_log_path is not None
            else None
        )
        self.full_connection_diag_whitelist_path: Path | None = (
            Path(connection_diag_whitelist_path)
            if connection_diag_whitelist_path is not None
            else None
        )
        self.tmpdir: str = tempfile.gettempdir()
        if self.full_connection_diag_log_path is None:
            self.full_connection_diag_log_path = Path(self.tmpdir)
        else:
            if not self.full_connection_diag_log_path.is_absolute():
                logger.warning(
                    f"Path {self.full_connection_diag_log_path} for connection test is not absolute."
                )
                self.full_connection_diag_log_path = Path(self.tmpdir)
            elif not self.full_connection_diag_log_path.exists():
                logger.warning(
                    f"Path {self.full_connection_diag_log_path} for connection test does not exist."
                )
                self.full_connection_diag_log_path = Path(self.tmpdir)

        self.report_file: Path = (
            self.full_connection_diag_log_path / "SnowflakeConnectionTestReport.txt"
        )
        logger.info(f"Reporting to file {self.report_file}")

        if self.full_connection_diag_whitelist_path is not None:
            if not self.full_connection_diag_whitelist_path.is_absolute():
                logger.warning(
                    f"Path '{self.full_connection_diag_whitelist_path}' for connection test whitelist is not absolute."
                )
                logger.warning(
                    "Will connect to Snowflake for whitelist json instead.  If you did not provide a valid "
                    "password, please make sure to update and run again."
                )
                self.full_connection_diag_whitelist_path = None
            elif not self.full_connection_diag_whitelist_path.exists():
                logger.warning(
                    f"File '{self.full_connection_diag_whitelist_path}' for connection test whitelist does not exist."
                )
                logger.warning(
                    "Will connect to Snowflake for whitelist json instead.  If you did not provide a valid "
                    "password, please make sure to update and run again."
                )
                self.full_connection_diag_whitelist_path = None

        self.whitelist_sql: str = "select /* snowflake-connector-python:connection_diagnostics */ system$whitelist();"

        if self.__is_privatelink():
            self.ocsp_urls.append(f"ocsp.{self.host}")
            self.whitelist_sql = "select system$whitelist_privatelink();"
        else:
            self.ocsp_urls.append("ocsp.snowflakecomputing.com")

        self.whitelist_retrieval_success: bool = False
        self.cursor: SnowflakeCursor | None = None

    def __parse_proxy(self, proxy_url: str) -> tuple[str, str, str, str]:
        parsed = urlparse(proxy_url)
        proxy_host = parsed.hostname
        proxy_port = parsed.port
        proxy_user = parsed.username
        proxy_password = parsed.password
        return proxy_host, proxy_port, proxy_user, proxy_password

    def __test_socket_get_cert(
        self,
        host: str,
        port: int = 443,
        timeout: int = 10,
        host_type: str = "SNOWFLAKE_URL",
    ) -> str:
        try:
            self.__list_ips(host, host_type=host_type)
            connect_creds: str = ""
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.settimeout(timeout)
            if self.proxy_host is not None:
                proxy_addr = (self.proxy_host, self.proxy_port)
                if self.proxy_user is not None:
                    proxy_auth = f"{self.proxy_user}:{self.proxy_password}"
                    proxy_auth = proxy_auth.encode("utf-8")
                    credentials = base64.b64encode(proxy_auth).decode().strip("\n")
                    connect_creds = f"Proxy-Authorization: Basic {credentials}\r\n"
                conn.connect(proxy_addr)
            else:
                conn.connect((host, int(port)))

            if port == 443:
                if self.proxy_host is not None:
                    connect = f"CONNECT {host}:{port} HTTP/1.1\r\n{connect_creds}"
                    connect = f"{connect}Host: {host}\r\n\r\n"
                    conn.send(str.encode(connect))
                    conn.recv(4096).decode("utf-8")

                context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                sock = context.wrap_socket(conn, server_hostname=host)
                certificate = ssl.DER_cert_to_PEM_cert(sock.getpeercert(True))
                conn.close()
                return certificate
            else:
                if self.proxy_host is not None:
                    connect = (
                        f"CONNECT {host}:{port} HTTP/1.1\r\n{connect_creds}\r\n\r\n"
                    )
                else:
                    connect = (
                        f"GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
                    )

                conn.send(str.encode(connect))
                response = conn.recv(4096).decode("utf-8")
                conn.close()

            if response is not None:
                good_responses = "(200|301|cloudfront)"
                if not re.search(good_responses, response):
                    self.__append_message(
                        host_type, f"{host}:{port}: URL Check: Failed: {response}"
                    )
                    return "FAILED"
            self.__append_message(
                host_type, f"{host}:{port}: URL Check: Connected Successfully"
            )
            return "SUCCESS"
        except ssl.SSLError as e:
            if "WRONG_VERSION_NUMBER" in str(e):
                self.__append_message(
                    host_type,
                    f"{host}:{port}: URL Check: Failed: Proxy Auth Error: {e}",
                )
            return "FAILED"
        except Exception as e:
            self.__append_message(
                host_type, f"{host}:{port}: URL Check: Failed: Unknown Exception: {e}"
            )
            return "FAILED"

        self.__append_message(
            host_type, f"{host}:{port}: URL Check: Connected Successfully"
        )
        return "SUCCESS"

    def run_post_test(self) -> None:
        results: list[str] = []
        if self.full_connection_diag_whitelist_path is None:
            if self.cursor is not None:
                try:
                    results = self.cursor.execute(
                        self.whitelist_sql, _is_internal=True
                    ).fetchall()[0][0]
                    results = json.loads(str(results))
                    self.whitelist_retrieval_success = True
                except Exception as e:
                    logger.warning(f"Unable to do whitelist checks: exception: {e}")
        else:
            results_file = open(self.full_connection_diag_whitelist_path)
            results = json.load(results_file)
            self.whitelist_retrieval_success = True

        for result in results:
            host_type = result["type"]
            host = result["host"]
            host_port = result["port"]

            if host_type in ("OCSP_RESPONDER"):
                if host not in self.ocsp_urls:
                    self.__test_socket_get_cert(
                        host, port=host_port, host_type=host_type
                    )
            elif host_type in ("STAGE", "OUT_OF_BAND_TELEMETRY"):
                try:
                    self.__https_host_report(host, port=host_port, host_type=host_type)
                except Exception:
                    pass

    def __is_privatelink(self) -> bool:
        return "privatelink" in self.host

    def __list_ips(self, host: str, host_type: str = "SNOWFLAKE_URL") -> None:
        try:
            ips = socket.gethostbyname_ex(host)[2]
            base_message = f"{host}: nslookup results"
            if "snowflakecomputing" in host:
                for ip in ips:
                    if ipaddress.ip_address(ip).is_private:
                        if not self.__is_privatelink():
                            self.__append_message(
                                host_type,
                                f"{base_message}: private ip: {ip}: WARNING: this is not "
                                f"typical for a non-privatelink account",
                            )
                    else:
                        if self.__is_privatelink():
                            self.__append_message(
                                host_type,
                                f"{base_message}: public ip: {ip}: WARNING: privatelink accounts "
                                f"must have a private ip.",
                            )
                        else:
                            self.__append_message(
                                host_type, f"{base_message}: public ip: {ip}"
                            )
            else:
                self.__append_message(host_type, f"{base_message}: {ips}")
        except Exception as e:
            logger.warning(f"Connectivity Test Exception in list_ips: {e}")

    def __https_host_report(
        self, host: str, port: int = 443, host_type: str = "SNOWFLAKE_URL"
    ):
        try:
            certificate = self.__test_socket_get_cert(
                host, port=port, host_type=host_type
            )
            if "BEGIN CERTIFICATE" in certificate:
                x509 = OpenSSL.crypto.load_certificate(
                    OpenSSL.crypto.FILETYPE_PEM, certificate
                )

                result = {
                    "subject": dict(x509.get_subject().get_components()),
                    "issuer": dict(x509.get_issuer().get_components()),
                    "serialNumber": x509.get_serial_number(),
                    "version": x509.get_version(),
                    "notBefore": datetime.strptime(
                        str(x509.get_notBefore().decode("utf-8")), "%Y%m%d%H%M%SZ"
                    ),
                    "notAfter": datetime.strptime(
                        str(x509.get_notAfter().decode("utf-8")), "%Y%m%d%H%M%SZ"
                    ),
                }
                self.cert_info[host] = result
                extensions = (
                    x509.get_extension(i) for i in range(x509.get_extension_count())
                )
                extension_data = {}
                for e in extensions:
                    extension_data[e.get_short_name().decode("utf-8")] = str(e)

                _, _, host_suffix = host.partition(".")
                if host_suffix in str(result["subject"]):
                    self.__append_message(
                        host_type, f"{host}:{port}: URL Check: Connected Successfully"
                    )
                elif "subjectAltName" in extension_data:
                    if host_suffix in str(extension_data["subjectAltName"]):
                        self.__append_message(
                            host_type,
                            f"{host}:{port}: URL Check: Connected Successfully",
                        )
                    else:
                        self.__append_message(
                            host_type,
                            f"{host}:{port}: URL Check: Failed: Certificate mismatch: Host not in subject or alt names",
                        )
                self.__append_message(host_type, f"{host}: Cert info:")
                self.__append_message(
                    host_type, f"{host}: subject: {result['subject']}"
                )
                self.__append_message(host_type, f"{host}: issuer: {result['issuer']}")
                self.__append_message(
                    host_type, f"{host}: serialNumber: {result['serialNumber']}"
                )
                self.__append_message(
                    host_type, f"{host}: version: {result['version']}"
                )
                self.__append_message(
                    host_type, f"{host}: notBefore: {result['notBefore']}"
                )
                self.__append_message(
                    host_type, f"{host}: notAfter: {result['notAfter']}"
                )

                if host_type == "SNOWFLAKE_URL":
                    if "authorityInfoAccess" in extension_data:
                        ocsp_urls_orig = re.findall(
                            r"(https?://\S+)", extension_data["authorityInfoAccess"]
                        )
                        for url in ocsp_urls_orig:
                            self.ocsp_urls.append(url.split("/")[2])
                    else:
                        self.__append_message(
                            "INITIAL", "Unable to find ocsp URLs in certificate."
                        )

                    if "crlDistributionPoints" in extension_data:
                        crl_urls_orig = re.findall(
                            r"(https?://\S+)", extension_data["crlDistributionPoints"]
                        )
                        for url in crl_urls_orig:
                            self.crl_urls.append(url.split("/")[2])
                    else:
                        self.__append_message(
                            "IGNORE", "Unable to find crl URLs in certificate."
                        )

                if "subjectAltName" in extension_data:
                    self.__append_message(
                        host_type,
                        f"{host}: subjectAltName: {extension_data['subjectAltName']}",
                    )

                self.__append_message(host_type, f"{host}: crlUrls: {self.crl_urls}")
                self.__append_message(host_type, f"{host}: ocspURLs: {self.ocsp_urls}")

        except Exception as e:
            logger.warning(f"Connectivity Test Exception in https_host_report: {e}")

    def __get_issuer_string(self, issuer: dict[bytes, bytes]) -> str:
        issuer: dict[str, str] = {
            y.decode("ascii"): issuer.get(y).decode("ascii") for y in issuer.keys()
        }
        issuer_str: str = (
            re.sub('[{}"]', "", json.dumps(issuer)).replace(": ", "=").replace(",", ";")
        )
        return issuer_str

    def __append_message(self, host_type: str, message: str) -> None:
        self.test_results[host_type].append(f"{host_type}: {message}")

    def __check_for_proxies(self) -> None:
        # TODO: See if we need to do anything for noproxy
        # If we need more proxy checks, this site might work
        # curl -k -v https://amibehindaproxy.com 2>&1 | tee | grep alert
        env_proxy_backup: dict[str, str] = {}
        proxy_keys = ("HTTP_PROXY", "HTTPS_PROXY", "https_proxy", "http_proxy")
        restore_keys = []

        for proxy_key in proxy_keys:
            if proxy_key in os.environ.keys():
                env_proxy_backup[proxy_key] = os.environ.get(proxy_key)
                del os.environ[proxy_key]
                restore_keys.append(proxy_key)

        host_type = "PROXY"
        system_proxies = getproxies()
        self.__append_message(
            host_type,
            f"Proxies with Env vars removed(SYSTEM PROXIES): {system_proxies}",
        )

        if "https" in system_proxies.keys():
            proxy_host, proxy_port, proxy_user, proxy_password = self.__parse_proxy(
                getproxies()["https"]
            )
            if proxy_user is not None:
                proxy_url_example = (
                    f"http://{proxy_user}:{proxy_password}@{proxy_host}:{proxy_port}"
                )
            else:
                proxy_url_example = f"http://{proxy_host}:{proxy_port}"
            self.__append_message(
                host_type,
                f"""If there are failures, try using the SYSTEM PROXY: On Windows, do
                                                 "set HTTPS_PROXY='{proxy_url_example}'".  On Linux/Mac, do
                                                  "export HTTPS_PROXY='{proxy_url_example}'" """,
            )

        for restore_key in restore_keys:
            os.environ[restore_key] = env_proxy_backup[restore_key]

        self.__append_message(
            host_type, f"Proxies with Env vars restored(ENV PROXIES): {getproxies()}"
        )

        cert_authorities = (
            "C=US; O=Google Trust Services LLC",
            "C=US; O=Amazon",
            "C=US; O=DigiCert Inc",
        )

        check_pattern = f"(^{'|^'.join(cert_authorities)})"
        issuer = self.__get_issuer_string(self.cert_info[self.host]["issuer"])
        if not re.search(check_pattern, issuer):
            self.__append_message(
                host_type,
                f"There is likely a proxy because the issuer for {self.host} is "
                f"not correct. Got {issuer} and expected one of {cert_authorities}",
            )

        test_host = "www.google.com"
        self.__https_host_report(test_host, port=443, host_type="IGNORE")
        issuer = self.__get_issuer_string(self.cert_info[test_host]["issuer"])
        if not re.search(check_pattern, issuer):
            self.__append_message(
                host_type,
                f"There is likely a proxy because the issuer for {test_host} is "
                f"not correct. Got {issuer} and expected one of {cert_authorities}",
            )

        # Get Windows proxy info from Registry just in case:
        if IS_WINDOWS:
            registry_start_key = "Software\\Microsoft\\Windows\\CurrentVersion"
            hkey_strings = ["HKEY_CURRENT_USER", "HKEY_LOCAL_MACHINE"]
            for hkey_str in hkey_strings:
                self.__walk_win_registry(host_type, hkey_str, registry_start_key)

        try:
            # Using a URL that does not exist is a check for a transparent proxy
            cert_reqs = "CERT_NONE"
            urllib3.disable_warnings()
            if self.proxy_host is None:
                http = urllib3.PoolManager(cert_reqs=cert_reqs)
            else:
                default_headers = urllib3.util.make_headers(
                    proxy_basic_auth=f"{self.proxy_user}:{self.proxy_password}"
                )
                http = urllib3.ProxyManager(
                    os.environ["HTTPS_PROXY"],
                    proxy_headers=default_headers,
                    timeout=10.0,
                    cert_reqs=cert_reqs,
                )
            resp = http.request(
                "GET", "https://ireallyshouldnotexistatallanywhere.com", timeout=10.0
            )

            # squid does not throw exception.  Check HTML
            if "does not exist" in str(resp.data.decode("utf-8")):
                self.__append_message(
                    host_type, "It is likely there is a proxy based on HTTP response."
                )
        except Exception as e:
            if "NewConnectionError" in str(e):
                self.__append_message(
                    host_type,
                    f"Proxy check using invalid URL did not show proxy: Review result, "
                    f"but you can probably ignore: Result: {e}",
                )
            elif "ProxyError" in str(e):
                self.__append_message(
                    host_type, f"It is likely there is a proxy based on Exception: {e}"
                )
            else:
                self.__append_message(
                    host_type,
                    f"Could not determine if a proxy does or does not exist based on Exception: {e}",
                )

    def run_test(self) -> None:
        self.__check_for_proxies()
        self.ocsp_urls = list(set(self.ocsp_urls))
        for url in self.ocsp_urls:
            self.__test_socket_get_cert(url, port=80, host_type="OCSP_RESPONDER")

    def generate_report(self) -> None:
        message = (
            "=========Connectivity diagnostic report================================"
        )
        initial_joined_results = "\n".join(self.test_results["INITIAL"])
        message = f"{message}\n" f"{initial_joined_results}\n"

        proxy_joined_results = "\n".join(self.test_results["PROXY"])
        message = (
            f"{message}\n"
            "=========Proxy information - These are best guesses, not guarantees====\n"
            f"{proxy_joined_results}\n"
        )

        snowflake_url_joined_results = "\n".join(self.test_results["SNOWFLAKE_URL"])
        message = (
            f"{message}\n"
            "=========Snowflake URL information=====================================\n"
            f"{snowflake_url_joined_results}\n"
        )

        if self.whitelist_retrieval_success:
            snowflake_stage_joined_results = "\n".join(self.test_results["STAGE"])
            message = (
                f"{message}\n"
                "=========Snowflake Stage information===================================\n"
                "We retrieved stage info from the whitelist\n"
                f"{snowflake_stage_joined_results}\n"
            )
        else:
            message = (
                f"{message}\n"
                "=========Snowflake Stage information - Unavailable=====================\n"
                "We could not connect to Snowflake to get whitelist, so we do not have stage\n"
                f"diagnostic info\n"
            )

        message = (
            f"{message}\n"
            "=========Snowflake OCSP information===================================="
        )
        snowflake_ocsp_joined_results = "\n".join(self.test_results["OCSP_RESPONDER"])
        if self.whitelist_retrieval_success:
            message = (
                f"{message}\n"
                "We were able to retrieve system whitelist.\n"
                "These OCSP hosts came from the certificate and the whitelist."
            )
        else:
            message = (
                f"{message}\n"
                "We were unable to retrieve system whitelist.\n"
                "These OCSP hosts only came from the certificate."
            )
        message = f"{message}\n" f"{snowflake_ocsp_joined_results}\n"

        if self.whitelist_retrieval_success:
            snowflake_telemetry_joined_results = "\n".join(
                self.test_results["OUT_OF_BAND_TELEMETRY"]
            )
            message = (
                f"{message}\n"
                "=========Snowflake Out of bound telemetry check========================\n"
                f"{snowflake_telemetry_joined_results}\n"
            )

        logger.debug(message)
        self.report_file.write_text(message)

    def __get_win_registry_values(self, registry_key: AnyStr) -> dict[str, str]:
        """Gets values from windows registry key"""
        registry_key_values: dict = {}
        i = 0
        while True:
            try:
                registry_key_value = winreg.EnumValue(registry_key, i)
            except OSError:
                break
            registry_key_values[registry_key_value[0]] = registry_key_value[1:]
            i = i + 1
        return registry_key_values

    def __walk_win_registry(
        self, host_type: str, hkey_str: str, registry_key_str: str
    ) -> None:
        """Walks the windows registry to search for key relating to proxies"""
        if hkey_str == "HKEY_CURRENT_USER":
            hkey = winreg.HKEY_CURRENT_USER
        elif hkey_str == "HKEY_LOCAL_MACHINE":
            hkey = winreg.HKEY_LOCAL_MACHINE
        else:
            hkey = None

        registry = winreg.OpenKey(hkey, registry_key_str)
        i = 0
        if hkey is not None:
            try:
                while True:
                    registry_key = winreg.EnumKey(registry, i)
                    i = i + 1
                    if registry_key:
                        new_registry_key_str = os.path.join(
                            registry_key_str, registry_key
                        )
                        if (
                            "internet settings" in str(registry_key).lower()
                            or "wpad" in str(registry_key).lower()
                        ):
                            new_registry_key = winreg.OpenKey(
                                hkey, new_registry_key_str, 0, winreg.KEY_READ
                            )
                            values = self.__get_win_registry_values(new_registry_key)
                            if "AutoConfigURL" in values.keys():
                                wpad = values["AutoConfigURL"][0]
                                self.__append_message(
                                    host_type,
                                    f"There may be a proxy: Found a Wpad in Windows "
                                    f"registry: Check proxy config for auto detect "
                                    f"script: hkey: {hkey_str} : {new_registry_key_str} : "
                                    f"wpad: {wpad}",
                                )
                                # Let's see if we can get the wpad proxy info
                                http = urllib3.PoolManager(timeout=10.0)
                                url = f"http://{wpad}/wpad.dat"
                                try:
                                    resp = http.request("GET", url)
                                    proxy_info = resp.data.decode("utf-8")
                                    self.__append_message(
                                        host_type,
                                        f"Wpad request returned possible proxy: {proxy_info}",
                                    )
                                except Exception:
                                    pass

                            elif "ProxyServer" in values.keys():
                                proxy = values["ProxyServer"][0]
                                self.__append_message(
                                    host_type,
                                    f"There may be a proxy: Found a proxy server in "
                                    f"Windows registry: hkey: {hkey} :"
                                    f" {new_registry_key_str} : ProxyServer: {proxy}",
                                )
                            elif "ProxyEnable" in values.keys():
                                proxy_enable = values["ProxyEnable"][0]
                                if proxy_enable == 1:
                                    self.__append_message(
                                        host_type,
                                        f"There may be a proxy: Proxy is enabled per the "
                                        f"registry: hkey: {hkey} : {new_registry_key_str}"
                                        f" : ProxyEnable: {proxy_enable}",
                                    )
                            else:
                                self.__append_message(
                                    host_type,
                                    f"Found Wpad key in registry: Most likely nothing, "
                                    f"but review: hkey: {hkey_str} : "
                                    f"{new_registry_key_str}: value: {values}",
                                )

                        self.__walk_win_registry(
                            host_type, hkey_str, new_registry_key_str
                        )
            except OSError:
                pass
