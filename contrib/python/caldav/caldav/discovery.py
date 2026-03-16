#!/usr/bin/env python
"""
RFC 6764 - Locating Services for Calendaring and Contacts (CalDAV/CardDAV)

This module implements DNS-based service discovery for CalDAV and CardDAV
servers as specified in RFC 6764. It allows clients to discover service
endpoints from just a domain name or email address.

Discovery methods (in order of preference):
1. DNS SRV records (_caldavs._tcp / _carddavs._tcp for TLS)
2. DNS TXT records (for path information)
3. Well-Known URIs (/.well-known/caldav or /.well-known/carddav)

SECURITY CONSIDERATIONS:
    DNS-based discovery is vulnerable to attacks if DNS is not secured with DNSSEC:

    - DNS Spoofing: Attackers can provide malicious SRV/TXT records pointing to
      attacker-controlled servers
    - Downgrade Attacks: Malicious DNS can specify non-TLS services, causing
      credentials to be sent in plaintext
    - Man-in-the-Middle: Even with HTTPS, attackers can redirect to their servers

    MITIGATIONS:
    - require_tls=True (DEFAULT): Only accept HTTPS connections, preventing
      downgrade attacks
    - ssl_verify_cert=True (DEFAULT): Verify TLS certificates per RFC 6125
    - Domain validation (RFC 6764 Section 8): Discovered hostnames must be
      in the same domain as the queried domain to prevent redirection attacks
    - Use DNSSEC when possible for DNS integrity
    - Manually verify discovered endpoints for sensitive applications
    - Consider certificate pinning for known domains

    For high-security environments, manual configuration may be preferable to
    automatic discovery.

See: https://datatracker.ietf.org/doc/html/rfc6764
"""
import logging
from dataclasses import dataclass
from typing import List
from typing import Optional
from typing import Tuple
from urllib.parse import urljoin
from urllib.parse import urlparse

import dns.exception
import dns.resolver

try:
    import niquests as requests
except ImportError:
    import requests

from caldav.lib.error import DAVError

log = logging.getLogger(__name__)


class DiscoveryError(DAVError):
    """Raised when service discovery fails"""

    pass


@dataclass
class ServiceInfo:
    """Information about a discovered CalDAV/CardDAV service"""

    url: str
    hostname: str
    port: int
    path: str
    tls: bool
    priority: int = 0
    weight: int = 0
    source: str = "unknown"  # 'srv', 'txt', 'well-known', 'manual'
    username: Optional[str] = None  # Extracted from email address if provided

    def __str__(self) -> str:
        return f"ServiceInfo(url={self.url}, source={self.source}, priority={self.priority}, username={self.username})"


def _is_subdomain_or_same(discovered_domain: str, original_domain: str) -> bool:
    """
    Check if discovered domain is the same as or a subdomain of the original domain.

    This prevents DNS hijacking attacks where malicious DNS records redirect
    to completely different domains (e.g., acme.com -> evil.hackers.are.us).

    Args:
        discovered_domain: The hostname discovered via DNS SRV/TXT or well-known URI
        original_domain: The domain from the user's identifier

    Returns:
        True if discovered_domain is safe (same domain or subdomain), False otherwise

    Examples:
        >>> _is_subdomain_or_same('calendar.example.com', 'example.com')
        True
        >>> _is_subdomain_or_same('example.com', 'example.com')
        True
        >>> _is_subdomain_or_same('evil.com', 'example.com')
        False
        >>> _is_subdomain_or_same('subdomain.calendar.example.com', 'example.com')
        True
        >>> _is_subdomain_or_same('exampleXcom.evil.com', 'example.com')
        False
    """
    # Normalize to lowercase for comparison
    discovered = discovered_domain.lower().strip(".")
    original = original_domain.lower().strip(".")

    # Same domain is always allowed
    if discovered == original:
        return True

    # Check if discovered is a subdomain of original
    # Must end with .original_domain to be a valid subdomain
    if discovered.endswith("." + original):
        return True

    return False


def _extract_domain(identifier: str) -> Tuple[str, Optional[str]]:
    """
    Extract domain and optional username from an email address or URL.

    Args:
        identifier: Email address (user@example.com) or domain (example.com)

    Returns:
        A tuple of (domain, username) where username is None if not present

    Examples:
        >>> _extract_domain('user@example.com')
        ('example.com', 'user')
        >>> _extract_domain('example.com')
        ('example.com', None)
        >>> _extract_domain('https://caldav.example.com/path')
        ('caldav.example.com', None)
    """
    # If it looks like a URL, parse it
    if "://" in identifier:
        parsed = urlparse(identifier)
        return (parsed.hostname or identifier, None)

    # If it contains @, it's an email address
    if "@" in identifier:
        parts = identifier.split("@")
        username = parts[0].strip() if parts[0] else None
        domain = parts[-1].strip()
        return (domain, username)

    # Otherwise assume it's already a domain
    return (identifier.strip(), None)


def _parse_txt_record(txt_data: str) -> Optional[str]:
    """
    Parse TXT record data to extract the path attribute.

    According to RFC 6764, TXT records contain attribute=value pairs.
    We're looking for the 'path' attribute.

    Args:
        txt_data: TXT record data (e.g., "path=/caldav/")

    Returns:
        The path value or None if not found

    Examples:
        >>> _parse_txt_record('path=/caldav/')
        '/caldav/'
        >>> _parse_txt_record('path=/caldav/ other=value')
        '/caldav/'
    """
    # TXT records are key=value pairs separated by spaces
    for pair in txt_data.split():
        if "=" in pair:
            key, value = pair.split("=", 1)
            if key.strip().lower() == "path":
                return value.strip()
    return None


def _srv_lookup(
    domain: str, service_type: str, use_tls: bool = True
) -> List[Tuple[str, int, int, int]]:
    """
    Perform DNS SRV record lookup.

    Args:
        domain: The domain to query
        service_type: Either 'caldav' or 'carddav'
        use_tls: If True, query for TLS service (_caldavs), else non-TLS (_caldav)

    Returns:
        List of tuples: (hostname, port, priority, weight)
        Sorted by priority (lower is better), then randomized by weight
    """
    # Construct the SRV record name
    # RFC 6764 defines: _caldavs._tcp, _caldav._tcp, _carddavs._tcp, _carddav._tcp
    service_suffix = "s" if use_tls else ""
    srv_name = f"_{service_type}{service_suffix}._tcp.{domain}"

    log.debug(f"Performing SRV lookup for {srv_name}")

    try:
        answers = dns.resolver.resolve(srv_name, "SRV")
        results = []

        for rdata in answers:
            hostname = str(rdata.target).rstrip(".")
            port = int(rdata.port)
            priority = int(rdata.priority)
            weight = int(rdata.weight)

            log.debug(
                f"Found SRV record: {hostname}:{port} (priority={priority}, weight={weight})"
            )
            results.append((hostname, port, priority, weight))

        # Sort by priority (lower is better), then by weight (for weighted random selection)
        results.sort(key=lambda x: (x[2], -x[3]))
        return results

    except (
        dns.resolver.NXDOMAIN,
        dns.resolver.NoAnswer,
        dns.exception.DNSException,
    ) as e:
        log.debug(f"SRV lookup failed for {srv_name}: {e}")
        return []


def _txt_lookup(domain: str, service_type: str, use_tls: bool = True) -> Optional[str]:
    """
    Perform DNS TXT record lookup to find the service path.

    Args:
        domain: The domain to query
        service_type: Either 'caldav' or 'carddav'
        use_tls: If True, query for TLS service (_caldavs), else non-TLS (_caldav)

    Returns:
        The path from the TXT record, or None if not found
    """
    service_suffix = "s" if use_tls else ""
    txt_name = f"_{service_type}{service_suffix}._tcp.{domain}"

    log.debug(f"Performing TXT lookup for {txt_name}")

    try:
        answers = dns.resolver.resolve(txt_name, "TXT")

        for rdata in answers:
            # TXT records can have multiple strings; join them
            txt_data = "".join(
                [
                    s.decode("utf-8") if isinstance(s, bytes) else s
                    for s in rdata.strings
                ]
            )
            log.debug(f"Found TXT record: {txt_data}")

            path = _parse_txt_record(txt_data)
            if path:
                return path

    except (
        dns.resolver.NXDOMAIN,
        dns.resolver.NoAnswer,
        dns.exception.DNSException,
    ) as e:
        log.debug(f"TXT lookup failed for {txt_name}: {e}")

    return None


def _well_known_lookup(
    domain: str, service_type: str, timeout: int = 10, ssl_verify_cert: bool = True
) -> Optional[ServiceInfo]:
    """
    Try to discover service via Well-Known URI (RFC 5785).

    According to RFC 6764, if SRV/TXT lookup fails, clients should try:
    - https://domain/.well-known/caldav
    - https://domain/.well-known/carddav

    Security: Redirects to different domains are validated per RFC 6764 Section 8.

    Args:
        domain: The domain to query
        service_type: Either 'caldav' or 'carddav'
        timeout: Request timeout in seconds
        ssl_verify_cert: Whether to verify SSL certificates

    Returns:
        ServiceInfo if successful, None otherwise
    """
    well_known_path = f"/.well-known/{service_type}"
    url = f"https://{domain}{well_known_path}"

    log.debug(f"Trying well-known URI: {url}")

    try:
        # We expect a redirect to the actual service URL
        # Use HEAD or GET with allow_redirects
        response = requests.get(
            url,
            timeout=timeout,
            verify=ssl_verify_cert,
            allow_redirects=False,  # We want to see the redirect
        )

        # RFC 6764 says we should follow redirects
        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get("Location")
            if location:
                log.debug(f"Well-known URI redirected to: {location}")

                # Make it an absolute URL if it's relative
                final_url = urljoin(url, location)
                parsed = urlparse(final_url)
                redirect_hostname = parsed.hostname or domain

                # RFC 6764 Section 8 Security: Validate redirect target is in same domain
                if not _is_subdomain_or_same(redirect_hostname, domain):
                    log.warning(
                        f"RFC 6764 Security: Rejecting well-known redirect to different domain. "
                        f"Queried domain: {domain}, Redirect target: {redirect_hostname}. "
                        f"Ignoring this redirect for security."
                    )
                    return None

                return ServiceInfo(
                    url=final_url,
                    hostname=redirect_hostname,
                    port=parsed.port or (443 if parsed.scheme == "https" else 80),
                    path=parsed.path or "/",
                    tls=parsed.scheme == "https",
                    source="well-known",
                )

        # If we get 200 OK, the well-known URI itself is the service endpoint
        if response.status_code == 200:
            log.debug(f"Well-known URI is the service endpoint: {url}")
            return ServiceInfo(
                url=url,
                hostname=domain,
                port=443,
                path=well_known_path,
                tls=True,
                source="well-known",
            )

    except requests.exceptions.RequestException as e:
        log.debug(f"Well-known URI lookup failed: {e}")

    return None


def discover_service(
    identifier: str,
    service_type: str = "caldav",
    timeout: int = 10,
    ssl_verify_cert: bool = True,
    prefer_tls: bool = True,
    require_tls: bool = True,
) -> Optional[ServiceInfo]:
    """
    Discover CalDAV or CardDAV service for a domain or email address.

    This is the main entry point for RFC 6764 service discovery.
    It tries multiple methods in order:
    1. DNS SRV records (with TLS preferred)
    2. DNS TXT records for path information
    3. Well-Known URIs as fallback

    SECURITY WARNING:
        RFC 6764 discovery relies on DNS, which can be spoofed if not using DNSSEC.
        An attacker controlling DNS could:
        - Redirect connections to a malicious server
        - Downgrade from HTTPS to HTTP to capture credentials
        - Perform man-in-the-middle attacks

        By default, require_tls=True prevents HTTP downgrade attacks.
        For production use, consider:
        - Using DNSSEC-validated domains
        - Manual verification of discovered endpoints
        - Pinning certificates for known domains

    Args:
        identifier: Domain name (example.com) or email address (user@example.com)
        service_type: Either 'caldav' or 'carddav'
        timeout: Timeout for HTTP requests in seconds
        ssl_verify_cert: Whether to verify SSL certificates
        prefer_tls: If True, try TLS services first (only used if require_tls=False)
        require_tls: If True (default), ONLY accept TLS connections. This prevents
                     DNS-based downgrade attacks to plaintext HTTP. Set to False
                     only if you explicitly need to support non-TLS servers and
                     trust your DNS infrastructure.

    Returns:
        ServiceInfo object with discovered service details, or None if discovery fails

    Raises:
        DiscoveryError: If service_type is invalid

    Examples:
        >>> info = discover_service('user@example.com', 'caldav')
        >>> if info:
        ...     print(f"Service URL: {info.url}")

        >>> # Allow non-TLS (INSECURE - only for testing)
        >>> info = discover_service('user@example.com', 'caldav', require_tls=False)
    """
    if service_type not in ("caldav", "carddav"):
        raise DiscoveryError(
            reason=f"Invalid service_type: {service_type}. Must be 'caldav' or 'carddav'"
        )

    domain, username = _extract_domain(identifier)
    log.info(f"Discovering {service_type} service for domain: {domain}")
    if username:
        log.debug(f"Username extracted from identifier: {username}")

    # Try SRV/TXT records first (RFC 6764 section 5)
    # Security: require_tls=True prevents downgrade attacks
    if require_tls:
        tls_options = [True]  # Only accept TLS connections
        log.debug("require_tls=True: Only attempting TLS discovery")
    else:
        # Prefer TLS services over non-TLS when both are allowed
        tls_options = [True, False] if prefer_tls else [False, True]
        log.warning("require_tls=False: Allowing non-TLS connections (INSECURE)")

    for use_tls in tls_options:
        srv_records = _srv_lookup(domain, service_type, use_tls)

        if srv_records:
            # Use the highest priority record (first in sorted list)
            hostname, port, priority, weight = srv_records[0]

            # RFC 6764 Section 8 Security: Validate discovered hostname is in same domain
            # "clients SHOULD check that the target FQDN returned in the SRV record
            # matches the original service domain that was queried"
            if not _is_subdomain_or_same(hostname, domain):
                log.warning(
                    f"RFC 6764 Security: Rejecting SRV record pointing to different domain. "
                    f"Queried domain: {domain}, Target FQDN: {hostname}. "
                    f"This may indicate DNS hijacking or misconfiguration."
                )
                continue  # Try next TLS option or fall back to well-known

            # Try to get path from TXT record
            path = _txt_lookup(domain, service_type, use_tls)
            if not path:
                # RFC 6764 section 5: If no TXT record, try well-known URI for path
                log.debug("No TXT record found, using root path")
                path = "/"

            # Construct the service URL
            scheme = "https" if use_tls else "http"
            # Only include port in URL if it's non-standard
            default_port = 443 if use_tls else 80
            if port != default_port:
                url = f"{scheme}://{hostname}:{port}{path}"
            else:
                url = f"{scheme}://{hostname}{path}"

            log.info(f"Discovered {service_type} service via SRV: {url}")

            return ServiceInfo(
                url=url,
                hostname=hostname,
                port=port,
                path=path,
                tls=use_tls,
                priority=priority,
                weight=weight,
                source="srv",
                username=username,
            )

    # Fallback to well-known URI (RFC 6764 section 5)
    log.debug("SRV lookup failed, trying well-known URI")
    well_known_info = _well_known_lookup(domain, service_type, timeout, ssl_verify_cert)

    if well_known_info:
        # Preserve username from email address
        well_known_info.username = username
        log.info(
            f"Discovered {service_type} service via well-known URI: {well_known_info.url}"
        )
        return well_known_info

    # All discovery methods failed
    log.warning(f"Failed to discover {service_type} service for {domain}")
    return None


def discover_caldav(
    identifier: str,
    timeout: int = 10,
    ssl_verify_cert: bool = True,
    prefer_tls: bool = True,
    require_tls: bool = True,
) -> Optional[ServiceInfo]:
    """
    Convenience function to discover CalDAV service.

    Args:
        identifier: Domain name or email address
        timeout: Timeout for HTTP requests in seconds
        ssl_verify_cert: Whether to verify SSL certificates
        prefer_tls: If True, try TLS services first
        require_tls: If True (default), only accept TLS connections

    Returns:
        ServiceInfo object or None
    """
    return discover_service(
        identifier=identifier,
        service_type="caldav",
        timeout=timeout,
        ssl_verify_cert=ssl_verify_cert,
        prefer_tls=prefer_tls,
        require_tls=require_tls,
    )


def discover_carddav(
    identifier: str,
    timeout: int = 10,
    ssl_verify_cert: bool = True,
    prefer_tls: bool = True,
    require_tls: bool = True,
) -> Optional[ServiceInfo]:
    """
    Convenience function to discover CardDAV service.

    Args:
        identifier: Domain name or email address
        timeout: Timeout for HTTP requests in seconds
        ssl_verify_cert: Whether to verify SSL certificates
        prefer_tls: If True, try TLS services first
        require_tls: If True (default), only accept TLS connections

    Returns:
        ServiceInfo object or None
    """
    return discover_service(
        identifier=identifier,
        service_type="carddav",
        timeout=timeout,
        ssl_verify_cert=ssl_verify_cert,
        prefer_tls=prefer_tls,
        require_tls=require_tls,
    )
