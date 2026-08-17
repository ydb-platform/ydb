# Security changelog

## Fixed in {{ ydb-short-name }} 22.4.44, 2022-11-28 {#28-11-2022}

### CVE-2022-28228 {#cve-2022-28228}

Out-of-bounds read was discovered in {{ ydb-short-name }} server. An attacker could construct a query with an insert statement that would allow them to access confidential information or cause a crash.

Link to CVE: [https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-28228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-28228).

Credits: Maxim Arnold.

## Fixed in {{ ydb-short-name }} Go SDK v3.53.3, 2023-10-17 {#17-10-2023}

### CVE-2023-45825 {#cve-2023-45825}

Token in custom credentials object can leak through logs.

Link to CVE: [https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2023-45825](https://nvd.nist.gov/vuln/detail/CVE-2023-45825).

Credits: Sergey Foster.

## Fixed in {{ ydb-short-name }} 25.3.1.25, 2026-04-03 {#03-04-2026}

### CVE-2026-10549 {#cve-2026-10549}

LDAP filter injection vulnerability allows an attacker with valid LDAP credentials to bypass group membership checks and gain unauthorized access to the database.

Link to CVE: [https://www.cve.org/CVERecord?id=CVE-2026-10549](https://www.cve.org/CVERecord?id=CVE-2026-10549).
