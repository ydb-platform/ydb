# Security changelog

## Fixed in YDB 22.4.44, 11.28.2022 {#28-11-2022}

### CVE-2022-28228 {#cve-2022-28228}

Out-of-bounds read was discovered in YDB server. An attacker could construct a query with insert statement that would allow him to read sensitive information from other memory locations or cause a crash.

Link to CVE: [https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-28228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-28228).
