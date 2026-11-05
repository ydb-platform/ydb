# Security policy for GLib

 * [Supported Versions](#Supported-Versions)
 * [Reporting a Vulnerability](#Reporting-a-Vulnerability)
 * [Security Announcements](#Security-Announcements)
 * [Acknowledgements](#Acknowledgements)

## Supported Versions

Upstream GLib only supports the most recent stable release series, and the
current development release series. Any older stable release series are no
longer supported, although they may still receive backported security updates
in long-term support distributions. Such support is up to the distributions,
though.

Under GLib’s versioning scheme, stable release series have an *even* minor
component (for example, 2.66.0, 2.66.1, 2.68.3), and development release series
have an *odd* minor component (2.67.1, 2.69.0).

## Signed Releases

The git tags for all releases ≥2.58.0 are signed by a maintainer using
[git-evtag](https://github.com/cgwalters/git-evtag). The maintainer will use
their personal GPG key; there is currently not necessarily a formal chain of
trust for these keys. Please [create an issue](https://gitlab.gnome.org/GNOME/glib/-/issues/new)
if you would like to work on improving this.

Unsigned releases ≥2.58.0 should not be trusted. Releases prior to 2.58.0 were
not signed.

## Reporting a Vulnerability

If you think you've identified a security issue in GLib, GObject or GIO, please
**do not** report the issue publicly via a mailing list, IRC, a public issue on
the GitLab issue tracker, a merge request, or any other public venue.

Instead, report a
[*confidential* issue in the GitLab issue tracker](https://gitlab.gnome.org/GNOME/glib/-/issues/new?issue[confidential]=1),
with the “This issue is confidential” box checked. Please include as many
details as possible, including a minimal reproducible example of the issue, and
an idea of how exploitable/severe you think it is.

**Do not** provide a merge request to fix the issue, as there is currently no
way to make confidential merge requests on gitlab.gnome.org. If you have patches
which fix the security issue, please attach them to your confidential issue as
patch files.

Confidential issues are only visible to the reporter and the GLib maintainers.

As per the [GNOME security policy](https://security.gnome.org/), the next steps
are then:
 * The report is triaged.
 * Code is audited to find any potential similar problems.
 * If it is determined, in consultation with the submitter, that a CVE is
   required, the submitter obtains one via [cveform.mitre.org](https://cveform.mitre.org/).
 * The fix is prepared for the development branch, and for the most recent
   stable branch.
 * The fix is submitted to the public repository.
 * On the day the issue and fix are made public, an announcement is made on the
   [public channels listed below](#Security-Announcements).
 * A new release containing the fix is issued.

## Security Announcements

Security announcements are made publicly via the
[`distributor` tag on discourse.gnome.org](https://discourse.gnome.org/tag/distributor)
and cross-posted to the
[distributor-list](https://mail.gnome.org/mailman/listinfo/distributor-list).

Announcements for security issues with wide applicability or high impact may
additionally be made via
[oss-security@lists.openwall.com](https://www.openwall.com/lists/oss-security/).

## Acknowledgements

This text was partially based on the
[github.com/containers security policy](https://github.com/containers/common/blob/HEAD/SECURITY.md),
and partially based on the [flatpak security policy](https://github.com/flatpak/flatpak/blob/HEAD/SECURITY.md).
