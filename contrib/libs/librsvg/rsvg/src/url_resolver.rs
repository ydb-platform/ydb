//! Determine which URLs are allowed for loading.

use std::fmt;
use std::ops::Deref;
use url::Url;

use crate::error::AllowedUrlError;

/// Decides which URLs are allowed to be loaded.
///
/// Currently only contains the base URL.
///
/// The plan is to add:
/// base_only:    Only allow to load content from the same base URL. By default
//                this restriction is enabled and requires to provide base_url.
/// include_xml:  Allows to use xi:include with XML. Enabled by default.
/// include_text: Allows to use xi:include with text. Enabled by default.
/// local_only:   Only allow to load content from the local filesystem.
///               Enabled by default.
#[derive(Clone)]
pub struct UrlResolver {
    /// Base URL; all relative references will be resolved with respect to this.
    pub base_url: Option<Url>,
}

impl UrlResolver {
    /// Creates a `UrlResolver` with defaults, and sets the `base_url`.
    pub fn new(base_url: Option<Url>) -> Self {
        UrlResolver { base_url }
    }

    /// Decides which URLs are allowed to be loaded based on the presence of a base URL.
    ///
    /// This function implements the policy described in "Security and locations of
    /// referenced files" in the [crate
    /// documentation](index.html#security-and-locations-of-referenced-files).
    pub fn resolve_href(&self, href: &str) -> Result<AllowedUrl, AllowedUrlError> {
        let url = Url::options()
            .base_url(self.base_url.as_ref())
            .parse(href)
            .map_err(AllowedUrlError::UrlParseError)?;

        // Allow loads of data: from any location
        if url.scheme() == "data" {
            return Ok(AllowedUrl(url));
        }

        // Queries are not allowed.
        if url.query().is_some() {
            return Err(AllowedUrlError::NoQueriesAllowed);
        }

        // Fragment identifiers are not allowed.  They should have been stripped
        // upstream, by NodeId.
        if url.fragment().is_some() {
            return Err(AllowedUrlError::NoFragmentIdentifierAllowed);
        }

        // All other sources require a base url
        if self.base_url.is_none() {
            return Err(AllowedUrlError::BaseRequired);
        }

        let base_url = self.base_url.as_ref().unwrap();

        // Deny loads from differing URI schemes
        if url.scheme() != base_url.scheme() {
            return Err(AllowedUrlError::DifferentUriSchemes);
        }

        // resource: is allowed to load anything from other resources
        if url.scheme() == "resource" {
            return Ok(AllowedUrl(url));
        }

        // Non-file: isn't allowed to load anything
        if url.scheme() != "file" {
            return Err(AllowedUrlError::DisallowedScheme);
        }

        // The rest of this function assumes file: URLs; guard against
        // incorrect refactoring.
        assert!(url.scheme() == "file");
        assert!(base_url.scheme() == "file");

        // Hostnames are not allowed.  The URL crate rejects them on Unix; but on Windows
        // they are allowed.  In that case, reject them here.
        if url.host().is_some() {
            return Err(AllowedUrlError::NoHostAllowed);
        }

        // If we have a base_uri of "file:///foo/bar.svg", and resolve an href of ".",
        // Url.parse() will give us "file:///foo/".  We don't want that, so check
        // if the last path segment is empty - it will not be empty for a normal file.

        if let Some(mut segments) = url.path_segments() {
            if segments
                .next_back()
                .expect("URL path segments always contain at last 1 element")
                .is_empty()
            {
                return Err(AllowedUrlError::NotSiblingOrChildOfBaseFile);
            }
        } else {
            unreachable!("the file: URL cannot have an empty path");
        }

        // We have two file: URIs.  Now canonicalize them (remove .. and symlinks, etc.)
        // and see if the directories match

        let url_path = url
            .to_file_path()
            .map_err(|_| AllowedUrlError::InvalidPathInUrl)?;
        let base_path = base_url
            .to_file_path()
            .map_err(|_| AllowedUrlError::InvalidPathInBaseUrl)?;

        let base_parent = base_path.parent();
        if base_parent.is_none() {
            return Err(AllowedUrlError::BaseIsRoot);
        }

        let base_parent = base_parent.unwrap();

        let path_canon = url_path
            .canonicalize()
            .map_err(|_| AllowedUrlError::CanonicalizationError)?;
        let parent_canon = base_parent
            .canonicalize()
            .map_err(|_| AllowedUrlError::CanonicalizationError)?;

        if path_canon.starts_with(parent_canon) {
            // Finally, convert the canonicalized path back to a URL.
            let path_to_url = Url::from_file_path(path_canon).unwrap();
            Ok(AllowedUrl(path_to_url))
        } else {
            Err(AllowedUrlError::NotSiblingOrChildOfBaseFile)
        }
    }
}

/// Wrapper for URLs which are allowed to be loaded
///
/// SVG files can reference other files (PNG/JPEG images, other SVGs,
/// CSS files, etc.).  This object is constructed by checking whether
/// a specified `href` (a possibly-relative filename, for example)
/// should be allowed to be loaded, given the base URL of the SVG
/// being loaded.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AllowedUrl(Url);

impl Deref for AllowedUrl {
    type Target = Url;

    fn deref(&self) -> &Url {
        &self.0
    }
}

impl fmt::Display for AllowedUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    #[test]
    fn disallows_relative_file_with_no_base_file() {
        let url_resolver = UrlResolver::new(None);
        assert!(matches!(
            url_resolver.resolve_href("foo.svg"),
            Err(AllowedUrlError::UrlParseError(
                url::ParseError::RelativeUrlWithoutBase
            ))
        ));
    }

    #[test]
    fn disallows_different_schemes() {
        let url_resolver = UrlResolver::new(Some(
            Url::parse("http://example.com/malicious.svg").unwrap(),
        ));
        assert!(matches!(
            url_resolver.resolve_href("file:///etc/passwd"),
            Err(AllowedUrlError::DifferentUriSchemes)
        ));
    }

    fn make_file_uri(p: &str) -> String {
        if cfg!(windows) {
            format!("file:///c:{}", p)
        } else {
            format!("file://{}", p)
        }
    }

    #[test]
    fn disallows_base_is_root() {
        let url_resolver = UrlResolver::new(Some(Url::parse(&make_file_uri("/")).unwrap()));
        assert!(matches!(
            url_resolver.resolve_href("foo.svg"),
            Err(AllowedUrlError::BaseIsRoot)
        ));
    }

    #[test]
    fn disallows_non_file_scheme() {
        let url_resolver = UrlResolver::new(Some(Url::parse("http://foo.bar/baz.svg").unwrap()));
        assert!(matches!(
            url_resolver.resolve_href("foo.svg"),
            Err(AllowedUrlError::DisallowedScheme)
        ));
    }

    #[test]
    fn disallows_hostname_in_base_url() {
        let base_url = Url::parse("file://www.example.com/some/file.svg").unwrap();
        let url_resolver = UrlResolver::new(Some(base_url));
        assert!(dbg!(url_resolver.resolve_href("bar/foo.svg")).is_err());
    }

    #[test]
    fn disallows_hostname_in_href_no_base_url() {
        let url_resolver = UrlResolver::new(None);
        assert!(dbg!(url_resolver.resolve_href("file://www.example.com/foo.svg")).is_err());
    }

    #[test]
    fn disallows_hostname_in_href_with_base_url() {
        let base_url = Url::parse("file:///foo/bar.svg").unwrap();
        let url_resolver = UrlResolver::new(Some(base_url));
        assert!(dbg!(url_resolver.resolve_href("file://www.example.com/foo.svg")).is_err());
    }

    #[test]
    fn disallows_unc_path() {
        let base_url = Url::parse("file:///foo/bar.svg").unwrap();
        let url_resolver = UrlResolver::new(Some(base_url));
        assert!(dbg!(url_resolver.resolve_href("//server/share_name/foo/bar.svg")).is_err());
    }

    #[test]
    fn allows_data_url_with_no_base_file() {
        let url_resolver = UrlResolver::new(None);
        assert_eq!(
            url_resolver
                .resolve_href("data:image/jpeg;base64,xxyyzz")
                .unwrap()
                .as_ref(),
            "data:image/jpeg;base64,xxyyzz",
        );
    }

    fn url_from_test_fixtures(filename_relative_to_librsvg_srcdir: &str) -> Url {
        let path = PathBuf::from(filename_relative_to_librsvg_srcdir);
        let absolute = path
            .canonicalize()
            .expect("files from test fixtures are supposed to canonicalize");
        Url::from_file_path(absolute).unwrap()
    }

    #[test]
    fn allows_relative() {
        let base_url = url_from_test_fixtures("tests/fixtures/loading/bar.svg");
        let url_resolver = UrlResolver::new(Some(base_url));

        let resolved = url_resolver.resolve_href("foo.svg").unwrap();
        let resolved_str = resolved.as_str();
        assert!(resolved_str.ends_with("/loading/foo.svg"));
    }

    #[test]
    fn allows_sibling() {
        let url_resolver = UrlResolver::new(Some(url_from_test_fixtures(
            "tests/fixtures/loading/bar.svg",
        )));
        let resolved = url_resolver
            .resolve_href(url_from_test_fixtures("tests/fixtures/loading/foo.svg").as_str())
            .unwrap();

        let resolved_str = resolved.as_str();
        assert!(resolved_str.ends_with("/loading/foo.svg"));
    }

    #[test]
    fn allows_child_of_sibling() {
        let url_resolver = UrlResolver::new(Some(url_from_test_fixtures(
            "tests/fixtures/loading/bar.svg",
        )));
        let resolved = url_resolver
            .resolve_href(url_from_test_fixtures("tests/fixtures/loading/subdir/baz.svg").as_str())
            .unwrap();

        let resolved_str = resolved.as_str();
        assert!(resolved_str.ends_with("/loading/subdir/baz.svg"));
    }

    // Ignore on Windows since we test for /etc/passwd
    #[cfg(unix)]
    #[test]
    fn disallows_non_sibling() {
        let url_resolver = UrlResolver::new(Some(url_from_test_fixtures(
            "tests/fixtures/loading/bar.svg",
        )));
        assert!(matches!(
            url_resolver.resolve_href(&make_file_uri("/etc/passwd")),
            Err(AllowedUrlError::NotSiblingOrChildOfBaseFile)
        ));
    }

    #[test]
    fn disallows_queries() {
        let url_resolver = UrlResolver::new(Some(
            Url::parse(&make_file_uri("/example/bar.svg")).unwrap(),
        ));
        assert!(matches!(
            url_resolver.resolve_href(".?../../../../../../../../../../etc/passwd"),
            Err(AllowedUrlError::NoQueriesAllowed)
        ));
    }

    #[test]
    fn disallows_weird_relative_uris() {
        let url_resolver = UrlResolver::new(Some(
            Url::parse(&make_file_uri("/example/bar.svg")).unwrap(),
        ));

        assert!(
            url_resolver
                .resolve_href(".@../../../../../../../../../../etc/passwd")
                .is_err()
        );
        assert!(
            url_resolver
                .resolve_href(".$../../../../../../../../../../etc/passwd")
                .is_err()
        );
        assert!(
            url_resolver
                .resolve_href(".%../../../../../../../../../../etc/passwd")
                .is_err()
        );
        assert!(
            url_resolver
                .resolve_href(".*../../../../../../../../../../etc/passwd")
                .is_err()
        );
        assert!(
            url_resolver
                .resolve_href("~/../../../../../../../../../../etc/passwd")
                .is_err()
        );
    }

    #[test]
    fn disallows_dot_sibling() {
        let url_resolver = UrlResolver::new(Some(
            Url::parse(&make_file_uri("/example/bar.svg")).unwrap(),
        ));

        assert!(matches!(
            url_resolver.resolve_href("."),
            Err(AllowedUrlError::NotSiblingOrChildOfBaseFile)
        ));
        assert!(matches!(
            url_resolver.resolve_href(".#../../../../../../../../../../etc/passwd"),
            Err(AllowedUrlError::NoFragmentIdentifierAllowed)
        ));
    }

    #[test]
    fn disallows_fragment() {
        // UrlResolver::resolve_href() explicitly disallows fragment identifiers.
        // This is because they should have been stripped before calling that function,
        // by NodeId or the Iri machinery.
        let url_resolver =
            UrlResolver::new(Some(Url::parse("https://example.com/foo.svg").unwrap()));

        assert!(matches!(
            url_resolver.resolve_href("bar.svg#fragment"),
            Err(AllowedUrlError::NoFragmentIdentifierAllowed)
        ));
    }

    #[cfg(windows)]
    #[test]
    fn invalid_url_from_test_suite() {
        // This is required for Url to panic.
        let resolver =
            UrlResolver::new(Some(Url::parse("file:///c:/foo.svg").expect("initial url")));
        // With this, it doesn't panic:
        //   let resolver = UrlResolver::new(None);

        // The following panics, when using a base URL
        //   match resolver.resolve_href("file://invalid.css") {
        // so, use a less problematic case, hopefully
        match resolver.resolve_href("file://") {
            Ok(_) => println!("yay!"),
            Err(e) => println!("err: {}", e),
        }
    }
}
