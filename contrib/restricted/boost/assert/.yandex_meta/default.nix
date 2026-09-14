self: super: with self; {
  boost_assert = stdenv.mkDerivation rec {
    pname = "boost_assert";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "assert";
      rev = "boost-${version}";
      hash = "sha256-uHV4FXgfaV2b0KRwx+WFAQESjsSdTuVjeISmQ9+iD14=";
    };
  };
}
