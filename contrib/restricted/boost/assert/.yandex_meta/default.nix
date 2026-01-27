self: super: with self; {
  boost_assert = stdenv.mkDerivation rec {
    pname = "boost_assert";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "assert";
      rev = "boost-${version}";
      hash = "sha256-uTS34jOzHoo4yz1ZcN6gHYYzR7ihcPlvVrmOHnRK8BU=";
    };
  };
}
