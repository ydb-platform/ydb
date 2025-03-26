self: super: with self; {
  boost_variant2 = stdenv.mkDerivation rec {
    pname = "boost_variant2";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "variant2";
      rev = "boost-${version}";
      hash = "sha256-lcKjtj83zJ6VdbSSNZnRsi7XonYrWMMpZHAaGv2PSvc=";
    };
  };
}
