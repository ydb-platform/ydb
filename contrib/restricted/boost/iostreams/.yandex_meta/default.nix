self: super: with self; {
  boost_iostreams = stdenv.mkDerivation rec {
    pname = "boost_iostreams";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "iostreams";
      rev = "boost-${version}";
      hash = "sha256-5wd8jbRGSFCQ96qbwqsHL5CRymgTvGKKCTErQ1IuxME=";
    };
  };
}
