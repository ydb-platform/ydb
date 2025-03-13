self: super: with self; {
  boost_config = stdenv.mkDerivation rec {
    pname = "boost_config";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "config";
      rev = "boost-${version}";
      hash = "sha256-BpML8PxUuRVPfdQ7bf60GHPM44TXtg2nfWmCodzfO4Q=";
    };
  };
}
