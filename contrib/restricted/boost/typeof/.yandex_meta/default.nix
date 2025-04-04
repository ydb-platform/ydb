self: super: with self; {
  boost_typeof = stdenv.mkDerivation rec {
    pname = "boost_typeof";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "typeof";
      rev = "boost-${version}";
      hash = "sha256-xalriryMDMTLSSXkyhORlUtFazO5cb9Vahs66RCQRNQ=";
    };
  };
}
