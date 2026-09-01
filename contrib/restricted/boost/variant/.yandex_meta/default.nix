self: super: with self; {
  boost_variant = stdenv.mkDerivation rec {
    pname = "boost_variant";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "variant";
      rev = "boost-${version}";
      hash = "sha256-+tdNUFjONHdCFReq1AH/pQSMsAWVhlm4SHpGswGzQW0=";
    };
  };
}
