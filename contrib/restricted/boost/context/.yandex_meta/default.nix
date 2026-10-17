self: super: with self; {
  boost_context = stdenv.mkDerivation rec {
    pname = "boost_context";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "context";
      rev = "boost-${version}";
      hash = "sha256-kVRssJghufFPbyQzXmQD+/C1TDPAPICtnKwYBRpdybg=";
    };
  };
}
