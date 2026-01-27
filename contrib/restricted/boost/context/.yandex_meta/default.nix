self: super: with self; {
  boost_context = stdenv.mkDerivation rec {
    pname = "boost_context";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "context";
      rev = "boost-${version}";
      hash = "sha256-1mwyVpsERSLozeecRfyZSrss42TiL2D7jLvNBtisX5o=";
    };
  };
}
