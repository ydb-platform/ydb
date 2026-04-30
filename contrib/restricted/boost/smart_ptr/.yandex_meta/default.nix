self: super: with self; {
  boost_smart_ptr = stdenv.mkDerivation rec {
    pname = "boost_smart_ptr";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "smart_ptr";
      rev = "boost-${version}";
      hash = "sha256-upn8kCx96qtL3HVooQMPmB/1SFmihhnmux4HgcAbxgA=";
    };
  };
}
