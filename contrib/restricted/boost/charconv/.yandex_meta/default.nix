self: super: with self; {
  boost_charconv = stdenv.mkDerivation rec {
    pname = "boost_charconv";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "charconv";
      rev = "boost-${version}";
      hash = "sha256-5ppDHzINy47oA85OtVAEHQ2t2pjNVKmMiPexDbdBaio=";
    };
  };
}
