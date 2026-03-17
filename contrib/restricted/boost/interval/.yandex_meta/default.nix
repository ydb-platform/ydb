self: super: with self; {
  boost_interval = stdenv.mkDerivation rec {
    pname = "boost_interval";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "interval";
      rev = "boost-${version}";
      hash = "sha256-Bgh6+BEgsjRc5eiDpGmEpJAyMBf+DpoiCh2xAzQRlBg=";
    };
  };
}
