self: super: with self; {
  boost_container = stdenv.mkDerivation rec {
    pname = "boost_container";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "container";
      rev = "boost-${version}";
      hash = "sha256-fE/wPWYQKBSirmIGZ4z8s4cC3CK2ecBuHxGBZSFIOkc=";
    };
  };
}
