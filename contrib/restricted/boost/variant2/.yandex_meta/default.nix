self: super: with self; {
  boost_variant2 = stdenv.mkDerivation rec {
    pname = "boost_variant2";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "variant2";
      rev = "boost-${version}";
      hash = "sha256-3+0CKH21AhJAXdAeYr1c4HdqnyT0twqr2vQIY6PcT40=";
    };
  };
}
