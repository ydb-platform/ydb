self: super: with self; {
  boost_detail = stdenv.mkDerivation rec {
    pname = "boost_detail";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "detail";
      rev = "boost-${version}";
      hash = "sha256-CNJh3zHJQdlmJUTkRxZ/QF14aLsrKivrRF1u7s927FY=";
    };
  };
}
