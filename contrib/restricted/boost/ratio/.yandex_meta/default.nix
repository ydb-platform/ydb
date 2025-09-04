self: super: with self; {
  boost_ratio = stdenv.mkDerivation rec {
    pname = "boost_ratio";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "ratio";
      rev = "boost-${version}";
      hash = "sha256-I99O0iGVqMnVjcV2MdmS1Q91UR6OJyV7WX+DbrCUT70=";
    };
  };
}
