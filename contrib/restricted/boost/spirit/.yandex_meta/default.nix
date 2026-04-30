self: super: with self; {
  boost_spirit = stdenv.mkDerivation rec {
    pname = "boost_spirit";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "spirit";
      rev = "boost-${version}";
      hash = "sha256-z6THPFrx3Ry04hS0UCOPBKhxQfIcg3L0ofAw1VDWjSE=";
    };
  };
}
