self: super: with self; {
  boost_locale = stdenv.mkDerivation rec {
    pname = "boost_locale";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "locale";
      rev = "boost-${version}";
      hash = "sha256-KQg12DCqTqG3KcBLobg6+B/XEDzaag8JiC52p6ecJ6Y=";
    };
  };
}
