self: super: with self; {
  boost_spirit = stdenv.mkDerivation rec {
    pname = "boost_spirit";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "spirit";
      rev = "boost-${version}";
      hash = "sha256-JQqWG6AEuU5FO3jbdwYtmLYwzPFZzdFz3yBkkc/a628=";
    };
  };
}
