self: super: with self; {
  boost_foreach = stdenv.mkDerivation rec {
    pname = "boost_foreach";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "foreach";
      rev = "boost-${version}";
      hash = "sha256-y0KM60OZtxt0ZsKLWVcD708BGVHjtbEpq4X+WdDAuXY=";
    };
  };
}
