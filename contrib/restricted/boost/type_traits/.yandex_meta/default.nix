self: super: with self; {
  boost_type_traits = stdenv.mkDerivation rec {
    pname = "boost_type_traits";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "type_traits";
      rev = "boost-${version}";
      hash = "sha256-f9d2+F0L3NOsyyIAXQxvw0YCSa5mZ2NJqPkAFDGHK4g=";
    };
  };
}
