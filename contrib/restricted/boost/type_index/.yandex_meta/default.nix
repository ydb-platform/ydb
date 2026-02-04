self: super: with self; {
  boost_type_index = stdenv.mkDerivation rec {
    pname = "boost_type_index";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "type_index";
      rev = "boost-${version}";
      hash = "sha256-kYtO2jIkZq/Tc4VUNfH8ETpwVQK+ykBEc/S+r2YiXsE=";
    };
  };
}
