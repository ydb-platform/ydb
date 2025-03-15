self: super: with self; {
  boost_function_types = stdenv.mkDerivation rec {
    pname = "boost_function_types";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "function_types";
      rev = "boost-${version}";
      hash = "sha256-GOyxfIPqyCTHm7Snm0UcTHQaR8bNA5QlNbX+0J1fwSI=";
    };
  };
}
