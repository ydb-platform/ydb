self: super: with self; {
  boost_function = stdenv.mkDerivation rec {
    pname = "boost_function";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "function";
      rev = "boost-${version}";
      hash = "sha256-ILvaKlIxr8XVJyQFwDI+wXjPaOyUo8P0VaJo2AtpjTA=";
    };
  };
}
