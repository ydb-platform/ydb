self: super: with self; {
  boost_function = stdenv.mkDerivation rec {
    pname = "boost_function";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "function";
      rev = "boost-${version}";
      hash = "sha256-OsSx0OF1brwuswfNA0ynh2SeVw0Wl/6QKyZ9UQIIYZI=";
    };
  };
}
