pkgs: attrs: with pkgs; with attrs; rec {
  version = "4.3.1";

  src = fetchFromGitHub {
    owner = "swig";
    repo = "swig";
    rev = "v${version}";
    hash = "sha256-wEqbKDgXVU8kQxdh7uC+EZ0u5leeoYh2d/61qB4guOg=";
  };

  # Setup split build to make copy_sources omit generated files.
  preConfigure = attrs.preConfigure + ''
    mkdir -p build
    cd build
    ln -s ../configure .
  '';

  configureFlags = [
    "--build=x86_64-pc-linux-gnu"
    "--without-tcl"
  ];

  buildInputs = [ pcre2 ];
}
