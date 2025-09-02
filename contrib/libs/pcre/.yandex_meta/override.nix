pkgs: attrs: with pkgs; rec {
  version = "8.45";

  src = fetchurl {
    url = "https://downloads.sourceforge.net/project/pcre/pcre/${version}/pcre-${version}.tar.bz2";
    hash = "sha256-Ta5v3NK7C7bDe1+Xwzwr6VTadDmFNpzdrDVG4yGL/7g=";
  };


  buildInputs = [ zlib ];

  configureFlags = [
    "--enable-cpp"
    "--enable-unicode-properties"
    "--enable-pcre16"
    "--enable-pcre32"
    "--enable-jit"
  ];

  postConfigure = ''
    cp pcre_chartables.c.dist pcre_chartables.c
  '';
}
