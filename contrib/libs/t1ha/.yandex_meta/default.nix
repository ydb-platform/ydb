self: super: with self; {
  t1ha = stdenv.mkDerivation rec {
    pname = "t1ha";
    version = "2.1.4";

    src = fetchFromGitHub {
      owner = "PositiveTechnologies";
      repo = "t1ha";
      rev = "v${version}";
      sha256 = "01l6rd401z9y67lns7fdyp0kr69irkjmnpiwq4rvzf0gk0l2bvzd";
    };

    doCheck = true;
  };
}
