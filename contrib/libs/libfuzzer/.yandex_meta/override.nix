pkgs: attrs: with pkgs; with attrs; rec {
  version = "15.0.5";

  src = let
    pname = "compiler-rt";
    source = fetchFromGitHub {
      owner = "llvm";
      repo = "llvm-project";
      rev = "llvmorg-${version}";
      hash = "sha256-lYwtqpodBLPgA+BpdesZ5JetcLccpBKSrE1Pqyj+Wvw=";
    };
  in (runCommand "${pname}-src-${version}" {} (''
    mkdir -p "$out"
    cp -r ${source}/cmake "$out"
    cp -r ${source}/${pname} "$out"
  '')).overrideAttrs(attrs: rec {
    urls = source.urls;
  });
  sourceRoot = "compiler-rt-src-${version}/compiler-rt";

  patches = [
    ./cmake-afl.patch
    ./no-fuchsia.patch
  ];

  NIX_CFLAGS_COMPILE = [ ]; # Remove SCUDO_DEFAULT_OPTIONS.
}
