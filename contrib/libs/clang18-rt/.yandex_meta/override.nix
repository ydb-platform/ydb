pkgs: attrs: with pkgs; with attrs; rec {
  version = "18.1.8";

  src = let
    pname = "compiler-rt";
    source = fetchFromGitHub {
      owner = "llvm";
      repo = "llvm-project";
      rev = "llvmorg-${version}";
      hash = "sha256-iiZKMRo/WxJaBXct9GdAcAT3cz9d9pnAcO1mmR6oPNE=";
    };
  in (runCommand "${pname}-src-${version}" {} (''
    mkdir -p "$out"
    cp -r ${source}/cmake "$out"
    cp -r ${source}/${pname} "$out"
  '')).overrideAttrs(attrs: rec {
    urls = source.urls;
  });
  sourceRoot = "compiler-rt-src-${version}/compiler-rt";

  postConfigure = ''
    substituteInPlace "$PWD/build.ninja" --replace "/${yamaker-llvm.dev}/lib/cmake/llvm/LLVM-Config.cmake" ""
  '';

  patches = [];
}
